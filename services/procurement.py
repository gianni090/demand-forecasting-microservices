"""Durable purchase-order saga and compensations."""

import asyncio
import logging
from time import time

from services.contracts import Problem
from services.platform import Service, new_id, page_response, stable_id, timestamp


logger = logging.getLogger("lab4.procurement")
ACTIVE = {"PREPARING", "CANCELLING"}


class Procurement(Service):
    name = "procurement-service"

    def __init__(self, store, network):
        super().__init__(store, network)
        self.pause_planning = asyncio.Event()
        self.pause_planning.set()

    def createPurchaseOrder(self, ctx):
        body = ctx.body
        if not body["productId"].strip() or "/" in body["productId"]:
            raise Problem(422, "INVALID_PRODUCT", "A valid product identifier is required.")
        orders = self.store.all("purchase-order")
        if any(o["recommendationId"] == body["recommendationId"] and o["status"] != "CANCELLED" for o in orders):
            raise Problem(409, "RECOMMENDATION_ALREADY_ORDERED", "This recommendation already has an order.")
        if any(o["productId"] == body["productId"] and o["status"] in ACTIVE for o in orders):
            raise Problem(409, "ACTIVE_ORDER_EXISTS", "An order for this product is already being prepared.")
        order_id = new_id()
        created = timestamp()
        order = {"id": order_id, "recommendationId": body["recommendationId"], "productId": body["productId"],
                 "status": "PREPARING", "sagaStep": "CLAIM", "createdAt": created, "updatedAt": created}
        self.store.put("purchase-order", order_id, order)
        self.store.put("saga", order_id, {
            "id": order_id, "correlation": ctx.correlation, "expectedVersion": body["recommendationVersion"],
            "claimAttempted": False, "planAttempted": False, "nextAttempt": 0,
        })
        self.audit(order, ctx.correlation)
        return order

    def getPurchaseOrder(self, ctx):
        return self.require("purchase-order", ctx.params["orderId"])

    def listPurchaseOrders(self, ctx):
        orders = sorted(self.store.all("purchase-order"),
                        key=lambda order: (order["createdAt"], order["id"]), reverse=True)
        return page_response(orders, ctx.params["page"], ctx.params["size"])

    def cancelPurchaseOrder(self, ctx):
        order = self.getPurchaseOrder(ctx)
        if order["status"] in {"CONFIRMED", "RECEIVED"}:
            raise Problem(409, "ORDER_ALREADY_CONFIRMED", "A confirmed or received order cannot be cancelled.")
        if order["status"] == "PREPARING":
            saga = self.require("saga", order["id"])
            saga["reason"] = "USER_CANCEL"
            saga["nextAttempt"] = 0
            self.store.put("saga", order["id"], saga)
            return self.advance(order, "CANCELLING", "COMPENSATE", saga["correlation"])
        return order

    def audit(self, order, correlation):
        self.store.put("saga-audit", new_id(), {
            "orderId": order["id"], "correlationId": correlation,
            "status": order["status"], "step": order["sagaStep"], "at": order["updatedAt"],
        })
        logger.info("saga order=%s status=%s step=%s error=%s correlation=%s", order["id"], order["status"],
                    order["sagaStep"], order.get("lastError"), correlation)

    def advance(self, order, status, step, correlation):
        order.update(status=status, sagaStep=step, updatedAt=timestamp())
        self.store.put("purchase-order", order["id"], order)
        self.audit(order, correlation)
        return order

    async def tick(self):
        pending = sorted(
            (saga for saga in self.store.all("saga") if saga["nextAttempt"] <= time()),
            key=lambda saga: self.require("purchase-order", saga["id"])["createdAt"],
        )
        for saga in pending:
            order = self.require("purchase-order", saga["id"])
            if order["sagaStep"] == "PLAN" and not self.pause_planning.is_set():
                continue
            if order["status"] == "CANCELLING":
                await self.compensate(order, saga)
            elif order["sagaStep"] in {"CLAIM", "PLAN"}:
                await self.prepare_order(order, saga)
            elif order["sagaStep"] == "COMMIT":
                self.confirm(order, saga)
            return

    async def prepare_order(self, order, saga):
        order_id, rec_id = order["id"], order["recommendationId"]
        step, correlation = order["sagaStep"], saga["correlation"]
        flag = "claimAttempted" if step == "CLAIM" else "planAttempted"
        saga[flag] = True
        self.store.put("saga", order_id, saga)
        try:
            if step == "CLAIM":
                result = await self.rest(
                    "replenishment-service", "POST", f"/internal/v1/recommendations/{rec_id}/claim",
                    correlation, body={"orderId": order_id, "expectedVersion": saga["expectedVersion"]},
                    key=stable_id(order_id, "claim"),
                )
                if result["productId"] != order["productId"]:
                    raise Problem(409, "PRODUCT_MISMATCH", "The recommendation is for a different product.")
            else:
                await self.create_plan(order, saga)
        except Problem as error:
            self.record_preparation_failure(order_id, step, error)
            return
        with self.store.transaction():
            current = self.require("purchase-order", order_id)
            if current["status"] != "PREPARING":
                return
            if step == "CLAIM":
                current.update(quantity=result["quantity"], arrivalDate=result["arrivalDate"])
                saga["stockVersion"] = result["stockVersion"]
                self.store.put("saga", order_id, saga)
            self.advance(current, "PREPARING", "PLAN" if step == "CLAIM" else "COMMIT", correlation)

    async def create_plan(self, order, saga):
        order_id = order["id"]
        correlation = saga["correlation"]
        body = {field: order[field] for field in ("productId", "quantity", "arrivalDate")}
        body.update(orderId=order_id, expectedStockVersion=saga["stockVersion"])
        try:
            await self.rest("inventory-service", "POST", "/internal/v1/inbound-plans", correlation,
                            body=body, key=stable_id(order_id, "plan"))
        except Problem as error:
            if error.status != 503:
                raise
            found = await self.rest("inventory-service", "GET", f"/internal/v1/inbound-plans/{order_id}", correlation)
            if found["status"] != "PROVISIONAL" or any(
                found[key] != body[key] for key in ("productId", "quantity", "arrivalDate")
            ):
                raise Problem(409, "PLAN_CONFLICT", "The existing plan does not match the order.") from error

    def record_preparation_failure(self, order_id, step, error):
        with self.store.transaction():
            order = self.require("purchase-order", order_id)
            saga = self.require("saga", order_id)
            if step == "CLAIM" and 400 <= error.status < 500 and error.code != "PRODUCT_MISMATCH":
                # A definite claim rejection grants no right to release someone else's claim.
                saga["claimAttempted"] = False
            if order["status"] == "PREPARING":
                saga["reason"] = "STOCK_CHANGED" if error.code == "STOCK_VERSION_CONFLICT" else "DEPENDENCY_FAILED"
                order["lastError"] = error.code
            self.store.put("saga", order_id, saga)
            self.advance(order, "CANCELLING", "COMPENSATE", saga["correlation"])

    def confirm(self, order, saga):
        with self.store.transaction():
            self.advance(order, "CONFIRMED", "DONE", saga["correlation"])
            payload = {field: order[field] for field in ("recommendationId", "productId", "quantity", "arrivalDate")}
            self.emit("purchase-order.confirmed", order["id"], 1, saga["correlation"],
                      {"orderId": order["id"], **payload})
            self.store.delete("saga", order["id"])

    async def compensate(self, order, saga):
        order_id, rec_id = order["id"], order["recommendationId"]
        correlation, reason = saga["correlation"], saga["reason"]
        try:
            if saga["planAttempted"]:
                await self.rest("inventory-service", "POST", f"/internal/v1/inbound-plans/{order_id}/cancel",
                                correlation, key=stable_id(order_id, "cancel-plan"))
            if saga["claimAttempted"]:
                await self.rest("replenishment-service", "POST", f"/internal/v1/recommendations/{rec_id}/release",
                                correlation, body={"orderId": order_id, "reason": reason},
                                key=stable_id(order_id, "release"))
        except Problem as error:
            with self.store.transaction():
                saga["nextAttempt"] = time() + 1
                self.store.put("saga", order_id, saga)
                order.setdefault("lastError", error.code)
                self.advance(order, "CANCELLING", "COMPENSATE", correlation)
            return
        with self.store.transaction():
            self.advance(order, "CANCELLED", "DONE", correlation)
            self.emit("purchase-order.cancelled", order_id, 1, correlation, {
                "orderId": order_id, "recommendationId": rec_id, "productId": order["productId"], "reason": reason,
            })
            self.store.delete("saga", order_id)

    def apply_event(self, event, prepared):
        payload = event["payload"]
        order = self.require("purchase-order", payload["orderId"])
        if order["status"] == "RECEIVED":
            return
        if order["status"] != "CONFIRMED" or any(order[field] != payload[field] for field in ("productId", "quantity")):
            raise Problem(409, "RECEIPT_MISMATCH", "The receipt does not match a confirmed order.")
        self.advance(order, "RECEIVED", "DONE", event["correlationId"])
