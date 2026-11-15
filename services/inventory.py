"""Warehouse balances, inbound plans and receipts."""

from services.contracts import Problem
from services.platform import Service, new_id, stable_id, timestamp


class Inventory(Service):
    name = "inventory-service"

    def snapshot(self, product):
        balance = self.require("stock", product)
        plans = [plan for plan in self.store.all("inbound-plan")
                 if plan.get("productId") == product and plan["status"] == "CONFIRMED"]
        return {**balance, "confirmedInbound": sorted(plans, key=lambda plan: (plan["arrivalDate"], plan["orderId"]))}

    def change_stock(self, product, balance, delta, reason, correlation):
        stock = {
            "productId": product, "warehouseId": "WH-1", "onHand": balance["onHand"] + delta,
            "version": balance["version"] + 1, "asOf": timestamp(),
        }
        self.store.put("stock", product, stock)
        self.emit("inventory.changed", stable_id(self.name, product), stock["version"], correlation,
                  {"productId": product, "stockVersion": stock["version"], "reason": reason})
        return stock

    def getPublicStock(self, ctx):
        return self.snapshot(ctx.params["productId"])

    def getInternalStock(self, ctx):
        return self.snapshot(ctx.params["productId"])

    def adjustStock(self, ctx):
        body = ctx.body
        product, delta = body["productId"], body["delta"]
        balance = self.store.get("stock", product) or {"version": 0, "onHand": 0}
        if body["expectedVersion"] != balance["version"]:
            raise Problem(409, "STOCK_VERSION_CONFLICT", "The current balance has a different version.")
        if not product.strip() or "/" in product or delta == 0 or balance["onHand"] + delta < 0:
            raise Problem(422, "INVALID_ADJUSTMENT", "Use a valid product and nonzero delta; stock cannot be negative.")
        stock = self.change_stock(product, balance, delta, "ADJUSTMENT", ctx.correlation)
        self.store.put("movement", new_id(), {**body, "stockVersion": stock["version"], "createdAt": timestamp()})
        return self.snapshot(product)

    def createInboundPlan(self, ctx):
        body = ctx.body
        order_id, product = body["orderId"], body["productId"]
        previous = self.store.get("inbound-plan", order_id)
        if previous:
            if previous["status"] == "CANCELLED":
                raise Problem(409, "PLAN_CANCELLED", "This order already has a cancellation record.")
            if self.store.get("plan-request", order_id) != body:
                raise Problem(409, "PLAN_CONFLICT", "This order already has a different inbound plan.")
            return previous
        balance = self.require("stock", product)
        if balance["version"] != body["expectedStockVersion"]:
            raise Problem(409, "STOCK_VERSION_CONFLICT", "Stock changed since the recommendation was calculated.")
        if any(plan.get("productId") == product and plan["status"] == "PROVISIONAL"
               for plan in self.store.all("inbound-plan")):
            raise Problem(409, "PROVISIONAL_PLAN_EXISTS", "This product already has a provisional plan.")
        plan = {field: value for field, value in body.items() if field != "expectedStockVersion"}
        plan["status"] = "PROVISIONAL"
        self.store.put("plan-request", order_id, body)
        self.store.put("inbound-plan", order_id, plan)
        return plan

    def getInboundPlan(self, ctx):
        return self.require("inbound-plan", ctx.params["orderId"])

    def cancel_plan(self, order_id):
        plan = self.store.get("inbound-plan", order_id) or {"orderId": order_id}
        if plan.get("status") in {"CONFIRMED", "RECEIVED"}:
            raise Problem(409, "PLAN_CONFIRMED", "A confirmed or received inbound plan cannot be cancelled.")
        plan["status"] = "CANCELLED"
        self.store.put("inbound-plan", order_id, plan)
        return plan

    def cancelInboundPlan(self, ctx):
        return self.cancel_plan(ctx.params["orderId"])

    def receiveOrder(self, ctx):
        body = ctx.body
        order_id = body["orderId"]
        plan = self.require("inbound-plan", order_id)
        if plan["status"] != "CONFIRMED":
            raise Problem(409, "PLAN_NOT_RECEIVABLE", "Only a confirmed plan can be received once.")
        if body["quantity"] != plan["quantity"]:
            raise Problem(422, "RECEIPT_QUANTITY_MISMATCH", "A full receipt must match the planned quantity.")
        product = plan["productId"]
        self.store.put("inbound-plan", order_id, {**plan, "status": "RECEIVED"})
        balance = self.change_stock(product, self.require("stock", product), body["quantity"],
                                    "RECEIPT", ctx.correlation)
        receipt = {**body, "id": new_id(), "productId": product, "stockVersion": balance["version"]}
        self.store.put("receipt", order_id, receipt)
        self.store.put("movement", receipt["id"], receipt)
        self.emit("stock.received", order_id, balance["version"], ctx.correlation, {
            "orderId": order_id, "receiptId": receipt["id"], "productId": product,
            "quantity": body["quantity"], "stockVersion": balance["version"],
        })
        return receipt

    def apply_event(self, event, prepared):
        payload = event["payload"]
        order_id = payload["orderId"]
        if event["eventType"] == "purchase-order.cancelled":
            self.cancel_plan(order_id)
            return
        plan = self.require("inbound-plan", order_id)
        if plan["status"] in {"CONFIRMED", "RECEIVED"}:
            return
        if plan["status"] != "PROVISIONAL" or any(
            plan[key] != payload[key] for key in ("productId", "quantity", "arrivalDate")
        ):
            raise Problem(409, "PLAN_CONFLICT", "Confirmation does not match the provisional plan.")
        self.store.put("inbound-plan", order_id, {**plan, "status": "CONFIRMED"})
        product = plan["productId"]
        self.change_stock(product, self.require("stock", product), 0,
                          "INBOUND_CONFIRMED", event["correlationId"])
