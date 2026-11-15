"""Stock policy, recommendations and exclusive claims."""

from datetime import date, datetime, timedelta, timezone
from math import ceil

from services.contracts import Problem
from services.platform import Service, now, page_response, path_segment, stable_id, timestamp


POLICY_VERSION = "policy-v1"


def replenishment_quantity(forecast, snapshot):
    as_of = date.fromisoformat(forecast["asOfDate"])
    protection_end = as_of + timedelta(weeks=2)
    position = snapshot["onHand"] + sum(
        plan["quantity"] for plan in snapshot["confirmedInbound"]
        if plan["status"] == "CONFIRMED" and date.fromisoformat(plan["arrivalDate"]) <= protection_end
    )
    target = ceil(forecast["demandP90"])
    return position, target, max(0, target - position)


class Replenishment(Service):
    name = "replenishment-service"

    async def read_inputs(self, forecast_id, correlation):
        forecast = await self.rest("forecast-service", "GET", f"/internal/v1/forecasts/{forecast_id}", correlation)
        product = path_segment(forecast["productId"])
        stock = await self.rest("inventory-service", "GET", f"/internal/v1/stocks/{product}", correlation)
        return {"forecast": forecast, "stock": stock}

    def stock_version(self, product):
        entry = self.store.get("stock-version", product)
        return entry["version"] if entry else 0

    def view(self, record):
        if record["status"] == "READY" and (
            datetime.fromisoformat(record["validUntil"]) <= now()
            or record["stockVersion"] < self.stock_version(record["productId"])
        ):
            return {**record, "status": "STALE"}
        return record

    def save_calculation(self, forecast, stock):
        rec_id = stable_id(self.name, forecast["id"], stock["version"], POLICY_VERSION)
        existing = self.store.get("recommendation", rec_id)
        if existing:
            return self.view(existing)
        position, target, quantity = replenishment_quantity(forecast, stock)
        arrival = date.fromisoformat(forecast["asOfDate"]) + timedelta(weeks=1)
        valid_until = datetime(arrival.year, arrival.month, arrival.day, tzinfo=timezone.utc)
        record = self.view({
            "id": rec_id, "version": 1, "productId": forecast["productId"], "forecastId": forecast["id"],
            "stockVersion": stock["version"], "inventoryPosition": position, "targetLevel": target,
            "quantity": quantity, "arrivalDate": arrival.isoformat(),
            "validUntil": valid_until.isoformat().replace("+00:00", "Z"), "status": "READY",
        })
        self.store.put("recommendation", rec_id, record)
        self.store.put("calculation", rec_id, {"createdAt": timestamp(), "policyVersion": POLICY_VERSION})
        return record

    async def prepare_createRecommendation(self, ctx):
        return await self.read_inputs(ctx.body["forecastId"], ctx.correlation)

    def createRecommendation(self, ctx):
        return self.save_calculation(ctx.prepared["forecast"], ctx.prepared["stock"])

    def getRecommendation(self, ctx):
        return self.view(self.require("recommendation", ctx.params["recommendationId"]))

    def listRecommendations(self, ctx):
        records = sorted(self.store.all("recommendation"),
                         key=lambda item: (self.require("calculation", item["id"])["createdAt"], item["id"]),
                         reverse=True)
        return page_response([self.view(record) for record in records], ctx.params["page"], ctx.params["size"])

    def claimRecommendation(self, ctx):
        rec_id, order_id = ctx.params["recommendationId"], ctx.body["orderId"]
        record = self.view(self.require("recommendation", rec_id))
        if self.store.get("claim-fence", f"{rec_id}/{order_id}"):
            raise Problem(409, "CLAIM_CANCELLED", "This order's claim was cancelled.")
        if record.get("claimedBy") == order_id and record["status"] in {"CLAIMED", "APPLIED"}:
            return record
        if record["status"] == "CLAIMED":
            raise Problem(409, "CLAIM_ALREADY_USED", "Another order holds this recommendation.")
        if (record["status"] != "READY" or record["version"] != ctx.body["expectedVersion"]
                or not record["quantity"]):
            raise Problem(409, "RECOMMENDATION_UNAVAILABLE", "The recommendation is stale, empty or already applied.")
        record.update(status="CLAIMED", claimedBy=order_id)
        self.store.put("recommendation", rec_id, record)
        return record

    def release(self, rec_id, order_id, reason):
        record = self.require("recommendation", rec_id)
        if record.get("claimedBy") == order_id and record["status"] == "APPLIED":
            raise Problem(409, "CLAIM_ALREADY_APPLIED", "An applied recommendation cannot be released.")
        self.store.put("claim-fence", f"{rec_id}/{order_id}", {"orderId": order_id, "reason": reason})
        if record.get("claimedBy") == order_id and record["status"] == "CLAIMED":
            record.pop("claimedBy")
            record["status"] = "STALE" if reason == "STOCK_CHANGED" else "READY"
            record = self.view(record)
            self.store.put("recommendation", rec_id, record)
        return self.view(record)

    def releaseRecommendation(self, ctx):
        return self.release(ctx.params["recommendationId"], ctx.body["orderId"], ctx.body["reason"])

    async def prepare_event(self, event):
        if event["eventType"] != "forecast.completed":
            return None
        try:
            return await self.read_inputs(event["payload"]["forecastId"], event["correlationId"])
        except Problem as error:
            if error.status != 404:
                raise
            # No stock is registered yet. The forecast can be used after the first adjustment.
            return None

    def apply_event(self, event, prepared):
        payload = event["payload"]
        event_type = event["eventType"]
        if event_type == "forecast.completed":
            if prepared:
                self.save_calculation(prepared["forecast"], prepared["stock"])
            return
        if event_type == "forecast.failed":
            self.store.put("forecast-failure", payload["jobId"], payload)
            return
        if event_type == "inventory.changed":
            product, version = payload["productId"], payload["stockVersion"]
            if version <= self.stock_version(product):
                return
            self.store.put("stock-version", product, {"version": version})
            for record in self.store.all("recommendation"):
                if (record["productId"] == product and record["status"] == "READY"
                        and record["stockVersion"] < version):
                    self.store.put("recommendation", record["id"], {**record, "status": "STALE"})
            return
        rec_id, order_id = payload["recommendationId"], payload["orderId"]
        if event_type == "purchase-order.cancelled":
            if self.store.get("recommendation", rec_id):
                self.release(rec_id, order_id, payload["reason"])
            return
        record = self.require("recommendation", rec_id)
        if record["status"] == "APPLIED" and record.get("claimedBy") == order_id:
            return
        if record.get("claimedBy") != order_id or record["status"] != "CLAIMED":
            raise Problem(409, "CLAIM_MISMATCH", "Order confirmation does not match the recommendation claim.")
        self.store.put("recommendation", rec_id, {**record, "status": "APPLIED"})
