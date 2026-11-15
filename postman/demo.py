"""Executable API collection for both Lab 4 scenarios. All requests go to the gateway."""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx


def utc_monday():
    today = datetime.now(timezone.utc).date()
    return today - timedelta(days=today.weekday())


def sales_fixture(product):
    monday = utc_monday()
    rows = ["productId,soldOn,quantity,operation"]
    for week in range(14, 0, -1):
        rows.append(f"{product},{monday - timedelta(weeks=week)},20,SALE")
    return "\n".join(rows).encode()


def ensure(condition, detail):
    if not condition:
        raise RuntimeError(detail)


class DemoClient:
    def __init__(self, http, quiet=False):
        self.http = http
        self.tokens = {}
        self.quiet = quiet

    async def login(self):
        for role in ("analyst", "purchaser", "warehouse-manager", "admin"):
            reply = await self.http.post("/auth/lab-token", json={"role": role})
            reply.raise_for_status()
            self.tokens[role] = reply.json()["access_token"]

    def note(self, message):
        if not self.quiet:
            print(message, flush=True)

    async def request(self, role, method, path, *, key=None, expected=200, **kwargs):
        headers = {"Authorization": "Bearer " + self.tokens[role]}
        headers.update(kwargs.pop("headers", {}))
        if method == "POST":
            headers["Idempotency-Key"] = key or str(uuid4())
        response = await self.http.request(method, path, headers=headers, **kwargs)
        ensure(response.status_code == expected, f"{method} {path}: expected {expected}, got "
               f"{response.status_code}\n{response.text}")
        return response.json()

    async def wait(self, read, ready):
        async with asyncio.timeout(25):
            while True:
                result = await read()
                if ready(result):
                    return result
                await asyncio.sleep(0.08)

    async def initialize(self, tag):
        product = "SKU-" + tag + "-" + uuid4().hex[:8]
        initial = await self.request(
            "warehouse-manager", "POST", "/api/v1/inventory-adjustments", expected=201,
            json={"productId": product, "delta": 10, "expectedVersion": 0, "reason": "Демонстрационный остаток"},
        )
        imported = await self.request(
            "analyst", "POST", "/api/v1/imports", expected=202,
            files={"file": ("sales.csv", sales_fixture(product), "text/csv")},
        )
        completed = await self.wait(
            lambda: self.request("analyst", "GET", f"/api/v1/imports/{imported['id']}"),
            lambda data: data["status"] in {"COMPLETED", "FAILED"},
        )
        ensure(completed["status"] == "COMPLETED" and completed["acceptedRows"] == 14, str(completed))
        page = await self.wait(
            lambda: self.request("purchaser", "GET", "/api/v1/recommendations", params={"size": 100}),
            lambda data: any(item["productId"] == product for item in data["items"]),
        )
        recommendation = next(item for item in page["items"] if item["productId"] == product)
        ensure(recommendation["quantity"] == 30 and recommendation["targetLevel"] == 40, str(recommendation))
        self.note(f"{product} | CSV {completed['acceptedRows']} строк, datasetVersion={completed['datasetVersion']} | "
                  f"p90={recommendation['targetLevel']}, остаток={initial['onHand']}, к закупке={recommendation['quantity']}")
        return product, initial, completed, recommendation

    async def create_order(self, product, recommendation, key=None, headers=None):
        return await self.request(
            "purchaser", "POST", "/api/v1/purchase-orders", expected=202, key=key,
            headers=headers or {}, json={"recommendationId": recommendation["id"],
                                         "recommendationVersion": recommendation["version"], "productId": product},
        )

    async def order(self, order_id):
        return await self.request("purchaser", "GET", f"/api/v1/purchase-orders/{order_id}")

    async def recommendation(self, rec_id):
        return await self.request("purchaser", "GET", f"/api/v1/recommendations/{rec_id}")

    async def stock(self, product):
        return await self.request("warehouse-manager", "GET", f"/api/v1/stocks/{product}")

    async def planning(self, action):
        return await self.request("admin", "POST", f"/__demo/planning/{action}")

    async def happy(self):
        product, initial, _, rec = await self.initialize("OK")
        key, correlation = str(uuid4()), str(uuid4())
        order = await self.create_order(product, rec, key, {"X-Correlation-Id": correlation})
        replay = await self.create_order(product, rec, key, {"X-Correlation-Id": correlation})
        ensure(replay == order and order["status"] == "PREPARING", str(replay))
        order_id = order["id"]
        confirmed = await self.wait(lambda: self.order(order_id), lambda data: data["status"] == "CONFIRMED")
        applied = await self.wait(lambda: self.recommendation(rec["id"]), lambda data: data["status"] == "APPLIED")
        snapshot = await self.wait(
            lambda: self.stock(product),
            lambda data: any(plan["orderId"] == order_id for plan in data["confirmedInbound"]),
        )
        ensure(snapshot["onHand"] == initial["onHand"] and confirmed["quantity"] == 30, str(snapshot))
        payload = {"orderId": order_id, "quantity": confirmed["quantity"],
                   "receivedAt": datetime.now(timezone.utc).isoformat()}
        receipt_key = str(uuid4())
        receipt = await self.request("warehouse-manager", "POST", "/api/v1/receipts", expected=201, key=receipt_key,
                                     json=payload, headers={"X-Correlation-Id": correlation})
        receipt_retry = await self.request("warehouse-manager", "POST", "/api/v1/receipts", expected=201,
                                           key=receipt_key, json=payload)
        ensure(receipt == receipt_retry, "Repeating a receipt must return the same receipt.")
        received = await self.wait(lambda: self.order(order_id), lambda data: data["status"] == "RECEIVED")
        final = await self.stock(product)
        ensure(final["onHand"] == 40 and not final["confirmedInbound"], str(final))
        self.note(f"УСПЕХ | orderId={order_id} | PREPARING → CONFIRMED → {received['status']} | "
                  f"рекомендация {applied['status']} | остаток {initial['onHand']} → {final['onHand']} | "
                  f"повторы заказа и приёмки идемпотентны | correlationId={correlation}")
        return {"order": received, "stock": final, "recommendation": applied, "receipt": receipt}

    async def failure(self):
        product, initial, _, rec = await self.initialize("CONFLICT")
        correlation = str(uuid4())
        await self.planning("pause")
        try:
            order = await self.create_order(product, rec, headers={"X-Correlation-Id": correlation})
            await self.wait(lambda: self.order(order["id"]), lambda data: data["sagaStep"] == "PLAN")
            claimed = await self.recommendation(rec["id"])
            ensure(claimed.get("claimedBy") == order["id"] and claimed["status"] == "CLAIMED", str(claimed))
            await self.request(
                "warehouse-manager", "POST", "/api/v1/inventory-adjustments", expected=201,
                json={"productId": product, "delta": 7, "expectedVersion": initial["version"],
                      "reason": "Остаток изменился после закрепления рекомендации"},
            )
        finally:
            await self.planning("resume")
        cancelled = await self.wait(lambda: self.order(order["id"]), lambda data: data["status"] == "CANCELLED")
        stale = await self.recommendation(rec["id"])
        stock = await self.stock(product)
        ensure(cancelled["lastError"] == "STOCK_VERSION_CONFLICT", str(cancelled))
        ensure(stale["status"] == "STALE" and "claimedBy" not in stale, str(stale))
        ensure(stock["onHand"] == 17 and not stock["confirmedInbound"], str(stock))
        replacement = await self.request("purchaser", "POST", "/api/v1/recommendations", expected=201,
                                         json={"forecastId": rec["forecastId"]})
        ensure(replacement["quantity"] == 23 and replacement["id"] != rec["id"], str(replacement))
        self.note(f"КОМПЕНСАЦИЯ | orderId={cancelled['id']} | {cancelled['lastError']} → {cancelled['status']} | "
                  f"рекомендация {stale['status']}, закрепление снято | остаток {stock['onHand']}, "
                  f"новая рекомендация {replacement['quantity']} | correlationId={correlation}")
        return {"order": cancelled, "stock": stock, "recommendation": stale, "replacement": replacement}


async def run(args):
    async with httpx.AsyncClient(base_url=args.url, trust_env=False, timeout=8) as http:
        demo = DemoClient(http)
        health = await http.get("/health")
        health.raise_for_status()
        await demo.login()
        if args.scenario in {"happy", "all"}:
            await demo.happy()
        if args.scenario in {"failure", "all"}:
            await demo.failure()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--scenario", choices=("all", "happy", "failure"), default="all")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
