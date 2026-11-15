import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import httpx

from postman.demo import DemoClient, sales_fixture, utc_monday
from services.contracts import CONTRACTS
from services.runner import BUSINESS, run_cluster


@asynccontextmanager
async def demo_cluster(path):
    async with run_cluster(path, port=0, demo_faults=True) as urls:
        async with httpx.AsyncClient(base_url=urls["gateway"], trust_env=False, timeout=8) as http:
            demo = DemoClient(http, quiet=True)
            await demo.login()
            yield demo, urls


class HttpTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name)

    def test_contract_operations_are_all_implemented(self):
        services = {cls.name: cls for cls in BUSINESS}
        operations = [op for ops in CONTRACTS.values() for op in ops]
        self.assertEqual(len(operations), 22)
        for op in operations:
            self.assertTrue(callable(getattr(services[op.service], op.name)))

    async def test_full_happy_path_through_gateway(self):
        async with demo_cluster(self.path) as (demo, _):
            result = await demo.happy()
            self.assertEqual(result["order"]["status"], "RECEIVED")
            self.assertEqual(result["stock"]["onHand"], 40)
            self.assertEqual(result["recommendation"]["status"], "APPLIED")
            self.assertEqual(result["receipt"]["quantity"], 30)
            self.assertEqual(len(list(self.path.glob("*-service.sqlite3"))), 5)

    async def test_stock_conflict_compensates(self):
        async with demo_cluster(self.path) as (demo, _):
            result = await demo.failure()
            self.assertEqual(result["order"]["lastError"], "STOCK_VERSION_CONFLICT")
            self.assertEqual(result["replacement"]["quantity"], 23)

    async def test_roles_routes_idempotency_and_problem_details(self):
        async with demo_cluster(self.path) as (demo, urls):
            purchase_token = {"Authorization": "Bearer " + demo.tokens["purchaser"]}
            denied = await demo.http.get("/api/v1/purchase-orders")
            self.assertEqual(denied.status_code, 401)
            missing = await demo.http.post("/api/v1/purchase-orders", headers=purchase_token, json={})
            self.assertEqual(missing.status_code, 422)
            correlation = str(uuid4())
            wrong_role = await demo.http.post(
                "/api/v1/purchase-orders", headers={"Authorization": "Bearer " + demo.tokens["analyst"],
                                                   "X-Correlation-Id": correlation},
            )
            self.assertEqual(wrong_role.status_code, 403)
            self.assertEqual(wrong_role.headers["content-type"], "application/problem+json")
            self.assertEqual(wrong_role.json()["correlationId"], correlation)
            self.assertEqual(wrong_role.headers["X-Correlation-Id"], correlation)
            hidden = await demo.http.get("/internal/v1/sales/weekly", headers=purchase_token)
            self.assertEqual(hidden.status_code, 404)
            direct = await demo.http.get(urls["procurement-service"] + "/api/v1/purchase-orders",
                                         headers=purchase_token)
            self.assertEqual(direct.status_code, 403)
            _, initial, _, _ = await demo.initialize("HTTP")
            body = {"productId": initial["productId"], "delta": 2, "expectedVersion": initial["version"],
                    "reason": "second inventory count"}
            key = str(uuid4())
            result, repeated = await asyncio.gather(*[
                demo.request("warehouse-manager", "POST", "/api/v1/inventory-adjustments",
                             key=key, expected=201, json=body)
                for _ in range(2)
            ])
            self.assertEqual(result, repeated)
            self.assertEqual(result["onHand"], 12)
            error = await demo.request("warehouse-manager", "POST", "/api/v1/inventory-adjustments",
                                       key=key, expected=409, json={**body, "delta": 3})
            self.assertEqual(error["code"], "IDEMPOTENCY_CONFLICT")

    async def test_reported_forecast_and_failed_job(self):
        async with demo_cluster(self.path) as (demo, _):
            product, _, imported, recommendation = await demo.initialize("FORECAST")
            body = {"productId": product, "datasetVersion": imported["datasetVersion"],
                    "asOfDate": utc_monday().isoformat(), "horizonWeeks": 2, "method": "last_value"}
            request = ("analyst", "POST", "/api/v1/forecast-jobs")
            job = await demo.request(*request, expected=202, json=body)
            repeated = await demo.request(*request, expected=202, json=body)
            self.assertEqual(job["id"], repeated["id"])
            status = await demo.request("analyst", "GET", f"/api/v1/forecast-jobs/{job['id']}")
            self.assertEqual(status["forecastId"], recommendation["forecastId"])
            bad = await demo.request(*request, expected=202, json={**body, "productId": "MISSING"})
            failed = await demo.wait(
                lambda: demo.request("analyst", "GET", f"/api/v1/forecast-jobs/{bad['id']}"),
                lambda data: data["status"] == "FAILED",
            )
            self.assertEqual(failed["errorCode"], "HISTORY_NOT_FOUND")
            # Exact file replay with a new key returns the same import.
            dup = await demo.request("analyst", "POST", "/api/v1/imports", expected=202,
                                     files={"file": ("sales.csv", sales_fixture(product), "text/csv")})
            self.assertEqual(dup["id"], imported["id"])

    async def test_cancellation_and_pending_saga_survive_restart(self):
        async with demo_cluster(self.path) as (demo, _):
            product, _, _, rec = await demo.initialize("CANCEL")
            await demo.planning("pause")
            first = await demo.create_order(product, rec)
            await demo.wait(lambda: demo.order(first["id"]), lambda order: order["sagaStep"] == "PLAN")
            await demo.request("purchaser", "POST", f"/api/v1/purchase-orders/{first['id']}/cancel", expected=202)
            await demo.wait(lambda: demo.order(first["id"]), lambda order: order["status"] == "CANCELLED")
            released = await demo.recommendation(rec["id"])
            self.assertEqual(released["status"], "READY")
            self.assertNotIn("claimedBy", released)
            second = await demo.create_order(product, released)
            await demo.wait(lambda: demo.order(second["id"]), lambda order: order["sagaStep"] == "PLAN")
        async with demo_cluster(self.path) as (demo, _):
            confirmed = await demo.wait(lambda: demo.order(second["id"]), lambda order: order["status"] == "CONFIRMED")
            self.assertEqual(confirmed["quantity"], 30)
            self.assertEqual((await demo.order(first["id"]))["status"], "CANCELLED")
            rejected = await demo.request("purchaser", "POST", f"/api/v1/purchase-orders/{second['id']}/cancel",
                                          expected=409)
            self.assertEqual(rejected["code"], "ORDER_ALREADY_CONFIRMED")
