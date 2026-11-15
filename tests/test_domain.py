import asyncio
from datetime import date
import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from mocks.broker import LocalBroker
from services.contracts import Problem
from services.forecast import predict_quantiles
from services.inventory import Inventory
from services.platform import RequestData
from services.replenishment import replenishment_quantity
from services.sales import aggregate_csv
from services.store import Store


class DomainTests(unittest.TestCase):
    def test_csv_aggregates_sales_and_excludes_returns(self):
        history, accepted, rejected, cutoff = aggregate_csv(
            b"productId,soldOn,quantity,operation\n"
            b"A,2026-11-02,12,SALE\nA,2026-11-04,8,SALE\n"
            b"A,2026-11-05,5,RETURN\nA,wrong,3,SALE\n"
        )
        self.assertEqual((accepted, rejected, cutoff), (3, 1, date(2026, 11, 9)))
        self.assertEqual(history["A"], {"2026-11-02": 20})

    def test_quantiles_and_model_artifacts(self):
        history = [20] * 14
        for method in ("last_value", "tsb", "lightgbm"):
            with self.subTest(method=method):
                estimates, artifact = predict_quantiles(method, history)
                self.assertEqual(estimates, sorted(estimates))
                self.assertGreaterEqual(estimates[0], 0)
                self.assertTrue(artifact)
                self.assertAlmostEqual(estimates[1], 40, delta=2)
        with self.assertRaises(Problem) as problem:
            predict_quantiles("lightgbm", [20, 20])
        self.assertEqual(problem.exception.code, "INSUFFICIENT_HISTORY")

    def test_replenishment_includes_only_confirmed_plans_in_window(self):
        forecast = {"asOfDate": "2026-11-02", "demandP90": 39.5}
        stock = {"onHand": 10, "confirmedInbound": [
            {"status": "CONFIRMED", "quantity": 6, "arrivalDate": "2026-11-09"},
            {"status": "CONFIRMED", "quantity": 50, "arrivalDate": "2026-11-17"},
            {"status": "PROVISIONAL", "quantity": 50, "arrivalDate": "2026-11-09"},
        ]}
        self.assertEqual(replenishment_quantity(forecast, stock), (16, 40, 24))
        self.assertEqual(replenishment_quantity({**forecast, "demandP90": 2}, stock), (16, 2, 0))

    def test_cancelled_plan_fences_late_creation(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "inventory.sqlite3")
            try:
                inventory = Inventory(store, None)
                order_id = str(uuid4())
                correlation = str(uuid4())
                with store.transaction():
                    result = inventory.cancel_plan(order_id)
                self.assertEqual(result, {"orderId": order_id, "status": "CANCELLED"})
                body = {"orderId": order_id, "productId": "A", "quantity": 24,
                        "arrivalDate": "2026-11-09", "expectedStockVersion": 1}
                ctx = RequestData("test", correlation, "POST", {}, body, None)
                with self.assertRaises(Problem) as conflict, store.transaction():
                    inventory.createInboundPlan(ctx)
                self.assertEqual(conflict.exception.code, "PLAN_CANCELLED")
            finally:
                store.close()

    def test_broker_retries_then_dead_letters(self):
        calls = []

        class DownConsumer:
            name = "forecast-service"

            async def consume(self, event):
                calls.append(event["eventId"])
                raise RuntimeError("unavailable")

        with tempfile.TemporaryDirectory() as folder:
            broker = LocalBroker(Path(folder) / "broker.sqlite3", retry_delays=(0, 0, 0))
            try:
                event_id, import_id = str(uuid4()), str(uuid4())
                event = {
                    "eventId": event_id, "eventType": "sales.imported", "schemaVersion": 1,
                    "occurredAt": "2026-11-02T09:00:00Z", "correlationId": str(uuid4()),
                    "aggregateId": import_id, "aggregateVersion": 1,
                    "payload": {"importId": import_id, "datasetVersion": 1, "asOfDate": "2026-11-02",
                                "productIds": ["A"], "acceptedRows": 1},
                }
                broker.publish("sales-service", event)
                for _ in range(4):
                    asyncio.run(broker.deliver(DownConsumer()))
                self.assertEqual(calls, [event_id] * 4)
                self.assertEqual(broker.db.execute("SELECT count(*) FROM messages").fetchone()[0], 0)
                dead = broker.db.execute("SELECT queue FROM dead_letters").fetchone()[0]
                self.assertEqual(dead, "dlq.q.forecast-service.sales.imported")
            finally:
                broker.close()
