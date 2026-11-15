"""CSV snapshots and immutable weekly sales, owned by sales-service."""

import asyncio
from collections import defaultdict
import csv
from datetime import date, timedelta
from hashlib import sha256
from io import BytesIO, TextIOWrapper
import logging

from services.contracts import Problem
from services.platform import Service, new_id, page_response, timestamp


logger = logging.getLogger("lab4.sales")
WEEK = timedelta(weeks=1)
COLUMNS = {"productId", "soldOn", "quantity"}


def monday(value: date) -> date:
    return value - timedelta(days=value.weekday())


def aggregate_csv(contents: bytes):
    weeks: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    accepted = rejected = 0
    latest = None
    with TextIOWrapper(BytesIO(contents), encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        if not reader.fieldnames or not COLUMNS.issubset(reader.fieldnames):
            raise ValueError("Required columns: productId, soldOn, quantity.")
        for number, row in enumerate(reader, start=1):
            if number > 1_000_000:
                raise ValueError("The CSV exceeds one million rows.")
            try:
                product = (row.get("productId") or "").strip()
                sold_on = date.fromisoformat(row["soldOn"])
                quantity = int(row["quantity"])
                operation = (row.get("operation") or "SALE").strip().upper()
                if (not product or "/" in product or sold_on.isoformat() != row["soldOn"] or quantity <= 0
                        or operation not in {"SALE", "RETURN", "CANCEL"}):
                    raise ValueError("Invalid sale row.")
            except (ValueError, TypeError):
                rejected += 1
                continue
            accepted += 1
            latest = max(latest, sold_on) if latest else sold_on
            if operation == "SALE":
                weeks[product][monday(sold_on).isoformat()] += quantity
    if not weeks:
        raise ValueError("At least one valid sale is required.")
    if latest > date.max - 3 * WEEK:
        raise ValueError("The closing week is outside the supported date range.")
    return weeks, accepted, rejected, monday(latest) + WEEK


class Sales(Service):
    name = "sales-service"

    def createImport(self, ctx):
        checksum = sha256(ctx.body).hexdigest()
        existing = self.store.get("import-checksum", checksum)
        if existing:
            return self.require("import", existing["id"])
        import_id = new_id()
        entry = {"id": import_id, "status": "PENDING", "createdAt": timestamp()}
        self.store.upload(import_id, ctx.body)
        self.store.put("import-checksum", checksum, {"id": import_id})
        self.store.put("import", import_id, entry)
        self.store.put("pending-import", import_id, {"id": import_id, "correlation": ctx.correlation})
        return entry

    def getImport(self, ctx):
        return self.require("import", ctx.params["importId"])

    def getWeeklySales(self, ctx):
        params = ctx.params
        product = params["productId"]
        version = params["datasetVersion"]
        dataset = self.require("dataset", str(version))
        sales = self.require("weekly-sales", f"{version}/{product}")["weeks"]
        start = date.fromisoformat(params["fromDate"])
        end = date.fromisoformat(params["toDate"])
        if start > end:
            raise Problem(422, "INVALID_DATE_RANGE", "fromDate must not exceed toDate.")
        last = min(end, date.fromisoformat(dataset["asOfDate"]) - WEEK)
        if start > last:
            return {**params, "items": [], "total": 0}
        first = max(start + timedelta(days=(-start.weekday()) % 7), date.fromisoformat(min(sales)))
        total = max(0, (last - first).days // 7 + 1)
        offset = (params["page"] - 1) * params["size"]
        items = []
        for index in range(offset, min(offset + params["size"], total)):
            week = (first + index * WEEK).isoformat()
            items.append({"weekStart": week, "quantity": sales.get(week, 0)})
        return {**params, "items": items, "total": total}

    async def tick(self):
        pending = self.store.all("pending-import")
        if not pending:
            return
        task = min(pending, key=lambda item: self.require("import", item["id"])["createdAt"])
        import_id = task["id"]
        entry = self.require("import", import_id)
        entry["status"] = "PROCESSING"
        self.store.put("import", import_id, entry)
        contents = self.store.upload_contents(import_id)
        try:
            weeks, accepted, rejected, as_of = await asyncio.to_thread(aggregate_csv, contents)
        except (ValueError, UnicodeError, csv.Error) as error:
            logger.warning("import=%s correlation=%s failed: %s", import_id, task["correlation"], error)
            with self.store.transaction():
                self.store.put("import", import_id, {**entry, "status": "FAILED"})
                self.store.delete("pending-import", import_id)
            return
        with self.store.transaction():
            counter = self.store.get("counter", "dataset") or {"version": 0}
            version = counter["version"] + 1
            self.store.put("counter", "dataset", {"version": version})
            self.store.put("dataset", str(version), {"asOfDate": as_of.isoformat(), "importId": import_id})
            for product, series in weeks.items():
                self.store.put("weekly-sales", f"{version}/{product}", {"weeks": series})
            self.store.put("import", import_id, {**entry, "status": "COMPLETED", "datasetVersion": version,
                                                "acceptedRows": accepted, "rejectedRows": rejected})
            self.emit("sales.imported", import_id, version, task["correlation"], {
                "importId": import_id, "datasetVersion": version, "asOfDate": as_of.isoformat(),
                "productIds": sorted(weeks), "acceptedRows": accepted,
            })
            self.store.delete("pending-import", import_id)
