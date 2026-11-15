"""Forecast jobs and the immutable two-week forecast contract."""

import asyncio
from datetime import date
import logging

from services.contracts import Problem
from services.platform import Service, new_id, stable_id, timestamp
from services.sales import WEEK, monday


logger = logging.getLogger("lab4.forecast")
QUANTILES = (0.1, 0.5, 0.9)
MODEL_VERSIONS = {"last_value": "last-value-v1", "tsb": "tsb-v1", "lightgbm": "lgbm-v1"}


def quantile(values, probability):
    values = sorted(values)
    index = (len(values) - 1) * probability
    low = int(index)
    high = min(low + 1, len(values) - 1)
    return values[low] + (index - low) * (values[high] - values[low])


def tsb_total(values, alpha=0.2):
    probability = float(values[0] > 0)
    size = float(values[0])
    for value in values[1:]:
        probability += alpha * (bool(value) - probability)
        if value:
            size = size + alpha * (value - size) if size else value
    return 2 * probability * size


def predict_quantiles(method, history):
    minimum = 10 if method == "lightgbm" else 3
    if len(history) < minimum:
        raise Problem(422, "INSUFFICIENT_HISTORY", f"{method} requires at least {minimum} complete weeks.")
    if method == "lightgbm":
        from lightgbm import Dataset, train
        from numpy import asarray

        features = [history[index - 3:index] for index in range(3, len(history) - 1)]
        targets = [sum(history[index:index + 2]) for index in range(3, len(history) - 1)]
        dataset = Dataset(asarray(features), label=targets)
        predictions, boosters = [], []
        for probability in QUANTILES:
            model = train({"objective": "quantile", "alpha": probability, "verbosity": -1,
                           "num_threads": 1, "seed": 42, "min_data_in_leaf": 2, "num_leaves": 7},
                          dataset, num_boost_round=60)
            predictions.append(float(model.predict(asarray([history[-3:]]), num_threads=1)[0]))
            boosters.append(model.model_to_string())
        return sorted(max(0.0, value) for value in predictions), {"boosters": boosters}
    estimate = (lambda values: 2 * values[-1]) if method == "last_value" else tsb_total
    prediction = estimate(history)
    residuals = [
        sum(history[index:index + 2]) - estimate(history[:index])
        for index in range(max(1, len(history) - 53), len(history) - 1)
    ]
    return [max(0.0, prediction + quantile(residuals, q)) for q in QUANTILES], {"residuals": residuals}


class Forecast(Service):
    name = "forecast-service"

    def enqueue_job(self, body, correlation):
        natural_key = stable_id(self.name, body["productId"], body["datasetVersion"], body["asOfDate"], body["method"])
        existing = self.store.get("forecast-key", natural_key)
        if existing:
            return self.require("forecast-job", existing["id"])
        job_id = new_id()
        job = {"id": job_id, "productId": body["productId"], "status": "PENDING", "createdAt": timestamp()}
        self.store.put("forecast-job", job_id, job)
        self.store.put("forecast-key", natural_key, {"id": job_id})
        self.store.put("pending-forecast", job_id, {"id": job_id, "parameters": body, "correlation": correlation})
        return job

    def createForecastJob(self, ctx):
        if date.fromisoformat(ctx.body["asOfDate"]) > date.max - 2 * WEEK:
            raise Problem(422, "INVALID_AS_OF_DATE", "The two-week horizon is outside the supported date range.")
        return self.enqueue_job(ctx.body, ctx.correlation)

    def getForecastJob(self, ctx):
        return self.require("forecast-job", ctx.params["jobId"])

    def getForecast(self, ctx):
        return self.require("forecast", ctx.params["forecastId"])

    def apply_event(self, event, prepared):
        payload = event["payload"]
        for product in payload["productIds"]:
            self.enqueue_job({"productId": product, "datasetVersion": payload["datasetVersion"],
                              "asOfDate": payload["asOfDate"], "horizonWeeks": 2, "method": "last_value"},
                             event["correlationId"])

    async def sales_history(self, body, correlation):
        cutoff = monday(date.fromisoformat(body["asOfDate"]))
        if cutoff.toordinal() <= 7:
            raise Problem(422, "INSUFFICIENT_HISTORY", "There are no complete weeks before the date.")
        window = {
            "productId": body["productId"], "datasetVersion": body["datasetVersion"],
            "fromDate": date.fromordinal(max(1, cutoff.toordinal() - 104 * 7)).isoformat(), "toDate": (cutoff - WEEK).isoformat(),
            "size": 100,
        }
        page = 1
        values = []
        while True:
            data = await self.rest("sales-service", "GET", "/internal/v1/sales/weekly",
                                   correlation, params={**window, "page": page})
            values.extend(item["quantity"] for item in data["items"])
            if len(values) >= data["total"]:
                return values
            page += 1

    async def tick(self):
        pending = self.store.all("pending-forecast")
        if not pending:
            return
        task = min(pending, key=lambda item: self.require("forecast-job", item["id"])["createdAt"])
        job_id = task["id"]
        body, correlation = task["parameters"], task["correlation"]
        job = self.require("forecast-job", job_id)
        self.store.put("forecast-job", job_id, {**job, "status": "RUNNING"})
        try:
            history = await self.sales_history(body, correlation)
            values, model_artifact = await asyncio.to_thread(predict_quantiles, body["method"], history)
        except Problem as error:
            code = "HISTORY_NOT_FOUND" if error.status == 404 else error.code
        except Exception:
            logger.exception("forecast job=%s correlation=%s", job_id, correlation)
            code = "FORECAST_FAILED"
        else:
            code = None
        with self.store.transaction():
            if code:
                self.store.put("forecast-job", job_id, {**job, "status": "FAILED", "errorCode": code})
                self.emit("forecast.failed", job_id, 1, correlation, {
                    "jobId": job_id, "productId": body["productId"], "code": code,
                })
            else:
                forecast_id = new_id()
                forecast = {**body, "id": forecast_id, "modelVersion": MODEL_VERSIONS[body["method"]],
                            "targetQuantile": 0.9, "demandP10": values[0], "demandP50": values[1],
                            "demandP90": values[2], "createdAt": timestamp()}
                self.store.put("forecast", forecast_id, forecast)
                self.store.put("model-artifact", forecast_id, model_artifact)
                self.store.put("forecast-job", job_id, {**job, "status": "SUCCEEDED", "forecastId": forecast_id})
                self.emit("forecast.completed", forecast_id, 1, correlation, {
                    "forecastId": forecast_id, "productId": body["productId"],
                    "datasetVersion": body["datasetVersion"],
                })
            self.store.delete("pending-forecast", job_id)
