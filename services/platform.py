"""HTTP plumbing and infrastructure shared by the five business services."""

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import hmac
import logging
import random
import sqlite3
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid4, uuid5

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import httpx
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException

from mocks.auth import LocalIdentity, Principal
from services.contracts import CONTRACTS, EVENT_SCHEMAS, Problem, correlation_id
from services.store import Store, encode


logger = logging.getLogger("lab4")
MAX_CSV = 100 * 1024 * 1024
PUBLIC_ROLES = {
    "createImport": {"analyst"},
    "getImport": {"analyst"},
    "createForecastJob": {"analyst"},
    "getForecastJob": {"analyst"},
    "getPublicStock": {"warehouse-manager", "purchaser", "analyst"},
    "adjustStock": {"warehouse-manager"},
    "receiveOrder": {"warehouse-manager"},
    "createRecommendation": {"analyst", "purchaser"},
    "listRecommendations": {"analyst", "purchaser"},
    "getRecommendation": {"analyst", "purchaser"},
    "createPurchaseOrder": {"purchaser"},
    "listPurchaseOrders": {"purchaser"},
    "getPurchaseOrder": {"purchaser"},
    "cancelPurchaseOrder": {"purchaser"},
}
INTERNAL_ROLES = {
    "getWeeklySales": {"forecast-service"},
    "getForecast": {"replenishment-service"},
    "getInternalStock": {"replenishment-service"},
    "createInboundPlan": {"procurement-service"},
    "getInboundPlan": {"procurement-service"},
    "cancelInboundPlan": {"procurement-service"},
    "claimRecommendation": {"procurement-service"},
    "releaseRecommendation": {"procurement-service"},
}


def now() -> datetime:
    return datetime.now(timezone.utc)


def timestamp() -> str:
    return now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_id() -> str:
    return str(uuid4())


def stable_id(*parts) -> str:
    return str(uuid5(NAMESPACE_URL, encode(parts)))


def path_segment(value: str) -> str:
    return quote(value, safe="")


def page_response(items: list, page: int, size: int) -> dict:
    start = (page - 1) * size
    return {"page": page, "size": size, "total": len(items), "items": items[start:start + size]}


def json_response(value, status: int, correlation: str):
    return JSONResponse(value, status_code=status, headers={"X-Correlation-Id": correlation})


def install_http_errors(app: FastAPI, name: str):
    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.correlation = new_id()
        try:
            request.state.correlation = correlation_id(request.headers.get("X-Correlation-Id"))
        except Problem as error:
            return problem_response(request, error)
        response = await call_next(request)
        response.headers.setdefault("X-Correlation-Id", request.state.correlation)
        logger.info("%s %s %s %d correlation=%s", name, request.method, request.url.path,
                    response.status_code, request.state.correlation)
        return response

    @app.exception_handler(Problem)
    async def expected_error(request: Request, error: Problem):
        return problem_response(request, error)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException):
        code = {400: "BAD_REQUEST", 404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(
            error.status_code, "HTTP_ERROR"
        )
        return problem_response(request, Problem(error.status_code, code, str(error.detail)))

    @app.exception_handler(sqlite3.OperationalError)
    async def storage_error(request: Request, error: sqlite3.OperationalError):
        logger.error("storage error in %s correlation=%s: %s", name, request.state.correlation, error)
        return problem_response(request, Problem(503, "STORAGE_UNAVAILABLE", "The storage is unavailable."))


def problem_response(request: Request, error: Problem):
    correlation = request.state.correlation
    response = JSONResponse({
        "type": "urn:problem:" + error.code.lower().replace("_", "-"),
        "title": error.code.replace("_", " ").capitalize(),
        "status": error.status,
        "detail": error.detail,
        "code": error.code,
        "instance": request.url.path,
        "correlationId": correlation,
    }, status_code=error.status, headers={"X-Correlation-Id": correlation},
       media_type="application/problem+json")
    if error.status == 401:
        response.headers["WWW-Authenticate"] = "Bearer"
    return response


@dataclass(frozen=True)
class RequestData:
    subject: str
    correlation: str
    endpoint: str
    params: dict
    body: dict | bytes | None
    key: str | None
    prepared: dict | None = None

    @property
    def fingerprint(self):
        data = self.body if isinstance(self.body, bytes) else encode(self.body).encode()
        return sha256(data).hexdigest()


async def decode_request(request: Request, operation, principal) -> RequestData:
    params: dict = {}
    key = None
    for ref in operation.spec.get("parameters", []):
        parameter = operation.resolve(ref)
        name = parameter["name"]
        schema = parameter["schema"]
        where = parameter["in"]
        if where == "header" and name != "Idempotency-Key":
            continue
        source = {"header": request.headers, "query": request.query_params, "path": request.path_params}[where]
        value = source.get(name, schema.get("default"))
        if value is None:
            if parameter.get("required"):
                raise Problem(422, "VALIDATION_ERROR", f"{name} is required.")
            continue
        if schema.get("type") == "integer":
            try:
                value = int(value)
            except (ValueError, TypeError) as error:
                raise Problem(422, "VALIDATION_ERROR", f"{name} must be an integer.") from error
        operation.validate(value, schema)
        if where == "header":
            key = value.lower()
        else:
            params[name] = value.lower() if schema.get("format") == "uuid" else value

    body = None
    content = operation.spec.get("requestBody", {}).get("content", {})
    if "application/json" in content:
        if request.headers.get("content-type", "").partition(";")[0].strip().lower() != "application/json":
            raise Problem(400, "INVALID_JSON", "Content-Type must be application/json.")
        try:
            body = await request.json()
        except ValueError as error:
            raise Problem(400, "INVALID_JSON", "Expected a valid JSON body.") from error
        operation.validate(body, content["application/json"]["schema"])
    if "multipart/form-data" in content:
        if not request.headers.get("content-type", "").lower().startswith("multipart/form-data;"):
            raise Problem(400, "INVALID_UPLOAD", "Expected a multipart CSV file named file.")
        async with request.form(max_files=1, max_fields=0) as form:
            upload = form.get("file")
            if not isinstance(upload, UploadFile):
                raise Problem(400, "INVALID_UPLOAD", "The multipart field file is required.")
            if upload.size is not None and upload.size > MAX_CSV:
                raise Problem(422, "FILE_TOO_LARGE", "CSV exceeds 100 MiB.")
            body = await upload.read(MAX_CSV + 1)
            if len(body) > MAX_CSV:
                raise Problem(422, "FILE_TOO_LARGE", "CSV exceeds 100 MiB.")
            if not body:
                raise Problem(400, "INVALID_UPLOAD", "The CSV file is empty.")
    return RequestData(principal.subject, request.state.correlation, request.method + " " + request.url.path,
                       params, body, key)


class Service:
    name: str

    def __init__(self, store: Store, network):
        self.store = store
        self.network = network

    def require(self, kind: str, key: str) -> dict:
        item = self.store.get(kind, key)
        if item is None:
            raise Problem(404, "NOT_FOUND", f"{kind} {key} was not found.")
        return item

    def emit(self, event_type: str, aggregate: str, version: int, correlation: str, payload: dict):
        event = {
            "eventId": new_id(), "eventType": event_type, "schemaVersion": 1, "occurredAt": timestamp(),
            "correlationId": correlation, "aggregateId": aggregate, "aggregateVersion": version,
            "payload": payload,
        }
        EVENT_SCHEMAS[event_type].validate(event)
        self.store.enqueue(event)

    async def consume(self, event: dict):
        event_id = event["eventId"]
        if self.store.seen(event_id):
            return
        prepared = await self.prepare_event(event)
        with self.store.transaction():
            if not self.store.seen(event_id):
                self.apply_event(event, prepared)
                self.store.mark_seen(event_id)

    async def prepare_event(self, event):
        return None

    def apply_event(self, event, prepared):
        raise ValueError(f"{self.name} has no handler for {event['eventType']}")

    async def tick(self):
        pass

    def relay(self, broker):
        for event in self.store.pending_events():
            broker.publish(self.name, event)
            self.store.sent(event["eventId"])

    async def rest(self, target: str, method: str, path: str, correlation: str, *, params=None,
                   body=None, key=None):
        return await self.network.call(self.name, target, method, path, correlation,
                                       params=params, body=body, key=key)


class LocalNetwork:
    def __init__(self, clients: httpx.AsyncClient, identity: LocalIdentity, urls: dict):
        self.client = clients
        self.identity = identity
        self.urls = urls

    async def call(self, caller, target, method, path, correlation, *, params, body, key):
        headers = {"Authorization": "Bearer " + self.identity.issue(caller, caller, "service"),
                   "X-Correlation-Id": correlation}
        if key:
            headers["Idempotency-Key"] = key
        delays = (0.5, 1, 2)
        for attempt in range(len(delays) + 1):
            try:
                async with asyncio.timeout(5):
                    response = await self.client.request(
                        method, self.urls[target] + path, headers=headers, params=params, json=body,
                        timeout=httpx.Timeout(5, connect=2),
                    )
                data = response.json()
                if response.is_success:
                    return data
                problem = Problem(response.status_code, data["code"], data["detail"])
            except (httpx.RequestError, TimeoutError, ValueError, KeyError):
                problem = Problem(503, "DEPENDENCY_UNAVAILABLE", f"{target} is temporarily unavailable.")
            if problem.status not in (502, 503, 504) or attempt == len(delays):
                raise problem
            await asyncio.sleep(delays[attempt] + random.uniform(0, 0.1))


def service_app(service: Service, identity: LocalIdentity) -> FastAPI:
    app = FastAPI(title=service.name, docs_url=None, redoc_url=None, openapi_url=None)
    install_http_errors(app, service.name)

    def register(operation):
        handler = getattr(service, operation.name)
        prepare = getattr(service, "prepare_" + operation.name, None)

        async def route(request: Request):
            principal: Principal = identity.authenticate(request.headers.get("Authorization", ""))
            if operation.public:
                if (principal.kind != "user" or principal.role not in PUBLIC_ROLES[operation.name]
                        or not hmac.compare_digest(request.headers.get("X-Lab-Gateway", ""), identity.gateway_proof)):
                    raise Problem(403, "FORBIDDEN", "The request must use the gateway with a permitted role.")
            elif principal.kind != "service" or principal.role not in INTERNAL_ROLES[operation.name]:
                raise Problem(403, "FORBIDDEN", "This operation requires a permitted service identity.")
            ctx = await decode_request(request, operation, principal)
            if ctx.key is None:
                result = handler(ctx)
            else:
                fingerprint = ctx.fingerprint

                def replay():
                    record = service.store.replay(ctx.subject, ctx.endpoint, ctx.key)
                    if record is None:
                        return None
                    saved_fingerprint, result = record
                    if fingerprint != saved_fingerprint:
                        raise Problem(409, "IDEMPOTENCY_CONFLICT", "This key belongs to a different request body.")
                    return result

                result = replay()
                if result is None:
                    if prepare:
                        ctx = replace(ctx, prepared=await prepare(ctx))
                    with service.store.transaction():
                        result = replay()
                        if result is None:
                            result = handler(ctx)
                            operation.validate(result, operation.response_schema())
                            service.store.remember(ctx.subject, ctx.endpoint, ctx.key, fingerprint, result)
            if ctx.key is None:
                operation.validate(result, operation.response_schema())
            return json_response(result, operation.success_status, ctx.correlation)

        app.add_api_route(operation.path, route, methods=[operation.method], name=operation.name)

    for operation in CONTRACTS[service.name]:
        register(operation)
    return app
