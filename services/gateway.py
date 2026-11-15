"""The only client entry point for the local Lab 4 deployment."""

from collections import deque
from time import monotonic

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
import httpx

from mocks.auth import ROLES
from services.contracts import PUBLIC_OPERATIONS, Problem
from services.platform import install_http_errors


FORWARDED_HEADERS = {"authorization", "content-type", "content-length", "idempotency-key"}
RESPONSE_HEADERS = {"content-type", "x-correlation-id", "www-authenticate"}


def gateway_app(client, urls, identity, procurement, demo_faults):
    app = FastAPI(title="Demand Forecasting Lab 4 Gateway", docs_url=None, redoc_url=None, openapi_url=None)
    install_http_errors(app, "gateway")
    recent = deque()

    @app.get("/health")
    async def health():
        return {"status": "ok", "services": list(urls)}

    @app.post("/auth/lab-token")
    async def token(request: Request):
        try:
            body = await request.json()
        except ValueError as error:
            raise Problem(400, "INVALID_JSON", "Expected a valid JSON body.") from error
        role = body.get("role") if isinstance(body, dict) else None
        if not isinstance(role, str) or role not in ROLES:
            raise Problem(422, "INVALID_ROLE", "Choose an available development role.")
        return JSONResponse({"access_token": identity.issue("lab-" + role, role, "user"), "token_type": "Bearer"})

    if demo_faults:
        @app.post("/__demo/planning/{action}")
        async def control_demo(request: Request, action: str):
            principal = identity.authenticate(request.headers.get("Authorization", ""))
            if principal.kind != "user" or principal.role != "admin":
                raise Problem(403, "FORBIDDEN", "This control requires the development admin role.")
            if action == "pause":
                procurement.pause_planning.clear()
            elif action == "resume":
                procurement.pause_planning.set()
            else:
                raise Problem(404, "NOT_FOUND", "Unknown planning control.")
            return {"planning": "running" if procurement.pause_planning.is_set() else "paused"}

    @app.api_route("/api/{path:path}", methods=["GET", "POST"])
    async def forward(request: Request, path: str):
        operation = next(
            (op for op in PUBLIC_OPERATIONS if op.method == request.method and op.matcher.fullmatch(request.url.path)),
            None,
        )
        if operation is None:
            raise Problem(404, "NOT_FOUND", "The route is not published by the gateway.")
        current = monotonic()
        while recent and recent[0] <= current - 60:
            recent.popleft()
        if len(recent) >= 1200:
            raise Problem(429, "RATE_LIMITED", "Too many requests to the local gateway.")
        recent.append(current)
        headers = {key: value for key, value in request.headers.items() if key in FORWARDED_HEADERS}
        headers.update({"X-Lab-Gateway": identity.gateway_proof, "X-Correlation-Id": request.state.correlation})
        try:
            target = urls[operation.service] + request.scope["raw_path"].decode("ascii")
            upstream = await client.request(
                request.method, target, headers=headers, params=request.query_params,
                content=request.stream() if request.method == "POST" else None, timeout=30,
            )
        except httpx.RequestError as error:
            raise Problem(503, "SERVICE_UNAVAILABLE", f"{operation.service} is temporarily unavailable.") from error
        return Response(upstream.content, status_code=upstream.status_code,
                        headers={key: value for key, value in upstream.headers.items() if key in RESPONSE_HEADERS})

    return app
