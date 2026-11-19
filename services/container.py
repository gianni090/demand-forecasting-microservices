"""Run one Lab 4 business service in a container."""

import argparse
import asyncio
from contextlib import asynccontextmanager, closing
import os
from pathlib import Path

import httpx
import uvicorn

from mocks.auth import LocalIdentity
from mocks.broker import LocalBroker
from services.platform import LocalNetwork, service_app
from services.runner import BUSINESS, exclusive_directory, run_worker
from services.store import Store


def create_app(service, data_dir: Path):
    identity = LocalIdentity(
        secret=os.getenv("LAB_AUTH_SECRET"),
        gateway_proof=os.getenv("LAB_GATEWAY_PROOF"),
    )
    urls = {
        cls.name: os.getenv(cls.name.upper().replace("-", "_") + "_URL",
                            f"http://{cls.name}:8000")
        for cls in BUSINESS
    }

    @asynccontextmanager
    async def lifespan(_app):
        broker_path = Path(os.getenv("LAB_BROKER_PATH", str(data_dir / "broker.sqlite3")))
        with closing(LocalBroker(broker_path)) as broker:
            async with httpx.AsyncClient(trust_env=False) as client:
                service.network = LocalNetwork(client, identity, urls)
                worker = asyncio.create_task(run_worker(service, broker))
                try:
                    yield
                finally:
                    worker.cancel()
                    await asyncio.gather(worker, return_exceptions=True)

    app = service_app(service, identity, lifespan=lifespan)

    @app.get("/healthz")
    async def health():
        service.store.db.execute("SELECT 1").fetchone()
        return {"status": "ok", "service": service.name}

    return app


def main():
    services = {cls.name: cls for cls in BUSINESS}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=services)
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    database = args.data_dir / f"{args.service}.sqlite3"
    with exclusive_directory(args.data_dir), closing(Store(database)) as store:
        service = services[args.service](store, None)
        app = create_app(service, args.data_dir)
        uvicorn.run(app, host="0.0.0.0", port=args.port, timeout_graceful_shutdown=3)


if __name__ == "__main__":
    main()
