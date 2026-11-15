"""Run six HTTP listeners over loopback and the local broker in one command."""

import argparse
import asyncio
from contextlib import asynccontextmanager, contextmanager
import logging
import os
from pathlib import Path
import socket

import httpx
import uvicorn

from mocks.auth import LocalIdentity
from mocks.broker import LocalBroker
from services.forecast import Forecast
from services.gateway import gateway_app
from services.inventory import Inventory
from services.platform import LocalNetwork, service_app
from services.procurement import Procurement
from services.replenishment import Replenishment
from services.sales import Sales
from services.store import Store


logger = logging.getLogger("lab4.runner")
BUSINESS = (Sales, Forecast, Inventory, Replenishment, Procurement)


class LocalServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        yield


def listener(port):
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", port))
        sock.listen(socket.SOMAXCONN)
        return sock
    except BaseException:
        sock.close()
        raise


@contextmanager
def exclusive_directory(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".runner.lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            if os.name == "nt":
                from msvcrt import LK_NBLCK, locking

                locking(lock.fileno(), LK_NBLCK, 1)
            else:
                from fcntl import LOCK_EX, LOCK_NB, flock

                flock(lock.fileno(), LOCK_EX | LOCK_NB)
        except OSError as error:
            raise RuntimeError(f"Another Lab 4 instance is already using {directory}.") from error
        yield


async def run_worker(service, broker):
    while True:
        try:
            service.relay(broker)
            await broker.deliver(service)
            await service.tick()
        except Exception:
            logger.exception("Worker failure in %s", service.name)
            await asyncio.sleep(0.5)
        await asyncio.sleep(0.05)


@asynccontextmanager
async def run_cluster(data_dir: Path, port: int = 8080, demo_faults: bool = False):
    data_dir.mkdir(parents=True, exist_ok=True)
    sockets = {}
    servers = []
    server_tasks = []
    workers = []
    stores = {}
    broker = None
    try:
        sockets["gateway"] = listener(port)
        for cls in BUSINESS:
            sockets[cls.name] = listener(0)
            stores[cls.name] = Store(data_dir / f"{cls.name}.sqlite3")
        urls = {name: f"http://127.0.0.1:{sock.getsockname()[1]}" for name, sock in sockets.items()}
        identity = LocalIdentity()
        broker = LocalBroker(data_dir / "broker.sqlite3")
        async with httpx.AsyncClient(trust_env=False) as client:
            service_urls = {name: url for name, url in urls.items() if name != "gateway"}
            network = LocalNetwork(client, identity, service_urls)
            services = {cls.name: cls(stores[cls.name], network) for cls in BUSINESS}
            apps = {name: service_app(service, identity) for name, service in services.items()}
            apps["gateway"] = gateway_app(client, service_urls, identity, services[Procurement.name], demo_faults)
            for name, app in apps.items():
                config = uvicorn.Config(app, log_config=None, access_log=False, lifespan="off", timeout_graceful_shutdown=3)
                server = LocalServer(config)
                servers.append(server)
                server_tasks.append(asyncio.create_task(server.serve(sockets=[sockets[name]])))
            try:
                async with asyncio.timeout(10):
                    while not all(server.started for server in servers):
                        for task in server_tasks:
                            if task.done():
                                task.result()
                                raise RuntimeError("An HTTP listener exited during startup.")
                        await asyncio.sleep(0.01)
                workers = [asyncio.create_task(run_worker(service, broker)) for service in services.values()]
                yield urls
            finally:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                for server in servers:
                    server.should_exit = True
                await asyncio.gather(*server_tasks, return_exceptions=True)
    finally:
        for sock in sockets.values():
            sock.close()
        for store in stores.values():
            store.close()
        if broker:
            broker.close()


async def serve(args):
    async with run_cluster(args.data_dir, args.port, args.demo_faults) as urls:
        print(f"Lab 4 Gateway: {urls['gateway']}", flush=True)
        await asyncio.Event().wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8080, help="Gateway port. Pass 0 for an available port.")
    parser.add_argument("--data-dir", type=Path, default=Path(".lab4-data"), help="Private SQLite directory.")
    parser.add_argument("--demo-faults", action="store_true", help="Enable the admin pause/resume demo controls.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    try:
        with exclusive_directory(args.data_dir):
            asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass
    except (OSError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
