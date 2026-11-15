"""Register and validate the operations and messages defined in Lab 3."""

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker
import yaml


ROOT = Path(__file__).resolve().parents[1]
FORMAT = FormatChecker()
HTTP_METHODS = {"get", "post", "put", "patch", "delete"}


class Problem(Exception):
    def __init__(self, status: int, code: str, detail: str):
        super().__init__(detail)
        self.status = status
        self.code = code
        self.detail = detail


def correlation_id(value: str | None) -> str:
    if value is None:
        return str(uuid4())
    if not FORMAT.conforms(value, "uuid"):
        raise Problem(422, "INVALID_CORRELATION_ID", "X-Correlation-Id must be a UUID.")
    return value.lower()


@dataclass(frozen=True)
class Operation:
    service: str
    method: str
    path: str
    spec: dict
    source: dict
    matcher: re.Pattern

    @property
    def name(self) -> str:
        return self.spec["operationId"]

    @property
    def public(self) -> bool:
        return "public" in self.spec["tags"]

    @property
    def success_status(self) -> int:
        return next(int(code) for code in self.spec["responses"] if 200 <= int(code) < 300)

    def resolve(self, node: dict) -> dict:
        if "$ref" not in node:
            return node
        data = self.source
        for part in node["$ref"].removeprefix("#/").split("/"):
            data = data[part]
        return data

    def validate(self, value: Any, schema: dict) -> None:
        validator = Draft202012Validator(
            {**schema, "components": self.source["components"]}, format_checker=FORMAT
        )
        error = next(validator.iter_errors(value), None)
        if error is not None:
            location = ".".join(str(part) for part in error.absolute_path)
            message = f"{location}: {error.message}" if location else error.message
            raise Problem(422, "VALIDATION_ERROR", message)

    def response_schema(self) -> dict:
        response = self.spec["responses"][self.success_status]
        return self.resolve(response)["content"]["application/json"]["schema"]


def load_contracts() -> dict[str, list[Operation]]:
    contracts = {}
    for path in sorted((ROOT / "api").glob("*-service.yaml")):
        source = yaml.safe_load(path.read_text(encoding="utf-8"))
        contracts[path.stem] = [
            Operation(
                path.stem, method.upper(), route, spec, source,
                re.compile(re.sub(r"\\\{[a-zA-Z]+\\\}", "[^/]+", re.escape(route))),
            )
            for route, methods in source["paths"].items()
            for method, spec in methods.items()
            if method in HTTP_METHODS
        ]
    return contracts


CONTRACTS = load_contracts()
PUBLIC_OPERATIONS = [op for ops in CONTRACTS.values() for op in ops if op.public]
MESSAGES = {}
for operations in CONTRACTS.values():
    MESSAGES.update(operations[0].source.get("x-messages", {}))
EVENT_SCHEMAS = {
    path.name.removesuffix(".schema.json"): Draft202012Validator(
        json.loads(path.read_text(encoding="utf-8")), format_checker=FORMAT
    )
    for path in (ROOT / "api" / "events").glob("*.schema.json")
}
