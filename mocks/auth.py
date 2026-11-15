"""Signed development tokens in place of the Keycloak identity provider."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from secrets import token_urlsafe

import jwt

from services.contracts import Problem


ROLES = {"analyst", "purchaser", "warehouse-manager", "admin"}
ISSUER = "lab4-local-auth"
AUDIENCE = "demand-lab"


@dataclass(frozen=True)
class Principal:
    subject: str
    role: str
    kind: str


class LocalIdentity:
    def __init__(self):
        self.secret = token_urlsafe(48)
        self.gateway_proof = token_urlsafe(48)

    def issue(self, subject: str, role: str, kind: str) -> str:
        now = datetime.now(timezone.utc)
        return jwt.encode({
            "iss": ISSUER, "aud": AUDIENCE, "sub": subject, "role": role, "kind": kind,
            "iat": now, "exp": now + timedelta(hours=2),
        }, self.secret, algorithm="HS256")

    def authenticate(self, header: str) -> Principal:
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise Problem(401, "UNAUTHENTICATED", "A Bearer token is required.")
        try:
            claims = jwt.decode(token, self.secret, algorithms=["HS256"], issuer=ISSUER,
                                audience=AUDIENCE, options={"require": ["sub", "exp", "iat"]})
            return Principal(claims["sub"], claims["role"], claims["kind"])
        except (jwt.PyJWTError, KeyError) as error:
            raise Problem(401, "UNAUTHENTICATED", "The token is invalid or expired.") from error
