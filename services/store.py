"""A local SQLite database per service, with atomic outbox and idempotency records."""

from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
from time import time


def encode(data) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Store:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA busy_timeout = 5000;
            CREATE TABLE IF NOT EXISTS entities (
                kind TEXT NOT NULL, id TEXT NOT NULL, document TEXT NOT NULL,
                PRIMARY KEY (kind, id)
            );
            CREATE TABLE IF NOT EXISTS uploads (id TEXT PRIMARY KEY, contents BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS idempotency (
                subject TEXT, path TEXT, key TEXT, fingerprint TEXT, result TEXT,
                expires REAL, PRIMARY KEY (subject, path, key)
            );
            CREATE TABLE IF NOT EXISTS inbox (event_id TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS outbox (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE, message TEXT NOT NULL
            );
        """)

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def get(self, kind: str, key: str):
        row = self.db.execute(
            "SELECT document FROM entities WHERE kind = ? AND id = ?", (kind, key)
        ).fetchone()
        return json.loads(row["document"]) if row else None

    def put(self, kind: str, key: str, document: dict):
        self.db.execute("""
            INSERT INTO entities VALUES (?, ?, ?)
            ON CONFLICT (kind, id) DO UPDATE SET document = excluded.document
        """, (kind, key, encode(document)))

    def delete(self, kind: str, key: str):
        self.db.execute("DELETE FROM entities WHERE kind = ? AND id = ?", (kind, key))

    def all(self, kind: str):
        rows = self.db.execute(
            "SELECT document FROM entities WHERE kind = ? ORDER BY id", (kind,)
        ).fetchall()
        return [json.loads(row["document"]) for row in rows]

    def replay(self, subject: str, path: str, key: str):
        row = self.db.execute(
            "SELECT fingerprint, result FROM idempotency WHERE subject = ? AND path = ? "
            "AND key = ? AND expires > ?", (subject, path, key, time())
        ).fetchone()
        return (row["fingerprint"], json.loads(row["result"])) if row else None

    def remember(self, subject: str, path: str, key: str, fingerprint: str, result: dict):
        self.db.execute(
            "INSERT OR REPLACE INTO idempotency VALUES (?, ?, ?, ?, ?, ?)",
            (subject, path, key, fingerprint, encode(result), time() + 7 * 86400),
        )

    def upload(self, key: str, contents: bytes):
        self.db.execute("INSERT INTO uploads VALUES (?, ?)", (key, contents))

    def upload_contents(self, key: str):
        return self.db.execute("SELECT contents FROM uploads WHERE id = ?", (key,)).fetchone()[0]

    def seen(self, event_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM inbox WHERE event_id = ?", (event_id,)).fetchone() is not None

    def mark_seen(self, event_id: str):
        self.db.execute("INSERT INTO inbox VALUES (?)", (event_id,))

    def enqueue(self, event: dict):
        self.db.execute("INSERT INTO outbox(event_id, message) VALUES (?, ?)", (event["eventId"], encode(event)))

    def pending_events(self):
        return [
            json.loads(row["message"]) for row in
            self.db.execute("SELECT message FROM outbox ORDER BY seq LIMIT 50")
        ]

    def sent(self, event_id: str):
        self.db.execute("DELETE FROM outbox WHERE event_id = ?", (event_id,))

    def close(self):
        self.db.close()
