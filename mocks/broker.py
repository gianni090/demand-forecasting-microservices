"""Local RabbitMQ substitute with topic routing, retry and dead-letter queues."""

import json
import logging
from pathlib import Path
import sqlite3
from time import time

from jsonschema.exceptions import ValidationError

from services.contracts import EVENT_SCHEMAS, MESSAGES
from services.store import encode


logger = logging.getLogger("lab4.broker")


class LocalBroker:
    def __init__(self, path: Path, retry_delays=(5, 30, 120)):
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.retry_delays = retry_delays
        self.db.executescript("""
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL,
                event_id TEXT NOT NULL, message TEXT NOT NULL, due REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, UNIQUE (queue, event_id)
            );
            CREATE INDEX IF NOT EXISTS messages_due ON messages (queue, due, seq);
            CREATE TABLE IF NOT EXISTS dead_letters (
                queue TEXT, event_id TEXT, message TEXT, error TEXT,
                PRIMARY KEY (queue, event_id)
            );
        """)

    def publish(self, producer: str, event: dict):
        event_type = event["eventType"]
        binding = MESSAGES[event_type]
        if binding["publisher"] != producer:
            raise ValueError(f"{producer} does not publish {event_type}")
        EVENT_SCHEMAS[event_type].validate(event)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.executemany(
                "INSERT OR IGNORE INTO messages(queue, event_id, message, due) VALUES (?, ?, ?, ?)",
                [(name, event["eventId"], encode(event), time()) for name in binding["queues"]],
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        logger.info("publish %s correlation=%s", event_type, event["correlationId"])

    async def deliver(self, service):
        for binding in MESSAGES.values():
            if service.name not in binding["subscribers"]:
                continue
            queue = f"q.{service.name}.{binding['routingKey']}"
            message = self.db.execute(
                "SELECT * FROM messages WHERE queue = ? AND due <= ? ORDER BY seq LIMIT 1",
                (queue, time()),
            ).fetchone()
            if message is None:
                continue
            try:
                event = json.loads(message["message"])
                EVENT_SCHEMAS[event["eventType"]].validate(event)
                await service.consume(event)
            except Exception as error:
                self.retry_or_dead_letter(message, error)
            else:
                self.db.execute("DELETE FROM messages WHERE seq = ?", (message["seq"],))
                logger.info("ack %s correlation=%s", queue, event["correlationId"])

    def retry_or_dead_letter(self, message, error):
        attempts = message["attempts"]
        if attempts < len(self.retry_delays) and not isinstance(error, ValidationError):
            self.db.execute("UPDATE messages SET due = ?, attempts = ? WHERE seq = ?",
                            (time() + self.retry_delays[attempts], attempts + 1, message["seq"]))
            logger.warning("retry %s event=%s reason=%s", message["queue"], message["event_id"], error)
            return
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("INSERT OR REPLACE INTO dead_letters VALUES (?, ?, ?, ?)",
                            (f"dlq.{message['queue']}", message["event_id"], message["message"], str(error)))
            self.db.execute("DELETE FROM messages WHERE seq = ?", (message["seq"],))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        logger.error("dead-letter %s event=%s reason=%s", message["queue"], message["event_id"], error)

    def close(self):
        self.db.close()
