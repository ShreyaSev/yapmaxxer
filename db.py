"""
Storage shared by webhook.py and worker.py.

One SQLite file plays two roles for now:
  * events  -> the queue AND the idempotency store. A UNIQUE constraint on
               comment_id means the same comment can only ever be inserted once,
               no matter how many times Meta redelivers it.
  * rules   -> keyword -> reply mappings, editable without redeploying.

Swapping the queue for Redis/SQS later only touches enqueue() and claim_next();
the dedupe table stays.
"""

import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "bot.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id                 INTEGER PRIMARY KEY,
    comment_id         TEXT    NOT NULL UNIQUE,     -- idempotency key
    media_id           TEXT,
    commenter_id       TEXT,
    commenter_username TEXT,
    text               TEXT,
    parent_id          TEXT,                        -- set if this is a reply to another comment
    received_at        REAL    NOT NULL,
    status             TEXT    NOT NULL DEFAULT 'pending',
        -- pending | processing | sent | no_match | skipped | failed
    attempts           INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    REAL,                        -- backoff: don't claim before this time
    rule_id            INTEGER,
    reply_id           TEXT,                        -- ID Meta returns for our reply
    last_error         TEXT,
    processed_at       REAL
);
CREATE INDEX IF NOT EXISTS idx_events_status ON events (status, received_at);

CREATE TABLE IF NOT EXISTS rules (
    id        INTEGER PRIMARY KEY,
    keyword   TEXT    NOT NULL,
    reply     TEXT    NOT NULL,
    media_id  TEXT,                                 -- NULL = applies to every post
    priority  INTEGER NOT NULL DEFAULT 0,           -- higher wins when several match
    active    INTEGER NOT NULL DEFAULT 1
);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")      # lets webhook write while worker reads
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()