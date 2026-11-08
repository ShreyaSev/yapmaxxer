"""
Worker: turns queued comment events into replies.

Loop:
  claim one pending event (compare-and-set, so several workers can run)
  -> skip our own comments and nested replies
  -> match against rules
  -> per-user cooldown check
  -> POST /<comment_id>/replies
  -> record outcome (sent / no_match / skipped / failed), or schedule a retry

Run with:
  export IG_TOKEN="..."  IG_ID="..."
  export GRAPH_HOST="graph.facebook.com"   # or graph.instagram.com, whichever your token worked on
  python worker.py
"""

import logging
import os
import re
import time
import unicodedata

import requests

import db

IG_TOKEN = os.environ["IG_TOKEN"]
IG_ID = os.environ["IG_ID"]
GRAPH_HOST = os.environ.get("GRAPH_HOST", "graph.facebook.com")
API_VERSION = os.environ.get("GRAPH_API_VERSION", "v22.0")

POLL_INTERVAL = 1.0           # seconds between polls when the queue is empty
MAX_ATTEMPTS = 5
COOLDOWN_SECONDS = 10 * 60    # one reply per (commenter, rule) per 10 min
RETRYABLE_GRAPH_CODES = {1, 2, 4, 17, 32, 613}   # transient / rate-limit codes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")


# ---------- matching ----------

def normalize(text: str) -> str:
    """Lowercase, fold fancy Unicode, drop punctuation (keep emoji), collapse spaces."""
    text = unicodedata.normalize("NFKC", text or "").lower()
    text = text.replace("\ufe0f", "").replace("\ufe0e", "")   # invisible emoji variation selectors
    # Unicode categories starting with 'P' are punctuation; emoji are 'S' (symbols) and survive.
    text = "".join(" " if unicodedata.category(ch).startswith("P") else ch for ch in text)
    return re.sub(r"\s+", " ", text).strip()


def find_rule(conn, media_id, text):
    """First active rule whose keyword appears as a whole word. Post-specific
    rules and higher priority win."""
    rows = conn.execute(
        """
        SELECT * FROM rules
        WHERE active = 1 AND (media_id IS NULL OR media_id = ?)
        ORDER BY (media_id IS NOT NULL) DESC, priority DESC, id ASC
        """,
        (media_id,),
    ).fetchall()
    norm = normalize(text)
    for rule in rows:
        kw = re.escape(normalize(rule["keyword"]))
        if re.search(rf"(?<!\w){kw}(?!\w)", norm):
            return rule
    return None


# ---------- queue ----------

def claim_next(conn):
    """Compare-and-set claim: only succeeds if the row is still 'pending'.
    If two workers race, exactly one wins."""
    now = time.time()
    row = conn.execute(
        """
        SELECT id FROM events
        WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY received_at LIMIT 1
        """,
        (now,),
    ).fetchone()
    if row is None:
        return None
    cur = conn.execute(
        "UPDATE events SET status = 'processing', attempts = attempts + 1 "
        "WHERE id = ? AND status = 'pending'",
        (row["id"],),
    )
    if cur.rowcount != 1:
        return None  # lost the race; next loop will pick another
    return conn.execute("SELECT * FROM events WHERE id = ?", (row["id"],)).fetchone()


def finish(conn, event_id, status, **fields):
    fields["processed_at"] = time.time()
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(
        f"UPDATE events SET status = ?, {sets} WHERE id = ?",
        (status, *fields.values(), event_id),
    )


def schedule_retry(conn, ev, error: str):
    if ev["attempts"] >= MAX_ATTEMPTS:
        finish(conn, ev["id"], "failed", last_error=error)
        log.error("event %s failed permanently: %s", ev["id"], error)
        return
    delay = 2 ** ev["attempts"] * 5   # 10s, 20s, 40s, 80s ...
    conn.execute(
        "UPDATE events SET status = 'pending', next_attempt_at = ?, last_error = ? WHERE id = ?",
        (time.time() + delay, error, ev["id"]),
    )
    log.warning("event %s retry in %ss: %s", ev["id"], delay, error)


def in_cooldown(conn, commenter_id, rule_id) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM events
        WHERE commenter_id = ? AND rule_id = ? AND status = 'sent' AND processed_at > ?
        LIMIT 1
        """,
        (commenter_id, rule_id, time.time() - COOLDOWN_SECONDS),
    ).fetchone()
    return row is not None


# ---------- Graph API ----------

class GraphError(Exception):
    def __init__(self, message, code=None, http_status=None, retryable=False):
        super().__init__(message)
        self.code, self.http_status, self.retryable = code, http_status, retryable


def send_reply(comment_id: str, message: str) -> str:
    url = f"https://{GRAPH_HOST}/{API_VERSION}/{comment_id}/replies"
    try:
        resp = requests.post(url, data={"message": message, "access_token": IG_TOKEN}, timeout=10)
    except requests.RequestException as e:
        raise GraphError(f"network: {e}", retryable=True)

    data = resp.json() if resp.content else {}
    if "error" in data or resp.status_code >= 400:
        err = data.get("error", {})
        code = err.get("code")
        retryable = resp.status_code >= 500 or code in RETRYABLE_GRAPH_CODES
        raise GraphError(err.get("message", f"HTTP {resp.status_code}"), code, resp.status_code, retryable)
    return data["id"]


# ---------- main ----------

def process(conn, ev):
    if ev["commenter_id"] == IG_ID:
        finish(conn, ev["id"], "skipped", last_error="own comment")
        return
    if ev["parent_id"]:
        # Instagram won't accept a reply to a reply; only top-level comments.
        finish(conn, ev["id"], "skipped", last_error="nested reply")
        return

    rule = find_rule(conn, ev["media_id"], ev["text"])
    if rule is None:
        finish(conn, ev["id"], "no_match")
        log.info("no rule for comment %s: %r", ev["comment_id"], ev["text"])
        return

    if in_cooldown(conn, ev["commenter_id"], rule["id"]):
        finish(conn, ev["id"], "skipped", rule_id=rule["id"], last_error="cooldown")
        log.info("cooldown: %s already got rule %s recently", ev["commenter_username"], rule["id"])
        return

    try:
        reply_id = send_reply(ev["comment_id"], rule["reply"])
    except GraphError as e:
        if e.retryable:
            schedule_retry(conn, ev, str(e))
        else:
            finish(conn, ev["id"], "failed", rule_id=rule["id"], last_error=str(e))
            log.error("non-retryable error on %s: %s", ev["comment_id"], e)
        return

    finish(conn, ev["id"], "sent", rule_id=rule["id"], reply_id=reply_id)
    log.info("replied to %s (%s) with rule %s -> %s", ev["commenter_username"], ev["comment_id"], rule["id"], reply_id)


def main():
    db.init()
    conn = db.connect()
    log.info("worker started; host=%s", GRAPH_HOST)
    try:
        while True:
            ev = claim_next(conn)
            if ev is None:
                time.sleep(POLL_INTERVAL)
                continue
            process(conn, ev)
    except KeyboardInterrupt:
        log.info("worker stopping")
    finally:
        conn.close()


if __name__ == "__main__":
    main()