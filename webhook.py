"""
Instagram webhook receiver.

  GET  /webhook  -> Meta's one-time verification handshake
  POST /webhook  -> verify signature, enqueue comment events, return 200 fast

The handler does NO Instagram API calls. It validates, writes a row, returns.
Everything slow or failure-prone happens in worker.py.

Run with:
  export META_APP_SECRET="..."      # App settings > Basic > App secret
  export IG_VERIFY_TOKEN="..."      # the string you entered in the dashboard
  uvicorn webhook:app --port 8000 --reload
"""

import hashlib
import hmac
import json
import logging
import os
import time

from fastapi import FastAPI, HTTPException, Request, Response

import db

APP_SECRET = os.environ["META_APP_SECRET"]
VERIFY_TOKEN = os.environ["IG_VERIFY_TOKEN"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("webhook")

app = FastAPI()
db.init()


@app.get("/webhook")
async def verify(request: Request) -> Response:
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == VERIFY_TOKEN:
        log.info("webhook verified")
        return Response(content=params.get("hub.challenge", ""), media_type="text/plain")
    log.warning("verification failed: bad token or mode")
    raise HTTPException(status_code=403, detail="verification failed")


@app.post("/webhook")
async def receive(request: Request) -> dict:
    body = await request.body()

    # 1. Authenticate: HMAC over the RAW bytes, constant-time compare.
    signature = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        log.warning("bad signature; rejecting")
        raise HTTPException(status_code=401, detail="bad signature")

    payload = json.loads(body)

    # 2. Enqueue. Loop everything: one delivery can carry many events.
    conn = db.connect()
    try:
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                if change.get("field") == "comments":
                    enqueue_comment(conn, change.get("value", {}))
            for msg in entry.get("messaging", []):
                # Story replies / DMs land here. Logged only for now; handled later.
                log.info("message event from %s (not yet handled)", msg.get("sender", {}).get("id"))
    finally:
        conn.close()

    # 3. Return immediately. Meta only wants to know we got it.
    return {"ok": True}


def enqueue_comment(conn, value: dict) -> None:
    """INSERT OR IGNORE = idempotency. A redelivered comment is a no-op."""
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO events
            (comment_id, media_id, commenter_id, commenter_username, text, parent_id, received_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            value.get("id"),
            value.get("media", {}).get("id"),
            value.get("from", {}).get("id"),
            value.get("from", {}).get("username"),
            value.get("text"),
            value.get("parent_id"),
            time.time(),
        ),
    )
    if cur.rowcount == 1:
        log.info("enqueued comment %s: %r", value.get("id"), value.get("text"))
    else:
        log.info("duplicate comment %s ignored", value.get("id"))