"""
Minimal Instagram webhook receiver.

Two jobs only, for now:
  1. GET  /webhook  -> answer Meta's verification handshake
  2. POST /webhook  -> verify the signature, log the payload, return 200 fast

Run with:
  export META_APP_SECRET="..."      # App settings > Basic > App secret
  export IG_VERIFY_TOKEN="..."      # any string you choose; you'll paste the same one into the dashboard
  uvicorn webhook:app --port 8000 --reload
"""

import hashlib
import hmac
import json
import logging
import os

from fastapi import FastAPI, HTTPException, Request, Response

APP_SECRET = os.environ["META_APP_SECRET"]
VERIFY_TOKEN = os.environ["IG_VERIFY_TOKEN"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("webhook")

app = FastAPI()


@app.get("/webhook")
async def verify(request: Request) -> Response:
    """Meta calls this once when you save the callback URL in the dashboard.

    It sends hub.mode, hub.verify_token and hub.challenge as query params.
    If the token matches ours, we echo the challenge back as plain text.
    """
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == VERIFY_TOKEN:
        log.info("webhook verified")
        return Response(content=params.get("hub.challenge", ""), media_type="text/plain")
    log.warning("verification failed: bad token or mode")
    raise HTTPException(status_code=403, detail="verification failed")


@app.post("/webhook")
async def receive(request: Request) -> dict:
    """Meta POSTs here for every subscribed event.

    Order matters: read the RAW bytes first, verify the HMAC over those exact
    bytes, and only then parse JSON. Re-serialised JSON would hash differently.
    """
    body = await request.body()

    signature = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        log.warning("bad signature; rejecting")
        raise HTTPException(status_code=401, detail="bad signature")

    payload = json.loads(body)
    log.info("event received:\n%s", json.dumps(payload, indent=2))

    # Nothing else yet. Later this is where we enqueue and return immediately.
    return {"ok": True}