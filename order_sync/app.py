#!/opt/shopify-python/bin/python3

"""Minimal Flask application to receive shopify webhook events.

The server only accepts requests targeting the expected public domain and
enqueues the order referenced by each verified webhook.

A Shipmondo webhook is authenticated solely by the HMAC signature on its
``data`` token, so the token is treated as a bearer credential: the algorithm
is pinned, freshness is required, and every accepted token is recorded so a
captured one cannot be replayed.

Environment:
    EXPECTED_HOST                    Host header the service answers on.
    WEBHOOK_PATH                     Path prefix the webhook is mounted under.
    SHIPMONDO_JWT_KEY                HMAC key the tokens are signed with.
    SHIPMONDO_JWT_MAX_AGE            Seconds a token stays valid (default 300).
    SHIPMONDO_JWT_REQUIRED_CLAIMS    Claims that must be present (default "iat").
    SHIPMONDO_JWT_ISSUER/_AUDIENCE   Verified when set.
    WEBHOOK_DEDUPE_TTL               Seconds a delivery id is remembered.
    ORDER_SYNC_LOG_LEVEL             Log level, default INFO.

Diagnostics:
    Every path that ends without an order reaching the queue logs a line
    tagged ``DROPPED-<reason>``; an accepted delivery logs ``ENQUEUED`` with
    the rq job id, which ties it to the worker's log for that order::

        journalctl -u order-sync -S -3d | grep -E 'DROPPED|ENQUEUED'
"""

from __future__ import annotations
import hashlib
import logging
import os
import time
import jwt
from flask import Flask, Request, Response, abort, jsonify, request
from valkey import Valkey
from rq import Queue
from shopify import handle_order

try:  # the client fork does not guarantee this alias
    from valkey.exceptions import ValkeyError
except ImportError:  # pragma: no cover - any store failure must fail closed
    ValkeyError = Exception

logging.basicConfig(
    level=os.environ.get("ORDER_SYNC_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

EXPECTED_HOST = os.environ.get("EXPECTED_HOST")
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH")
SECRET = os.environ.get("SHOPIFY_APP_SECRET")
JWTKEY = os.environ.get("SHIPMONDO_JWT_KEY")

# Freshness: a signed token with no expiry is valid forever, so require a claim
# we can age it against and reject anything older than the window.
JWT_MAX_AGE = int(os.environ.get("SHIPMONDO_JWT_MAX_AGE", 300))
JWT_LEEWAY = int(os.environ.get("SHIPMONDO_JWT_LEEWAY", 60))
JWT_REQUIRED_CLAIMS = [
    claim
    for claim in os.environ.get("SHIPMONDO_JWT_REQUIRED_CLAIMS", "iat")
    .replace(",", " ")
    .split()
]
JWT_ISSUER = os.environ.get("SHIPMONDO_JWT_ISSUER")
JWT_AUDIENCE = os.environ.get("SHIPMONDO_JWT_AUDIENCE")

# How long an accepted delivery is remembered for replay detection.
DEDUPE_TTL = int(os.environ.get("WEBHOOK_DEDUPE_TTL", 7 * 24 * 3600))
# Webhook bodies are a few hundred bytes; anything larger is not ours.
MAX_BODY_BYTES = int(os.environ.get("WEBHOOK_MAX_BODY_BYTES", 64 * 1024))

app: Flask = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_BODY_BYTES

_valkey = Valkey()
queue = Queue(connection=_valkey)

if not JWT_REQUIRED_CLAIMS:
    logger.warning(
        "SHIPMONDO_JWT_REQUIRED_CLAIMS is empty: tokens without exp/iat will be "
        "accepted and can only be stopped by the replay cache."
    )

logger.info(
    "order-sync listening for %s/create on host %r; token window %ds (+%ds leeway), "
    "dedupe ttl %ds, body cap %d bytes",
    WEBHOOK_PATH, EXPECTED_HOST, JWT_MAX_AGE, JWT_LEEWAY, DEDUPE_TTL, MAX_BODY_BYTES,
)


def _safe(value: object, limit: int = 80) -> str:
    """Render an untrusted value for a log line.

    Everything logged about a rejected delivery comes from a caller we have not
    authenticated, so collapse newlines (no forging extra log records) and cap
    the length before it reaches the journal.
    """
    text = str(value)
    text = text.replace("\r", " ").replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "…"


def _unverified_claims(token: str) -> dict:
    """Best-effort peek at a token's claims, for diagnostics only.

    The signature is deliberately not checked: these values exist so a rejected
    delivery can be traced back to an order number in the log. They are never
    trusted and never acted on.
    """
    try:
        claims = jwt.decode(token, options={"verify_signature": False})
    except jwt.PyJWTError:
        return {}
    return claims if isinstance(claims, dict) else {}


def _token_age(claims: dict) -> str:
    """Return the age of a token in seconds, rendered for a log line."""
    issued_at = claims.get("iat")
    if issued_at is None:
        return "unknown"
    try:
        return f"{time.time() - float(issued_at):.1f}s"
    except (TypeError, ValueError):
        return "unparseable"


def _host_allows_request(req: Request) -> bool:
    """Return True when the Host header matches the expected domain."""
    host_value = req.headers.get("Host", "")
    # Discard an eventual port suffix before comparing.
    host_without_port = host_value.split(":", 1)[0].lower()
    return host_without_port == EXPECTED_HOST


@app.before_request
def enforce_host_restriction() -> None:
    """Reject any request targeting a different domain.

    This is canonical-routing hygiene, not authentication — a direct caller
    controls its own Host header. The JWT check below is the actual control.
    """
    if not _host_allows_request(request):
        # A proxy that rewrites Host drops every delivery here, before any of
        # the webhook logging below runs, so say so explicitly.
        logger.warning(
            "DROPPED-HOST %s %s: Host %r does not match the expected %r",
            request.method,
            request.path,
            _safe(request.headers.get("Host")),
            EXPECTED_HOST,
        )
        abort(403, description="Host not allowed")


def _decode_webhook_token(token: str) -> dict:
    """Verify and decode a Shipmondo webhook token.

    Raises:
        jwt.PyJWTError: When the signature, claims or freshness do not hold.
    """
    decoded = jwt.decode(
        token,
        JWTKEY,
        algorithms=["HS256"],
        leeway=JWT_LEEWAY,
        issuer=JWT_ISSUER,
        audience=JWT_AUDIENCE,
        options={
            "require": JWT_REQUIRED_CLAIMS,
            "verify_exp": True,
            "verify_iss": bool(JWT_ISSUER),
            "verify_aud": bool(JWT_AUDIENCE),
        },
    )
    issued_at = decoded.get("iat")
    if issued_at is not None:
        age = time.time() - float(issued_at)
        if age > JWT_MAX_AGE + JWT_LEEWAY:
            raise jwt.InvalidTokenError(
                f"Token is {int(age)}s old, older than the {JWT_MAX_AGE}s window."
            )
    return decoded


def _claim_delivery(token: str, decoded: dict) -> bool:
    """Record this delivery and return whether it is the first time we see it.

    Keyed on the token's ``jti`` when the sender provides one and on a digest of
    the token itself otherwise, so a retried delivery of a *new* webhook is
    still processed while a captured token cannot be replayed.
    """
    jti = decoded.get("jti")
    identity = str(jti) if jti else hashlib.sha256(token.encode("utf-8")).hexdigest()
    return bool(_valkey.set(f"webhook:seen:{identity}", b"1", nx=True, ex=DEDUPE_TTL))


def _release_delivery(token: str, decoded: dict) -> None:
    """Undo :func:`_claim_delivery` so a failed enqueue can be retried."""
    jti = decoded.get("jti")
    identity = str(jti) if jti else hashlib.sha256(token.encode("utf-8")).hexdigest()
    try:
        _valkey.delete(f"webhook:seen:{identity}")
    except ValkeyError:
        logger.exception("Could not release the replay key for a failed enqueue")


@app.route(WEBHOOK_PATH + "/create", methods=["POST"])
def shipmondo_webhook() -> Response:
    """Receive a shipmondo webhook and acknowledge it.

    Before a webhook method is created or activated, Shipmondo sends a
    verification request to confirm the endpoint is reachable. That probe
    does not follow the normal data model (no signed ``data`` token, no
    order fields), so a payload without a token is accepted with HTTP 200
    rather than rejected, letting the method activate. A payload that *does*
    carry a token must verify: a bad or stale one is rejected with 401.
    """
    payload = request.get_json(silent=True)
    token = payload.get("data") if isinstance(payload, dict) else None
    if not token:
        # Verification probe or unexpected shape: acknowledge so Shipmondo
        # can create/activate the method. If Shipmondo ever changes the payload
        # shape, every delivery lands here and is silently thrown away, so log
        # what we actually received.
        logger.warning(
            "DROPPED-NO-TOKEN accepted a payload with no data token: type=%s keys=%s",
            type(payload).__name__,
            sorted(payload)[:20] if isinstance(payload, dict) else "-",
        )
        return jsonify({"status": "accepted"}), 200
    if not isinstance(token, str):
        logger.warning(
            "DROPPED-BAD-TOKEN the data token is a %s, not a string",
            type(token).__name__,
        )
        return jsonify({"error": "unauthorized"}), 401

    try:
        decoded = _decode_webhook_token(token)
    except jwt.PyJWTError as exc:
        # Which order was lost matters more than the reason, so dig the order
        # number out of the (unverified) claims before giving up on it.
        claims = _unverified_claims(token)
        claim_data = claims.get("data") if isinstance(claims.get("data"), dict) else {}
        logger.warning(
            "DROPPED-TOKEN order=%s shipmondo=%s age=%s jti=%s: %s",
            _safe(claim_data.get("order_id")),
            _safe(claim_data.get("id")),
            _token_age(claims),
            _safe(claims.get("jti")),
            exc,
        )
        return jsonify({"error": "unauthorized"}), 401

    data = decoded.get("data", {}) if isinstance(decoded, dict) else {}
    if data.get("id") is None or data.get("order_id") is None:
        logger.warning(
            "DROPPED-NO-IDS a verified token carried no id/order_id: "
            "claims=%s data_keys=%s",
            sorted(decoded)[:20] if isinstance(decoded, dict) else "-",
            sorted(data)[:20] if isinstance(data, dict) else "-",
        )
        return jsonify({"status": "ignored"}), 200

    try:
        first_delivery = _claim_delivery(token, decoded)
    except ValkeyError:
        logger.exception("Replay store unavailable; refusing the webhook")
        return jsonify({"error": "unavailable"}), 503
    if not first_delivery:
        # Shipmondo retrying a delivery we already accepted looks identical to a
        # replay, and the first attempt may well have died in the worker.
        logger.warning(
            "DROPPED-DUPLICATE order=%s shipmondo=%s jti=%s age=%s: already seen "
            "within the last %ds, not re-queued",
            _safe(data.get("order_id")),
            _safe(data.get("id")),
            _safe(decoded.get("jti")),
            _token_age(decoded),
            DEDUPE_TTL,
        )
        return jsonify({"status": "duplicate"}), 200

    try:
        job = queue.enqueue(handle_order,
                    int(data.get("id")),
                    int(data.get("order_id")))
    except Exception:
        # Let Shipmondo retry: drop the replay marker so the retry is accepted.
        _release_delivery(token, decoded)
        logger.exception("Could not enqueue order %s", data.get("order_id"))
        return jsonify({"error": "unavailable"}), 503
    logger.info(
        "ENQUEUED job=%s order=%s shipmondo=%s age=%s",
        getattr(job, "id", "-"),
        _safe(data.get("order_id")),
        _safe(data.get("id")),
        _token_age(decoded),
    )
    return jsonify({"status": "ok"}), 200


@app.errorhandler(403)
def forbidden(error: Exception) -> Response:  # pragma: no cover - simple mapping
    return jsonify({"error": "forbidden", "message": str(error)}), 403


@app.errorhandler(400)
def bad_request(error: Exception) -> Response:  # pragma: no cover - simple mapping
    logger.warning("DROPPED-400 %s %s: %s", request.method, request.path, error)
    return jsonify({"error": "bad_request", "message": str(error)}), 400


@app.errorhandler(405)
def method_not_allowed(error: Exception) -> Response:  # pragma: no cover - simple mapping
    logger.warning("DROPPED-405 %s %s: %s", request.method, request.path, error)
    return jsonify({"error": "method_not_allowed", "message": str(error)}), 405


@app.errorhandler(404)
def not_found(_error: Exception) -> Response:  # pragma: no cover - simple mapping
    # A delivery aimed at the wrong path never reaches the handler above.
    logger.warning(
        "DROPPED-404 %s %s: no route (webhook is mounted at %s/create)",
        request.method, request.path, WEBHOOK_PATH,
    )
    return jsonify({"error": "not_found", "message": "Endpoint not found"}), 404


@app.errorhandler(413)
def payload_too_large(_error: Exception) -> Response:  # pragma: no cover - simple mapping
    logger.warning(
        "DROPPED-413 %s %s: body over the %d byte cap (Content-Length %s)",
        request.method, request.path, MAX_BODY_BYTES,
        _safe(request.headers.get("Content-Length")),
    )
    return jsonify({"error": "payload_too_large", "message": "Request body too large"}), 413


if __name__ == "__main__":
    app.run(host=os.environ.get("ORDER_SYNC_HOST", "127.0.0.1"), port=8000)
