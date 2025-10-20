#!/opt/shopify-python/bin/python3

"""Minimal Flask application to receive shopify webhook events.

The server only accepts requests targeting the expected public domain and
prints the JSON payload of each accepted webhook to standard output.
"""

from __future__ import annotations
import os
import jwt
from flask import Flask, Request, Response, abort, jsonify, request
from valkey import Valkey
from rq import Queue
from shopify import handle_order

EXPECTED_HOST = os.environ.get("EXPECTED_HOST")
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH")
SECRET = os.environ.get("SHOPIFY_APP_SECRET")
JWTKEY = os.environ.get("SHIPMONDO_JWT_KEY")

app: Flask = Flask(__name__)

queue = Queue(connection=Valkey())


def _host_allows_request(req: Request) -> bool:
    """Return True when the Host header matches the expected domain."""
    host_value = req.headers.get("Host", "")
    # Discard an eventual port suffix before comparing.
    host_without_port = host_value.split(":", 1)[0].lower()
    return host_without_port == EXPECTED_HOST


@app.before_request
def enforce_host_restriction() -> None:
    """Reject any request targeting a different domain."""
    if not _host_allows_request(request):
        abort(403, description="Host not allowed")


@app.route(WEBHOOK_PATH + "/create", methods=["POST"])
def shipmondo_webhook() -> Response:
    """Receive a shipmondo webhook, print its payload, and acknowledge."""
    payload = request.get_json(silent=True)
    if payload is None:
        abort(400, description="Expected JSON body")
    # Here you would process the Shipmondo webhook payload as needed.
    data = jwt.decode(payload.get("data"), JWTKEY, algorithms="HS256")
    if not isinstance(data, dict):
        abort(400, description="Invalid JWT payload")
    queue.enqueue(handle_order,
                int(data.get("id")),
                int(payload.get("order_id")))
    return jsonify({"status": "ok"}), 200


@app.errorhandler(403)
def forbidden(error: Exception) -> Response:  # pragma: no cover - simple mapping
    return jsonify({"error": "forbidden", "message": str(error)}), 403


@app.errorhandler(400)
def bad_request(error: Exception) -> Response:  # pragma: no cover - simple mapping
    return jsonify({"error": "bad_request", "message": str(error)}), 400


@app.errorhandler(405)
def method_not_allowed(error: Exception) -> Response:  # pragma: no cover - simple mapping
    return jsonify({"error": "method_not_allowed", "message": str(error)}), 405


@app.errorhandler(404)
def not_found(_error: Exception) -> Response:  # pragma: no cover - simple mapping
    return jsonify({"error": "not_found", "message": "Endpoint not found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
