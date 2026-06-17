"""Ordering-template helpers for UI-managed templates.

A *template* is a vendor ordering config keyed by a saved grid-view name and
stored outside this package (the web app keeps them in SQLite). This module
owns the validation/normalisation of a template before it is persisted.
"""
from __future__ import annotations

from .common import OrderError

# API types accepted by the api_backend dispatcher.
_OPENAPI_TYPES = ("openapi", "rest", "swagger", "openapi3")
_DEFAULT_ATTACHMENT_NAME = "PO_{order_number}.xlsx"


def _normalize_attachment(att: dict | None) -> dict:
    att = att or {}
    return {
        "enabled": bool(att.get("enabled", True)),
        "filename": (str(att.get("filename") or "").strip() or _DEFAULT_ATTACHMENT_NAME),
    }


def validate_template(name: str, data: dict) -> dict:
    """Validate and normalise a template for ``name`` into its storable shape.

    Raises :class:`OrderError` (status 400) on any invalid input.
    """
    if not name or not str(name).strip():
        raise OrderError("A view name is required.", status=400)
    if not isinstance(data, dict):
        raise OrderError("Template payload must be an object.", status=400)

    method = (data.get("method") or "").lower()
    out: dict = {"method": method, "label": (str(data.get("label") or name)).strip() or name}

    # Destination location (required) — every order creates a Shopify transfer
    # into this location. Common to both email and API templates.
    location_id = (str(data.get("location_id") or "")).strip()
    if not location_id:
        raise OrderError("A destination location is required.", status=400)
    out["location_id"] = location_id
    out["location_name"] = (str(data.get("location_name") or "")).strip()

    if method == "email":
        email = data.get("email") or {}
        if not isinstance(email, dict):
            raise OrderError("Email configuration must be an object.", status=400)
        to = (str(email.get("to") or "")).strip()
        to_env = (str(email.get("to_env") or "")).strip()
        if not to and not to_env:
            raise OrderError(
                "Email templates need a recipient address (or a recipient env var).", status=400
            )
        subject = (str(email.get("subject") or "")).strip()
        if not subject:
            raise OrderError("An email subject is required.", status=400)
        body = email.get("body") or ""
        if not str(body).strip():
            raise OrderError("An email body is required.", status=400)
        out["email"] = {
            "to": to,
            "to_env": to_env,
            "subject": subject,
            "body": body,
            "attachment": _normalize_attachment(email.get("attachment")),
        }
        return out

    if method == "api":
        api = data.get("api")
        if not isinstance(api, dict) or not api:
            raise OrderError("API configuration is required and must be an object.", status=400)
        api_type = (api.get("type") or "openapi").lower()
        if api_type not in _OPENAPI_TYPES and api_type != "graphql":
            raise OrderError(f"Unknown API type '{api_type}'.", status=400)
        place = api.get("place_order") or {}
        if not isinstance(place, dict) or not place:
            raise OrderError("API templates need a place_order section.", status=400)
        if api_type == "graphql":
            if not place.get("mutation"):
                raise OrderError("GraphQL API templates need place_order.mutation.", status=400)
        elif not place.get("path"):
            raise OrderError("OpenAPI templates need place_order.path.", status=400)
        out["api"] = api
        return out

    raise OrderError("Template method must be 'email' or 'api'.", status=400)
