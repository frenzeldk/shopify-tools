"""Config-driven purchase-order ordering.

All vendor ordering logic is described in ``orders.yaml`` (see
``config.config_path``) rather than hardcoded. Each vendor selects a ``method``
— ``email`` or ``api`` (with ``api.type`` of ``openapi`` or ``graphql``) — and
this package dispatches to the matching backend.

Public surface used by the web app:
  - list_vendors()      -> per-config-name {method, label, api_type}
  - is_supported(name)  -> whether a config exists for the name
  - order_method(name)  -> "email" | "api" | None
  - place_order(vendor, items, columns, *, send_email=None) -> result dict
  - OrderError          -> raised on bad input / validation / upstream failure
"""
from __future__ import annotations

from typing import Callable

from . import api_backend, config as _config, email_backend
from .common import (
    OrderError,
    build_order_workbook,
    make_order_number,
    normalize_columns,
    normalize_items,
)
from .templates import validate_template

__all__ = [
    "OrderError",
    "build_order_workbook",
    "make_order_number",
    "list_vendors",
    "is_supported",
    "order_method",
    "get_vendor",
    "place_order",
    "reload_config",
    "register_vendors_provider",
    "register_defaults_provider",
    "file_vendors",
    "file_defaults",
    "config_path",
    "validate_template",
]


def reload_config() -> dict:
    """Force a reload of the YAML config (e.g. after editing it)."""
    return _config.load_config(force=True)


def register_vendors_provider(fn) -> None:
    """Register a live vendors source (e.g. DB-backed templates)."""
    _config.register_vendors_provider(fn)


def register_defaults_provider(fn) -> None:
    """Register a live `defaults` source (e.g. DB-backed settings)."""
    _config.register_defaults_provider(fn)


def file_vendors() -> dict[str, dict]:
    """Vendors declared in the YAML file ({} if absent) — used for migration."""
    return _config.file_vendors()


def file_defaults() -> dict:
    """`defaults` declared in the YAML file ({} if absent) — used for migration."""
    return _config.file_defaults()


def config_path():
    """Path to the YAML config file (may not exist)."""
    return _config.config_path()


def list_vendors() -> dict[str, dict]:
    """Per-config-name ordering metadata for the frontend."""
    return _config.vendor_methods()


def get_vendor(name: str) -> dict | None:
    return _config.get_vendor(name)


def is_supported(name: str) -> bool:
    return name in _config.vendors()


def order_method(name: str) -> str | None:
    return (_config.get_vendor(name) or {}).get("method")


def place_order(
    vendor: str,
    items: list[dict],
    columns: list[dict] | None = None,
    *,
    send_email: Callable[..., tuple[bool, str]] | None = None,
) -> dict:
    """Place an order for ``vendor`` (a purchase-order configuration name).

    ``items`` are raw grid rows; they are normalised here. ``send_email`` is
    the transport used by email vendors (the package owns the rendering, the
    caller owns delivery). Raises :class:`OrderError` on any failure.
    """
    vcfg = _config.get_vendor(vendor)
    if not vcfg:
        raise OrderError(f"'{vendor}' has no ordering configuration.", status=400)

    method = (vcfg.get("method") or "").lower()
    norm_items = normalize_items(items)
    if not norm_items:
        raise OrderError("No items with a valid SKU and positive quantity.", status=400)
    norm_columns = normalize_columns(columns)

    if method == "email":
        return email_backend.place_order(vendor, vcfg, norm_items, norm_columns, send_email=send_email)
    if method == "api":
        return api_backend.place_order(vendor, vcfg, norm_items, norm_columns)
    raise OrderError(f"Unknown ordering method '{method}' for {vendor}.", status=400)
