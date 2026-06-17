"""Load and cache the purchase-order ordering configuration (YAML).

The config file is resolved from ``$PURCHASE_ORDER_CONFIG`` or, by default,
``orders.yaml`` next to this module. Vendors are keyed by the purchase-order
*configuration name* exactly as shown in the grid (the saved view name), so a
selected grid view maps straight to its ordering backend with no hardcoding.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "orders.yaml"

_lock = threading.Lock()
_cache: dict | None = None
# Optional callables returning the live config (e.g. DB-backed). When registered
# they override the YAML file as the source of truth, making everything editable
# at runtime. The YAML file then only matters for one-off migration.
_vendors_provider = None
_defaults_provider = None


def config_path() -> Path:
    """Resolve the active config path (env override wins)."""
    return Path(os.environ.get("PURCHASE_ORDER_CONFIG") or DEFAULT_CONFIG_PATH)


def load_config(force: bool = False) -> dict:
    """Load (and cache) the YAML config. Pass ``force=True`` to reload.

    The YAML file is optional: ordering config lives in the DB, so a missing
    file simply yields an empty config rather than an error.
    """
    global _cache
    with _lock:
        if _cache is not None and not force:
            return _cache
        path = config_path()
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = yaml.safe_load(handle) or {}
        except FileNotFoundError:
            data = {}
        if not isinstance(data, dict):
            raise ValueError(f"Purchase-order config at {path} is not a mapping.")
        _cache = data
        return data


def register_defaults_provider(fn) -> None:
    """Register a callable returning the live `defaults` dict (e.g. from a DB)."""
    global _defaults_provider
    _defaults_provider = fn


def file_defaults() -> dict:
    """The `defaults` declared in the YAML file ({} if no file)."""
    return load_config().get("defaults") or {}


def defaults() -> dict:
    if _defaults_provider is not None:
        return _defaults_provider() or {}
    return file_defaults()


def register_vendors_provider(fn) -> None:
    """Register a callable returning the live vendors dict (e.g. from a DB).

    Once registered, :func:`vendors` (and everything built on it) reads from
    ``fn()`` instead of the YAML file, making templates editable at runtime.
    Pass ``None`` to revert to the file.
    """
    global _vendors_provider
    _vendors_provider = fn


def file_vendors() -> dict[str, dict]:
    """The vendors declared in the YAML file ({} if no file)."""
    return load_config().get("vendors") or {}


def vendors() -> dict[str, dict]:
    if _vendors_provider is not None:
        return _vendors_provider() or {}
    return file_vendors()


def get_vendor(name: str) -> dict | None:
    return vendors().get(name)


def vendor_methods() -> dict[str, dict]:
    """Per-config-name ordering metadata for the frontend to render buttons.

    Returns ``{config_name: {"method": "email"|"api", "label": str,
    "api_type": str|None}}``.
    """
    out: dict[str, dict] = {}
    for name, cfg in vendors().items():
        cfg = cfg or {}
        api = cfg.get("api") or {}
        out[name] = {
            "method": cfg.get("method"),
            "label": cfg.get("label", name),
            "api_type": api.get("type") if cfg.get("method") == "api" else None,
        }
    return out
