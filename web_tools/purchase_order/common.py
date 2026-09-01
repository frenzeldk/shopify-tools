"""Shared helpers for the config-driven purchase-order subsystem.

Holds the pieces every ordering backend needs regardless of transport:
the error type, item/column normalisation, the XLSX order workbook, a
case-insensitive nested lookup, and the small request-body value DSL used to
turn YAML config into concrete request payloads.
"""
from __future__ import annotations

import io
import time
from typing import Any, Iterable

from openpyxl import Workbook


class OrderError(Exception):
    """Raised when an order cannot be prepared, validated or placed.

    ``status`` is an HTTP-ish hint the web route maps straight onto the
    response code (e.g. 400 for bad input, 409 for a business-rule block,
    401/403/423/502 forwarded from an upstream API). ``details`` is a list of
    per-item / per-error dicts surfaced to the client, and ``extra`` carries
    any additional top-level fields (e.g. ``order_total``).
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        details: list[dict] | None = None,
        extra: dict | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.details = details or []
        self.extra = extra or {}


def make_order_number(prefix: str = "WT") -> str:
    """Generate a unique order reference for our side of the order."""
    return f"{prefix}-{int(time.time())}"


def dig(data: Any, path: Iterable[Any], default: Any = None) -> Any:
    """Walk a case-insensitive key ``path`` through nested dicts.

    Tolerates the common B2B quirk of a spec declaring camelCase while the
    live envelope returns PascalCase (``value`` vs ``Value``).
    """
    cur = data
    for key in path or []:
        if not isinstance(cur, dict):
            return default
        lowered = {k.lower(): k for k in cur if isinstance(k, str)}
        actual = lowered.get(str(key).lower())
        if actual is None:
            return default
        cur = cur[actual]
    return cur


def pick(d: Any, *keys: str, default: Any = None) -> Any:
    """Return the first matching top-level key in ``d``, case-insensitively."""
    if not isinstance(d, dict):
        return default
    lowered = {k.lower(): k for k in d if isinstance(k, str)}
    for key in keys:
        actual = lowered.get(key.lower())
        if actual is not None:
            return d[actual]
    return default


def normalize_items(raw: Any) -> list[dict]:
    """Keep only dict items with a non-empty SKU and a positive integer qty."""
    items: list[dict] = []
    for it in raw or []:
        if not isinstance(it, dict):
            continue
        sku = str(it.get("sku") or "").strip()
        if not sku:
            continue
        try:
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        if qty <= 0:
            continue
        items.append({**it, "sku": sku, "quantity": qty})
    return items


# Header label / item field used when the client supplies no column spec.
DEFAULT_COLUMNS: list[dict[str, str]] = [
    {"field": "sku", "label": "SKU"},
    {"field": "quantity", "label": "Quantity"},
    {"field": "title", "label": "Title"},
    {"field": "product_title", "label": "Product Title"},
    {"field": "product_vendor", "label": "Vendor"},
    {"field": "barcode", "label": "Barcode"},
]


def normalize_columns(raw: Any) -> list[dict] | None:
    """Normalise the grid column spec; ``None`` falls back to DEFAULT_COLUMNS."""
    cols: list[dict] = []
    for col in raw or []:
        if not isinstance(col, dict):
            continue
        field = str(col.get("field") or "").strip()
        if not field:
            continue
        cols.append({"field": field, "label": col.get("label") or field})
    return cols or None


# Spreadsheet clients treat a cell starting with one of these as a formula, so
# an order line coming from the grid could otherwise execute in the supplier's
# spreadsheet.  Leading whitespace is ignored when deciding, because Excel
# ignores it too.
_FORMULA_LEAD = ("=", "+", "-", "@")
# Excel's own hard limit is 32767; stay under it and bound the attachment size.
MAX_CELL_CHARS = 4000
# Upper bound on how many columns a caller may request in the order workbook.
MAX_COLUMNS = 64


def _cell_value(value: Any) -> Any:
    """Coerce a raw item value into something openpyxl can safely write.

    Numbers and booleans pass through; every string is written as text with any
    formula-leading character neutralised by a single-quote prefix (the standard
    "escape as text" marker openpyxl/Excel understand).
    """
    if value is None or isinstance(value, (int, float, bool)):
        return value
    text = value if isinstance(value, str) else str(value)
    # Strip control characters openpyxl would reject or that hide the payload.
    text = "".join(ch for ch in text if ch == "\n" or ch >= " ")
    if len(text) > MAX_CELL_CHARS:
        text = text[:MAX_CELL_CHARS]
    if text.lstrip().startswith(_FORMULA_LEAD):
        return "'" + text
    return text


def build_order_workbook(items: list[dict], columns: list[dict] | None = None) -> bytes:
    """Build an XLSX workbook of the order lines and return it as bytes."""
    columns = (columns or DEFAULT_COLUMNS)[:MAX_COLUMNS]
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Order Lines"

    sheet.append([_cell_value(col.get("label") or col.get("field") or "") for col in columns])
    for it in items:
        sheet.append([_cell_value(it.get(col.get("field"))) for col in columns])

    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def split_by_stock(
    items: list[dict], stock_map: dict[str, float]
) -> tuple[list[dict], list[dict]]:
    """Split items into (orderable, blocked) using an sku -> available map."""
    orderable: list[dict] = []
    blocked: list[dict] = []
    for it in items:
        sku = it.get("sku")
        qty = int(it.get("quantity") or 0)
        if sku not in stock_map:
            blocked.append({**it, "available": 0, "reason": "Not found in vendor catalog."})
            continue
        available = stock_map[sku]
        if available < qty:
            blocked.append(
                {**it, "available": available, "reason": f"Insufficient stock (available: {available})."}
            )
            continue
        orderable.append({**it, "available": available})
    return orderable, blocked


# ── Request-body value DSL ────────────────────────────────────────────────────
#
# Config request bodies / GraphQL variables are plain YAML, with a few directive
# dicts resolved at request time. Directives use a ``$`` sigil so they can never
# collide with a real API field name (e.g. an ``items`` field):
#   {$env: VAR, $default: ...}       -> environment variable (or default / "")
#   {$ref: order_number|address_id}  -> a runtime value produced by the flow
#   {$value: "PO-{order_number}"}    -> literal string with {placeholder} format
#   {$items: {dest: source, ...}}    -> a list built from the order items, each
#                                        row mapping dest -> item[source]
# Any other dict / list is resolved recursively; bare scalars pass through,
# with strings getting {placeholder} substitution from the format context.

def _directive_type(spec: dict) -> str | None:
    """Identify a directive dict by its exact key shape."""
    keys = set(spec)
    if "$env" in keys and keys <= {"$env", "$default"}:
        return "env"
    if keys == {"$ref"}:
        return "ref"
    if keys == {"$value"}:
        return "value"
    if keys == {"$items"}:
        return "items"
    return None


def resolve_value(spec: Any, ctx: dict, _depth: int = 0) -> Any:
    """Resolve a config value ``spec`` against a runtime context.

    ``ctx`` has keys: ``fmt`` (dict for str.format placeholders), ``refs``
    (dict of runtime values for the ``ref`` directive) and ``items`` (the
    order lines for the ``items`` directive).

    The ``$env`` directive may only name an allowlisted environment variable
    (see :mod:`purchase_order.security`) — a stored template must not be able
    to select an arbitrary process secret.
    """
    # Imported lazily: security imports OrderError from this module.
    from . import security

    if _depth > security.MAX_VALUE_DEPTH:
        raise OrderError("Ordering template is nested too deeply.", status=400)

    if isinstance(spec, dict):
        directive = _directive_type(spec)
        if directive == "env":
            try:
                return security.resolve_env(spec["$env"], spec.get("$default", ""))
            except security.PolicyError as exc:
                raise OrderError(str(exc), status=exc.status) from exc
        if directive == "value":
            v = spec["$value"]
            return v.format(**ctx.get("fmt", {})) if isinstance(v, str) else v
        if directive == "ref":
            return (ctx.get("refs") or {}).get(spec["$ref"])
        if directive == "items":
            mapping = spec["$items"] or {}
            rows: list[dict] = []
            for it in ctx.get("items") or []:
                row: dict = {}
                for dest, src in mapping.items():
                    val = it.get(src)
                    if src == "quantity":
                        try:
                            val = int(val or 0)
                        except (TypeError, ValueError):
                            val = 0
                    row[dest] = val
                rows.append(row)
            return rows
        return {k: resolve_value(v, ctx, _depth + 1) for k, v in spec.items()}
    if isinstance(spec, list):
        return [resolve_value(v, ctx, _depth + 1) for v in spec]
    if isinstance(spec, str):
        try:
            return spec.format(**ctx.get("fmt", {}))
        except (KeyError, IndexError):
            return spec
    return spec


def format_context(order_number: str, company: str, address_id: Any = None) -> dict:
    """Build the {placeholder} substitution map used across backends."""
    return {
        "order_number": order_number,
        "company_name": company,
        "address_id": "" if address_id is None else str(address_id),
    }
