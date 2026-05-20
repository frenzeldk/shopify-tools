"""Entire-M / Helikon-Tex integration.

Bundles two distinct concerns that share the same vendor:

1. Helikon-Tex partner-portal image listing (Apache directory + basic auth).
   The B2B API only exposes photos as indexed base64 blobs without color-code
   metadata, so per-color matching still requires the portal.

2. Entire-M B2B API client (sandbox.yaml). Bearer-token auth with cached
   token, JSON envelopes `{value, status, errors, isSuccess}`.

Public surface used by app.py:
  - get_helikon_listing()
  - classify_helikon_images(product_code, all_files)
  - stage_helikon_images(filenames)
  - helikon_image_url(filename) / helikon_image_basic_auth()
  - fetch_vendor_products()           — drop-in replacement for parse_vendor_csv
  - get_stocks(skus)
  - get_addresses()
  - place_order(items, address_id, order_number)
  - EntireMAPIError                    — raised on API/business errors
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import threading
import time
from typing import Any, Iterable

import requests

logger = logging.getLogger(__name__)


# ── Helikon-Tex partner portal (image listing) ────────────────────────────────

_HELIKON_BASE_URL = os.environ.get("HELIKON_BASE_URL")
_HELIKON_AUTH = (os.environ.get("HELIKON_USER"), os.environ.get("HELIKON_PASSWORD"))
_helikon_listing_cache: list[str] | None = None


def helikon_image_url(filename: str) -> str:
    return _HELIKON_BASE_URL + filename


def helikon_image_basic_auth() -> tuple[str | None, str | None]:
    return _HELIKON_AUTH


def get_helikon_listing() -> list[str]:
    """Fetch and in-process-cache the Apache directory listing of Helikon-Tex images."""
    global _helikon_listing_cache
    if _helikon_listing_cache is not None:
        return _helikon_listing_cache
    resp = requests.get(_HELIKON_BASE_URL, auth=_HELIKON_AUTH, timeout=30)
    resp.raise_for_status()
    filenames = re.findall(
        r'href="([^"?/][^"]*\.(?:jpg|jpeg|png|webp))"',
        resp.text,
        re.IGNORECASE,
    )
    _helikon_listing_cache = filenames
    return filenames


def classify_helikon_images(product_code: str, all_files: list[str]) -> dict:
    """Split Helikon image filenames for a product code into variant and additional groups.

    Additional categories (with ``category`` field):
      - "front"  : filename suffix contains the word "front"
      - "back"   : 4th dash-separated field starts with "back"
      - "detail" : 4th field starts with "detail"
      - "a"      : 4th field starts with "a"
    Variant: everything else (field 4 is the color code).
    """
    prefix = product_code.lower() + "-"
    variant_images: list[dict] = []
    additional_images: list[dict] = []
    for fname in all_files:
        if not fname.lower().startswith(prefix):
            continue
        stem = fname.rsplit(".", 1)[0]
        parts = stem.split("-")
        field4 = parts[3].lower() if len(parts) > 3 else ""
        suffix_lower = fname[len(prefix):].lower()
        if "front" in suffix_lower:
            additional_images.append({"filename": fname, "category": "front"})
        elif field4.startswith("back"):
            additional_images.append({"filename": fname, "category": "back"})
        elif field4.startswith("detail"):
            additional_images.append({"filename": fname, "category": "detail"})
        elif field4.startswith("a"):
            additional_images.append({"filename": fname, "category": "a"})
        else:
            variant_images.append({"filename": fname, "color_code": parts[3] if len(parts) > 3 else ""})
    return {"variant_images": variant_images, "additional_images": additional_images}


def stage_helikon_images(filenames: list[str]) -> dict[str, str | None]:
    """Download Helikon images with basic auth and stage-upload them to Shopify.

    Returns a mapping of filename → Shopify resourceUrl (None if upload failed).
    """
    from shopify import staged_upload_with_fallback

    result: dict[str, str | None] = {}
    for fname in filenames:
        url = _HELIKON_BASE_URL + fname
        try:
            resp = requests.get(url, auth=_HELIKON_AUTH, timeout=30)
            resp.raise_for_status()
            mime = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            resource_url = staged_upload_with_fallback(fname, resp.content, mime)
            result[fname] = resource_url
        except Exception as exc:
            logger.warning("stage_helikon_images: failed to upload %s: %s", fname, exc)
            result[fname] = None
    return result


# ── Entire-M B2B API client ───────────────────────────────────────────────────

_API_BASE_URL = (os.environ.get("ENTIRE_M_BASE_URL") or "https://api-sandbox.entirem.com").rstrip("/")
_API_CLIENT_ID = os.environ.get("ENTIRE_M_CLIENT_ID")
_API_CLIENT_SECRET = os.environ.get("ENTIRE_M_CLIENT_SECRET")
_API_LANGUAGE = os.environ.get("ENTIRE_M_LANGUAGE", "EN")

_token_lock = threading.Lock()
_cached_token: str | None = None


class EntireMAPIError(Exception):
    """Raised when the Entire-M API returns a non-success envelope or HTTP error."""

    def __init__(self, message: str, *, status: int | None = None, errors: list[dict] | None = None):
        super().__init__(message)
        self.status = status
        self.errors = errors or []


def _pick(d: Any, *keys: str, default: Any = None) -> Any:
    """Return d[key] for the first matching key, comparing case-insensitively.

    The sandbox OpenAPI spec declares fields in camelCase but the actual JSON
    envelope uses PascalCase (Value, IsSuccess, AccessToken). Accept either.
    """
    if not isinstance(d, dict):
        return default
    lowered = {k.lower(): k for k in d.keys() if isinstance(k, str)}
    for key in keys:
        actual = lowered.get(key.lower())
        if actual is not None:
            return d[actual]
    return default


def _login() -> str:
    if not _API_CLIENT_ID or not _API_CLIENT_SECRET:
        raise EntireMAPIError(
            "Entire-M API credentials not configured (set ENTIRE_M_CLIENT_ID / ENTIRE_M_CLIENT_SECRET)."
        )
    resp = requests.post(
        f"{_API_BASE_URL}/api/v1/auth/login",
        json={"clientId": _API_CLIENT_ID, "clientSecret": _API_CLIENT_SECRET},
        timeout=30,
    )
    if resp.status_code != 200:
        raise EntireMAPIError(
            f"Entire-M login failed (HTTP {resp.status_code}): {resp.text}",
            status=resp.status_code,
        )
    payload = resp.json()
    value = _pick(payload, "value") or {}
    token = _pick(value, "accessToken") or _pick(payload, "accessToken")
    if not token:
        raise EntireMAPIError(
            f"Entire-M login response missing accessToken. Payload: {payload!r}",
            status=resp.status_code,
        )
    return token


def _get_token(force_refresh: bool = False) -> str:
    global _cached_token
    with _token_lock:
        if _cached_token and not force_refresh:
            return _cached_token
        _cached_token = _login()
        return _cached_token


# Path → required scope mapping (from sandbox.yaml tag descriptions).
_PATH_SCOPES: list[tuple[str, str]] = [
    ("/api/v1/customer", "api:customer-information:read"),
    ("/api/v1/stocks", "api:stocks:read"),
    ("/api/v1/prices", "api:prices:read"),
    ("/api/v1/products", "api:products:read"),
    ("/api/v1/offer", "api:offer:read"),
    ("/api/v1/documents", "api:documents:read"),
    ("/api/v1/orders", "api:orders:read or api:orders:write"),
]


def _required_scope(path: str) -> str | None:
    for prefix, scope in _PATH_SCOPES:
        if path.startswith(prefix):
            return scope
    return None


def _request(method: str, path: str, *, json_body: dict | None = None, params: dict | None = None) -> dict:
    """Authenticated request with one automatic re-login on 401."""
    url = f"{_API_BASE_URL}{path}"
    for attempt in (1, 2):
        token = _get_token(force_refresh=(attempt == 2))
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        resp = requests.request(method, url, json=json_body, params=params, headers=headers, timeout=60)
        if resp.status_code == 401 and attempt == 1:
            continue
        break

    if resp.status_code == 403:
        scope = _required_scope(path)
        scope_hint = f" (requires scope `{scope}`)" if scope else ""
        raise EntireMAPIError(
            f"Entire-M {method} {path}: access denied (HTTP 403). The configured "
            f"client credentials do not have permission for this endpoint{scope_hint}. "
            "Ask the Entire-M API administrator to grant the required scope to your "
            "ClientId, then retry.",
            status=403,
        )

    text = resp.text
    try:
        payload = resp.json()
    except ValueError:
        raise EntireMAPIError(
            f"Entire-M {method} {path} returned non-JSON (HTTP {resp.status_code}): {text[:200]}",
            status=resp.status_code,
        )

    is_success = _pick(payload, "isSuccess")
    if resp.status_code >= 400 or is_success is False:
        errors = _pick(payload, "errors") or []
        message = "; ".join(
            (_pick(e, "errorMessage") or "").strip()
            for e in errors
            if isinstance(e, dict) and _pick(e, "errorMessage")
        ) or f"Entire-M {method} {path} failed (HTTP {resp.status_code})"
        raise EntireMAPIError(message, status=resp.status_code, errors=errors)

    return payload


def get_addresses() -> list[dict]:
    """GET /api/v1/customer/addresses."""
    payload = _request("GET", "/api/v1/customer/addresses")
    return _pick(payload, "value") or []


def get_stocks(skus: Iterable[str]) -> dict[str, float]:
    """POST /api/v1/stocks — returns mapping sku → available quantity."""
    items = [{"sku": s} for s in dict.fromkeys(skus) if s]
    if not items:
        return {}
    payload = _request("POST", "/api/v1/stocks", json_body={"items": items})
    result: dict[str, float] = {}
    for row in _pick(payload, "value") or []:
        sku = _pick(row, "sku")
        qty = _pick(row, "quantity")
        if sku is None or qty is None:
            continue
        try:
            result[sku] = float(qty)
        except (TypeError, ValueError):
            continue
    return result


def get_prices(skus: Iterable[str], page_number: int = 1) -> list[dict]:
    """POST /api/v1/prices for a list of SKUs (one page)."""
    items = [{"sku": s} for s in dict.fromkeys(skus) if s]
    if not items:
        return []
    payload = _request("POST", "/api/v1/prices", json_body={"items": items, "pageNumber": page_number})
    value = _pick(payload, "value") or {}
    return _pick(value, "prices") or []


def get_products(skus: Iterable[str], language: str | None = None) -> list[dict]:
    """POST /api/v1/products — returns a flat list of products across all pages."""
    items = [{"sku": s} for s in dict.fromkeys(skus) if s]
    if not items:
        return []
    out: list[dict] = []
    page = 1
    while True:
        payload = _request(
            "POST",
            "/api/v1/products",
            json_body={
                "items": items,
                "language": (language or _API_LANGUAGE).upper(),
                "pageNumber": page,
            },
        )
        value = _pick(payload, "value") or {}
        out.extend(_pick(value, "products") or [])
        if int(_pick(value, "pagesLeft") or 0) <= 0:
            break
        page += 1
    return out


def place_order(items: list[dict], address_id: int, order_number: str) -> dict:
    """POST /api/v1/orders.

    items: list of {"sku": str, "quantity": int}
    Returns the parsed envelope. Raises EntireMAPIError on any failure.
    """
    body = {
        "addressID": int(address_id),
        "orderNumber": str(order_number),
        "items": [
            {"sku": str(it["sku"]), "quantity": int(it["quantity"])}
            for it in items
            if it.get("sku") and int(it.get("quantity") or 0) > 0
        ],
    }
    if not body["items"]:
        raise EntireMAPIError("place_order called with no orderable items.")
    return _request("POST", "/api/v1/orders", json_body=body)


def make_order_number(prefix: str = "WT") -> str:
    """Generate a unique order number for our side of the call (<=100 chars)."""
    return f"{prefix}-{int(time.time())}"


def split_orderable_items(
    requested: list[dict],
) -> tuple[list[dict], list[dict], dict[str, float]]:
    """Look up stock for each requested item and split into orderable / blocked lists.

    requested: list of {"sku": str, "quantity": int, ...passthrough}
    Returns (orderable, blocked, stock_map).
      - orderable: items where stock >= requested quantity (quantity clamped to int).
      - blocked: items that cannot be ordered, each with an added "reason" field.
    """
    skus = [it["sku"] for it in requested if it.get("sku")]
    stock_map = get_stocks(skus)
    orderable: list[dict] = []
    blocked: list[dict] = []
    for it in requested:
        sku = it.get("sku")
        try:
            qty = int(it.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        if not sku or qty <= 0:
            blocked.append({**it, "reason": "Invalid SKU or zero quantity."})
            continue
        if sku not in stock_map:
            blocked.append({**it, "available": 0, "reason": "Not found in Entire-M catalog."})
            continue
        available = stock_map[sku]
        if available < qty:
            blocked.append({**it, "available": available, "reason": f"Insufficient stock (available: {available})."})
            continue
        orderable.append({**it, "available": available})
    return orderable, blocked, stock_map


# ── Vendor catalog (drop-in replacement for the legacy Helikon CSV export) ────


def _normalize_size(size: str) -> str:
    """Shorten repeated-X sizes: XXS→2XS, XXL→2XL, XXXL→3XL, etc."""
    m = re.match(r"^(X{2,})(S|L)$", size, re.IGNORECASE)
    if m:
        return f"{len(m.group(1))}X{m.group(2).upper()}"
    return size


def parse_vendor_csv(csv_content: str) -> list[dict]:
    """Parse the Helikon-Tex / Entire-M CSV export into normalised rows.

    SKU structure:  {ProductCode}-{ColorCode}[-{SizeCode}]
      ProductCode = first 3 dash-separated parts  (e.g. TS-CTT-CO)
      ColorCode   = 4th part                      (e.g. 01)
      SizeCode    = optional 5th part             (e.g. B05)
    """
    reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
    products: list[dict] = []
    for row in reader:
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
        sku = row.get("SKU", "")
        if not sku:
            continue

        full_name = row.get("Name", "")
        sku_parts = sku.split("-")
        product_code = "-".join(sku_parts[:3]) if len(sku_parts) >= 3 else sku
        if " - " in full_name:
            base_name, color = full_name.rsplit(" - ", 1)
        else:
            base_name = full_name
            color = ""

        raw_size = row.get("Size", "").strip()
        if "/" in raw_size:
            raw_size = raw_size.split("/", 1)[0].strip()

        products.append({
            "sku": sku,
            "ean": row.get("EAN13", ""),
            "hs_code": row.get("CN", ""),
            "size": _normalize_size(raw_size),
            "name": full_name,
            "product_code": product_code,
            "base_name": base_name,
            "color": color,
            "size_eu": row.get("ProductSizeEU", ""),
            "size_usa": row.get("ProductSizeUSA", ""),
            "price": row.get("DiscountPrice", "") or row.get("ProductRegularPrice", ""),
            "msrp": row.get("ProductMSRPPrice", ""),
            "currency": row.get("DiscountCurrency", "") or row.get("ProductRegularCurrency", ""),
            "weight": row.get("ProductWeight", ""),
            "weight_unit": row.get("ProductWeightUnit", ""),
            "country_of_origin": row.get("Country", ""),
        })
    return products


def fetch_vendor_products(language: str | None = None) -> list[dict]:
    """Fetch the full Entire-M catalog via the API and normalise to the legacy CSV shape.

    This is the API-backed replacement for the manual CSV-upload flow. Prices and
    stocks are fetched alongside so the consumer can fill ProductRegularPrice and
    decide stock availability for any downstream comparison/order workflow.
    """
    lang = (language or _API_LANGUAGE).upper()
    products_by_sku: dict[str, dict] = {}
    page = 1
    while True:
        payload = _request(
            "POST",
            "/api/v1/products",
            json_body={"language": lang, "pageNumber": page},
        )
        value = _pick(payload, "value") or {}
        for p in _pick(value, "products") or []:
            sku = _pick(p, "sku") or ""
            if not sku:
                continue
            sku_parts = sku.split("-")
            product_code = "-".join(sku_parts[:3]) if len(sku_parts) >= 3 else sku
            full_name = _pick(p, "name") or ""
            if " - " in full_name:
                base_name, color = full_name.rsplit(" - ", 1)
            else:
                base_name, color = full_name, ""
            products_by_sku[sku] = {
                "sku": sku,
                "ean": _pick(p, "ean") or "",
                "hs_code": "",
                "size": "",
                "name": full_name,
                "product_code": product_code,
                "base_name": base_name,
                "color": color,
                "size_eu": "",
                "size_usa": "",
                "price": "",
                "msrp": "",
                "currency": "",
                "weight": str(_pick(p, "weight") or ""),
                "weight_unit": _pick(p, "weightUnit") or "",
                "country_of_origin": "",
            }
        if int(_pick(value, "pagesLeft") or 0) <= 0:
            break
        page += 1

    if products_by_sku:
        price_page = 1
        while True:
            payload = _request(
                "POST",
                "/api/v1/prices",
                json_body={"pageNumber": price_page},
            )
            value = _pick(payload, "value") or {}
            for pr in _pick(value, "prices") or []:
                sku = _pick(pr, "sku")
                if not sku or sku not in products_by_sku:
                    continue
                details = _pick(pr, "priceDetails") or []
                wholesale = next((d for d in details if (_pick(d, "priceType") or "").lower() == "wholesale"), None)
                msrp = next((d for d in details if (_pick(d, "priceType") or "").lower() == "msrp"), None)
                if wholesale:
                    products_by_sku[sku]["price"] = str(_pick(wholesale, "price") or "")
                    products_by_sku[sku]["currency"] = _pick(wholesale, "currency") or ""
                if msrp:
                    products_by_sku[sku]["msrp"] = str(_pick(msrp, "price") or "")
            if int(_pick(value, "pagesLeft") or 0) <= 0:
                break
            price_page += 1

    return list(products_by_sku.values())
