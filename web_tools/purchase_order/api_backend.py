"""API-based ordering backend: OpenAPI/REST and GraphQL.

Both transports are fully config-driven (host, auth, response envelope and
field mapping all come from YAML). The order is placed by walking a small,
declarative ``flow`` of pre-flight steps followed by a place-order request:

  fetch_address  -> look up a delivery address, expose its id as ``address_id``
  stock_filter   -> drop items the vendor cannot fulfil
  price_minimum  -> validate currency + minimum order value
  place_order    -> the actual order request / mutation

Each step's endpoints, field names, price-type priority, minimum value and
currency live in config, so a new B2B vendor of the same shape needs no code.
"""
from __future__ import annotations

import os
import threading
from typing import Any

import requests

from . import config as cfg
from .common import (
    OrderError,
    dig,
    format_context,
    make_order_number,
    pick,
    resolve_value,
    split_by_stock,
)


# ── REST / OpenAPI transport ──────────────────────────────────────────────────


class RestClient:
    """Config-driven authenticated JSON client with envelope parsing."""

    def __init__(self, vendor: str, api: dict):
        self.vendor = vendor
        self.api = api
        self.base_url = (
            os.environ.get(api.get("base_url_env") or "") or api.get("base_url") or ""
        ).rstrip("/")
        self.timeout = api.get("timeout", 60)
        self.envelope = api.get("envelope") or {}
        self.auth = api.get("auth") or {}
        self._lock = threading.Lock()
        self._cached_token: str | None = None

    # auth ---------------------------------------------------------------
    def _login(self) -> str:
        login = self.auth.get("login") or {}
        body = resolve_value(login.get("json") or {}, {"fmt": {}, "refs": {}, "items": []})
        if any(v == "" for v in body.values()):
            raise OrderError(
                f"{self.vendor} API credentials not configured (check the login env vars).",
                status=400,
            )
        resp = requests.request(
            login.get("method", "POST"),
            f"{self.base_url}{login['path']}",
            json=body,
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise OrderError(
                f"{self.vendor} login failed (HTTP {resp.status_code}): {resp.text[:200]}",
                status=resp.status_code,
            )
        token = dig(resp.json(), login.get("token_path") or ["accessToken"])
        if not token:
            raise OrderError(f"{self.vendor} login response missing token.", status=resp.status_code)
        return token

    def _token(self, force: bool = False) -> str | None:
        atype = self.auth.get("type", "none")
        if atype == "login_bearer":
            with self._lock:
                if self._cached_token and not force:
                    return self._cached_token
                self._cached_token = self._login()
                return self._cached_token
        if atype == "bearer":
            return os.environ.get(self.auth.get("token_env") or "", "")
        return None

    def _headers(self, token: str | None) -> dict:
        headers = {"Accept": "application/json"}
        atype = self.auth.get("type", "none")
        if atype in ("login_bearer", "bearer"):
            scheme = self.auth.get("scheme", "Bearer")
            headers[self.auth.get("header", "Authorization")] = f"{scheme} {token}".strip()
        elif atype == "header":
            ctx = {"fmt": {}, "refs": {}, "items": []}
            for key, val in (self.auth.get("headers") or {}).items():
                headers[key] = resolve_value(val, ctx)
        return headers

    def _basic(self):
        if self.auth.get("type") == "basic":
            return (
                os.environ.get(self.auth.get("username_env") or "", ""),
                os.environ.get(self.auth.get("password_env") or "", ""),
            )
        return None

    # request ------------------------------------------------------------
    def request(self, method: str, path: str, *, json_body: dict | None = None, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        atype = self.auth.get("type", "none")
        basic = self._basic()
        resp = None
        for attempt in (1, 2):
            token = self._token(force=(attempt == 2))
            resp = requests.request(
                method, url, json=json_body, params=params,
                headers=self._headers(token), auth=basic, timeout=self.timeout,
            )
            if resp.status_code == 401 and attempt == 1 and atype == "login_bearer":
                continue
            break
        return self._parse(method, path, resp)

    def _parse(self, method: str, path: str, resp: requests.Response) -> dict:
        if resp.status_code == 403:
            raise OrderError(
                f"{self.vendor} {method} {path}: access denied (HTTP 403). The configured "
                "credentials do not have permission for this endpoint.",
                status=403,
            )
        try:
            payload = resp.json()
        except ValueError:
            raise OrderError(
                f"{self.vendor} {method} {path} returned non-JSON (HTTP {resp.status_code}): {resp.text[:200]}",
                status=resp.status_code,
            )
        success_path = self.envelope.get("success_path")
        is_success = dig(payload, success_path) if success_path else None
        if resp.status_code >= 400 or is_success is False:
            errors = dig(payload, self.envelope.get("errors_path") or ["errors"]) or []
            msg_key = self.envelope.get("error_message_key", "errorMessage")
            message = "; ".join(
                (pick(e, msg_key) or "").strip()
                for e in errors
                if isinstance(e, dict) and pick(e, msg_key)
            ) or f"{self.vendor} {method} {path} failed (HTTP {resp.status_code})"
            status = resp.status_code if resp.status_code >= 400 else 502
            raise OrderError(message, status=status, details=errors)
        return payload


# ── REST flow steps ────────────────────────────────────────────────────────────


def _rest_request_spec(step: dict) -> dict:
    """A step may nest its request under ``request`` or carry it flat."""
    return step.get("request") or step


def _step_fetch_address(client: RestClient, step: dict, fmt: dict) -> tuple[dict, Any]:
    req = _rest_request_spec(step)
    body = resolve_value(req.get("json") or None, {"fmt": fmt, "refs": {}, "items": []})
    payload = client.request(req.get("method", "GET"), req["path"], json_body=body)
    rows = dig(payload, step.get("value_path") or client.envelope.get("value_path") or [])
    if not isinstance(rows, list):
        rows = [rows] if rows else []
    if not rows:
        raise OrderError("No delivery addresses available from the vendor.", status=502)
    index = 0 if step.get("select", "first") in ("first", "only") else int(step.get("select"))
    address = rows[index]
    id_fields = step.get("id_field", "addressId")
    if isinstance(id_fields, str):
        id_fields = [id_fields]
    address_id: Any = None
    for field in id_fields:
        address_id = pick(address, field)
        if address_id is not None:
            break
    try:
        address_id = int(address_id or 0)
    except (TypeError, ValueError):
        pass
    return address, address_id


def _step_stock_filter(client: RestClient, step: dict, items: list[dict], fmt: dict) -> tuple[list[dict], list[dict]]:
    req = _rest_request_spec(step)
    body = resolve_value(req.get("json") or {}, {"fmt": fmt, "refs": {}, "items": items})
    payload = client.request(req.get("method", "POST"), req["path"], json_body=body)
    resp = step.get("response") or {}
    rows = dig(payload, resp.get("rows_path") or ["value"]) or []
    sku_key = resp.get("sku_key", "sku")
    qty_key = resp.get("qty_key", "quantity")
    stock_map: dict[str, float] = {}
    for row in rows:
        sku = pick(row, sku_key)
        qty = pick(row, qty_key)
        if sku is None or qty is None:
            continue
        try:
            stock_map[sku] = float(qty)
        except (TypeError, ValueError):
            continue
    return split_by_stock(items, stock_map)


def _step_price_minimum(client: RestClient, step: dict, orderable: list[dict], blocked: list[dict], fmt: dict) -> None:
    req = _rest_request_spec(step)
    resp = step.get("response") or {}
    priority = [p.lower() for p in (resp.get("price_type_priority") or [])]
    price_type_key = resp.get("price_type_key", "priceType")
    price_key = resp.get("price_key", "price")
    currency_key = resp.get("currency_key", "currency")
    details_key = resp.get("details_key", "priceDetails")
    sku_key = resp.get("sku_key", "sku")
    rows_path = resp.get("rows_path") or ["value", "prices"]
    paginate = req.get("paginate") or {}

    price_map: dict[str, dict] = {}
    page = 1
    while True:
        body = resolve_value(req.get("json") or {}, {"fmt": fmt, "refs": {}, "items": orderable})
        if paginate:
            body[paginate.get("page_param", "pageNumber")] = page
        payload = client.request(req.get("method", "POST"), req["path"], json_body=body)
        for pr in dig(payload, rows_path) or []:
            sku = pick(pr, sku_key)
            if not sku:
                continue
            details = pick(pr, details_key) or []
            chosen = None
            for pt in priority:
                chosen = next(
                    (d for d in details if (pick(d, price_type_key) or "").lower() == pt), None
                )
                if chosen is not None:
                    break
            if chosen is None:
                continue
            try:
                price = float(pick(chosen, price_key))
            except (TypeError, ValueError):
                continue
            price_map[sku] = {"price": price, "currency": (pick(chosen, currency_key) or "").upper()}
        if not paginate:
            break
        if int(dig(payload, paginate.get("pages_left_path") or ["value", "pagesLeft"]) or 0) <= 0:
            break
        page += 1

    _validate_minimum(step, orderable, blocked, price_map)


def _validate_minimum(step: dict, orderable: list[dict], blocked: list[dict], price_map: dict[str, dict]) -> None:
    minimum = float(step.get("minimum") or 0)
    want_currency = (step.get("currency") or "").upper()
    missing: list[str] = []
    currencies: set[str] = set()
    total = 0.0
    for it in orderable:
        p = price_map.get(it["sku"])
        if not p:
            missing.append(it["sku"])
            continue
        it["unit_price"] = p["price"]
        it["currency"] = p["currency"]
        it["line_total"] = round(p["price"] * it["quantity"], 2)
        if p["currency"]:
            currencies.add(p["currency"])
        total += p["price"] * it["quantity"]
    total = round(total, 2)

    if missing:
        raise OrderError(
            f"Could not retrieve purchasing price for {len(missing)} SKU(s); aborting to avoid "
            f"undercutting the minimum order value.",
            status=409,
            details=[{"sku": s, "reason": "No price returned by the vendor."} for s in missing],
            extra={"order_total": total},
        )
    if want_currency and currencies and currencies != {want_currency}:
        raise OrderError(
            f"Vendor returned prices in unexpected currencies {sorted(currencies)} "
            f"(expected {want_currency}). Order aborted.",
            status=409,
            details=[
                {"sku": it["sku"], "currency": it.get("currency"), "unit_price": it.get("unit_price")}
                for it in orderable
            ],
            extra={"order_total": total},
        )
    if minimum and total < minimum:
        cur = want_currency or (next(iter(currencies)) if currencies else "")
        raise OrderError(
            f"Order total {total:.2f} {cur} is below the minimum order value of "
            f"{minimum:.0f} {cur}. No order was placed.",
            status=409,
            details=[
                {
                    "sku": it["sku"],
                    "quantity": it["quantity"],
                    "unit_price": it.get("unit_price"),
                    "line_total": it.get("line_total"),
                    "currency": it.get("currency"),
                }
                for it in orderable
            ]
            + [
                {"sku": b.get("sku"), "reason": b.get("reason"), "available": b.get("available")}
                for b in blocked
            ],
            extra={"order_total": total},
        )


def _no_orderable_error(blocked: list[dict]) -> OrderError:
    return OrderError(
        "None of the requested items can be ordered (insufficient stock or unknown SKUs).",
        status=409,
        details=[
            {"sku": b.get("sku"), "reason": b.get("reason"), "available": b.get("available")}
            for b in blocked
        ],
    )


def place_order_rest(vendor: str, vcfg: dict, items: list[dict], columns: list[dict] | None) -> dict:
    api = vcfg.get("api") or {}
    client = RestClient(vendor, api)
    company = cfg.defaults().get("company_name", "")
    order_number = make_order_number(cfg.defaults().get("order_number_prefix", "WT"))

    refs: dict[str, Any] = {"order_number": order_number}
    address: dict | None = None
    orderable: list[dict] = list(items)
    blocked: list[dict] = []

    for step in api.get("flow") or []:
        stype = step.get("type")
        fmt = format_context(order_number, company, refs.get("address_id"))
        if stype == "fetch_address":
            address, address_id = _step_fetch_address(client, step, fmt)
            refs["address_id"] = address_id
        elif stype == "stock_filter":
            orderable, blk = _step_stock_filter(client, step, orderable, fmt)
            blocked.extend(blk)
            if not orderable:
                raise _no_orderable_error(blocked)
        elif stype == "price_minimum":
            _step_price_minimum(client, step, orderable, blocked, fmt)
        else:
            raise OrderError(f"Unknown order flow step '{stype}' for {vendor}.", status=500)

    po = api.get("place_order") or {}
    fmt = format_context(order_number, company, refs.get("address_id"))
    body = resolve_value(po.get("json") or {}, {"fmt": fmt, "refs": refs, "items": orderable})
    client.request(po.get("method", "POST"), po["path"], json_body=body)

    return {
        "vendor": vendor,
        "method": "api",
        "order_number": order_number,
        "address": address,
        "ordered": orderable,
        "not_ordered": blocked,
    }


# ── GraphQL transport ───────────────────────────────────────────────────────────


class GraphQLClient:
    def __init__(self, vendor: str, api: dict):
        self.vendor = vendor
        self.api = api
        self.endpoint = (
            os.environ.get(api.get("base_url_env") or "") or api.get("base_url") or ""
        ).rstrip("/")
        self.timeout = api.get("timeout", 60)
        self.auth = api.get("auth") or {}

    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        atype = self.auth.get("type", "none")
        if atype == "bearer":
            token = os.environ.get(self.auth.get("token_env") or "", "")
            headers[self.auth.get("header", "Authorization")] = f"{self.auth.get('scheme', 'Bearer')} {token}".strip()
        elif atype == "header":
            ctx = {"fmt": {}, "refs": {}, "items": []}
            for key, val in (self.auth.get("headers") or {}).items():
                headers[key] = resolve_value(val, ctx)
        return headers

    def _basic(self):
        if self.auth.get("type") == "basic":
            return (
                os.environ.get(self.auth.get("username_env") or "", ""),
                os.environ.get(self.auth.get("password_env") or "", ""),
            )
        return None

    def execute(self, query: str, variables: dict) -> dict:
        resp = requests.post(
            self.endpoint,
            json={"query": query, "variables": variables},
            headers=self._headers(),
            auth=self._basic(),
            timeout=self.timeout,
        )
        try:
            payload = resp.json()
        except ValueError:
            raise OrderError(
                f"{self.vendor} GraphQL returned non-JSON (HTTP {resp.status_code}): {resp.text[:200]}",
                status=resp.status_code,
            )
        if payload.get("errors"):
            msgs = "; ".join(e.get("message", "") for e in payload["errors"] if isinstance(e, dict))
            raise OrderError(
                f"{self.vendor} GraphQL error: {msgs}",
                status=resp.status_code if resp.status_code >= 400 else 502,
                details=payload["errors"],
            )
        if resp.status_code >= 400:
            raise OrderError(f"{self.vendor} GraphQL HTTP {resp.status_code}: {resp.text[:200]}", status=resp.status_code)
        return payload.get("data") or {}


def place_order_graphql(vendor: str, vcfg: dict, items: list[dict], columns: list[dict] | None) -> dict:
    api = vcfg.get("api") or {}
    client = GraphQLClient(vendor, api)
    company = cfg.defaults().get("company_name", "")
    order_number = make_order_number(cfg.defaults().get("order_number_prefix", "WT"))

    orderable: list[dict] = list(items)
    blocked: list[dict] = []

    for step in api.get("flow") or []:
        stype = step.get("type")
        fmt = format_context(order_number, company)
        if stype == "stock_filter":
            resp = step.get("response") or {}
            variables = resolve_value(step.get("variables") or {}, {"fmt": fmt, "refs": {}, "items": orderable})
            data = client.execute(step["query"], variables)
            rows = dig(data, resp.get("rows_path") or []) or []
            stock_map: dict[str, float] = {}
            for row in rows:
                sku = pick(row, resp.get("sku_key", "sku"))
                qty = pick(row, resp.get("qty_key", "quantity"))
                if sku is None or qty is None:
                    continue
                try:
                    stock_map[sku] = float(qty)
                except (TypeError, ValueError):
                    continue
            orderable, blk = split_by_stock(orderable, stock_map)
            blocked.extend(blk)
            if not orderable:
                raise _no_orderable_error(blocked)
        else:
            raise OrderError(f"Unsupported GraphQL flow step '{stype}' for {vendor}.", status=500)

    po = api.get("place_order") or {}
    fmt = format_context(order_number, company)
    variables = resolve_value(po.get("variables") or {}, {"fmt": fmt, "refs": {"order_number": order_number}, "items": orderable})
    client.execute(po["mutation"], variables)

    return {
        "vendor": vendor,
        "method": "api",
        "order_number": order_number,
        "address": None,
        "ordered": orderable,
        "not_ordered": blocked,
    }


# ── dispatch ─────────────────────────────────────────────────────────────────


def place_order(vendor: str, vcfg: dict, items: list[dict], columns: list[dict] | None) -> dict:
    api = vcfg.get("api") or {}
    api_type = (api.get("type") or "openapi").lower()
    if api_type in ("openapi", "rest", "swagger", "openapi3"):
        return place_order_rest(vendor, vcfg, items, columns)
    if api_type == "graphql":
        return place_order_graphql(vendor, vcfg, items, columns)
    raise OrderError(f"Unknown API type '{api_type}' for {vendor}.", status=500)
