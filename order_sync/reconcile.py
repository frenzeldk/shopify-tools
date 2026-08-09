#!/opt/shopify-python/bin/python3

"""Reconcile order state (paused / active / fulfilled) between Shopify and Shipmondo.

Walks every unfulfilled (or partially fulfilled) Shopify order from the last 90
days where payment has not been voided and the order total is greater than zero,
oldest first, and runs three checks per order:

  1. Return order   - log whether Shipmondo holds a return fulfillment.
  2. Fulfilled       - if Shipmondo reports the order (partly) fulfilled, mirror
                       that into Shopify (no customer notification) and capture
                       the matching payment.
  3. Stock           - resume the order (Shipmondo open + remove ``paused`` tag)
                       when the remaining items are in stock, otherwise pause it
                       (Shipmondo on_hold + ``paused`` tag). A local stock cache
                       is kept, mirroring ``resume.py``.

Every order produces a row in a global log that is written to a CSV at the end.

Use ``--dry-run`` to produce the report without changing anything in either
system.

NOTE: the fulfillment step (check 2) requires the Shopify app to hold the
``read_merchant_managed_fulfillment_orders`` / ``write_merchant_managed_fulfillment_orders``
(and/or the ``*_assigned_fulfillment_orders``) access scopes. Without them the
``fulfillmentOrders`` field and ``fulfillmentCreate`` mutation are denied; the
app detects this, logs it per order and continues with the other checks.
"""
from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime, timedelta

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError

import shipmondo

SHOPIFY_URL = os.environ.get("SHOPIFY_URL")
SHOPIFY_HEADER = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}

transport = AIOHTTPTransport(url=SHOPIFY_URL, headers=SHOPIFY_HEADER, ssl=True)
gql_client = Client(transport=transport, fetch_schema_from_transport=True)

_INVENTORY_LOCATION_ID = "gid://shopify/Location/100013703511"
PAUSED_TAGS = ["paused", "Mangler Varer"]

# Local stock cache shared across the run (mirrors resume.py).
_inventory_cache: dict[str, int] = {}


# --------------------------------------------------------------------------- #
# Shopify - fetching orders
# --------------------------------------------------------------------------- #
def get_orders(days: int) -> list[dict]:
    """Fetch unfulfilled / partial open orders created within the last *days*.

    Orders are returned oldest first. Voided orders are excluded by the query;
    orders with a total of zero are filtered out here.

    Args:
        days: How many days back to look.

    Returns:
        List of order nodes as returned by the Shopify GraphQL API.
    """
    created_after = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    query = gql(
        """
        query getOpenOrders($query: String!, $cursor: String) {
            orders(first: 100, query: $query, after: $cursor,
                   sortKey: CREATED_AT, reverse: false) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        name
                        createdAt
                        tags
                        displayFinancialStatus
                        currentTotalPriceSet { shopMoney { amount currencyCode } }
                        totalCapturableSet { shopMoney { amount currencyCode } }
                        totalReceivedSet { shopMoney { amount currencyCode } }
                        transactions(first: 20) {
                            id
                            kind
                            status
                            parentTransaction { id }
                        }
                        lineItems(first: 100) {
                            edges {
                                node {
                                    id
                                    sku
                                    name
                                    currentQuantity
                                    variant {
                                        inventoryItem { id }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
    )
    search = (
        f"created_at:>={created_after} test:false -financial_status:voided "
        "(fulfillment_status:unfulfilled OR fulfillment_status:partial) status:open"
    )
    all_orders: list[dict] = []
    cursor = None
    while True:
        try:
            result = gql_client.execute(
                query, variable_values={"query": search, "cursor": cursor}
            )
        except TransportQueryError as exc:
            raise RuntimeError(f"Failed to fetch orders: {exc}") from exc
        connection = result["orders"]
        for edge in connection["edges"]:
            node = edge["node"]
            total = float(node["currentTotalPriceSet"]["shopMoney"]["amount"])
            if total > 0:
                all_orders.append(node)
        page_info = connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return all_orders


# --------------------------------------------------------------------------- #
# Shopify - inventory cache (mirrors resume.py)
# --------------------------------------------------------------------------- #
def _get_inventory_level(inventory_item_id: str) -> int:
    """Return the actual available quantity for the given inventory item ID."""
    query = gql(
        """
        query getInventoryLevel($id: ID!) {
            inventoryItem(id: $id) {
                id
                inventoryLevel(locationId: "%s") {
                    quantities(names: ["on_hand", "reserved", "damaged",
                    "safety_stock", "quality_control"]) {
                        name
                        quantity
                    }
                }
            }
        }
        """ % _INVENTORY_LOCATION_ID
    )
    result = gql_client.execute(query, variable_values={"id": inventory_item_id})
    levels = result["inventoryItem"]["inventoryLevel"]["quantities"]
    levels = {item["name"]: item["quantity"] for item in levels}
    return levels.get("on_hand", 0) - levels.get("reserved", 0) \
        - levels.get("damaged", 0) - levels.get("quality_control", 0) \
        - levels.get("safety_stock", 0)


def _reserve_stock(remaining: list[tuple[str, int]]) -> bool:
    """Reserve the remaining quantities against the local stock cache.

    Decreases the cached availability for each inventory item by the requested
    quantity (filling the cache from Shopify on first sight) and reports whether
    every item still has a non-negative balance afterwards.

    Args:
        remaining: ``(inventory_item_id, quantity)`` pairs to reserve.

    Returns:
        True when all items could be reserved without going negative.
    """
    for inventory_item_id, quantity in remaining:
        if inventory_item_id not in _inventory_cache:
            _inventory_cache[inventory_item_id] = _get_inventory_level(inventory_item_id)
        _inventory_cache[inventory_item_id] -= quantity
    return all(_inventory_cache[iid] >= 0 for iid, _ in remaining)


# --------------------------------------------------------------------------- #
# Shopify - tags
# --------------------------------------------------------------------------- #
def _tags_add(order_id: str, tags: list[str]) -> None:
    mutation = gql(
        """
        mutation tagsAdd($id: ID!, $tags: [String!]!) {
            tagsAdd(id: $id, tags: $tags) {
                userErrors { field message }
            }
        }
        """
    )
    result = gql_client.execute(mutation, variable_values={"id": order_id, "tags": tags})
    errors = result["tagsAdd"]["userErrors"]
    if errors:
        raise RuntimeError(f"tagsAdd failed for {order_id}: {errors}")


def _tags_remove(order_id: str, tags: list[str]) -> None:
    mutation = gql(
        """
        mutation tagsRemove($id: ID!, $tags: [String!]!) {
            tagsRemove(id: $id, tags: $tags) {
                userErrors { field message }
            }
        }
        """
    )
    result = gql_client.execute(mutation, variable_values={"id": order_id, "tags": tags})
    errors = result["tagsRemove"]["userErrors"]
    if errors:
        raise RuntimeError(f"tagsRemove failed for {order_id}: {errors}")


# --------------------------------------------------------------------------- #
# Shopify - fulfillment + capture
# --------------------------------------------------------------------------- #
def _get_fulfillment_state(order_id: str) -> tuple[list[dict], dict[str, float]]:
    """Return the order's fulfillment orders and what it has already fulfilled.

    Args:
        order_id: The Shopify order global ID.

    Returns:
        ``(fulfillment_orders, already_fulfilled_by_sku)`` where the second
        element sums the quantities of every non-cancelled Shopify fulfillment
        per SKU.

    Raises:
        RuntimeError: wrapping a Shopify error (e.g. a missing access scope).
    """
    query = gql(
        """
        query fulfillmentState($id: ID!) {
            order(id: $id) {
                fulfillments(first: 50) {
                    status
                    fulfillmentLineItems(first: 100) {
                        edges {
                            node {
                                quantity
                                lineItem { sku }
                            }
                        }
                    }
                }
                fulfillmentOrders(first: 20) {
                    edges {
                        node {
                            id
                            status
                            lineItems(first: 100) {
                                edges {
                                    node {
                                        id
                                        sku
                                        remainingQuantity
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
    )
    try:
        result = gql_client.execute(query, variable_values={"id": order_id})
    except TransportQueryError as exc:
        raise RuntimeError(str(exc)) from exc
    order = result["order"]

    already_fulfilled: dict[str, float] = {}
    for fulfillment in order["fulfillments"]:
        if fulfillment["status"] in ("CANCELLED", "ERROR", "FAILURE"):
            continue
        for edge in fulfillment["fulfillmentLineItems"]["edges"]:
            node = edge["node"]
            sku = node["lineItem"]["sku"]
            already_fulfilled[sku] = already_fulfilled.get(sku, 0.0) + node["quantity"]

    fulfillment_orders = [
        edge["node"] for edge in order["fulfillmentOrders"]["edges"]
    ]
    return fulfillment_orders, already_fulfilled


def _build_fulfillment_input(
    fulfillment_orders: list[dict],
    shipped_by_sku: dict[str, float],
    already_fulfilled_by_sku: dict[str, float],
) -> tuple[list[dict], dict[str, float]]:
    """Build ``lineItemsByFulfillmentOrder`` for ``fulfillmentCreate``.

    Only the units Shipmondo has shipped that Shopify has *not yet* fulfilled
    are scheduled, i.e. ``shipped - already_fulfilled`` per SKU. This avoids
    over-fulfilling when Shopify already holds an earlier (partial) fulfillment.

    Args:
        fulfillment_orders: Fulfillment orders from ``_get_fulfillment_state``.
        shipped_by_sku: Total quantity Shipmondo has shipped per SKU.
        already_fulfilled_by_sku: Quantity Shopify has already fulfilled per SKU.

    Returns:
        ``(groups, fulfilled_now_by_sku)`` - the line-item groups (empty groups
        omitted) and the quantity actually scheduled per SKU.
    """
    pool = {
        sku: max(0.0, qty - already_fulfilled_by_sku.get(sku, 0.0))
        for sku, qty in shipped_by_sku.items()
    }
    fulfilled_now: dict[str, float] = {}
    groups: list[dict] = []
    for order in fulfillment_orders:
        if order["status"] not in ("OPEN", "IN_PROGRESS"):
            continue
        line_inputs = []
        for edge in order["lineItems"]["edges"]:
            item = edge["node"]
            remaining = item["remainingQuantity"]
            if remaining <= 0:
                continue
            available = pool.get(item["sku"], 0)
            quantity = int(min(remaining, available))
            if quantity <= 0:
                continue
            pool[item["sku"]] = available - quantity
            fulfilled_now[item["sku"]] = fulfilled_now.get(item["sku"], 0.0) + quantity
            line_inputs.append({"id": item["id"], "quantity": quantity})
        if line_inputs:
            groups.append(
                {"fulfillmentOrderId": order["id"], "fulfillmentOrderLineItems": line_inputs}
            )
    return groups, fulfilled_now


def _create_fulfillment(groups: list[dict]) -> None:
    mutation = gql(
        """
        mutation fulfill($f: FulfillmentInput!) {
            fulfillmentCreate(fulfillment: $f) {
                fulfillment { id status }
                userErrors { field message }
            }
        }
        """
    )
    variables = {"f": {"notifyCustomer": False, "lineItemsByFulfillmentOrder": groups}}
    result = gql_client.execute(mutation, variable_values=variables)
    errors = result["fulfillmentCreate"]["userErrors"]
    if errors:
        raise RuntimeError(f"fulfillmentCreate failed: {errors}")


def _capture_payment(order: dict, amount: float) -> None:
    """Capture *amount* against the order's authorization transaction.

    ``finalCapture`` is intentionally omitted: some gateways (e.g. PensoPay /
    MobilePay) reject it, and leaving it unset lets the gateway decide.
    """
    auth = next(
        (
            t for t in order["transactions"]
            if t["kind"] == "AUTHORIZATION" and t["status"] == "SUCCESS"
        ),
        None,
    )
    if auth is None:
        raise RuntimeError("no successful authorization transaction to capture against")
    currency = order["currentTotalPriceSet"]["shopMoney"]["currencyCode"]
    mutation = gql(
        """
        mutation capture($input: OrderCaptureInput!) {
            orderCapture(input: $input) {
                transaction { id status }
                userErrors { field message }
            }
        }
        """
    )
    variables = {
        "input": {
            "id": order["id"],
            "parentTransactionId": auth["id"],
            "amount": f"{amount:.2f}",
            "currency": currency,
        }
    }
    result = gql_client.execute(mutation, variable_values=variables)
    errors = result["orderCapture"]["userErrors"]
    if errors:
        raise RuntimeError(f"orderCapture failed: {errors}")


# --------------------------------------------------------------------------- #
# Shipmondo helpers
# --------------------------------------------------------------------------- #
def _has_return(sales_order: dict) -> bool:
    """Return True when the Shipmondo order has an active return fulfillment."""
    for fulfillment in sales_order.get("order_fulfillments", []):
        if fulfillment.get("type") == "return" and not fulfillment.get("cancelled"):
            return True
    return False


def _shipped_by_sku(sales_order: dict) -> tuple[dict[str, float], dict[str, float]]:
    """Compute total outbound shipped quantities and VAT-inclusive unit prices per SKU.

    Only item lines (not shipping) shipped by non-cancelled outbound
    fulfillments are counted. The quantities are the *total* Shipmondo has
    shipped for the order, not what Shopify has yet to fulfil.

    Returns:
        ``(quantities_by_sku, unit_price_incl_vat_by_sku)``.
    """
    lines = {line["id"]: line for line in sales_order.get("order_lines", [])}
    shipped: dict[str, float] = {}
    unit_price: dict[str, float] = {}
    for fulfillment in sales_order.get("order_fulfillments", []):
        if fulfillment.get("type") != "outbound" or fulfillment.get("cancelled"):
            continue
        for fline in fulfillment.get("fulfillment_lines", []):
            line = lines.get(fline["order_line_id"])
            if not line or line.get("line_type") != "item":
                continue
            qty = float(fline.get("shipped_quantity") or 0)
            if qty <= 0:
                continue
            sku = line.get("item_sku")
            shipped[sku] = shipped.get(sku, 0.0) + qty
            ordered_qty = float(line.get("quantity") or 0) or 1.0
            unit_price[sku] = float(line.get("amount_including_vat") or 0) / ordered_qty
    return shipped, unit_price


# --------------------------------------------------------------------------- #
# Per-order processing
# --------------------------------------------------------------------------- #
def _remaining_line_items(order: dict, shipped_by_sku: dict[str, float]) -> list[tuple[str, int]]:
    """Return ``(inventory_item_id, qty)`` still needing stock after shipped units.

    Items without a variant (deleted) are skipped, as in ``resume.py``.
    """
    remaining = []
    consumed = dict(shipped_by_sku)
    for edge in order["lineItems"]["edges"]:
        item = edge["node"]
        variant = item.get("variant")
        if not variant:
            continue
        inventory_item_id = variant["inventoryItem"]["id"]
        qty = item["currentQuantity"]
        already = consumed.get(item["sku"], 0)
        take = min(qty, already)
        consumed[item["sku"]] = already - take
        qty -= int(take)
        if qty > 0:
            remaining.append((inventory_item_id, qty))
    return remaining


def process_order(order: dict, sales_order: dict | None, dry_run: bool) -> dict:
    """Run all reconciliation checks for a single order and return a log row.

    Args:
        order: The Shopify order node.
        sales_order: The matching Shipmondo sales order, or ``None`` if absent.
        dry_run: When True, report intended actions without making changes.
    """
    name = order["name"]
    total = order["currentTotalPriceSet"]["shopMoney"]["amount"]
    currency = order["currentTotalPriceSet"]["shopMoney"]["currencyCode"]
    capturable = float(order.get("totalCapturableSet", {}).get("shopMoney", {}).get("amount", 0))
    already_captured = float(order.get("totalReceivedSet", {}).get("shopMoney", {}).get("amount", 0))

    row = {
        "order_number": name,
        "created_at": order["createdAt"],
        "total": total,
        "currency": currency,
        "financial_status": order["displayFinancialStatus"],
        "shopify_tags": ", ".join(order["tags"]),
        "shipmondo_status": "",
        "shipmondo_fulfillment_status": "",
        "has_return": "",
        "actions": "",
        "capture_amount": "",
        "stock_ok": "",
        "notes": "",
    }
    actions: list[str] = []
    notes: list[str] = []

    if sales_order is None:
        row["notes"] = "no matching Shipmondo sales order"
        return row

    row["shipmondo_status"] = sales_order.get("order_status", "")
    fulfillment_status = sales_order.get("fulfillment_status", "")
    row["shipmondo_fulfillment_status"] = fulfillment_status

    # --- Check 1: return order -------------------------------------------- #
    has_return = _has_return(sales_order)
    row["has_return"] = "yes" if has_return else "no"
    if has_return:
        notes.append("contains return order")

    # --- Check 2: fulfilled in Shipmondo ---------------------------------- #
    shipped_by_sku: dict[str, float] = {}
    fully_fulfilled = fulfillment_status == "fulfilled"
    partially_fulfilled = fulfillment_status == "partially_fulfilled"
    if fully_fulfilled or partially_fulfilled:
        shipped_by_sku, unit_price_by_sku = _shipped_by_sku(sales_order)
        try:
            fulfillment_orders, already_fulfilled = _get_fulfillment_state(order["id"])
        except RuntimeError as exc:
            notes.append(f"fulfillment skipped (Shopify access): {exc}")
            fulfillment_orders = None

        if fulfillment_orders is not None:
            groups, _fulfilled_now = _build_fulfillment_input(
                fulfillment_orders, shipped_by_sku, already_fulfilled
            )
            if groups:
                if dry_run:
                    actions.append(
                        "would fully fulfill" if fully_fulfilled else "would partially fulfill"
                    )
                else:
                    _create_fulfillment(groups)
                    actions.append("fulfilled fully" if fully_fulfilled else "fulfilled partially")
            else:
                notes.append("nothing left to fulfill in Shopify")

            # Capture is only needed for PARTIAL fulfilments: Shopify
            # auto-captures when an order becomes completely fulfilled, so a full
            # fulfilment needs no manual capture. For a partial fulfilment we top
            # the captured amount up to the value of the shipped items, which also
            # catches an existing partial fulfilment that was never captured.
            if partially_fulfilled:
                shipped_value = round(
                    sum(qty * unit_price_by_sku.get(sku, 0.0)
                        for sku, qty in shipped_by_sku.items()),
                    2,
                )
                capture_amount = min(round(shipped_value - already_captured, 2),
                                     round(capturable, 2))
                if capture_amount > 0:
                    row["capture_amount"] = f"{capture_amount:.2f}"
                    if dry_run:
                        actions.append(f"would capture {capture_amount:.2f} {currency}")
                    else:
                        try:
                            _capture_payment(order, capture_amount)
                            actions.append(f"captured {capture_amount:.2f} {currency}")
                        except RuntimeError as exc:
                            notes.append(f"capture failed: {exc}")

    if fully_fulfilled:
        row["actions"] = "; ".join(actions)
        row["notes"] = "; ".join(notes)
        return row  # Order complete, no stock reconciliation needed.

    # --- Check 3: stock availability / pause-resume ----------------------- #
    remaining = _remaining_line_items(order, shipped_by_sku)
    can_fulfill = _reserve_stock(remaining) if remaining else True
    row["stock_ok"] = "yes" if can_fulfill else "no"

    is_paused = any(tag in order["tags"] for tag in PAUSED_TAGS)
    on_hold = sales_order.get("order_status") == "on_hold"

    if can_fulfill:
        if is_paused:
            if dry_run:
                actions.append("would remove paused tag")
            else:
                _tags_remove(order["id"], PAUSED_TAGS)
                actions.append("removed paused tag")
        if on_hold:
            if dry_run:
                actions.append("would resume in Shipmondo")
            else:
                if shipmondo.set_order_status(sales_order["id"], "open"):
                    actions.append("resumed in Shipmondo")
                else:
                    notes.append("Shipmondo resume failed")
        if not is_paused and not on_hold:
            actions.append("already active")
    else:
        if not is_paused:
            if dry_run:
                actions.append("would add paused tag")
            else:
                _tags_add(order["id"], ["paused"])
                actions.append("added paused tag")
        if not on_hold:
            if dry_run:
                actions.append("would pause in Shipmondo")
            else:
                if shipmondo.set_order_status(sales_order["id"], "on_hold"):
                    actions.append("paused in Shipmondo")
                else:
                    notes.append("Shipmondo pause failed")
        if is_paused and on_hold:
            actions.append("already paused")

    row["actions"] = "; ".join(actions)
    row["notes"] = "; ".join(notes)
    return row


# --------------------------------------------------------------------------- #
# CSV export
# --------------------------------------------------------------------------- #
def _print_progress(done: int, total: int, width: int = 40) -> None:
    """Render a single self-updating progress bar line (carriage-return based)."""
    fraction = done / total if total else 1.0
    filled = int(width * fraction)
    bar = "#" * filled + "-" * (width - filled)
    end = "\n" if done >= total else ""
    print(f"\r[{bar}] {done}/{total} ({fraction:5.1%})", end=end, flush=True)


# Group ordering: lower rank sorts to the top. Orders where an action was (or
# would be) taken come first, then orders needing attention, then no-op states.
_GROUP_ORDER = [
    "fulfilled",
    "paused",
    "resumed",
    "action",
    "error",
    "no Shipmondo order",
    "already paused",
    "already active",
    "no action",
]


def _classify(row: dict) -> str:
    """Return the state/action group a log row belongs to."""
    actions = row["actions"]
    notes = row["notes"]
    if notes.startswith("ERROR") or "failed" in notes:
        return "error"
    if actions and actions not in ("already paused", "already active"):
        if "fulfill" in actions or "captur" in actions:
            return "fulfilled"
        if "pause" in actions:
            return "paused"
        if "resume" in actions or "remove" in actions:
            return "resumed"
        return "action"
    if "no matching Shipmondo sales order" in notes:
        return "no Shipmondo order"
    return actions or "no action"


def write_report(rows: list[dict], path: str) -> None:
    """Write the global log to *path* as CSV, grouped by state/action.

    Each row is tagged with its ``group`` and the rows are ordered so that
    orders where an action was taken appear first; within a group the original
    oldest-first ordering is preserved (the sort is stable).
    """
    fieldnames = [
        "group",
        "order_number", "created_at", "total", "currency", "financial_status",
        "shopify_tags", "shipmondo_status", "shipmondo_fulfillment_status",
        "has_return", "actions", "capture_amount", "stock_ok", "notes",
    ]
    rank = {group: index for index, group in enumerate(_GROUP_ORDER)}
    for row in rows:
        row["group"] = _classify(row)
    ordered = sorted(rows, key=lambda r: rank.get(r["group"], len(_GROUP_ORDER)))
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ordered)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Produce the report without changing Shopify or Shipmondo.",
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="How many days back to consider (default: 90).",
    )
    parser.add_argument(
        "--output", default=None,
        help="CSV report path (default: reconcile_report_<timestamp>.csv).",
    )
    args = parser.parse_args()

    output = args.output or f"reconcile_report_{datetime.now():%Y%m%d_%H%M%S}.csv"

    mode = "DRY RUN - " if args.dry_run else ""
    orders = get_orders(args.days)

    # Pull every Shipmondo sales order for the window in one paginated sweep
    # (a few days of slack to cover Shopify/Shipmondo creation-time skew),
    # rather than one API request per order.
    created_at_min = (datetime.now() - timedelta(days=args.days + 3)).strftime(
        "%Y-%m-%dT00:00:00"
    )
    sales_orders = shipmondo.get_sales_orders_since(created_at_min)
    print(f"{mode}Processing {len(orders)} orders (last {args.days} days, oldest first). "
          f"Matched against {len(sales_orders)} Shipmondo sales orders.")

    total = len(orders)
    rows: list[dict] = []
    for index, order in enumerate(orders, start=1):
        sales_order = sales_orders.get(order["name"].lstrip("#"))
        try:
            row = process_order(order, sales_order, args.dry_run)
        except Exception as exc:  # pylint: disable=broad-except
            row = {
                "order_number": order["name"],
                "created_at": order.get("createdAt", ""),
                "total": order["currentTotalPriceSet"]["shopMoney"]["amount"],
                "currency": order["currentTotalPriceSet"]["shopMoney"]["currencyCode"],
                "financial_status": order.get("displayFinancialStatus", ""),
                "shopify_tags": ", ".join(order.get("tags", [])),
                "shipmondo_status": "",
                "shipmondo_fulfillment_status": "",
                "has_return": "",
                "actions": "",
                "capture_amount": "",
                "stock_ok": "",
                "notes": f"ERROR: {exc}",
            }
        rows.append(row)
        _print_progress(index, total)

    write_report(rows, output)

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["group"]] = counts.get(row["group"], 0) + 1
    print(f"{mode}Report written to {output} ({len(rows)} orders).")
    for group in _GROUP_ORDER:
        if counts.get(group):
            print(f"  {group}: {counts[group]}")


if __name__ == "__main__":
    main()
