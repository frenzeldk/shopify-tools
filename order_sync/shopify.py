"""Helpers to interact with the Shopify GraphQL API.

Diagnostics
-----------
Every step of ``handle_order`` is logged so a missed pause can be traced from
the webhook to the decision. Grep the journal for the order number to get the
whole story::

    journalctl -u order-sync-worker -S -3d | grep 'order=1234'

The log lines are tagged so each known failure mode is identifiable on its own:

    ``decision=``          the outcome of a job (in-stock / paused / failed).
    ``NEWLY-CAUGHT``       the order is short once other orders' commitments
                           and repeated line items are accounted for; the old
                           per-line check would have let it through.
    ``NO-INVENTORY``       a line was skipped, so it could never pause anything.
    ``TRUNCATED``          the order has more line items than we fetched.
    ``AMBIGUOUS-ORDER``    the order-number search matched more than one order.
    ``WRONG-ORDER``        the search matched orders, none of them this one.
    ``ORDER-NOT-FOUND``    the order number never resolved, so no check ran.
    ``FAILED``             the job raised; ``retries_left=0`` means the order
                           really was left unpaused.

Environment:
    ORDER_SYNC_LOG_LEVEL         Log level, default INFO. DEBUG adds the raw
                                 Shopify quantity payloads.
    ORDER_SYNC_LOOKUP_ATTEMPTS   Order-number lookups before giving up,
                                 default 4.
    ORDER_SYNC_LOOKUP_BACKOFF    Seconds before the second lookup, doubling
                                 each time after that, default 2.
"""

from __future__ import annotations
import logging
import os
import re
import time
from typing import Dict, Iterable, List

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from shipmondo import pause_order

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """Make sure our diagnostics survive inside the rq worker.

    ``handle_order`` runs in the worker process, not in the Flask app, and rq
    only configures its own loggers. Without this the records below are
    filtered out before they ever reach the journal.
    """
    level = os.environ.get("ORDER_SYNC_LOG_LEVEL", "INFO").upper()
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    logger.setLevel(level)


_configure_logging()

try:  # only present when we run under a worker
    from rq import get_current_job
except ImportError:  # pragma: no cover - direct invocation
    get_current_job = None


def _current_job():
    """Return the rq job we run under, or None outside a worker."""
    if get_current_job is None:
        return None
    try:
        return get_current_job()
    except Exception:  # pragma: no cover - no connection outside a worker
        return None


def _job_id() -> str:
    """Return the id of the rq job we run under, to join worker and web logs."""
    job = _current_job()
    return job.id if job is not None else "-"


def _retries_left() -> str:
    """Return how many rq retries remain, so a final failure is recognisable."""
    job = _current_job()
    remaining = getattr(job, "retries_left", None) if job is not None else None
    return "-" if remaining is None else str(remaining)


SHOPIFY_URL = os.environ.get("SHOPIFY_URL")
SHOPIFY_HEADER = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}

transport = AIOHTTPTransport(url=SHOPIFY_URL, headers=SHOPIFY_HEADER, ssl=True)
gql_client = Client(transport=transport, fetch_schema_from_transport=True)


_ORDER_GID_RE = re.compile(r"^gid://shopify/Order/\d+$")


def _normalize_order_id(order_id: str | int) -> str:
    """Return a GraphQL global ID for the given Shopify order identifier."""

    if isinstance(order_id, int):
        return f"gid://shopify/Order/{order_id}"

    order_id_str = str(order_id).strip()
    if not order_id_str:
        raise ValueError("order_id cannot be empty")

    if order_id_str.isdigit():
        return f"gid://shopify/Order/{order_id_str}"

    if _ORDER_GID_RE.match(order_id_str):
        return order_id_str

    raise ValueError(
        "order_id must be a numeric ID or a Shopify global ID (gid://shopify/Order/...)"
    )


def _add_tag_to_order(order_id: str | int, tag: str) -> List[str]:
    """Add *tag* to the Shopify order identified by *order_id*.

    Args:
        order_id: Numeric ID or GraphQL global ID of the target order.
        tag: The tag to append to the order.

    Returns:
        The list of tags currently assigned to the order after the update.

    Raises:
        ValueError: If *tag* is empty or *order_id* cannot be normalized.
        RuntimeError: If Shopify returns user errors or the transport fails.
    """

    tag_value = tag.strip()
    if not tag_value:
        raise ValueError("tag cannot be empty")

    order_gid = _normalize_order_id(order_id)

    mutation = gql(
        """
        mutation OrderTagAdd($id: ID!, $tags: [String!]!) {
          tagsAdd(id: $id, tags: $tags) {
            node {
              ... on Order {
                id
                tags
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
    )

    variables = {"id": order_gid, "tags": [tag_value]}

    try:
        result = gql_client.execute(mutation, variable_values=variables)
    except TransportQueryError as exc:  # pragma: no cover - network interaction
        raise RuntimeError(f"Failed to add tag to order {order_gid}: {exc}") from exc

    payload = result.get("tagsAdd")
    if not payload:
        raise RuntimeError("Unexpected response structure from Shopify")

    user_errors: Iterable[dict[str, str]] = payload.get("userErrors", [])
    if user_errors:
        formatted_errors = "; ".join(
            f"{err.get('field')}: {err.get('message')}" if
            err.get("field") else err.get("message", "Unknown error")
            for err in user_errors
        )
        raise RuntimeError(f"Failed to add tag to order {order_gid}: {formatted_errors}")

    node = payload.get("node") or {}
    tags: List[str] = node.get("tags", [])
    return tags


_INVENTORY_LOCATION_ID = "gid://shopify/Location/100013703511"

# ``committed`` is what Shopify has already promised to unfulfilled orders. It
# is the difference between stock that exists and stock this order can have.
_QUANTITY_NAMES = [
    "on_hand",
    "committed",
    "reserved",
    "damaged",
    "safety_stock",
    "quality_control",
]


def _fetch_inventory_quantities(inventory_item_id: str) -> Dict[str, int]:
    """Return the raw Shopify quantity breakdown for an inventory item.

    Args:
        inventory_item_id: The GraphQL global ID of the inventory item.

    Raises:
        RuntimeError: When the query fails, or when Shopify has no inventory
            level for the item at the configured location. That second case
            used to surface as an opaque ``TypeError`` that killed the job
            before it could pause anything.
    """
    query = gql(
        """
        query getInventoryLevel($id: ID!) {
            inventoryItem(id: $id) {
                id
                tracked
                inventoryLevel(locationId: "%s") {
                    quantities(names: [%s]) {
                        name
                        quantity
                    }
                }
            }
        }
        """ % (
            _INVENTORY_LOCATION_ID,
            ", ".join(f'"{name}"' for name in _QUANTITY_NAMES),
        )
    )
    try:
        result = gql_client.execute(query, variable_values={"id": inventory_item_id})
    except TransportQueryError as exc:  # pragma: no cover - network interaction
        logger.error(
            "NO-INVENTORY item=%s: Shopify rejected the inventory query: %s",
            inventory_item_id,
            exc,
        )
        raise RuntimeError(
            f"Failed to fetch inventory level for {inventory_item_id}: {exc}"
        ) from exc

    item = result.get("inventoryItem")
    if not item:
        logger.error(
            "NO-INVENTORY item=%s: Shopify returned no inventory item "
            "(deleted, or the token cannot read it)",
            inventory_item_id,
        )
        raise RuntimeError(f"No inventory item {inventory_item_id}")

    level = item.get("inventoryLevel")
    if not level:
        logger.error(
            "NO-INVENTORY item=%s tracked=%s: not stocked at location %s, so it has "
            "no quantities to check",
            inventory_item_id,
            item.get("tracked"),
            _INVENTORY_LOCATION_ID,
        )
        raise RuntimeError(
            f"Inventory item {inventory_item_id} has no level at {_INVENTORY_LOCATION_ID}"
        )

    levels = {entry["name"]: entry["quantity"] for entry in level.get("quantities", [])}
    levels["tracked"] = bool(item.get("tracked"))
    logger.debug("item=%s levels=%s", inventory_item_id, levels)
    return levels


def _physical_stock(levels: Dict[str, int]) -> int:
    """Return the sellable stock at the location, before anyone's commitments.

    On-hand less the quantities Shopify holds back as not sellable. This is what
    the check used to compare against on its own, which is why stock already
    promised to other orders still counted as free.
    """
    return levels.get("on_hand", 0) - levels.get("reserved", 0)\
        - levels.get("damaged", 0) - levels.get("quality_control", 0)\
        - levels.get("safety_stock", 0)


def _free_for_order(levels: Dict[str, int], needed: int) -> int:
    """Return the stock this order can actually draw on.

    Shopify leaves an order's units in ``on_hand`` until it ships and counts
    them in ``committed`` from the moment it is placed, so on-hand alone says
    nothing about whether anything is left for us. What matters is the stock
    committed to *other* orders: everything committed, less our own share.

    Our own share is capped out of ``committed`` rather than subtracted blindly.
    An order Shopify has not committed yet — this webhook can arrive first —
    then reads as leniently as it did before, instead of being paused over stock
    that is in fact its own.
    """
    committed_elsewhere = max(0, levels.get("committed", 0) - needed)
    return _physical_stock(levels) - committed_elsewhere


def _check_availability(order_id: str | int) -> bool:
    """Check if all items in the order can be fulfilled from available stock.

    Args:
        order_id: Numeric ID or GraphQL global ID of the target order.
        """

    order_gid = _normalize_order_id(order_id)

    query = gql(
        """
        query Order($id: ID!) {
          order(id: $id) {
            id
            name
            displayFulfillmentStatus
            lineItems(first: 100) {
              pageInfo {
                hasNextPage
              }
              edges {
                node {
                  title
                  sku
                  currentQuantity
                  variant {
                    id
                    inventoryItem {
                      id
                    }
                  }
                }
              }
            }
          }
        }
        """
    )

    variables = {"id": order_gid}

    try:
        result = gql_client.execute(query, variable_values=variables)
    except TransportQueryError as exc:  # pragma: no cover - network interaction
        raise RuntimeError(f"Failed to fetch order {order_gid}: {exc}") from exc

    order = result.get("order")
    if not order:
        raise RuntimeError(f"Order {order_gid} not found")

    connection = order.get("lineItems", {})
    line_items = connection.get("edges", [])
    if connection.get("pageInfo", {}).get("hasNextPage"):
        logger.warning(
            "TRUNCATED order=%s: more than %d line items, the rest were never checked",
            order.get("name"),
            len(line_items),
        )
    if not line_items:
        logger.warning(
            "order=%s (%s) has no line items; treating it as fulfillable",
            order.get("name"),
            order_gid,
        )

    logger.info(
        "Checking order=%s (%s) status=%s lines=%d",
        order.get("name"),
        order_gid,
        order.get("displayFulfillmentStatus"),
        len(line_items),
    )

    # Total the order up per inventory item before looking at stock. The same
    # variant can sit on several lines — bundles, or the same product added
    # twice — and a line checked on its own passes against the whole pool, so
    # two lines for the last unit both used to succeed.
    needed: Dict[str, int] = {}
    biggest_line: Dict[str, int] = {}
    lines_for: Dict[str, int] = {}

    for position, item in enumerate(line_items, start=1):
        node = item.get("node", {})
        title = node.get("title")
        wanted = node.get("currentQuantity", 0)
        variant = node.get("variant")
        if not variant:
            logger.warning(
                "NO-INVENTORY order=%s line=%d title=%r sku=%s qty=%s: no variant "
                "(custom or deleted product), skipped — this line can never pause "
                "the order",
                order.get("name"), position, title, node.get("sku"), wanted,
            )
            continue  # Skip items without a variant (e.g., custom/deleted items)
        inventory_item_id = variant.get("inventoryItem", {}).get("id")
        if not inventory_item_id:
            logger.warning(
                "NO-INVENTORY order=%s line=%d title=%r sku=%s qty=%s variant=%s: no "
                "inventory item, skipped — this line can never pause the order",
                order.get("name"), position, title, node.get("sku"), wanted,
                variant.get("id"),
            )
            continue  # No inventory item to check against

        needed[inventory_item_id] = needed.get(inventory_item_id, 0) + wanted
        biggest_line[inventory_item_id] = max(
            biggest_line.get(inventory_item_id, 0), wanted
        )
        lines_for[inventory_item_id] = lines_for.get(inventory_item_id, 0) + 1
        logger.info(
            "order=%s line=%d title=%r sku=%s item=%s qty=%s",
            order.get("name"), position, title, node.get("sku"),
            inventory_item_id, wanted,
        )

    fulfillable = True
    for inventory_item_id, wanted in needed.items():
        levels = _fetch_inventory_quantities(inventory_item_id)
        stock = _physical_stock(levels)
        free = _free_for_order(levels, wanted)
        item_ok = free >= wanted
        logger.info(
            "order=%s item=%s tracked=%s lines=%d needed=%s "
            "on_hand=%s committed=%s reserved=%s damaged=%s safety=%s qc=%s "
            "stock=%s free=%s ok=%s",
            order.get("name"), inventory_item_id, levels.get("tracked"),
            lines_for[inventory_item_id], wanted,
            levels.get("on_hand"), levels.get("committed"), levels.get("reserved"),
            levels.get("damaged"), levels.get("safety_stock"),
            levels.get("quality_control"),
            stock, free, item_ok,
        )

        if not item_ok:
            fulfillable = False
            # The old check passed a line whenever physical stock covered that
            # line alone, so anything it would have waved through is worth
            # calling out while the new maths beds in.
            if stock >= biggest_line[inventory_item_id]:
                reasons = []
                if stock >= wanted:
                    reasons.append(
                        f"{levels.get('committed')} is committed to other orders"
                    )
                if lines_for[inventory_item_id] > 1 and stock < wanted:
                    reasons.append(
                        f"{lines_for[inventory_item_id]} lines want {wanted} between them"
                    )
                logger.warning(
                    "NEWLY-CAUGHT order=%s item=%s: needs %s, only %s free (%s) — "
                    "the previous per-line check would have let this order through",
                    order.get("name"), inventory_item_id, wanted, free,
                    " and ".join(reasons) or "aggregated across lines",
                )

    logger.info(
        "order=%s (%s) fulfillable=%s across %d distinct inventory items",
        order.get("name"), order_gid, fulfillable, len(needed),
    )
    return fulfillable

# ``orders(query:)`` reads Shopify's search index, which trails order creation
# by seconds. This webhook fires seconds after the order exists, so the first
# look regularly misses an order that is plainly there — the miss that left
# orders unpaused. Retry over a short window before giving up.
LOOKUP_ATTEMPTS = int(os.environ.get("ORDER_SYNC_LOOKUP_ATTEMPTS", 4))
LOOKUP_BACKOFF = float(os.environ.get("ORDER_SYNC_LOOKUP_BACKOFF", 2))

_ORDER_BY_NAME_QUERY = gql(
    """
    query GetOrderByName($name: String!) {
      orders(first: 5, query: $name) {
        edges {
          node {
            id
            name
            createdAt
            displayFulfillmentStatus
          }
        }
      }
    }
    """
)


def _search_orders_by_name(handle: int) -> List[dict]:
    """Return the order nodes Shopify's search index holds for this order number."""
    try:
        result = gql_client.execute(
            _ORDER_BY_NAME_QUERY, variable_values={"name": f"name:{handle}"}
        )
    except TransportQueryError as exc:  # pragma: no cover - network interaction
        raise RuntimeError(f"Failed to fetch order with handle {handle}: {exc}") from exc
    return [edge.get("node", {}) for edge in result.get("orders", {}).get("edges", [])]


def _exact_match(handle: int, candidates: List[dict]) -> dict | None:
    """Return the candidate whose name is exactly this order number.

    ``name:`` is a partial match, so a search for one order number can return
    several orders. Pick by the same join ``reconcile.py`` uses — the order name
    without its leading ``#`` — rather than trusting the first hit, which is
    ordered by Shopify's default sort and can be an unrelated older order.
    """
    wanted = str(handle).lstrip("#")
    matches = [node for node in candidates if (node.get("name") or "").lstrip("#") == wanted]
    if len(matches) > 1:
        logger.warning(
            "AMBIGUOUS-ORDER handle=%s matched %d orders with the same name: %s — "
            "taking the first",
            handle, len(matches), [node.get("id") for node in matches],
        )
    return matches[0] if matches else None


def _get_shopify_id_from_handle(handle: int) -> str:
    """Fetch the Shopify order ID from its handle."""
    for attempt in range(1, LOOKUP_ATTEMPTS + 1):
        candidates = _search_orders_by_name(handle)
        order_node = _exact_match(handle, candidates)
        if order_node is not None:
            if attempt > 1:
                logger.info(
                    "handle=%s appeared on attempt %d/%d — the first look raced "
                    "Shopify's search index",
                    handle, attempt, LOOKUP_ATTEMPTS,
                )
            break
        if candidates:
            logger.warning(
                "WRONG-ORDER handle=%s attempt=%d/%d: search %r matched %s but none "
                "is the order we were asked about",
                handle, attempt, LOOKUP_ATTEMPTS, f"name:{handle}",
                [node.get("name") for node in candidates],
            )
        if attempt < LOOKUP_ATTEMPTS:
            delay = LOOKUP_BACKOFF * (2 ** (attempt - 1))
            logger.info(
                "handle=%s unresolved on attempt %d/%d, retrying in %.1fs",
                handle, attempt, LOOKUP_ATTEMPTS, delay,
            )
            time.sleep(delay)
    else:
        logger.error(
            "ORDER-NOT-FOUND handle=%s: search %r never resolved to this order in "
            "%d attempts, so no stock check ran",
            handle, f"name:{handle}", LOOKUP_ATTEMPTS,
        )
        raise RuntimeError(f"Order with handle {handle} not found")

    shopify_id = order_node.get("id")
    if not shopify_id:
        raise RuntimeError(f"Order with handle {handle} has no ID")

    logger.info(
        "handle=%s resolved to order=%s (%s) created=%s status=%s",
        handle, order_node.get("name"), shopify_id, order_node.get("createdAt"),
        order_node.get("displayFulfillmentStatus"),
    )
    return shopify_id

def handle_order(shipmondo_id: int, handle: int) -> None:
    """Handle the order by checking inventory and pausing if needed."""
    job = _job_id()
    logger.info("job=%s START shipmondo=%s order=%s", job, shipmondo_id, handle)
    try:
        shopify_id = _get_shopify_id_from_handle(handle)
        if _check_availability(shopify_id):
            logger.info(
                "job=%s DONE shipmondo=%s order=%s decision=in-stock",
                job, shipmondo_id, handle,
            )
            return
        _pause_order(shopify_id, shipmondo_id)
        logger.info(
            "job=%s DONE shipmondo=%s order=%s decision=paused",
            job, shipmondo_id, handle,
        )
    except Exception:
        # rq files the traceback in the failed registry where nobody reads it;
        # a missed pause has to be visible in the journal. retries_left=0 is the
        # one that actually left the order unpaused.
        logger.exception(
            "job=%s FAILED shipmondo=%s order=%s retries_left=%s decision=none — "
            "the order was NOT paused",
            job, shipmondo_id, handle, _retries_left(),
        )
        raise

def _pause_order(shopify_id: str | int, shipmondo_id: int) -> None:
    """Pause the order by adding a "paused" tag and pausing it
    in shipmondo."""
    logger.info("Pausing order=%s shipmondo=%s", shopify_id, shipmondo_id)
    try:
        tags = _add_tag_to_order(shopify_id, "paused")
    except Exception:
        logger.exception(
            "PAUSE-FAILED order=%s: could not add the paused tag, Shipmondo order "
            "%s was left open",
            shopify_id, shipmondo_id,
        )
        raise
    logger.info("Tagged order=%s as paused, tags are now %s", shopify_id, tags)

    try:
        pause_order(shipmondo_id)
    except Exception:
        logger.exception(
            "PAUSE-FAILED order=%s: tagged in Shopify but Shipmondo order %s could "
            "not be put on hold — the two systems now disagree",
            shopify_id, shipmondo_id,
        )
        raise
    logger.info("Paused Shipmondo order=%s for Shopify order=%s", shipmondo_id, shopify_id)
