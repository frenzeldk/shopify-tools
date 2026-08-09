"""Helpers to interact with the Shopify GraphQL API.

Diagnostics
-----------
Every step of ``handle_order`` is logged so a missed pause can be traced from
the webhook to the decision. Grep the journal for the order number to get the
whole story::

    journalctl -u order-sync-worker -S -3d | grep 'order=1234'

The log lines are tagged so each known failure mode is identifiable on its own:

    ``decision=``          the outcome of a job (in-stock / paused / failed).
    ``COMMITTED-BLIND``    the line passed only because stock committed to
                           other unfulfilled orders was counted as available.
    ``DUPLICATE-ITEM``     the same inventory item appears on several lines and
                           was checked against the full pool once per line.
    ``NO-INVENTORY``       a line was skipped, so it could never pause anything.
    ``TRUNCATED``          the order has more line items than we fetched.
    ``AMBIGUOUS-ORDER``    the order-number search matched more than one order.
    ``WRONG-ORDER``        the resolved order's name is not the one requested.
    ``FAILED``             the job raised, so the order was never paused.

Set ``ORDER_SYNC_LOG_LEVEL=DEBUG`` for the raw Shopify quantity payloads.
"""

from __future__ import annotations
import logging
import os
import re
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


def _job_id() -> str:
    """Return the id of the rq job we run under, to join worker and web logs."""
    if get_current_job is None:
        return "-"
    try:
        job = get_current_job()
    except Exception:  # pragma: no cover - no connection outside a worker
        return "-"
    return job.id if job is not None else "-"


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

# ``committed`` is fetched for diagnostics only: it is what Shopify has already
# promised to unfulfilled orders, and the availability formula below ignores it.
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


def _available(levels: Dict[str, int]) -> int:
    """Return the availability figure the pause check currently acts on.

    Note this is *not* Shopify's ``available``: ``committed`` is deliberately
    left out, so stock already promised to other unfulfilled orders still
    counts as free here.
    """
    return levels.get("on_hand", 0) - levels.get("reserved", 0)\
        - levels.get("damaged", 0) - levels.get("quality_control", 0)\
        - levels.get("safety_stock", 0)


def _available_after_commitments(levels: Dict[str, int]) -> int:
    """Return availability with ``committed`` subtracted, for comparison only."""
    return _available(levels) - levels.get("committed", 0)


def _get_inventory_level(inventory_item_id: str) -> int:
    """Return the availability figure used by the pause check.

    Args:
        inventory_item_id: The GraphQL global ID of the inventory item to check.
    """
    return _available(_fetch_inventory_quantities(inventory_item_id))


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

    # Cached per call so a repeated variant costs one lookup and, more
    # importantly, so the running total below sees the same pool each line
    # item was measured against.
    quantities: Dict[str, Dict[str, int]] = {}
    requested: Dict[str, int] = {}
    lines_per_item: Dict[str, int] = {}
    fulfillable = True

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

        if inventory_item_id not in quantities:
            quantities[inventory_item_id] = _fetch_inventory_quantities(inventory_item_id)
        levels = quantities[inventory_item_id]
        available_quantity = _available(levels)
        after_commitments = _available_after_commitments(levels)

        previously_requested = requested.get(inventory_item_id, 0)
        requested[inventory_item_id] = previously_requested + wanted
        lines_per_item[inventory_item_id] = lines_per_item.get(inventory_item_id, 0) + 1

        line_ok = available_quantity >= wanted
        logger.info(
            "order=%s line=%d title=%r sku=%s item=%s tracked=%s qty=%s "
            "on_hand=%s committed=%s reserved=%s damaged=%s safety=%s qc=%s "
            "available=%s available_after_commitments=%s line_ok=%s",
            order.get("name"), position, title, node.get("sku"), inventory_item_id,
            levels.get("tracked"), wanted,
            levels.get("on_hand"), levels.get("committed"), levels.get("reserved"),
            levels.get("damaged"), levels.get("safety_stock"),
            levels.get("quality_control"),
            available_quantity, after_commitments, line_ok,
        )

        if line_ok and after_commitments < wanted:
            logger.warning(
                "COMMITTED-BLIND order=%s line=%d title=%r item=%s: passed on "
                "available=%s but only %s is free once the %s committed to other "
                "unfulfilled orders is taken out",
                order.get("name"), position, title, inventory_item_id,
                available_quantity, after_commitments, levels.get("committed"),
            )

        if previously_requested and available_quantity < requested[inventory_item_id]:
            logger.warning(
                "DUPLICATE-ITEM order=%s line=%d item=%s: %d lines want %s in total "
                "but only %s is available; each line was checked against the full "
                "pool on its own",
                order.get("name"), position, inventory_item_id,
                lines_per_item[inventory_item_id], requested[inventory_item_id],
                available_quantity,
            )

        if not line_ok:
            fulfillable = False  # Not enough inventory to fulfill this line item

    logger.info(
        "order=%s (%s) fulfillable=%s across %d distinct inventory items",
        order.get("name"), order_gid, fulfillable, len(quantities),
    )
    return fulfillable

def _get_shopify_id_from_handle(handle: int) -> str:
    """Fetch the Shopify order ID from its handle."""
    query = gql(
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

    variables = {"name": f"name:{handle}"}

    try:
        result = gql_client.execute(query, variable_values=variables)
    except TransportQueryError as exc:  # pragma: no cover - network interaction
        raise RuntimeError(f"Failed to fetch order with handle {handle}: {exc}") from exc

    orders = result.get("orders", {}).get("edges", [])
    if not orders:
        raise RuntimeError(f"Order with handle {handle} not found")

    # ``name:`` is a partial match and the sort order is Shopify's default, so
    # record what else came back before committing to the first hit.
    candidates = [edge.get("node", {}) for edge in orders]
    if len(candidates) > 1:
        logger.warning(
            "AMBIGUOUS-ORDER handle=%s matched %d orders: %s — taking the first",
            handle,
            len(candidates),
            [
                (node.get("name"), node.get("createdAt"),
                 node.get("displayFulfillmentStatus"))
                for node in candidates
            ],
        )

    order_node = candidates[0]
    shopify_id = order_node.get("id")
    if not shopify_id:
        raise RuntimeError(f"Order with handle {handle} has no ID")

    resolved_name = order_node.get("name") or ""
    if resolved_name.lstrip("#") != str(handle).lstrip("#"):
        logger.error(
            "WRONG-ORDER handle=%s resolved to order=%s (%s) created=%s status=%s — "
            "the stock check is about to run against the wrong order",
            handle, resolved_name, shopify_id, order_node.get("createdAt"),
            order_node.get("displayFulfillmentStatus"),
        )
    else:
        logger.info(
            "handle=%s resolved to order=%s (%s) created=%s status=%s",
            handle, resolved_name, shopify_id, order_node.get("createdAt"),
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
        # a missed pause has to be visible in the journal.
        logger.exception(
            "job=%s FAILED shipmondo=%s order=%s decision=none — the order was "
            "NOT paused",
            job, shipmondo_id, handle,
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
