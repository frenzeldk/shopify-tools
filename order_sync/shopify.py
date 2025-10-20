"""Helpers to interact with the Shopify GraphQL API."""

from __future__ import annotations
import os
import re
from typing import Iterable, List

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from shipmondo import pause_order

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


def _check_availability(order_id: str | int) -> bool:
    """Check if all items in the order are available in inventory.

    Args:
        order_id: Numeric ID or GraphQL global ID of the target order.  
        """

    order_gid = _normalize_order_id(order_id)

    query = gql(
        """
        query Order($id: ID!) {
          order(id: $id) {
            lineItems(first: 100) {
              edges {
                node {
                  title
                  quantity
                  variant {
                    inventoryQuantity
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

    line_items = order.get("lineItems", {}).get("edges", [])
    for item in line_items:
        node = item.get("node", {})
        variant = node.get("variant")
        if not variant:
            continue  # Skip items without a variant (e.g., custom items)
        available_quantity = variant.get("inventoryQuantity", 0)
        if available_quantity < 0:
            return False  # Not enough inventory for this item

    return True  # All items are available

def handle_order(order_id: str | int, sid: int) -> None:
    """Handle the order by checking inventory and pausing if needed."""
    if not _check_availability(order_id):
        _pause_order(order_id, sid)

def _pause_order(order_id: str | int, sid: int) -> None:
    """Pause the order by adding a "paused" tag and pausing it
    in shipmondo."""
    _add_tag_to_order(order_id, "paused")
    pause_order(sid)
