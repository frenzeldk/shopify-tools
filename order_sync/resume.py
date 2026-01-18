#!/opt/shopify-python/bin/python3

"""Checks actual available quantities and activates orders as possible."""
import os
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from shipmondo import resume_order

SHOPIFY_URL = os.environ.get("SHOPIFY_URL")
SHOPIFY_HEADER = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}

transport = AIOHTTPTransport(url=SHOPIFY_URL, headers=SHOPIFY_HEADER, ssl=True)
gql_client = Client(transport=transport, fetch_schema_from_transport=True)

_inventory_cache = {}

def _get_inventory_level(inventory_item_id: str) -> None:
    """Add the actual available quantity for the given inventory item ID to the cache.

    Args:
        inventory_item_id: The numeric ID of the inventory item to check.
    """
    query = gql(
        """
        query getInventoryLevel($id: ID!) {
            inventoryItem(id: $id) {
                id
                inventoryLevel(locationId: "gid://shopify/Location/100013703511") {
                    quantities(names: ["on_hand", "reserved", "damaged", 
                    "safety_stock", "quality_control"]) {
                        name
                        quantity
                    }
                }
            }
        }
        """
    )
    try:
        result = gql_client.execute(query, variable_values={"id": inventory_item_id})
        levels = result["inventoryItem"]["inventoryLevel"]["quantities"]
        levels = {item["name"]: item["quantity"] for item in levels}
        available = levels.get("on_hand", 0) - levels.get("reserved", 0)\
            - levels.get("damaged", 0) - levels.get("quality_control", 0)\
            - levels.get("safety_stock", 0)

        _inventory_cache[inventory_item_id] = available
    except TransportQueryError as e:
        print(f"Error fetching inventory level: {e}")
        raise RuntimeError(f"Failed to fetch inventory level: {e}") from e

def _update_inventory_cache(line_items: list[dict]) -> None:
    """Update the inventory cache with available quantities for the given line items.
    Decreases the available quantity for each inventory item by the quantity in the line item.

    Args:
        line_items: List of line items as returned by Shopify GraphQL API.
    """
    for item in [node["node"] for node in line_items]:
        if not item["variant"]:
            continue  # Variant has been deleted
        inventory_item_id = item["variant"]["inventoryItem"]["id"]
        if inventory_item_id not in _inventory_cache:
            _get_inventory_level(inventory_item_id)
        _inventory_cache[inventory_item_id] = _inventory_cache.get(inventory_item_id, 0)\
            - item["currentQuantity"]

def _can_fulfill_order(line_items: list[dict]) -> bool:
    """Return True if all line items can be fulfilled from available stock.

    Args:
        line_items: List of line items as returned by Shopify GraphQL API.
    """
    for item in [node["node"] for node in line_items]:
        if not item["variant"]:
            continue  # Variant has been deleted
        inventory_item_id = item["variant"]["inventoryItem"]["id"]
        available_quantity = _inventory_cache.get(inventory_item_id, 0)
        if available_quantity < 0:
            return False
    return True

def _resume_orders(orders: list[dict]) -> None:
    """Check the given orders and activate them if they can be fulfilled.

    Args:
        orders: List of orders as returned by Shopify GraphQL API.
    """
    mutation = gql(
        """
        mutation tagsRemove($id: ID!, $tags: [String!]!) {
            tagsRemove(id: $id, tags: $tags) {
                userErrors {
                    field
                    message
                }
                node {
                    id
                }
            }
        }
        """
    )
    for order in orders:
        _update_inventory_cache(order["lineItems"]["edges"])
        if _can_fulfill_order(order["lineItems"]["edges"])\
            and ("paused" in order["tags"] or "Mangler Varer" in order["tags"]):
            try:
                shipmondo_result = resume_order(order["name"][1:])  # Remove leading # from order name
                if shipmondo_result is None:
                    continue
                result = gql_client.execute(
                    mutation, variable_values={"id": order["id"],
                                               "tags": ["paused", "Mangler Varer"]}
                )
                user_errors = result["tagsRemove"]["userErrors"]
                if user_errors:
                    print(f"Failed to remove tags from order {order['name']}: {user_errors}")
                else:
                    print(f"Removed tags from order {order['name']}")
            except TransportQueryError as e:
                print(f"Error removing tags from order {order['name']}: {e}")
                raise RuntimeError(f"Failed to remove tags from order {order['name']}: {e}") from e

def get_orders() -> list[dict]:
    """Fetch all unfulfilled orders.

    Returns:
        List of orders as returned by Shopify GraphQL API.
    """
    query = gql(
        """
        query getOpenOrders($query: String!, $cursor: String) {
            orders(first: 100, query: $query, after: $cursor) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        name
                        tags
                        lineItems(first: 100) {
                            edges {
                                node {
                                    currentQuantity
                                    variant {
                                        inventoryItem {
                                            id
                                        }
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
    all_orders: list[dict] = []
    cursor = None
    while True:
        try:
            result = gql_client.execute(
                query,
                variable_values={
                    "query": "test:false -financial_status:voided"
                    "(fulfillment_status:unfulfilled OR fulfillment_status:partial) status:open",
                    "cursor": cursor,
                },
            )
        except TransportQueryError as e:
            print(f"Error fetching orders: {e}")
            raise RuntimeError(f"Failed to fetch orders: {e}") from e
        orders_connection = result["orders"]
        all_orders.extend(edge["node"] for edge in orders_connection["edges"])
        page_info = orders_connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return all_orders

def main() -> None:
    """Main function to fetch and resume orders."""
    orders = get_orders()
    print(f"Fetched {len(orders)} unfulfilled orders.")
    _resume_orders(orders)

if __name__ == "__main__":
    main()
