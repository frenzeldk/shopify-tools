import os
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

# Shopify GraphQL setup
__SHOPIFY_URL__ = os.environ.get("SHOPIFY_URL")
__SHOPIFY_HEADER__ = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}

__transport__ = AIOHTTPTransport(url=__SHOPIFY_URL__, headers=__SHOPIFY_HEADER__, ssl=True)
__gql_client__ = Client(transport=__transport__, fetch_schema_from_transport=True)


# Query all variants for the vendor, including inventory and incoming stock
__VARIANTS_QUERY__ = gql("""
query ($cursor: String, $query: String!) {
  productVariants(first: 100, after: $cursor, query: $query) {
    edges {
      node {
        id
        barcode
        sku
        title
        inventoryQuantity
        inventoryItem {
          id
          tracked
          inventoryLevels(first: 10) {
            edges {
              node {
                quantities (names: ["available", "incoming"]){
                  name
                  quantity
                }
              }
            }
          }
        }
        product {
          title
          vendor
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
""")

def fetch_missing_inventory():
    """Fetch variants with negative inventory and calculate missing quantities."""
    missing = []
    cursor = None
    while True:
        variables = {"cursor": cursor, "query":"inventory_quantity:<0"}
        result = __gql_client__.execute(__VARIANTS_QUERY__, variable_values=variables)
        variants = result["productVariants"]["edges"]
        for v in variants:
            node = v["node"]
            # Sum available and incoming across all inventory levels
            available = 0
            incoming = 0
            inventory_levels = node["inventoryItem"].get("inventoryLevels", {}).get("edges", [])
            for level in inventory_levels:
                quantities = level["node"].get("quantities", [])
                for q in quantities:
                    if q["name"] == "available":
                        available += q["quantity"] or 0
                    elif q["name"] == "incoming":
                        incoming += q["quantity"] or 0
            # Define your threshold for "missing" (e.g., less than 0 in stock after incoming)
            total = available + incoming
            if total < 0:
                missing.append({
                    "sku": node["sku"],
                    "title": node["title"],
                    "barcode": node["barcode"],
                    "product_title": node["product"]["title"],
                    "product_vendor": node["product"]["vendor"],
                    "missing_qty": 0 - total  # Order enough to reach 0 in stock
                })
        page_info = result["productVariants"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return missing