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


# Query for inventory items with costs
__INVENTORY_VALUE_QUERY__ = gql("""
query ($cursor: String, $query: String!) {
  productVariants(first: 100, after: $cursor, query: $query) {
    edges {
      node {
        id
        sku
        title
        inventoryQuantity
        inventoryItem {
          id
          tracked
          unitCost {
            amount
          }
          inventoryLevels(first: 10) {
            edges {
              node {
                quantities(names: ["available"]) {
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


def calculate_brand_inventory_value(brand_name: str) -> float:
    """
    Calculate the total inventory value for all products of a specific brand.
    
    Args:
        brand_name: The vendor/brand name to filter products by
        
    Returns:
        The total value of inventory for the brand (cost * quantity)
    """
    total_value = 0.0
    cursor = None
    
    # Build query to filter by vendor (brand)
    query = f'vendor:"{brand_name}"'
    
    while True:
        variables = {"cursor": cursor, "query": query}
        result = __gql_client__.execute(__INVENTORY_VALUE_QUERY__, variable_values=variables)
        variants = result["productVariants"]["edges"]
        
        for v in variants:
            node = v["node"]
            inventory_item = node.get("inventoryItem", {})
            
            # Get unit cost
            unit_cost_data = inventory_item.get("unitCost")
            if unit_cost_data and unit_cost_data.get("amount"):
                unit_cost = float(unit_cost_data["amount"])
            else:
                unit_cost = 0.0
            
            # Sum available quantities across all inventory levels
            available_qty = 0
            inventory_levels = inventory_item.get("inventoryLevels", {}).get("edges", [])
            for level in inventory_levels:
                quantities = level["node"].get("quantities", [])
                for q in quantities:
                    if q["name"] == "available":
                        available_qty += q["quantity"] or 0
            
            # Calculate value for this variant
            variant_value = unit_cost * available_qty
            total_value += variant_value
        
        page_info = result["productVariants"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    
    return total_value

def update_variant_barcode(sku: str, barcode: str) -> dict:
    """
    Update the barcode for a product variant by SKU.
    
    Args:
        sku: The SKU to search for
        barcode: The new barcode value
    
    Returns:
        Dict with success status and message
    """
    # First, find the variant by SKU
    query = f'sku:{sku}'
    variables = {"cursor": None, "query": query}
    
    try:
        result = __gql_client__.execute(__VARIANTS_QUERY__, variable_values=variables)
        variants = result["productVariants"]["edges"]
        
        if not variants or len(variants) == 0:
            return {
                "success": False,
                "message": f"No variant found with SKU: {sku}"
            }
        
        # Get the first matching variant's ID
        variant_node = variants[0]["node"]
        variant_id = variant_node["id"]
        
        # Update the variant's barcode using GraphQL mutation
        mutation = gql("""
        mutation productVariantUpdate($input: ProductVariantInput!) {
          productVariantUpdate(input: $input) {
            productVariant {
              id
              barcode
              sku
            }
            userErrors {
              field
              message
            }
          }
        }
        """)
        
        mutation_variables = {
            "input": {
                "id": variant_id,
                "barcode": barcode
            }
        }
        
        mutation_result = __gql_client__.execute(mutation, variable_values=mutation_variables)
        user_errors = mutation_result["productVariantUpdate"]["userErrors"]
        
        if user_errors:
            error_messages = [f"{err['field']}: {err['message']}" for err in user_errors]
            return {
                "success": False,
                "message": f"Failed to update barcode: {', '.join(error_messages)}"
            }
        
        return {
            "success": True,
            "message": f"Successfully updated barcode for SKU {sku}",
            "barcode": barcode
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Error updating barcode: {str(e)}"
        }
