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


def update_variant_barcode(sku: str, barcode: str) -> tuple[bool, str]:
    """
    Update the barcode for a Shopify variant by SKU.
    
    Args:
        sku: The SKU of the variant to update
        barcode: The new barcode value
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # First, find the variant by SKU
        query = gql("""
        query ($query: String!) {
          productVariants(first: 1, query: $query) {
            edges {
              node {
                id
                sku
                barcode
              }
            }
          }
        }
        """)
        
        variables = {"query": f'sku:"{sku}"'}
        result = __gql_client__.execute(query, variable_values=variables)
        
        variants = result.get("productVariants", {}).get("edges", [])
        if not variants:
            return False, f"No variant found with SKU: {sku}"
        
        variant_id = variants[0]["node"]["id"]
        
        # Update the barcode using productUpdate mutation with nested variant
        # Note: Shopify's productVariantUpdate uses different input structure in newer API versions
        mutation = gql("""
        mutation updateProductVariant($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
              id
              sku
              barcode
            }
            userErrors {
              field
              message
            }
          }
        }
        """)
        
        # Extract product ID from variant ID (format: gid://shopify/ProductVariant/123)
        product_query = gql("""
        query getProductForVariant($variantId: ID!) {
          productVariant(id: $variantId) {
            product {
              id
            }
          }
        }
        """)
        
        product_result = __gql_client__.execute(product_query, variable_values={"variantId": variant_id})
        product_id = product_result.get("productVariant", {}).get("product", {}).get("id")
        
        if not product_id:
            return False, f"Could not find product for variant {sku}"
        
        mutation_variables = {
            "productId": product_id,
            "variants": [{
                "id": variant_id,
                "barcode": barcode
            }]
        }
        
        mutation_result = __gql_client__.execute(mutation, variable_values=mutation_variables)
        
        user_errors = mutation_result.get("productVariantsBulkUpdate", {}).get("userErrors", [])
        if user_errors:
            error_messages = ", ".join([err["message"] for err in user_errors])
            return False, f"Shopify error updating barcode for SKU {sku}: {error_messages}"
        
        return True, f"Updated barcode in Shopify for SKU {sku} to '{barcode}'"
        
    except Exception as e:
        return False, f"Error updating barcode in Shopify for SKU {sku}: {str(e)}"