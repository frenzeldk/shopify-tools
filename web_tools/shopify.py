import json
import os
import logging
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport

_log = logging.getLogger(__name__)

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


def calculate_brand_inventory_value(brand_name: str = None) -> float:
    """
    Calculate the total inventory value for all products of a specific brand,
    or for all products if no brand is specified.
    
    Args:
        brand_name: The vendor/brand name to filter products by. If None or empty,
                   calculates total value for all inventory.
        
    Returns:
        The total value of inventory for the brand (cost * quantity)
    """
    total_value = 0.0
    cursor = None
    
    # Build query to filter by vendor (brand) if provided
    if brand_name and brand_name.strip():
        query = f'vendor:"{brand_name}"'
    else:
        # Empty query to get all products
        query = ""
    
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


def fetch_order_customer(order_name: str) -> dict | None:
    """
    Look up a Shopify order by its display name (e.g. "#27542") and return
    the customer's first name and email address.

    Args:
        order_name: The order name with or without the '#' prefix.

    Returns:
        A dict with keys ``first_name`` and ``email``, or ``None`` when
        the order cannot be found or has no customer attached.
    """
    # Normalise: strip whitespace and ensure a leading '#'
    order_name = order_name.strip().lstrip("#")
    order_name = f"#{order_name}"

    query = gql("""
    query ($orderQuery: String!) {
      orders(first: 1, query: $orderQuery) {
        edges {
          node {
            name
            customer {
              firstName
              email
            }
          }
        }
      }
    }
    """)

    # Shopify search accepts the order name with or without the '#'.
    variables = {"orderQuery": f"name:{order_name}"}
    result = __gql_client__.execute(query, variable_values=variables)

    edges = result.get("orders", {}).get("edges", [])
    if not edges:
        return None

    customer = edges[0]["node"].get("customer")
    if not customer:
        return None

    return {
        "first_name": customer.get("firstName", ""),
        "email": customer.get("email", ""),
    }


# ── Product Tools helpers ─────────────────────────────────────────

import re as _re

def _normalize_size(size: str) -> str:
    """Shorten repeated-X sizes: XXS→2XS, XXL→2XL, XXXL→3XL, etc."""
    m = _re.match(r'^(X{2,})(S|L)$', size, _re.IGNORECASE)
    if m:
        return f"{len(m.group(1))}X{m.group(2).upper()}"
    return size


def parse_vendor_csv(csv_content: str) -> list[dict]:
    """
    Parse a vendor product CSV (semicolon-delimited) into a list of dicts.

    Expected columns (from Helikon-Tex / Entire M export):
      SKU; EAN13; CN; Size; Name; ProductSizeEU; ProductSizeUSA;
      ProductRegularPrice; ProductRegularCurrency; DiscountPrice;
      DiscountCurrency; ProductMSRPPrice; ProductMSRPCurrency;
      ProductWeight; ProductWeightUnit; Country

    SKU structure:  {ProductCode}-{ColorCode}[-{SizeCode}]
      ProductCode = first 3 dash-separated parts  (e.g. TS-CTT-CO)
      ColorCode   = 4th part                      (e.g. 01)
      SizeCode    = optional 5th part              (e.g. B05)
        Letter = length (A=Short, B=Regular, C=Long, D=XLong, U=Unisex)
        Digits = size   (01=2XS, 02=XS, 03=S, 04=M, 05=L, 06=XL, 07=2XL, 08=3XL, 09=4XL)

    Returns a list of normalised dicts with consistent keys.
    """
    import csv
    import io

    reader = csv.DictReader(
        io.StringIO(csv_content),
        delimiter=";",
    )

    products: list[dict] = []
    for row in reader:
        # Strip whitespace from keys and values
        row = {(k or "").strip(): (v or "").strip() for k, v in row.items()}

        sku = row.get("SKU", "")
        if not sku:
            continue

        full_name = row.get("Name", "")

        # Extract product code (first 3 dash-separated parts of SKU)
        sku_parts = sku.split("-")
        product_code = "-".join(sku_parts[:3]) if len(sku_parts) >= 3 else sku

        # Extract base product name and color from Name field
        # Format: "Base Product Name - Color"  (split on last " - ")
        if " - " in full_name:
            base_name, color = full_name.rsplit(" - ", 1)
        else:
            base_name = full_name
            color = ""

        # Normalise size: strip length suffix first (e.g. "XXXXL/Long" → "XXXXL")
        # then shorten repeated-X forms ("XXXXL" → "4XL").
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


def fetch_shopify_products_by_vendors(vendors: list[str]) -> dict[str, dict]:
    """
    Fetch all Shopify products for the given vendors, with full variant
    pagination.  Returns a dict keyed by **product ID** where each value
    contains the product info and a dict of its variants keyed by SKU.
    """
    products_map: dict[str, dict] = {}

    _PRODUCTS_QUERY = gql("""
    query getProductsByVendor($query: String!, $after: String) {
        products(first: 50, query: $query, after: $after) {
            edges {
                node {
                    id
                    title
                    vendor
                    handle
                    variants(first: 100) {
                        edges {
                            node {
                                id
                                sku
                                barcode
                                title
                                price
                                inventoryQuantity
                                inventoryItem {
                                    unitCost { amount }
                                    countryCodeOfOrigin
                                    harmonizedSystemCode
                                    measurement {
                                        weight { unit value }
                                    }
                                }
                                selectedOptions { name value }
                            }
                        }
                        pageInfo { hasNextPage endCursor }
                    }
                }
            }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    _VARIANT_PAGE_QUERY = gql("""
    query getVariantPage($productId: ID!, $after: String) {
        product(id: $productId) {
            variants(first: 100, after: $after) {
                edges {
                    node {
                        id
                        sku
                        barcode
                        title
                        price
                        inventoryQuantity
                        inventoryItem {
                            unitCost { amount }
                            countryCodeOfOrigin
                            harmonizedSystemCode
                            measurement {
                                weight { unit value }
                            }
                        }
                        selectedOptions { name value }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)

    def _parse_variant(v: dict) -> dict:
        inv_item = v.get("inventoryItem") or {}
        unit_cost_data = inv_item.get("unitCost")
        measurement = inv_item.get("measurement") or {}
        weight_data = measurement.get("weight") or {}
        return {
            "id": v["id"],
            "sku": v.get("sku") or "",
            "barcode": v.get("barcode") or "",
            "title": v.get("title") or "",
            "price": v.get("price") or "",
            "inventoryQuantity": v.get("inventoryQuantity", 0),
            "weight": weight_data.get("value"),
            "weightUnit": weight_data.get("unit"),
            "unitCost": float(unit_cost_data["amount"]) if unit_cost_data and unit_cost_data.get("amount") else None,
            "countryOfOrigin": inv_item.get("countryCodeOfOrigin") or "",
            "hsCode": inv_item.get("harmonizedSystemCode") or "",
            "selectedOptions": v.get("selectedOptions") or [],
        }

    for vendor in vendors:
        has_next_page = True
        after_cursor = None

        while has_next_page:
            variables = {"query": f'vendor:"{vendor}"', "after": after_cursor}
            result = __gql_client__.execute(_PRODUCTS_QUERY, variable_values=variables)

            for edge in result["products"]["edges"]:
                node = edge["node"]
                product_id = node["id"]
                variant_skus: dict[str, dict] = {}

                # First page of variants (from the product query)
                for v_edge in node["variants"]["edges"]:
                    parsed = _parse_variant(v_edge["node"])
                    variant_skus[parsed["sku"]] = parsed

                # Paginate remaining variants
                v_page_info = node["variants"]["pageInfo"]
                v_has_next = v_page_info.get("hasNextPage", False)
                v_cursor = v_page_info.get("endCursor")

                while v_has_next:
                    v_result = __gql_client__.execute(
                        _VARIANT_PAGE_QUERY,
                        variable_values={"productId": product_id, "after": v_cursor},
                    )
                    v_data = v_result.get("product", {}).get("variants", {})
                    for v_edge in v_data.get("edges", []):
                        parsed = _parse_variant(v_edge["node"])
                        variant_skus[parsed["sku"]] = parsed
                    v_pi = v_data.get("pageInfo", {})
                    v_has_next = v_pi.get("hasNextPage", False)
                    v_cursor = v_pi.get("endCursor")

                products_map[product_id] = {
                    "id": product_id,
                    "title": node["title"],
                    "vendor": node["vendor"],
                    "handle": node.get("handle", ""),
                    "variants": variant_skus,
                }

            page_info = result["products"]["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            after_cursor = page_info.get("endCursor")

    return products_map


def compare_vendor_products(
    vendor_products: list[dict],
    shopify_products: dict[str, dict],
) -> dict:
    """
    Compare vendor products against Shopify at the SKU and GTIN/EAN level.

    Groups vendor items by **product code** (first 3 dash-parts of the SKU)
    so that all colour/size variants of the same product are treated as one
    logical product.

    Strategy:
      1. Build flat lookup sets of all Shopify SKUs and barcodes.
      2. Group vendor items by product_code.
      3. For each group, check whether ANY variant's SKU or EAN already
         exists in Shopify.
         - If yes → product exists; unmatched variants are "new variants".
         - If no  → the whole group is "new products".
    """
    # SKU → Shopify product, barcode → Shopify product
    sku_to_product: dict[str, dict] = {}
    barcode_to_product: dict[str, dict] = {}
    known_skus: set[str] = set()
    known_barcodes: set[str] = set()

    for product in shopify_products.values():
        product_ref = {
            "id": product["id"],
            "title": product["title"],
        }
        for sku, variant in product["variants"].items():
            if sku:
                known_skus.add(sku)
                sku_to_product[sku] = product_ref
            barcode = variant.get("barcode", "")
            if barcode:
                known_barcodes.add(barcode)
                barcode_to_product[barcode] = product_ref

    new_products: list[dict] = []
    new_variants: list[dict] = []

    # Group vendor products by product code (first 3 dash-parts of SKU).
    # All colour and size variants of the same base product share a code.
    vendor_by_code: dict[str, list[dict]] = {}
    for item in vendor_products:
        code = item.get("product_code", "").strip()
        if code:
            vendor_by_code.setdefault(code, []).append(item)

    for product_code, items in vendor_by_code.items():
        # Try to find a matching Shopify product for this group by checking
        # whether ANY sibling SKU or EAN already exists in Shopify.
        matched_shopify_product: dict | None = None

        for item in items:
            sku = item.get("sku", "")
            ean = item.get("ean", "").strip()

            if sku and sku in sku_to_product:
                matched_shopify_product = sku_to_product[sku]
                break
            if ean and ean in barcode_to_product:
                matched_shopify_product = barcode_to_product[ean]
                break

        if matched_shopify_product is None:
            # Entirely new product — none of its variants exist in Shopify
            for item in items:
                new_products.append(item.copy())
        else:
            # Product exists — find variants whose SKU and EAN are both
            # absent from Shopify
            for item in items:
                sku = item.get("sku", "")
                ean = item.get("ean", "").strip()
                sku_exists = sku and sku in known_skus
                ean_exists = ean and ean in known_barcodes

                if not sku_exists and not ean_exists:
                    entry = item.copy()
                    entry["shopify_product_id"] = matched_shopify_product["id"]
                    entry["shopify_product_title"] = matched_shopify_product["title"]
                    new_variants.append(entry)

    return {
        "new_products": new_products,
        "new_variants": new_variants,
    }


# ── Color metaobject helpers ─────────────────────────────────────

def _discover_color_metaobject_type_from_definitions() -> str | None:
    """
    Fallback: scan all metaobjectDefinitions in the store and return the
    type string of one whose type contains 'color' (Shopify convention).
    Returns None if nothing matches.
    """
    import logging
    log = logging.getLogger(__name__)

    defs_query = gql("""
    query {
        metaobjectDefinitions(first: 250) {
            edges {
                node {
                    type
                    displayNameKey
                }
            }
        }
    }
    """)
    try:
        result = __gql_client__.execute(defs_query)
        for edge in result.get("metaobjectDefinitions", {}).get("edges", []):
            mo_type = edge["node"].get("type", "")
            if "color" in mo_type.lower() or "colour" in mo_type.lower():
                log.info(
                    "_discover_color_metaobject_type_from_definitions: "
                    "found type '%s' by scanning definitions", mo_type,
                )
                return mo_type
    except Exception as exc:
        log.warning(
            "_discover_color_metaobject_type_from_definitions: failed: %s", exc,
        )
    log.warning(
        "_discover_color_metaobject_type_from_definitions: no color-like "
        "metaobject definition found"
    )
    return None


def _discover_color_metaobject_type(product_id: str) -> str | None:
    """
    Given a product ID, look at its linked color option to discover the
    metaobject type string (e.g. 'shopify--color-pattern').
    Searches all options with a linkedMetafield for a value that is a
    Metaobject GID, regardless of the option's display name.
    Returns None if no linked color option is found.
    """
    import logging
    log = logging.getLogger(__name__)

    product_info_query = gql("""
    query productInfo($id: ID!) {
        product(id: $id) {
            options {
                name
                linkedMetafield { namespace key }
                optionValues {
                    linkedMetafieldValue
                }
            }
        }
    }
    """)
    result = __gql_client__.execute(product_info_query, variable_values={"id": product_id})
    options = result.get("product", {}).get("options", [])

    sample_gid = None
    matched_option = None
    for opt in options:
        if not opt.get("linkedMetafield"):
            continue
        for ov in opt.get("optionValues", []):
            val = ov.get("linkedMetafieldValue", "")
            if val and val.startswith("gid://shopify/Metaobject/"):
                sample_gid = val
                matched_option = opt["name"]
                break
        if sample_gid:
            break

    if not sample_gid:
        log.warning(
            "_discover_color_metaobject_type: no linked metaobject option "
            "found on %s (options: %s) — trying definitions fallback",
            product_id,
            [(o["name"], bool(o.get("linkedMetafield"))) for o in options],
        )
        return _discover_color_metaobject_type_from_definitions()

    log.info(
        "_discover_color_metaobject_type: found linked option '%s' on %s",
        matched_option, product_id,
    )

    type_query = gql("""
    query metaobjectType($id: ID!) {
        metaobject(id: $id) { type }
    }
    """)
    type_result = __gql_client__.execute(type_query, variable_values={"id": sample_gid})
    mo_type = type_result.get("metaobject", {}).get("type")
    log.info("_discover_color_metaobject_type: type = %s", mo_type)
    return mo_type


def fetch_color_metaobject_definition(product_id: str) -> dict:
    """
    Discover the color metaobject type from a product and return its
    field definitions along with the type string.

    Tries multiple strategies to fetch field definitions because
    different Shopify API versions expose different queries/fields:
      A) metaobject.definition.fieldDefinitions
      B) metaobjectDefinitions(first: 250) list query
      C) metaobject.fields introspection (fallback, no validations)

    Returns: {"type": "...", "fields": [{"key": "...", "name": "...",
              "type": "...", "required": bool, "validations": [...]}]}
    """
    import logging
    log = logging.getLogger(__name__)

    # Step 1: find a sample linked metaobject GID from any color-like option
    product_info_query = gql("""
    query productInfo($id: ID!) {
        product(id: $id) {
            options {
                name
                linkedMetafield { namespace key }
                optionValues {
                    linkedMetafieldValue
                }
            }
        }
    }
    """)
    result = __gql_client__.execute(
        product_info_query, variable_values={"id": product_id}
    )
    options = result.get("product", {}).get("options", [])

    sample_gid = None
    matched_option = None
    for opt in options:
        if not opt.get("linkedMetafield"):
            continue
        for ov in opt.get("optionValues", []):
            val = ov.get("linkedMetafieldValue", "")
            if val and val.startswith("gid://shopify/Metaobject/"):
                sample_gid = val
                matched_option = opt["name"]
                break
        if sample_gid:
            break

    if not sample_gid:
        log.warning(
            "fetch_color_metaobject_definition: no linked metaobject option "
            "found on %s (options: %s) — trying definitions fallback",
            product_id,
            [(o["name"], bool(o.get("linkedMetafield"))) for o in options],
        )
        # Fallback: discover the type from global definitions and
        # go straight to strategy B (list all definitions) for field defs
        fallback_type = _discover_color_metaobject_type_from_definitions()
        if not fallback_type:
            return {"type": None, "fields": []}
        # Jump to the field-definition fetch section with this type
        mo_type = fallback_type
        sample_gid = None  # no sample — skip strategy A later

    # Step 2: get the metaobject type first (only when we have a sample GID)
    if sample_gid:
        type_query = gql("""
        query metaobjectType($id: ID!) {
            metaobject(id: $id) { type }
        }
        """)
        type_result = __gql_client__.execute(
            type_query, variable_values={"id": sample_gid}
        )
        mo_type = (type_result.get("metaobject") or {}).get("type")
        if not mo_type:
            log.warning(
                "fetch_color_metaobject_definition: could not resolve type "
                "from metaobject %s", sample_gid,
            )
            return {"type": None, "fields": []}

    log.info("fetch_color_metaobject_definition: type=%s", mo_type)

    # Step 3: get field definitions — try multiple strategies because
    # different Shopify API versions expose different queries/fields.
    field_defs = []

    # Strategy A: metaobject.definition.fieldDefinitions (requires sample GID)
    if not field_defs and sample_gid:
        try:
            log.info("fetch_color_metaobject_definition: trying strategy A "
                      "(metaobject.definition.fieldDefinitions)")
            def_query_a = gql("""
            query metaobjectWithDefinition($id: ID!) {
                metaobject(id: $id) {
                    definition {
                        fieldDefinitions {
                            key
                            name
                            required
                            type { name }
                            validations { name value }
                        }
                    }
                }
            }
            """)
            def_result_a = __gql_client__.execute(
                def_query_a, variable_values={"id": sample_gid}
            )
            field_defs = (
                (def_result_a.get("metaobject") or {})
                .get("definition", {})
                .get("fieldDefinitions", [])
            )
            log.info("fetch_color_metaobject_definition: strategy A returned "
                      "%d field defs", len(field_defs))
        except Exception as exc:
            log.warning("fetch_color_metaobject_definition: strategy A failed: %s", exc)

    # Strategy B: list all metaobjectDefinitions and filter by type
    if not field_defs:
        try:
            log.info("fetch_color_metaobject_definition: trying strategy B "
                      "(metaobjectDefinitions list)")
            def_query_b = gql("""
            query {
                metaobjectDefinitions(first: 250) {
                    edges {
                        node {
                            type
                            fieldDefinitions {
                                key
                                name
                                required
                                type { name }
                                validations { name value }
                            }
                        }
                    }
                }
            }
            """)
            def_result_b = __gql_client__.execute(def_query_b)
            for edge in def_result_b.get("metaobjectDefinitions", {}).get("edges", []):
                node = edge.get("node", {})
                if node.get("type") == mo_type:
                    field_defs = node.get("fieldDefinitions", [])
                    break
            log.info("fetch_color_metaobject_definition: strategy B returned "
                      "%d field defs", len(field_defs))
        except Exception as exc:
            log.warning("fetch_color_metaobject_definition: strategy B failed: %s", exc)

    # Strategy C: use node() with inline fragment on MetaobjectDefinition
    # First we need the definition GID — extract from metaobject fields
    if not field_defs and sample_gid:
        try:
            log.info("fetch_color_metaobject_definition: trying strategy C "
                      "(metaobject.fields introspection)")
            # Query the metaobject's own fields to discover keys and types
            fields_query = gql("""
            query metaobjectFields($id: ID!) {
                metaobject(id: $id) {
                    fields { key type value }
                }
            }
            """)
            fields_result = __gql_client__.execute(
                fields_query, variable_values={"id": sample_gid}
            )
            raw_fields = (fields_result.get("metaobject") or {}).get("fields", [])
            # Build field_defs from the raw fields — we won't have
            # validation details but we can still identify types
            field_defs = [
                {
                    "key": f["key"],
                    "name": f["key"].replace("_", " ").title(),
                    "required": False,
                    "type": {"name": f.get("type", "unknown")},
                    "validations": [],
                }
                for f in raw_fields
            ]
            log.info("fetch_color_metaobject_definition: strategy C returned "
                      "%d field defs (from metaobject.fields)", len(field_defs))
        except Exception as exc:
            log.warning("fetch_color_metaobject_definition: strategy C failed: %s", exc)

    if not field_defs:
        log.error("fetch_color_metaobject_definition: all strategies failed "
                   "for type=%s", mo_type)
        return {"type": mo_type, "fields": []}

    fields = []
    for fd in field_defs:
        fields.append({
            "key": fd["key"],
            "name": fd.get("name", fd["key"]),
            "type": fd.get("type", {}).get("name", "unknown"),
            "required": fd.get("required", False),
            "validations": fd.get("validations", []),
        })

    log.info(
        "fetch_color_metaobject_definition: type=%s fields=%s",
        mo_type, [(f["key"], f["type"]) for f in fields],
    )
    return {"type": mo_type, "fields": fields}


def fetch_metaobject_options_for_field(field_validations: list[dict]) -> list[dict]:
    """
    Given the validations list from a metaobject field definition, fetch
    all valid options for a 'list.metaobject_reference' or
    'metaobject_reference' field.

    The validations should contain a 'metaobject_definition_id' entry
    whose value is a GID like 'gid://shopify/MetaobjectDefinition/...'.

    Returns a list of {"gid": "...", "displayName": "..."}.
    """
    import json
    import logging
    log = logging.getLogger(__name__)

    log.info(
        "fetch_metaobject_options_for_field: validations = %s",
        field_validations,
    )

    # Find the referenced metaobject definition GID
    ref_def_gid = None
    for v in field_validations:
        vname = v.get("name", "")
        raw = v.get("value", "")
        log.info(
            "fetch_metaobject_options_for_field: checking validation name=%s value=%s",
            vname, raw,
        )
        if vname == "metaobject_definition_id":
            # Value may be a plain GID or a JSON-encoded string
            try:
                parsed = json.loads(raw)
                ref_def_gid = parsed if isinstance(parsed, str) else raw
            except (json.JSONDecodeError, TypeError):
                ref_def_gid = raw
            break

    if not ref_def_gid:
        log.warning(
            "fetch_metaobject_options_for_field: no metaobject_definition_id "
            "in validations: %s", field_validations,
        )
        return []

    log.info(
        "fetch_metaobject_options_for_field: resolved definition GID = %s",
        ref_def_gid,
    )

    # Use the generic node() query to get the type from the MetaobjectDefinition
    # This is more reliable across API versions than the dedicated query.
    node_query = gql("""
    query nodeQuery($id: ID!) {
        node(id: $id) {
            ... on MetaobjectDefinition {
                type
            }
        }
    }
    """)
    try:
        node_result = __gql_client__.execute(node_query, variable_values={"id": ref_def_gid})
        ref_type = (node_result.get("node") or {}).get("type")
    except Exception as exc:
        log.exception(
            "fetch_metaobject_options_for_field: failed to query node for %s",
            ref_def_gid,
        )
        ref_type = None

    if not ref_type:
        log.warning(
            "fetch_metaobject_options_for_field: could not resolve type from %s, "
            "node result = %s", ref_def_gid, node_result if 'node_result' in dir() else 'N/A',
        )
        return []

    log.info(
        "fetch_metaobject_options_for_field: resolved type = %s",
        ref_type,
    )

    # Fetch all metaobjects of this referenced type
    list_query = gql("""
    query metaobjectsByType($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            edges { node { id displayName } }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    options = []
    after = None
    while True:
        result = __gql_client__.execute(list_query, variable_values={"type": ref_type, "after": after})
        for edge in result.get("metaobjects", {}).get("edges", []):
            node = edge["node"]
            options.append({
                "gid": node["id"],
                "displayName": (node.get("displayName") or "").strip(),
            })
        pi = result.get("metaobjects", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    log.info("fetch_metaobject_options_for_field: fetched %d options for type '%s'", len(options), ref_type)
    return sorted(options, key=lambda x: x["displayName"].lower())


def fetch_taxonomy_attribute_options(attribute_handle: str) -> list[dict]:
    """
    Fetch all values for a Shopify product taxonomy attribute
    (e.g. 'color', 'pattern') via the taxonomy API.

    The ``taxonomy`` query exposes ``categories`` whose ``attributes``
    connection returns a **union** of ``TaxonomyChoiceListAttribute |
    TaxonomyMeasurementAttribute | TaxonomyAttribute``.  Only
    ``TaxonomyChoiceListAttribute`` carries a ``values`` connection.

    We iterate categories until we find a ``TaxonomyChoiceListAttribute``
    whose lowercased ``name`` matches *attribute_handle* (Shopify does
    not expose a ``handle`` field on the attribute union types).

    Returns a sorted list of {"gid": "...", "displayName": "..."}.
    """
    import logging
    log = logging.getLogger(__name__)

    log.info("fetch_taxonomy_attribute_options: attribute_handle='%s'", attribute_handle)

    # The attribute_handle from the validation (e.g. "color", "pattern")
    # maps to the lowercased attribute name.
    target = attribute_handle.lower()

    try:
        query = gql("""
        query taxonomyLookup {
            taxonomy {
                categories(first: 10) {
                    nodes {
                        name
                        attributes(first: 50) {
                            nodes {
                                ... on TaxonomyChoiceListAttribute {
                                    id
                                    name
                                    values(first: 250) {
                                        nodes { id name }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """)
        result = __gql_client__.execute(query)
        categories = (
            result.get("taxonomy", {})
            .get("categories", {})
            .get("nodes", [])
        )

        for cat in categories:
            for attr in cat.get("attributes", {}).get("nodes", []):
                attr_name = (attr.get("name") or "").lower()
                if attr_name == target:
                    options = [
                        {
                            "gid": n["id"],
                            "displayName": (n.get("name") or "").strip(),
                        }
                        for n in attr.get("values", {}).get("nodes", [])
                    ]
                    log.info(
                        "fetch_taxonomy_attribute_options: found %d values "
                        "for '%s' via category '%s'",
                        len(options), attribute_handle, cat.get("name", "?"),
                    )
                    return sorted(options, key=lambda x: x["displayName"].lower())

        log.info(
            "fetch_taxonomy_attribute_options: attribute '%s' not found "
            "in first 10 categories — trying wider search",
            attribute_handle,
        )

        # Widen to 250 categories
        query_wide = gql("""
        query taxonomyLookupWide {
            taxonomy {
                categories(first: 250) {
                    nodes {
                        attributes(first: 50) {
                            nodes {
                                ... on TaxonomyChoiceListAttribute {
                                    id
                                    name
                                    values(first: 250) {
                                        nodes { id name }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """)
        result_wide = __gql_client__.execute(query_wide)
        categories_wide = (
            result_wide.get("taxonomy", {})
            .get("categories", {})
            .get("nodes", [])
        )

        for cat in categories_wide:
            for attr in cat.get("attributes", {}).get("nodes", []):
                attr_name = (attr.get("name") or "").lower()
                if attr_name == target:
                    options = [
                        {
                            "gid": n["id"],
                            "displayName": (n.get("name") or "").strip(),
                        }
                        for n in attr.get("values", {}).get("nodes", [])
                    ]
                    log.info(
                        "fetch_taxonomy_attribute_options: found %d values "
                        "for '%s' (wide search)",
                        len(options), attribute_handle,
                    )
                    return sorted(options, key=lambda x: x["displayName"].lower())

    except Exception as exc:
        log.warning(
            "fetch_taxonomy_attribute_options: taxonomy query failed: %s", exc,
        )

    log.error(
        "fetch_taxonomy_attribute_options: could not find values for '%s'",
        attribute_handle,
    )
    return []


def fetch_metaobject_type_details(metaobject_type: str) -> dict:
    """
    Fetch the metaobject definition (fields) and reference-field options
    for a given metaobject type string (e.g. ``"component_colors--farve"``).

    Returns::

        {
            "type": "component_colors--farve",
            "displayNameKey": "name",
            "fields": [
                {"key": "name", "name": "Name", "type": "single_line_text_field",
                 "required": True, "validations": []},
                {"key": "color", "name": "Color", "type": "color",
                 "required": False, "validations": []},
                ...
            ],
            "field_options": {
                "<field_key>": [{"gid": "...", "displayName": "..."}, ...],
            },
        }
    """
    import logging
    log = logging.getLogger(__name__)

    log.info("fetch_metaobject_type_details: type=%s", metaobject_type)

    # ── 1. Fetch the definition by listing all definitions ─────────
    def_query = gql("""
    query {
        metaobjectDefinitions(first: 250) {
            edges {
                node {
                    type
                    displayNameKey
                    fieldDefinitions {
                        key
                        name
                        required
                        type { name }
                        validations { name value }
                    }
                }
            }
        }
    }
    """)
    def_result = __gql_client__.execute(def_query)

    matched = None
    for edge in def_result.get("metaobjectDefinitions", {}).get("edges", []):
        node = edge["node"]
        if node.get("type") == metaobject_type:
            matched = node
            break

    if not matched:
        log.warning(
            "fetch_metaobject_type_details: no definition found for type '%s'",
            metaobject_type,
        )
        return {
            "type": metaobject_type,
            "displayNameKey": None,
            "fields": [],
            "field_options": {},
        }

    raw_fields = matched.get("fieldDefinitions", [])
    fields = []
    for fd in raw_fields:
        fields.append({
            "key": fd["key"],
            "name": fd.get("name", fd["key"]),
            "type": (fd.get("type", {}).get("name") or ""),
            "required": fd.get("required", False),
            "validations": fd.get("validations", []),
        })

    # ── 2. Fetch reference field options ───────────────────────────
    field_options: dict[str, list] = {}
    for field in fields:
        ft = field["type"].lower()
        if "metaobject_reference" in ft:
            if field.get("validations"):
                field_options[field["key"]] = fetch_metaobject_options_for_field(
                    field["validations"]
                )
            else:
                log.warning(
                    "fetch_metaobject_type_details: field '%s' has no validations",
                    field["key"],
                )
                field_options[field["key"]] = []
        elif "taxonomy_value_reference" in ft:
            attr_handle = None
            for v in field.get("validations", []):
                if v.get("name") == "product_taxonomy_attribute_handle":
                    attr_handle = v.get("value")
                    break
            if attr_handle:
                field_options[field["key"]] = fetch_taxonomy_attribute_options(
                    attr_handle
                )
            else:
                field_options[field["key"]] = []

    log.info(
        "fetch_metaobject_type_details: %d fields, field_options keys=%s",
        len(fields), list(field_options.keys()),
    )

    return {
        "type": metaobject_type,
        "displayNameKey": matched.get("displayNameKey"),
        "fields": fields,
        "field_options": field_options,
    }


def fetch_color_field_options(product_id: str) -> dict:
    """
    High-level helper: discover the color metaobject type, then for every
    field that references another metaobject (like base_color, base_pattern),
    fetch all valid choices.

    Returns: {
        "metaobject_type": "...",
        "fields": [...field definitions...],
        "field_options": {
            "<field_key>": [{"gid": "...", "displayName": "..."}, ...],
            ...
        }
    }
    """
    import logging
    log = logging.getLogger(__name__)

    definition = fetch_color_metaobject_definition(product_id)
    if not definition["type"]:
        log.warning("fetch_color_field_options: no definition type found")
        return {"metaobject_type": None, "fields": [], "field_options": {}}

    log.info(
        "fetch_color_field_options: definition has %d fields: %s",
        len(definition["fields"]),
        [(f["key"], f["type"]) for f in definition["fields"]],
    )

    field_options: dict[str, list] = {}
    for field in definition["fields"]:
        ft = (field["type"] or "").lower()
        if "metaobject_reference" in ft:
            log.info(
                "fetch_color_field_options: fetching options for field '%s' (type=%s)",
                field["key"], field["type"],
            )
            if field.get("validations"):
                field_options[field["key"]] = fetch_metaobject_options_for_field(
                    field["validations"]
                )
            else:
                log.warning(
                    "fetch_color_field_options: field '%s' has no validations, "
                    "cannot resolve referenced metaobject type", field["key"],
                )
                field_options[field["key"]] = []
        elif "taxonomy_value_reference" in ft:
            # Product taxonomy attribute reference (e.g. color, pattern)
            attr_handle = None
            for v in field.get("validations", []):
                if v.get("name") == "product_taxonomy_attribute_handle":
                    attr_handle = v.get("value")
                    break
            if attr_handle:
                log.info(
                    "fetch_color_field_options: fetching taxonomy values for "
                    "field '%s' (attribute='%s')",
                    field["key"], attr_handle,
                )
                field_options[field["key"]] = fetch_taxonomy_attribute_options(
                    attr_handle
                )
            else:
                log.warning(
                    "fetch_color_field_options: field '%s' is a taxonomy ref "
                    "but has no product_taxonomy_attribute_handle validation",
                    field["key"],
                )
                field_options[field["key"]] = []

    log.info(
        "fetch_color_field_options: field_options keys=%s counts=%s",
        list(field_options.keys()),
        {k: len(v) for k, v in field_options.items()},
    )

    return {
        "metaobject_type": definition["type"],
        "fields": definition["fields"],
        "field_options": field_options,
    }


def check_existing_color_metaobjects(product_id: str, color_names: list[str]) -> dict:
    """
    Check which color names already have a corresponding metaobject,
    and which colors are already present on the product's variants.

    Returns::

        {
            "existing": {"Olive Green": "gid://..."},  # metaobject exists globally
            "missing": ["Neon Pink"],                   # metaobject does not exist
            "on_product": ["Olive Green"],              # color already on a product variant
        }
    """
    import logging
    log = logging.getLogger(__name__)

    mo_type = _discover_color_metaobject_type(product_id)
    if not mo_type:
        # If we can't determine the type, treat all as missing
        return {"existing": {}, "missing": list(color_names), "on_product": []}

    # ── 1. Check which metaobjects exist globally ──────────────
    list_query = gql("""
    query metaobjectsByType($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            edges { node { id displayName } }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    all_names: dict[str, str] = {}
    after = None
    while True:
        result = __gql_client__.execute(list_query, variable_values={"type": mo_type, "after": after})
        for edge in result.get("metaobjects", {}).get("edges", []):
            node = edge["node"]
            dn = (node.get("displayName") or "").strip()
            all_names[dn] = node["id"]
        pi = result.get("metaobjects", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    existing = {}
    missing = []
    for name in color_names:
        if name in all_names:
            existing[name] = all_names[name]
        else:
            missing.append(name)

    # ── 2. Check which colors are already on the product ───────
    colors_on_product: set[str] = set()
    variants_query = gql("""
    query productVariantColors($id: ID!, $after: String) {
        product(id: $id) {
            variants(first: 100, after: $after) {
                edges {
                    node {
                        selectedOptions { name value }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)
    after = None
    while True:
        result = __gql_client__.execute(
            variants_query, variable_values={"id": product_id, "after": after}
        )
        for edge in result.get("product", {}).get("variants", {}).get("edges", []):
            for opt in edge["node"].get("selectedOptions", []):
                if opt["name"] == "Farve":
                    val = (opt.get("value") or "").strip()
                    if val:
                        colors_on_product.add(val)
        pi = result.get("product", {}).get("variants", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    on_product = [c for c in color_names if c in colors_on_product]

    # Build full list of available metaobjects for "replace with existing" UI
    available = sorted(
        [{"displayName": dn, "gid": gid} for dn, gid in all_names.items()],
        key=lambda x: x["displayName"],
    )

    log.info(
        "check_existing_color_metaobjects: %d existing, %d missing, %d already on product out of %d",
        len(existing), len(missing), len(on_product), len(color_names),
    )
    return {"existing": existing, "missing": missing, "on_product": on_product, "available": available}


def check_linked_option_values(product_id: str, variants_data: list[dict]) -> dict:
    """
    Pre-flight check: for every metafield-linked option on the product,
    find which values needed by *variants_data* do not yet exist as
    metaobjects in the global pool.

    Returns::

        {
            "options": {
                "Størrelse": {
                    "missing": ["XXXXL"],
                    "available": [{"displayName": "4XL", "gid": "gid://..."}, ...],
                    "metaobject_type": "shopify--size",
                },
                ...
            }
        }

    Only linked options that have at least one truly-missing value are
    included in the response.
    """
    import logging
    log = logging.getLogger(__name__)

    # ── Query product options ──────────────────────────────────
    product_info_query = gql("""
    query productInfo($id: ID!) {
        product(id: $id) {
            options {
                name
                linkedMetafield { namespace key }
                optionValues {
                    id
                    name
                    linkedMetafieldValue
                }
            }
        }
    }
    """)
    result = __gql_client__.execute(
        product_info_query, variable_values={"id": product_id}
    )
    options = result.get("product", {}).get("options", [])

    linked_options: dict[str, dict] = {}
    for opt in options:
        if not opt.get("linkedMetafield"):
            continue
        existing_names: set[str] = set()
        sample_gid = None
        for ov in opt.get("optionValues", []):
            dn = (ov.get("name") or "").strip()
            existing_names.add(dn)
            if not sample_gid and ov.get("linkedMetafieldValue"):
                sample_gid = ov["linkedMetafieldValue"]
        linked_options[opt["name"]] = {
            "existing_names": existing_names,
            "sample_gid": sample_gid,
        }

    if not linked_options:
        log.info("check_linked_option_values: no linked options on %s", product_id)
        return {"options": {}}

    # ── Collect needed values from variants ─────────────────────
    LENGTH_NAMES = {
        "A": "Short", "B": "Regular", "C": "Long",
        "D": "XLong", "U": "Unisex",
    }

    def _extract_length_letter(sku: str) -> str | None:
        parts = sku.split("-")
        if len(parts) >= 5:
            sc = parts[4]
            if sc and sc[0].isalpha():
                return sc[0].upper()
        return None

    length_letters = {
        _extract_length_letter(v.get("sku", ""))
        for v in variants_data
    } - {None}
    include_length = len(length_letters) > 1

    needed: dict[str, set[str]] = {}
    for v in variants_data:
        size = (v.get("size") or "").strip()
        if "/" in size:
            size = size.split("/", 1)[0].strip()
        size = _normalize_size(size)
        if size and size.lower() != "one size":
            needed.setdefault("Størrelse", set()).add(size)
        color = (v.get("color") or "").strip()
        if color:
            needed.setdefault("Farve", set()).add(color)
        if include_length:
            letter = _extract_length_letter(v.get("sku", ""))
            length_name = LENGTH_NAMES.get(letter, letter) if letter else None
            if length_name:
                needed.setdefault("Længde", set()).add(length_name)

    # ── For each linked option, check against global metaobject pool ─
    result_options: dict[str, dict] = {}

    list_query = gql("""
    query metaobjectsByType($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            edges { node { id displayName } }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    type_query = gql("""
    query metaobjectType($id: ID!) {
        metaobject(id: $id) { type }
    }
    """)

    for opt_name, opt_data in linked_options.items():
        needed_vals = needed.get(opt_name, set())
        if not needed_vals:
            continue

        # Values already on the product don't need metaobject resolution
        missing_from_product = needed_vals - opt_data["existing_names"]
        if not missing_from_product:
            continue

        sample_gid = opt_data["sample_gid"]
        if not sample_gid:
            log.warning(
                "check_linked_option_values: no sample GID for option '%s'",
                opt_name,
            )
            continue

        # Discover metaobject type
        type_result = __gql_client__.execute(
            type_query, variable_values={"id": sample_gid}
        )
        mo_type = (type_result.get("metaobject") or {}).get("type")
        if not mo_type:
            continue

        log.info(
            "check_linked_option_values: option '%s' → metaobject type '%s'",
            opt_name, mo_type,
        )

        # Fetch all metaobjects of this type
        all_metaobjects: list[dict] = []
        all_display_names: set[str] = set()
        after = None
        while True:
            lr = __gql_client__.execute(
                list_query, variable_values={"type": mo_type, "after": after}
            )
            for edge in lr.get("metaobjects", {}).get("edges", []):
                node = edge["node"]
                dn = (node.get("displayName") or "").strip()
                all_metaobjects.append({"displayName": dn, "gid": node["id"]})
                all_display_names.add(dn)
            pi = lr.get("metaobjects", {}).get("pageInfo", {})
            if not pi.get("hasNextPage"):
                break
            after = pi.get("endCursor")

        # Values that don't exist in the global pool at all
        truly_missing = sorted(
            v for v in missing_from_product if v not in all_display_names
        )

        if truly_missing:
            result_options[opt_name] = {
                "missing": truly_missing,
                "available": sorted(
                    all_metaobjects, key=lambda x: x["displayName"]
                ),
                "metaobject_type": mo_type,
            }
            log.info(
                "check_linked_option_values: option '%s' has %d truly missing: %s",
                opt_name, len(truly_missing), truly_missing,
            )

    return {"options": result_options}


def create_option_value_metaobject(metaobject_type: str, display_name: str) -> dict:
    """
    Create a simple metaobject (e.g. a size) given only its desired
    display name.  Discovers the definition's ``displayNameKey`` to
    determine which field to populate, then delegates to the generic
    :func:`create_color_metaobject` creator.

    Returns the same shape as ``create_color_metaobject``.
    """
    import logging
    log = logging.getLogger(__name__)

    # Find the display-name field key from the definition
    def_query = gql("""
    query {
        metaobjectDefinitions(first: 250) {
            edges {
                node {
                    type
                    displayNameKey
                    fieldDefinitions {
                        key
                        name
                        type { name }
                    }
                }
            }
        }
    }
    """)
    def_result = __gql_client__.execute(def_query)
    display_key = None
    for edge in def_result.get("metaobjectDefinitions", {}).get("edges", []):
        node = edge["node"]
        if node.get("type") == metaobject_type:
            display_key = node.get("displayNameKey")
            if not display_key:
                # Fallback: use the first single_line_text_field
                for fd in node.get("fieldDefinitions", []):
                    ft = (fd.get("type", {}).get("name") or "").lower()
                    if "single_line_text" in ft:
                        display_key = fd["key"]
                        break
            break

    if not display_key:
        log.warning(
            "create_option_value_metaobject: could not determine display "
            "name field for type '%s', trying 'name'", metaobject_type,
        )
        display_key = "name"

    log.info(
        "create_option_value_metaobject: type=%s display_key=%s value=%s",
        metaobject_type, display_key, display_name,
    )
    return create_color_metaobject(
        metaobject_type, display_name, {display_key: display_name}
    )


def create_color_metaobject(
    metaobject_type: str,
    display_name: str,
    fields: dict[str, any],
) -> dict:
    """
    Create a new color metaobject in Shopify.

    *metaobject_type*: e.g. 'component_colors--farve'
    *display_name*: The display name for the color (e.g. 'Olive Green')
    *fields*: A dict of field_key → value.  For metaobject references
              the value should be the GID string, for list.metaobject_reference
              it should be a JSON-encoded list of GID strings.

    Returns: {"metaobject": {"id": "...", "displayName": "..."}, "errors": [...]}
    """
    import logging
    import re
    log = logging.getLogger(__name__)

    # Build a URL-safe handle from the display name
    handle = re.sub(r'[^a-z0-9]+', '-', display_name.lower()).strip('-')

    field_inputs = []
    for key, value in fields.items():
        field_inputs.append({"key": key, "value": str(value)})

    create_mutation = gql("""
    mutation metaobjectCreate($metaobject: MetaobjectCreateInput!) {
        metaobjectCreate(metaobject: $metaobject) {
            metaobject {
                id
                displayName
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    metaobject_input = {
        "type": metaobject_type,
        "handle": handle,
        "fields": field_inputs,
    }

    log.info("create_color_metaobject: creating %s (type=%s, handle=%s, fields=%s)",
             display_name, metaobject_type, handle, field_inputs)

    try:
        result = __gql_client__.execute(
            create_mutation,
            variable_values={"metaobject": metaobject_input},
        )

        user_errors = result.get("metaobjectCreate", {}).get("userErrors", [])
        created = result.get("metaobjectCreate", {}).get("metaobject")

        errors = [f"{e.get('field', '?')}: {e['message']}" for e in user_errors] if user_errors else []

        if created:
            log.info("create_color_metaobject: created %s → %s",
                     created["displayName"], created["id"])
        else:
            log.warning("create_color_metaobject: no metaobject returned, errors=%s", errors)

        return {
            "metaobject": {"id": created["id"], "displayName": created["displayName"]} if created else None,
            "errors": errors,
        }

    except Exception as exc:
        log.exception("create_color_metaobject: exception during creation")
        return {"metaobject": None, "errors": [str(exc)]}


def generate_diagonal_swatch(
    top_left: dict,
    bottom_right: dict,
    size: int = 300,
) -> bytes:
    """
    Generate a *size*×*size* PNG swatch image split diagonally from the
    top-left corner to the bottom-right corner.

    Each half is defined by a dict ``{"type": "code"|"image", "value": ...}``.
    - ``type="code"`` → ``value`` is a hex colour string (e.g. ``#3498db``)
    - ``type="image"`` → ``value`` is an HTTP(S) URL to an image

    Returns the PNG image as raw ``bytes``.
    """
    import io
    import requests as _requests
    from PIL import Image, ImageDraw

    def _fill(spec: dict) -> Image.Image:
        """Return a *size*×*size* image for one half."""
        if spec.get("type") == "image":
            val = spec.get("value", "")
            if val.startswith("data:"):
                import base64 as _b64
                _, b64data = val.split(",", 1)
                img = Image.open(io.BytesIO(_b64.b64decode(b64data))).convert("RGBA")
                img = img.resize((size, size), Image.LANCZOS)
                return img
            if val.startswith("http"):
                resp = _requests.get(val, timeout=15)
                resp.raise_for_status()
                img = Image.open(io.BytesIO(resp.content)).convert("RGBA")
                img = img.resize((size, size), Image.LANCZOS)
                return img
        # Default: solid colour
        colour = spec.get("value", "#000000")
        img = Image.new("RGBA", (size, size), colour)
        return img

    top_img = _fill(top_left)
    bot_img = _fill(bottom_right)

    # Create the diagonal mask: white = top-left half, black = bottom-right
    mask = Image.new("L", (size, size), 0)
    draw = ImageDraw.Draw(mask)
    # Triangle covering top-left above the diagonal
    draw.polygon([(0, 0), (size, 0), (0, size)], fill=255)

    # Composite: use top_img where mask is white, bot_img elsewhere
    result = Image.composite(top_img, bot_img, mask)

    buf = io.BytesIO()
    result.convert("RGB").save(buf, format="PNG")
    return buf.getvalue()


def upload_swatch_bytes_to_shopify(
    png_bytes: bytes,
    filename: str = "swatch.png",
    alt: str = "",
) -> str:
    """
    Upload raw PNG bytes to Shopify via a staged upload and return the
    resulting file GID.

    Steps:
    1. Request a staged-upload target from Shopify.
    2. POST the bytes to the target URL.
    3. Call ``fileCreate`` with the ``resourceUrl``.
    4. Poll until READY.

    Returns the file GID (e.g. ``gid://shopify/MediaImage/…``).
    """
    import time
    import requests as _requests

    _log.info("upload_swatch_bytes_to_shopify: %d bytes, filename=%s", len(png_bytes), filename)

    # 1. Staged upload target
    targets = create_staged_uploads([{
        "filename": filename,
        "mimeType": "image/png",
        "fileSize": len(png_bytes),
    }])
    if not targets:
        raise RuntimeError("No staged upload targets returned")

    target = targets[0]
    upload_url = target["url"]
    resource_url = target["resourceUrl"]
    params = {p["name"]: p["value"] for p in target["parameters"]}

    # 2. Multipart POST to the staged URL
    files_payload = {
        "file": (filename, png_bytes, "image/png"),
    }
    resp = _requests.post(upload_url, data=params, files=files_payload, timeout=30)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"Staged upload POST failed: {resp.status_code} {resp.text[:200]}")
    _log.info("upload_swatch_bytes_to_shopify: staged upload complete, resourceUrl=%s", resource_url)

    # 3. fileCreate with the resourceUrl
    create_mutation = gql("""
    mutation fileCreate($files: [FileCreateInput!]!) {
        fileCreate(files: $files) {
            files {
                id
                alt
                ... on MediaImage { id image { url } }
            }
            userErrors { field message }
        }
    }
    """)

    result = __gql_client__.execute(create_mutation, variable_values={
        "files": [{
            "originalSource": resource_url,
            "filename": filename,
            "alt": alt,
            "contentType": "IMAGE",
        }],
    })

    user_errors = result.get("fileCreate", {}).get("userErrors", [])
    if user_errors:
        msgs = [e.get("message", "?") for e in user_errors]
        raise RuntimeError(f"fileCreate errors: {'; '.join(msgs)}")

    files_out = result.get("fileCreate", {}).get("files", [])
    if not files_out:
        raise RuntimeError("fileCreate returned no files")

    file_gid = files_out[0]["id"]
    _log.info("upload_swatch_bytes_to_shopify: created %s, polling…", file_gid)

    # 4. Poll until READY
    poll_query = gql("""
    query fileStatus($ids: [ID!]!) {
        nodes(ids: $ids) {
            ... on MediaImage { id fileStatus }
            ... on GenericFile { id fileStatus }
        }
    }
    """)
    for attempt in range(15):
        time.sleep(2)
        poll_result = __gql_client__.execute(poll_query, variable_values={"ids": [file_gid]})
        nodes = poll_result.get("nodes", [])
        if nodes and nodes[0]:
            status = nodes[0].get("fileStatus", "PROCESSING")
            _log.info("upload_swatch_bytes_to_shopify: poll %d — %s", attempt + 1, status)
            if status == "READY":
                return file_gid
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"File upload failed: {status}")
    _log.warning("upload_swatch_bytes_to_shopify: timed out, returning %s anyway", file_gid)
    return file_gid


def upload_file_to_shopify(source_url: str, alt: str = "") -> str:
    """
    Upload (or register) an external image file in Shopify's Files section
    using the ``fileCreate`` mutation and return the file GID that can be
    used as a ``file_reference`` value in metaobject fields.

    The function polls for up to ~30 seconds until the file reaches a
    terminal status (``READY``, ``FAILED``, ``UPLOADED``).

    Returns the file GID string (e.g. ``gid://shopify/MediaImage/…``),
    or raises ``RuntimeError`` on failure.
    """
    import logging
    import time
    import os
    log = logging.getLogger(__name__)

    # Derive a filename from the URL
    filename = os.path.basename(source_url.split("?")[0]) or "swatch.png"

    log.info("upload_file_to_shopify: creating file from %s (alt=%r)", source_url, alt)

    create_mutation = gql("""
    mutation fileCreate($files: [FileCreateInput!]!) {
        fileCreate(files: $files) {
            files {
                id
                alt
                ... on MediaImage {
                    id
                    image { url }
                }
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    result = __gql_client__.execute(create_mutation, variable_values={
        "files": [{
            "originalSource": source_url,
            "filename": filename,
            "alt": alt,
            "contentType": "IMAGE",
        }],
    })

    user_errors = result.get("fileCreate", {}).get("userErrors", [])
    if user_errors:
        msgs = [e.get("message", "Unknown error") for e in user_errors]
        log.error("upload_file_to_shopify: fileCreate errors: %s", msgs)
        raise RuntimeError(f"Shopify fileCreate errors: {'; '.join(msgs)}")

    files = result.get("fileCreate", {}).get("files", [])
    if not files:
        raise RuntimeError("Shopify fileCreate returned no files")

    file_gid = files[0].get("id")
    log.info("upload_file_to_shopify: created file %s, polling for READY…", file_gid)

    # Poll until the file is ready (Shopify processes asynchronously)
    poll_query = gql("""
    query fileStatus($ids: [ID!]!) {
        nodes(ids: $ids) {
            ... on MediaImage {
                id
                fileStatus
            }
            ... on GenericFile {
                id
                fileStatus
            }
        }
    }
    """)

    max_attempts = 15
    for attempt in range(max_attempts):
        time.sleep(2)
        poll_result = __gql_client__.execute(poll_query, variable_values={"ids": [file_gid]})
        nodes = poll_result.get("nodes", [])
        if nodes and nodes[0]:
            status = nodes[0].get("fileStatus", "PROCESSING")
            log.info("upload_file_to_shopify: poll %d/%d — status=%s",
                     attempt + 1, max_attempts, status)
            if status == "READY":
                log.info("upload_file_to_shopify: file %s is READY", file_gid)
                return file_gid
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"File upload failed with status: {status}")
        else:
            log.warning("upload_file_to_shopify: poll returned empty node for %s", file_gid)

    # If we reached here, return the GID anyway — it may still be processing
    # but Shopify often accepts the reference while it finalizes
    log.warning("upload_file_to_shopify: timed out waiting for READY, returning %s anyway", file_gid)
    return file_gid


def add_variants_to_shopify_product(product_id: str, variants_data: list[dict], color_image_urls: dict[str, str] | None = None) -> dict:
    """
    Add new variants to an existing Shopify product using
    productVariantsBulkCreate.

    Each entry in *variants_data* should contain:
      sku, barcode/ean, price, weight, country_of_origin, hs_code

    *color_image_urls* is an optional mapping of color name → image URL.
    When provided, images are uploaded to the product and assigned to the
    matching newly-created variants.

    Returns a dict with 'created' (list of created variant IDs) and
    'errors' (list of error messages).
    """
    import logging
    log = logging.getLogger(__name__)

    if not variants_data:
        log.info("add_variants: no variants_data, returning early")
        return {"created": [], "errors": []}

    # First, get the product's inventory location ID
    location_query = gql("""
    query {
        locations(first: 1) {
            edges {
                node {
                    id
                }
            }
        }
    }
    """)
    location_result = __gql_client__.execute(location_query)
    location_edges = location_result.get("locations", {}).get("edges", [])
    if not location_edges:
        return {"created": [], "errors": ["No inventory locations found in Shopify"]}
    location_id = location_edges[0]["node"]["id"]

    # Fetch the price from the first existing variant and the product's
    # option definitions (with IDs) to handle metafield-linked options.
    # Also fetch ALL existing variants so we can detect seed/duplicate
    # variants created by productOptionsCreate.
    product_info_query = gql("""
    query productInfo($id: ID!) {
        product(id: $id) {
            variants(first: 100) {
                edges {
                    node {
                        id
                        sku
                        price
                        selectedOptions {
                            name
                            value
                        }
                    }
                }
            }
            options {
                id
                name
                linkedMetafield {
                    namespace
                    key
                }
                optionValues {
                    id
                    name
                    linkedMetafieldValue
                }
            }
        }
    }
    """)
    product_info = __gql_client__.execute(
        product_info_query, variable_values={"id": product_id}
    )
    product_data = product_info.get("product", {})

    existing_price: str | None = None
    price_edges = product_data.get("variants", {}).get("edges", [])
    if price_edges:
        existing_price = price_edges[0]["node"].get("price")
    log.info(
        "add_variants: existing price for %s = %s", product_id, existing_price
    )

    # Build a map of existing variant option-combos → variant GID so we
    # can detect seed variants created by productOptionsCreate and update
    # them instead of trying to re-create them.
    existing_variants: dict[tuple, dict] = {}
    for edge in price_edges:
        node = edge["node"]
        combo = tuple(
            sorted(
                (so["name"], so["value"])
                for so in node.get("selectedOptions", [])
                if so["name"] != "Title"
            )
        )
        existing_variants[combo] = {
            "id": node["id"],
            "sku": node.get("sku", ""),
        }
    log.info(
        "add_variants: found %d existing variant(s) on product",
        len(existing_variants),
    )

    # Build option lookup structures.
    # For metafield-linked options we must use optionId + linkedMetafieldValue (the GID).
    # We key the values map by the human-readable 'name' so we can look up
    # the corresponding GID when building the mutation input.
    # option_name -> {"id": option GID, "linked": bool,
    #                 "values_by_name": {display_name: {"id": ov_id, "gid": linkedMetafieldValue}}}
    option_info: dict[str, dict] = {}
    for opt in product_data.get("options", []):
        is_linked = bool(opt.get("linkedMetafield"))
        values_by_name = {}
        for ov in opt.get("optionValues", []):
            display_name = (ov.get("name") or "").strip()
            values_by_name[display_name] = {
                "id": ov["id"],
                "gid": ov.get("linkedMetafieldValue"),  # the metafield GID
            }
        option_info[opt["name"]] = {
            "id": opt["id"],
            "linked": is_linked,
            "values_by_name": values_by_name,
        }
    log.info(
        "add_variants: option_info = %s",
        {k: {"id": v["id"], "linked": v["linked"], "values": {
            name: {"id": val["id"], "gid": val["gid"]}
            for name, val in v["values_by_name"].items()
        }} for k, v in option_info.items()},
    )

    EUR_TO_DKK = 7.5

    def _eur_to_dkk_retail(eur_price: float) -> str:
        """Convert EUR price to DKK retail price with tiered rounding.

        Rules:
        - Multiply by 7.5 to convert EUR → DKK
        - ≤ 100 DKK: round up to nearest 10, subtract 1  (e.g. 78 → 79)
        - 101–800 DKK: round up to nearest 50, subtract 1 (e.g. 420 → 449)
        - > 800 DKK: round up to nearest 100, subtract 1  (e.g. 850 → 899)
        """
        import math
        dkk = eur_price * EUR_TO_DKK
        if dkk <= 100:
            rounded = math.ceil(dkk / 10) * 10
        elif dkk <= 800:
            rounded = math.ceil(dkk / 50) * 50
        else:
            rounded = math.ceil(dkk / 100) * 100
        return f"{rounded - 1:.2f}"

    # Use module-level constants for length handling
    LENGTH_NAMES = _LENGTH_NAMES

    # Determine whether this batch of variants has multiple length values.
    # If so, include the "Længde" option on each variant.
    length_letters = set()
    for v in variants_data:
        letter = _extract_length_letter(v.get("sku", ""))
        if letter:
            length_letters.add(letter)
    include_length = len(length_letters) > 1
    log.info(
        "add_variants: length letters=%s → include_length=%s",
        length_letters, include_length,
    )

    # ── Pre-create missing metafield-linked option values ──────────
    # For metafield-linked options, new values must be created via
    # productOptionUpdate BEFORE we can reference them in the variant
    # bulk-create mutation.
    #
    # 1. Collect all option values we'll need across all variants.
    # 2. Find which ones are missing from the product.
    # 3. Create them and store the returned ProductOptionValue IDs.

    def _collect_needed_values(variants: list[dict]) -> dict[str, set[str]]:
        """Return {option_name: {value, ...}} for all values we'll need."""
        needed: dict[str, set[str]] = {}
        for v in variants:
            size = (v.get("size") or "").strip()
            if "/" in size:
                size = size.split("/", 1)[0].strip()
            size = _normalize_size(size)
            if size and size.lower() != "one size":
                needed.setdefault("Størrelse", set()).add(size)
            color = (v.get("color") or "").strip()
            if color:
                needed.setdefault("Farve", set()).add(color)
            if include_length:
                letter = _extract_length_letter(v.get("sku", ""))
                length_name = LENGTH_NAMES.get(letter, letter) if letter else None
                if length_name:
                    needed.setdefault("Længde", set()).add(length_name)
        return needed

    needed_values = _collect_needed_values(variants_data)

    update_option_mutation = gql("""
    mutation productOptionUpdate(
        $productId: ID!,
        $option: OptionUpdateInput!,
        $optionValuesToAdd: [OptionValueCreateInput!]
    ) {
        productOptionUpdate(
            productId: $productId,
            option: $option,
            optionValuesToAdd: $optionValuesToAdd
        ) {
            product {
                options {
                    id
                    name
                    optionValues {
                        id
                        name
                        linkedMetafieldValue
                    }
                }
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    def _refresh_option_info(mutation_result: dict) -> None:
        """Refresh option_info from the productOptionUpdate result."""
        updated_options = (
            mutation_result.get("productOptionUpdate", {})
            .get("product", {})
            .get("options", [])
        )
        for opt in updated_options:
            name = opt["name"]
            if name in option_info:
                new_values = {}
                for ov in opt.get("optionValues", []):
                    display_name = (ov.get("name") or "").strip()
                    new_values[display_name] = {
                        "id": ov["id"],
                        "gid": ov.get("linkedMetafieldValue"),
                    }
                option_info[name]["values_by_name"] = new_values
                option_info[name]["id"] = opt["id"]

    def _fetch_metaobject_gids(opt_info: dict, display_names: list[str]) -> dict[str, str]:
        """
        For a metafield-linked option, resolve display names to metaobject GIDs.

        Uses an existing option value's linkedMetafieldValue GID to discover the
        metaobject type, then fetches all metaobjects of that type and returns
        a mapping of display_name → metaobject GID.
        """
        # Find an existing GID to determine the metaobject type
        sample_gid = None
        for val in opt_info["values_by_name"].values():
            if val.get("gid"):
                sample_gid = val["gid"]
                break
        if not sample_gid:
            log.warning("_fetch_metaobject_gids: no existing GID to determine type")
            return {}

        # Query the sample metaobject to get its type
        type_query = gql("""
        query metaobjectType($id: ID!) {
            metaobject(id: $id) {
                type
            }
        }
        """)
        type_result = __gql_client__.execute(type_query, variable_values={"id": sample_gid})
        mo_type = type_result.get("metaobject", {}).get("type")
        if not mo_type:
            log.warning("_fetch_metaobject_gids: could not determine metaobject type from %s", sample_gid)
            return {}

        log.info("_fetch_metaobject_gids: metaobject type = %s", mo_type)

        # Fetch all metaobjects of this type and build display_name → GID map
        list_query = gql("""
        query metaobjectsByType($type: String!, $after: String) {
            metaobjects(type: $type, first: 250, after: $after) {
                edges {
                    node {
                        id
                        displayName
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
        """)

        name_to_gid: dict[str, str] = {}
        names_needed = set(display_names)
        after = None
        while True:
            list_result = __gql_client__.execute(
                list_query, variable_values={"type": mo_type, "after": after}
            )
            for edge in list_result.get("metaobjects", {}).get("edges", []):
                node = edge["node"]
                dn = (node.get("displayName") or "").strip()
                if dn in names_needed:
                    name_to_gid[dn] = node["id"]
            page_info = list_result.get("metaobjects", {}).get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")

        log.info("_fetch_metaobject_gids: resolved %d/%d names: %s",
                 len(name_to_gid), len(display_names), name_to_gid)
        return name_to_gid

    for opt_name, values in needed_values.items():
        info = option_info.get(opt_name)
        if not info:
            continue

        missing = [val for val in values if val not in info["values_by_name"]]
        if not missing:
            continue

        log.info(
            "add_variants: creating %d missing value(s) for option '%s' (linked=%s): %s",
            len(missing), opt_name, info["linked"], missing,
        )

        if info["linked"]:
            # Resolve display names to metaobject GIDs
            name_to_gid = _fetch_metaobject_gids(info, missing)
            values_to_add = []
            for val in missing:
                gid = name_to_gid.get(val)
                if gid:
                    values_to_add.append({"linkedMetafieldValue": gid})
                else:
                    log.warning(
                        "add_variants: could not resolve metaobject GID for '%s'='%s' — skipping",
                        opt_name, val,
                    )
            if not values_to_add:
                log.warning("add_variants: no resolvable values for '%s', skipping", opt_name)
                continue
        else:
            values_to_add = [{"name": val} for val in missing]

        try:
            result = __gql_client__.execute(update_option_mutation, variable_values={
                "productId": product_id,
                "option": {"id": info["id"]},
                "optionValuesToAdd": values_to_add,
            })

            user_errors = result.get("productOptionUpdate", {}).get("userErrors", [])
            if user_errors:
                log.warning(
                    "add_variants: errors creating option values for '%s': %s",
                    opt_name, user_errors,
                )

            _refresh_option_info(result)
            log.info(
                "add_variants: refreshed '%s' values: %s",
                opt_name,
                {k: v["id"] for k, v in option_info[opt_name]["values_by_name"].items()},
            )
        except Exception as exc:
            log.exception(
                "add_variants: failed to create option values for '%s'", opt_name
            )
            return {"created": [], "errors": [
                f"Failed to create option values for {opt_name}: {exc}"
            ]}

    # ── Build variant inputs ─────────────────────────────────────
    variant_inputs = []
    for v in variants_data:
        weight = None
        if v.get("weight"):
            try:
                weight = float(v["weight"])
            except (ValueError, TypeError):
                pass

        cost = None
        if v.get("price"):
            try:
                cost = round(float(v["price"]) * EUR_TO_DKK, 2)
            except (ValueError, TypeError):
                pass

        variant_input = {
            "barcode": v.get("ean", "") or v.get("barcode", ""),
            "inventoryPolicy": "DENY",
            "inventoryItem": {
                "sku": v.get("sku", ""),
                "tracked": True,
                "countryCodeOfOrigin": v.get("country_of_origin", "") or None,
                "harmonizedSystemCode": (v.get("hs_code", "") or "")[:6] or None,
                "cost": cost,
            },
            "inventoryQuantities": [{
                "locationId": location_id,
                "availableQuantity": 0,
            }],
        }

        # Set the retail price.
        # If the product already carries a non-zero price (existing variants),
        # reuse it.  Otherwise derive the retail price from the CSV cost.
        if existing_price is not None and float(existing_price) > 0:
            variant_input["price"] = existing_price
        elif v.get("price"):
            try:
                variant_input["price"] = _eur_to_dkk_retail(float(v["price"]))
            except (ValueError, TypeError):
                pass

        if weight is not None:
            variant_input["inventoryItem"]["measurement"] = {
                "weight": {
                    "value": weight,
                    "unit": "KILOGRAMS",
                },
            }

        # Build option values from Size, Color, and Length.
        # All option values are referenced by their ProductOptionValue GID.
        # Metafield-linked options also include the linkedMetafieldValue GID.
        # Missing values were pre-created above, so all should have IDs.
        option_values = []

        def _add_option(option_name: str, value: str) -> None:
            info = option_info.get(option_name)
            if not info:
                # Option doesn't exist on the product yet (e.g. newly created
                # product).  Use optionName + name so that
                # productVariantsBulkCreate auto-creates the option.
                option_values.append({
                    "optionName": option_name,
                    "name": value,
                })
                return
            existing = info["values_by_name"].get(value)
            if not existing:
                log.warning(
                    "add_variants: no ProductOptionValue ID for '%s'='%s' — skipping",
                    option_name, value,
                )
                return

            entry: dict = {
                "optionName": option_name,
                "id": existing["id"],
            }
            # Metafield-linked options also need the metaobject GID
            if info["linked"] and existing.get("gid"):
                entry["linkedMetafieldValue"] = existing["gid"]
            option_values.append(entry)

        size = (v.get("size") or "").strip()
        # Strip the length suffix (e.g. "XL/Regular" → "XL")
        if "/" in size:
            size = size.split("/", 1)[0].strip()
        size = _normalize_size(size)
        if size and size.lower() != "one size":
            _add_option("Størrelse", size)
        if v.get("color"):
            _add_option("Farve", v["color"])
        if include_length:
            letter = _extract_length_letter(v.get("sku", ""))
            length_name = LENGTH_NAMES.get(letter, letter) if letter else None
            if length_name:
                _add_option("Længde", length_name)

        if option_values:
            variant_input["optionValues"] = option_values

        # Build a combo key (same format as existing_variants) for dedup
        combo_parts = []
        for ov in option_values:
            opt_name = ov["optionName"]
            oi = option_info.get(opt_name, {})
            display_name = None
            if "id" in ov:
                for dn, vinfo in oi.get("values_by_name", {}).items():
                    if vinfo["id"] == ov["id"]:
                        display_name = dn
                        break
            if display_name is None:
                display_name = ov.get("name", "")
            combo_parts.append((opt_name, display_name))
        combo_key = tuple(sorted(combo_parts))

        variant_inputs.append((variant_input, combo_key))

    # ── Separate seed variants (already exist) from truly new ones ──
    # Seed variants were auto-created by productOptionsCreate and need
    # to be updated (to fill in SKU, barcode, cost, etc.) rather than
    # re-created.
    variants_to_create = []
    variants_to_update = []   # (existing_variant_id, variant_input)
    for vi, combo_key in variant_inputs:
        existing = existing_variants.get(combo_key)
        if existing:
            variants_to_update.append((existing["id"], vi))
        else:
            variants_to_create.append(vi)

    log.info(
        "add_variants: %d to create, %d to update (seed variants)",
        len(variants_to_create), len(variants_to_update),
    )

    all_created: list[dict] = []
    all_errors: list[str] = []

    # ── Update seed variants via productVariantsBulkUpdate ─────────
    if variants_to_update:
        update_inputs = []
        for var_id, vi in variants_to_update:
            update_entry: dict = {"id": var_id}
            if "barcode" in vi:
                update_entry["barcode"] = vi["barcode"]
            if "price" in vi:
                update_entry["price"] = vi["price"]
            if "inventoryPolicy" in vi:
                update_entry["inventoryPolicy"] = vi["inventoryPolicy"]
            if "inventoryItem" in vi:
                update_entry["inventoryItem"] = vi["inventoryItem"]
            update_inputs.append(update_entry)

        update_mutation = gql("""
        mutation productVariantsBulkUpdate(
            $productId: ID!,
            $variants: [ProductVariantsBulkInput!]!
        ) {
            productVariantsBulkUpdate(
                productId: $productId,
                variants: $variants
            ) {
                productVariants {
                    id
                    sku
                    barcode
                    title
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """)

        try:
            log.info(
                "add_variants: updating %d seed variant(s)", len(update_inputs)
            )
            update_result = __gql_client__.execute(
                update_mutation,
                variable_values={
                    "productId": product_id,
                    "variants": update_inputs,
                },
            )
            log.info("add_variants: update result = %s", update_result)

            update_errors = (
                update_result.get("productVariantsBulkUpdate", {})
                .get("userErrors", [])
            )
            updated_variants = (
                update_result.get("productVariantsBulkUpdate", {})
                .get("productVariants", [])
            )
            if update_errors:
                all_errors.extend(
                    f"{e['field']}: {e['message']}" for e in update_errors
                )
            for uv in (updated_variants or []):
                all_created.append({
                    "id": uv["id"],
                    "sku": uv["sku"],
                    "title": uv["title"],
                })
            log.info(
                "add_variants: updated=%d errors=%s",
                len(updated_variants or []),
                [e["message"] for e in update_errors] if update_errors else [],
            )
        except Exception as exc:
            log.exception("add_variants: exception during seed variant update")
            all_errors.append(f"Failed to update seed variant(s): {exc}")

    # ── Bulk-create the remaining new variants ─────────────────────
    if not variants_to_create:
        log.info("add_variants: no new variants to create (all were seed variants)")
        # Still handle images for updated variants
        if color_image_urls and all_created:
            image_errors = _attach_color_images(
                product_id, all_created, variants_data, color_image_urls, log
            )
            all_errors.extend(image_errors)
        if all_created:
            colors_already_uploaded = set(color_image_urls.keys()) if color_image_urls else set()
            reuse_errors = _reuse_existing_color_images(
                product_id, all_created, variants_data, colors_already_uploaded, log
            )
            all_errors.extend(reuse_errors)
        return {"created": all_created, "errors": all_errors}

    mutation = gql("""
    mutation productVariantsBulkCreate(
        $productId: ID!,
        $variants: [ProductVariantsBulkInput!]!,
        $strategy: ProductVariantsBulkCreateStrategy
    ) {
        productVariantsBulkCreate(
            productId: $productId,
            variants: $variants,
            strategy: $strategy
        ) {
            productVariants {
                id
                sku
                barcode
                title
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    # For products that only have the default "Title" option (i.e. newly
    # created products), use REMOVE_STANDALONE_VARIANT so the placeholder
    # variant is deleted and new options can be auto-created via optionName.
    is_default_only = (
        len(option_info) <= 1
        and "Title" in option_info
        and len(option_info.get("Title", {}).get("values_by_name", {})) <= 1
    )
    strategy = "REMOVE_STANDALONE_VARIANT" if is_default_only else None

    log.info(
        "add_variants: sending mutation for product %s with %d variant(s), strategy=%s",
        product_id, len(variants_to_create), strategy,
    )
    log.info("add_variants: variant_inputs = %s", variants_to_create)

    try:
        variables: dict = {
            "productId": product_id,
            "variants": variants_to_create,
        }
        if strategy:
            variables["strategy"] = strategy
        result = __gql_client__.execute(mutation, variable_values=variables)

        log.info("add_variants: raw result = %s", result)

        user_errors = result.get("productVariantsBulkCreate", {}).get("userErrors", [])
        created_variants = result.get("productVariantsBulkCreate", {}).get("productVariants", [])

        if user_errors:
            all_errors.extend(f"{e['field']}: {e['message']}" for e in user_errors)
        for cv in (created_variants or []):
            all_created.append({"id": cv["id"], "sku": cv["sku"], "title": cv["title"]})

        log.info("add_variants: created=%d errors=%d", len(created_variants or []), len(all_errors))

        # ── Attach color images to new variants ──────────────────
        if color_image_urls and all_created:
            image_errors = _attach_color_images(
                product_id, all_created, variants_data, color_image_urls, log
            )
            all_errors.extend(image_errors)

        # ── Reuse existing color images for new size variants ────
        # For colors that already have an image on existing variants
        # (and weren't covered by a fresh upload), assign the same
        # media to the newly created variants.
        if all_created:
            colors_already_uploaded = set(color_image_urls.keys()) if color_image_urls else set()
            reuse_errors = _reuse_existing_color_images(
                product_id, all_created, variants_data, colors_already_uploaded, log
            )
            all_errors.extend(reuse_errors)

        return {"created": all_created, "errors": all_errors}
    except Exception as exc:
        log.exception("add_variants: exception during mutation")
        all_errors.append(str(exc))
        return {"created": all_created, "errors": all_errors}


def _reuse_existing_color_images(
    product_id: str,
    created_variants: list[dict],
    variants_data: list[dict],
    colors_already_uploaded: set[str],
    log,
) -> list[str]:
    """
    For newly created variants whose color already has an image on
    *existing* variants of the same product, assign the same media.

    Skips any color that was freshly uploaded (in *colors_already_uploaded*).

    Returns a list of error messages (empty on success).
    """
    errors: list[str] = []

    # Build SKU → color lookup from the original variant data
    sku_to_color: dict[str, str] = {}
    for v in variants_data:
        sku = v.get("sku", "")
        color = (v.get("color") or "").strip()
        if sku and color:
            sku_to_color[sku] = color

    # Determine which colors the new variants need (excluding freshly uploaded)
    new_colors: set[str] = set()
    for cv in created_variants:
        color = sku_to_color.get(cv.get("sku", ""))
        if color and color not in colors_already_uploaded:
            new_colors.add(color)

    if not new_colors:
        log.info("_reuse_existing_color_images: no colors need image reuse")
        return errors

    log.info(
        "_reuse_existing_color_images: checking existing images for colors: %s",
        new_colors,
    )

    # Query all variants of the product with their selected options and media
    variants_query = gql("""
    query productVariants($id: ID!, $after: String) {
        product(id: $id) {
            variants(first: 100, after: $after) {
                edges {
                    node {
                        id
                        selectedOptions { name value }
                        media(first: 1) {
                            edges {
                                node {
                                    ... on MediaImage { id }
                                }
                            }
                        }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)

    # Build color → media ID from existing variants
    color_to_media: dict[str, str] = {}
    created_ids = {cv["id"] for cv in created_variants}
    after = None
    while True:
        result = __gql_client__.execute(
            variants_query, variable_values={"id": product_id, "after": after}
        )
        for edge in result.get("product", {}).get("variants", {}).get("edges", []):
            node = edge["node"]
            # Skip newly created variants (they won't have images yet)
            if node["id"] in created_ids:
                continue
            media_edges = node.get("media", {}).get("edges", [])
            if not media_edges:
                continue
            media_id = media_edges[0].get("node", {}).get("id")
            if not media_id:
                continue
            # Find the color option for this variant
            for opt in node.get("selectedOptions", []):
                if opt["name"] == "Farve":
                    color = (opt.get("value") or "").strip()
                    if color in new_colors and color not in color_to_media:
                        color_to_media[color] = media_id
                    break
        pi = result.get("product", {}).get("variants", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    if not color_to_media:
        log.info("_reuse_existing_color_images: no existing images found to reuse")
        return errors

    log.info(
        "_reuse_existing_color_images: found existing images: %s",
        {c: mid for c, mid in color_to_media.items()},
    )

    # Build color → list of new variant IDs that need the image
    color_to_new_ids: dict[str, list[str]] = {}
    for cv in created_variants:
        color = sku_to_color.get(cv.get("sku", ""))
        if color and color in color_to_media:
            color_to_new_ids.setdefault(color, []).append(cv["id"])

    if not color_to_new_ids:
        return errors

    # Assign existing media to new variants
    update_mutation = gql("""
    mutation productVariantsBulkUpdate(
        $productId: ID!,
        $variants: [ProductVariantsBulkInput!]!
    ) {
        productVariantsBulkUpdate(
            productId: $productId,
            variants: $variants
        ) {
            productVariants { id sku }
            userErrors { field message }
        }
    }
    """)

    for color, variant_ids in color_to_new_ids.items():
        media_id = color_to_media[color]
        log.info(
            "_reuse_existing_color_images: assigning media %s (color=%s) to %d new variant(s)",
            media_id, color, len(variant_ids),
        )
        variant_updates = [
            {"id": vid, "mediaId": media_id}
            for vid in variant_ids
        ]
        try:
            update_result = __gql_client__.execute(update_mutation, variable_values={
                "productId": product_id,
                "variants": variant_updates,
            })
            update_errors = update_result.get("productVariantsBulkUpdate", {}).get("userErrors", [])
            if update_errors:
                for ue in update_errors:
                    errors.append(f"Image reuse error ({color}): {ue.get('message', 'Unknown')}")
                log.warning("_reuse_existing_color_images: errors for '%s': %s", color, update_errors)
            else:
                log.info("_reuse_existing_color_images: assigned image for color '%s'", color)
        except Exception as exc:
            log.exception("_reuse_existing_color_images: failed for color '%s'", color)
            errors.append(f"Failed to reuse image for {color}: {exc}")

    return errors


def _attach_color_images(
    product_id: str,
    created_variants: list[dict],
    variants_data: list[dict],
    color_image_urls: dict[str, str],
    log,
) -> list[str]:
    """
    Upload images by URL to a Shopify product and assign them to the
    newly-created variants based on color.

    Uses the Shopify GraphQL ``productCreateMedia`` mutation to upload,
    then ``productVariantsBulkUpdate`` to assign each media item to its
    variants.

    Returns a list of error messages (empty if everything succeeded).
    """
    import time

    errors: list[str] = []

    # Build SKU → color lookup from the original variant data
    sku_to_color: dict[str, str] = {}
    for v in variants_data:
        sku = v.get("sku", "")
        color = (v.get("color") or "").strip()
        if sku and color:
            sku_to_color[sku] = color

    # Build color → list of created variant IDs
    color_to_variant_ids: dict[str, list[str]] = {}
    for cv in created_variants:
        sku = cv.get("sku", "")
        color = sku_to_color.get(sku)
        if color and color in color_image_urls:
            color_to_variant_ids.setdefault(color, []).append(cv["id"])

    if not color_to_variant_ids:
        log.info("_attach_color_images: no color/variant matches to attach images to")
        return errors

    log.info(
        "_attach_color_images: will upload %d image(s) for product %s: %s",
        len(color_to_variant_ids), product_id,
        {c: len(ids) for c, ids in color_to_variant_ids.items()},
    )

    # Step 1: Upload all images to the product using productCreateMedia
    create_media_mutation = gql("""
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
        productCreateMedia(productId: $productId, media: $media) {
            media {
                ... on MediaImage {
                    id
                    alt
                    status
                    image {
                        url
                    }
                }
            }
            mediaUserErrors {
                field
                message
            }
        }
    }
    """)

    media_inputs = []
    color_order = []  # track which color each media input corresponds to
    for color, url in color_image_urls.items():
        if color not in color_to_variant_ids:
            continue
        media_inputs.append({
            "originalSource": url,
            "alt": color,
            "mediaContentType": "IMAGE",
        })
        color_order.append(color)

    if not media_inputs:
        return errors

    try:
        media_result = __gql_client__.execute(create_media_mutation, variable_values={
            "productId": product_id,
            "media": media_inputs,
        })

        media_errors = media_result.get("productCreateMedia", {}).get("mediaUserErrors", [])
        if media_errors:
            for me in media_errors:
                errors.append(f"Image upload error: {me.get('message', 'Unknown error')}")
            log.warning("_attach_color_images: media errors: %s", media_errors)

        created_media = media_result.get("productCreateMedia", {}).get("media", [])
        log.info("_attach_color_images: created %d media item(s)", len(created_media))

    except Exception as exc:
        log.exception("_attach_color_images: failed to upload images")
        errors.append(f"Failed to upload images: {exc}")
        return errors

    # Step 2: Wait briefly for media processing
    if created_media:
        time.sleep(2)

    # Step 3: Assign each uploaded image to its corresponding variants
    update_variants_mutation = gql("""
    mutation productVariantsBulkUpdate(
        $productId: ID!,
        $variants: [ProductVariantsBulkInput!]!
    ) {
        productVariantsBulkUpdate(
            productId: $productId,
            variants: $variants
        ) {
            productVariants {
                id
                sku
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    for i, media_item in enumerate(created_media):
        if not media_item or i >= len(color_order):
            continue

        media_id = media_item.get("id")
        color = color_order[i]
        variant_ids = color_to_variant_ids.get(color, [])

        if not media_id or not variant_ids:
            continue

        log.info(
            "_attach_color_images: assigning media %s (color=%s) to %d variant(s)",
            media_id, color, len(variant_ids),
        )

        variant_updates = [
            {"id": vid, "mediaId": media_id}
            for vid in variant_ids
        ]

        try:
            update_result = __gql_client__.execute(update_variants_mutation, variable_values={
                "productId": product_id,
                "variants": variant_updates,
            })

            update_errors = update_result.get("productVariantsBulkUpdate", {}).get("userErrors", [])
            if update_errors:
                for ue in update_errors:
                    errors.append(f"Image assign error ({color}): {ue.get('message', 'Unknown error')}")
                log.warning("_attach_color_images: update errors for %s: %s", color, update_errors)
            else:
                log.info("_attach_color_images: successfully assigned image for color '%s'", color)
        except Exception as exc:
            log.exception("_attach_color_images: failed to assign image for color '%s'", color)
            errors.append(f"Failed to assign image for {color}: {exc}")

    return errors


# ── Product Images ─────────────────────────────────────────────────

def create_staged_uploads(files: list[dict]) -> list[dict]:
    """
    Create staged upload targets for file-based image uploads.

    *files* is a list of dicts with ``filename``, ``mimeType``, ``fileSize``.

    Returns a list of upload targets, each with:
    ``url``, ``resourceUrl``, ``parameters`` ([{name, value}]).
    """
    _log.info("create_staged_uploads: %d file(s)", len(files))

    mutation = gql("""
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
        stagedUploadsCreate(input: $input) {
            stagedTargets {
                url
                resourceUrl
                parameters {
                    name
                    value
                }
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    stage_inputs = [
        {
            "filename": f["filename"],
            "mimeType": f["mimeType"],
            "httpMethod": "POST",
            "resource": "IMAGE",
            "fileSize": str(f["fileSize"]),
        }
        for f in files
    ]

    result = __gql_client__.execute(mutation, variable_values={"input": stage_inputs})

    user_errors = result.get("stagedUploadsCreate", {}).get("userErrors", [])
    if user_errors:
        msgs = [e.get("message", "Unknown error") for e in user_errors]
        _log.warning("create_staged_uploads: errors: %s", msgs)
        raise RuntimeError(f"Staged upload errors: {'; '.join(msgs)}")

    targets = result.get("stagedUploadsCreate", {}).get("stagedTargets", [])
    _log.info("create_staged_uploads: got %d target(s)", len(targets))
    return [
        {
            "url": t["url"],
            "resourceUrl": t["resourceUrl"],
            "parameters": t["parameters"],
        }
        for t in targets
    ]


def fetch_product_images(product_id: str) -> list[dict]:
    """
    Fetch all media images for a product, returning them in order.

    Returns::
        [{"id": "gid://...", "alt": "...", "url": "https://..."}, ...]
    """
    query = gql("""
    query productMedia($id: ID!, $after: String) {
        product(id: $id) {
            media(first: 100, after: $after) {
                edges {
                    node {
                        ... on MediaImage {
                            id
                            alt
                            image {
                                url
                                width
                                height
                            }
                        }
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)

    images: list[dict] = []
    after = None
    while True:
        result = __gql_client__.execute(
            query, variable_values={"id": product_id, "after": after}
        )
        edges = result.get("product", {}).get("media", {}).get("edges", [])
        for edge in edges:
            node = edge.get("node", {})
            if not node.get("id"):
                continue
            img = node.get("image") or {}
            images.append({
                "id": node["id"],
                "alt": node.get("alt", ""),
                "url": img.get("url", ""),
                "width": img.get("width"),
                "height": img.get("height"),
            })
        page_info = result.get("product", {}).get("media", {}).get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")

    _log.info("fetch_product_images: product=%s found %d image(s)", product_id, len(images))
    return images


def add_product_images(product_id: str, image_urls: list[str]) -> dict:
    """
    Add images to a product by URL.

    Returns::
        {"images": [{"id": "gid://...", "url": "...", "alt": ""}], "errors": [...]}
    """
    import time

    if not image_urls:
        return {"images": [], "errors": []}

    _log.info("add_product_images: product=%s urls=%d", product_id, len(image_urls))

    mutation = gql("""
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
        productCreateMedia(productId: $productId, media: $media) {
            media {
                ... on MediaImage {
                    id
                    alt
                    status
                    image { url width height }
                }
            }
            mediaUserErrors {
                field
                message
            }
        }
    }
    """)

    media_inputs = [
        {"originalSource": url, "alt": "", "mediaContentType": "IMAGE"}
        for url in image_urls
    ]

    try:
        result = __gql_client__.execute(mutation, variable_values={
            "productId": product_id,
            "media": media_inputs,
        })

        errors_list = result.get("productCreateMedia", {}).get("mediaUserErrors", [])
        errors = [e.get("message", "Unknown error") for e in errors_list]
        if errors:
            _log.warning("add_product_images: errors: %s", errors)

        created = result.get("productCreateMedia", {}).get("media", [])

        # Wait briefly for processing, then re-fetch to get final URLs
        if created:
            time.sleep(2)

        return {
            "images": fetch_product_images(product_id),
            "errors": errors,
        }
    except Exception as exc:
        _log.exception("add_product_images: failed")
        return {"images": [], "errors": [str(exc)]}


def reorder_product_images(product_id: str, media_ids: list[str]) -> dict:
    """
    Reorder product media to match the given order of media IDs.

    Uses ``productReorderMedia`` which takes a list of
    ``{id, newPosition}`` moves.

    Returns::
        {"images": [...], "errors": [...]}
    """
    if not media_ids:
        return {"images": [], "errors": []}

    _log.info(
        "reorder_product_images: product=%s reordering %d media",
        product_id, len(media_ids),
    )

    mutation = gql("""
    mutation productReorderMedia($id: ID!, $moves: [MoveInput!]!) {
        productReorderMedia(id: $id, moves: $moves) {
            job { id }
            mediaUserErrors {
                field
                message
            }
        }
    }
    """)

    moves = [{"id": mid, "newPosition": str(i)} for i, mid in enumerate(media_ids)]

    try:
        result = __gql_client__.execute(mutation, variable_values={
            "id": product_id,
            "moves": moves,
        })

        errors_list = result.get("productReorderMedia", {}).get("mediaUserErrors", [])
        errors = [e.get("message", "Unknown error") for e in errors_list]
        if errors:
            _log.warning("reorder_product_images: errors: %s", errors)

        return {
            "images": fetch_product_images(product_id),
            "errors": errors,
        }
    except Exception as exc:
        _log.exception("reorder_product_images: failed")
        return {"images": [], "errors": [str(exc)]}


def delete_product_image(product_id: str, media_ids: list[str]) -> dict:
    """
    Delete media from a product.

    Returns::
        {"images": [...], "errors": [...]}
    """
    if not media_ids:
        return {"images": [], "errors": []}

    _log.info("delete_product_image: product=%s deleting %d media", product_id, len(media_ids))

    mutation = gql("""
    mutation productDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
        productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
            deletedMediaIds
            mediaUserErrors {
                field
                message
            }
        }
    }
    """)

    try:
        result = __gql_client__.execute(mutation, variable_values={
            "productId": product_id,
            "mediaIds": media_ids,
        })

        errors_list = result.get("productDeleteMedia", {}).get("mediaUserErrors", [])
        errors = [e.get("message", "Unknown error") for e in errors_list]
        if errors:
            _log.warning("delete_product_image: errors: %s", errors)

        return {
            "images": fetch_product_images(product_id),
            "errors": errors,
        }
    except Exception as exc:
        _log.exception("delete_product_image: failed")
        return {"images": [], "errors": [str(exc)]}


# ── Shopify Taxonomy ───────────────────────────────────────────────

def fetch_shopify_taxonomy() -> list[dict]:
    """
    Fetch the full Shopify product taxonomy tree (all levels).

    The Shopify ``taxonomy.categories`` endpoint returns only top-level
    categories when called without filter arguments.  To obtain the
    complete tree we first fetch the root categories and then request
    all descendants of each root via the ``descendantsOf`` argument.

    Returns a flat list of dicts sorted by ``fullName``::

        [{"id": "gid://shopify/TaxonomyCategory/...",
          "fullName": "Animals & Pet Supplies > ...",
          "name": "..."},
         ...]
    """
    _log.info("fetch_shopify_taxonomy: starting full taxonomy fetch")

    # ------------------------------------------------------------------
    # Step 1 – fetch root (top-level) categories
    # ------------------------------------------------------------------
    root_query = gql("""
    query taxonomyRoots($after: String) {
        taxonomy {
            categories(first: 250, after: $after) {
                edges {
                    node {
                        id
                        fullName
                        name
                        isLeaf
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)

    roots: list[dict] = []
    after = None
    while True:
        result = __gql_client__.execute(root_query, variable_values={"after": after})
        edges = result.get("taxonomy", {}).get("categories", {}).get("edges", [])
        for edge in edges:
            node = edge["node"]
            roots.append({
                "id": node["id"],
                "fullName": node.get("fullName", ""),
                "name": node.get("name", ""),
                "isLeaf": node.get("isLeaf", False),
            })
        pi = result.get("taxonomy", {}).get("categories", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    _log.info("fetch_shopify_taxonomy: fetched %d root categories", len(roots))

    # ------------------------------------------------------------------
    # Step 2 – for each root, fetch all descendants
    # ------------------------------------------------------------------
    desc_query = gql("""
    query taxonomyDescendants($rootId: ID!, $after: String) {
        taxonomy {
            categories(first: 250, after: $after, descendantsOf: $rootId) {
                edges {
                    node {
                        id
                        fullName
                        name
                        isLeaf
                    }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
    }
    """)

    # Use a dict keyed by ID to avoid duplicates
    all_categories: dict[str, dict] = {r["id"]: r for r in roots}

    for root in roots:
        after = None
        while True:
            result = __gql_client__.execute(
                desc_query,
                variable_values={"rootId": root["id"], "after": after},
            )
            edges = result.get("taxonomy", {}).get("categories", {}).get("edges", [])
            for edge in edges:
                node = edge["node"]
                all_categories[node["id"]] = {
                    "id": node["id"],
                    "fullName": node.get("fullName", ""),
                    "name": node.get("name", ""),
                    "isLeaf": node.get("isLeaf", False),
                }
            pi = result.get("taxonomy", {}).get("categories", {}).get("pageInfo", {})
            if not pi.get("hasNextPage"):
                break
            after = pi.get("endCursor")

    categories = sorted(all_categories.values(), key=lambda c: c["fullName"])
    _log.info("fetch_shopify_taxonomy: fetched %d total categories", len(categories))
    return categories


def fetch_all_product_tags() -> list[str]:
    """
    Fetch every distinct product tag from the shop.

    Uses the ``products`` connection to aggregate tags across all products.
    Returns a sorted, deduplicated list of tag strings.
    """
    _log.info("fetch_all_product_tags: starting")

    query = gql("""
    query productTags($cursor: String) {
        products(first: 250, after: $cursor) {
            edges {
                node {
                    tags
                }
            }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    tags: set[str] = set()
    cursor: str | None = None
    while True:
        result = __gql_client__.execute(query, variable_values={"cursor": cursor})
        for edge in result.get("products", {}).get("edges", []):
            for tag in edge["node"].get("tags", []):
                tags.add(tag)
        pi = result.get("products", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")

    sorted_tags = sorted(tags, key=str.lower)
    _log.info("fetch_all_product_tags: fetched %d unique tags", len(sorted_tags))
    return sorted_tags


def fetch_category_metafields(category_id: str) -> list[dict]:
    """
    Given a Shopify taxonomy category GID, return the list of
    metafield definitions (standard metafields) applicable to
    products in that category.

    Returns a list of dicts::

        [{"key": "material", "name": "Material",
          "namespace": "...", "type": "...",
          "validations": [...]}, ...]
    """
    _log.info("fetch_category_metafields: fetching for category %s", category_id)

    query = gql("""
    query categoryMetafields($id: ID!) {
        node(id: $id) {
            ... on TaxonomyCategory {
                id
                fullName
                attributes(first: 250) {
                    edges {
                        node {
                            ... on TaxonomyChoiceListAttribute {
                                id
                                name
                                values(first: 250) {
                                    edges {
                                        node {
                                            id
                                            name
                                        }
                                    }
                                }
                            }
                            ... on TaxonomyMeasurementAttribute {
                                id
                                name
                            }
                        }
                    }
                }
            }
        }
    }
    """)

    result = __gql_client__.execute(query, variable_values={"id": category_id})
    cat_data = result.get("node")
    if not cat_data:
        _log.warning("fetch_category_metafields: category %s not found", category_id)
        return []

    attr_edges = (cat_data.get("attributes") or {}).get("edges", [])
    metafields = []
    for edge in attr_edges:
        attr = edge["node"]
        # Skip empty nodes (TaxonomyAttribute only has id, no name)
        if not attr.get("name"):
            continue
        values = []
        for val_edge in (attr.get("values") or {}).get("edges", []):
            node = val_edge["node"]
            values.append({"id": node["id"], "name": node["name"]})
        metafields.append({
            "id": attr.get("id", ""),
            "name": attr.get("name", ""),
            "handle": attr.get("id", "").rsplit("/", 1)[-1] if attr.get("id") else "",
            "description": "",
            "values": values,
        })

    _log.info(
        "fetch_category_metafields: found %d attributes for %s",
        len(metafields), category_id,
    )
    return metafields


def set_product_category_metafields(
    product_id: str,
    metafield_values: list[dict],
) -> dict:
    """
    Set category-specific metafield values on a product.

    *metafield_values* is a list of dicts, each with:
      - ``name``  – human-readable attribute name (e.g. "Material")
      - ``value`` – taxonomy value GID for choice lists, or plain string

    The function discovers the correct ``namespace`` and ``key`` by
    querying the shop's ``metafieldDefinitions`` (owner type PRODUCT)
    and matching by name.

    Returns ``{"set": int, "errors": [str]}``.
    """
    if not metafield_values:
        return {"set": 0, "errors": []}

    _log.info(
        "set_product_category_metafields: product=%s values=%s",
        product_id, metafield_values,
    )

    # 1. Query all PRODUCT metafield definitions to find namespace/key
    defs_query = gql("""
    query metafieldDefs($ownerType: MetafieldOwnerType!, $after: String) {
        metafieldDefinitions(ownerType: $ownerType, first: 250, after: $after) {
            edges {
                node {
                    namespace
                    key
                    name
                    type { name }
                    validations {
                        name
                        value
                    }
                }
            }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    all_defs: list[dict] = []
    after = None
    while True:
        result = __gql_client__.execute(
            defs_query,
            variable_values={"ownerType": "PRODUCT", "after": after},
        )
        for edge in result.get("metafieldDefinitions", {}).get("edges", []):
            all_defs.append(edge["node"])
        page_info = result.get("metafieldDefinitions", {}).get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")

    _log.info(
        "set_product_category_metafields: found %d metafield definitions",
        len(all_defs),
    )

    # Build name → list of definitions lookup (multiple defs can share a name).
    from collections import defaultdict
    defs_by_name: dict[str, list[dict]] = defaultdict(list)
    for d in all_defs:
        defs_by_name[d["name"]].append(d)

    def _resolve_taxonomy_to_metaobject(
        defn: dict, value_name: str,
    ) -> str | None:
        """
        When a metafield definition has type ``*metaobject_reference*`` and the
        selected value is a TaxonomyValue GID, look up the corresponding
        Metaobject by matching ``displayName`` against *value_name*.

        Returns the Metaobject GID, or ``None`` if no match is found.
        """
        validations = defn.get("validations") or []
        metaobj_def_id = None
        for v in validations:
            if v.get("name") == "metaobject_definition_id":
                metaobj_def_id = v.get("value")
                break
        if not metaobj_def_id:
            _log.warning(
                "_resolve_taxonomy_to_metaobject: no metaobject_definition_id "
                "in validations for definition %s.%s",
                defn.get("namespace"), defn.get("key"),
            )
            return None

        # Get the metaobject-definition type handle
        type_q = gql("""
        query metaobjectDefType($id: ID!) {
            node(id: $id) {
                ... on MetaobjectDefinition { type }
            }
        }
        """)
        result = __gql_client__.execute(type_q, variable_values={"id": metaobj_def_id})
        type_handle = (result.get("node") or {}).get("type")
        if not type_handle:
            _log.warning(
                "_resolve_taxonomy_to_metaobject: could not resolve type for "
                "metaobject definition %s", metaobj_def_id,
            )
            return None

        # Fetch metaobjects of that type and match by displayName
        mo_query = gql("""
        query metaobjects($type: String!, $after: String) {
            metaobjects(type: $type, first: 250, after: $after) {
                edges {
                    node { id displayName }
                }
                pageInfo { hasNextPage endCursor }
            }
        }
        """)
        target = value_name.strip().lower()
        mo_after: str | None = None
        while True:
            result = __gql_client__.execute(
                mo_query,
                variable_values={"type": type_handle, "after": mo_after},
            )
            for edge in (result.get("metaobjects") or {}).get("edges", []):
                node = edge["node"]
                if (node.get("displayName") or "").strip().lower() == target:
                    _log.info(
                        "_resolve_taxonomy_to_metaobject: matched '%s' → %s",
                        value_name, node["id"],
                    )
                    return node["id"]
            page_info = (result.get("metaobjects") or {}).get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            mo_after = page_info.get("endCursor")

        _log.warning(
            "_resolve_taxonomy_to_metaobject: no metaobject with displayName "
            "'%s' found for type '%s'", value_name, type_handle,
        )
        return None

    def _pick_definition(attr_name: str, value: str) -> dict | None:
        """Choose the best metafield definition for a given attribute + value."""
        candidates = defs_by_name.get(attr_name, [])
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]

        # If the value is a TaxonomyValue GID, strongly prefer the
        # taxonomy_value_reference definition over metaobject_reference.
        is_taxonomy_value = "TaxonomyValue" in value

        def _score(d: dict) -> int:
            t = (d.get("type", {}).get("name", "") or "").lower()
            ns = d.get("namespace", "")
            score = 0
            if is_taxonomy_value and "taxonomy" in t:
                score -= 100     # perfect match for taxonomy values
            elif not is_taxonomy_value and "metaobject" in t:
                score -= 100     # perfect match for metaobject values
            if "taxonomy" in t:
                score -= 20
            if ns.startswith("shopify"):
                score -= 10
            return score

        return min(candidates, key=_score)

    # 2. Match attribute names and build metafields payload
    metafields_to_set: list[dict] = []
    for mv in metafield_values:
        attr_name = mv.get("name", "")
        value = mv.get("value", "")
        value_name = mv.get("value_name", "")
        if not attr_name or not value:
            continue

        defn = _pick_definition(attr_name, value)
        if not defn:
            _log.warning(
                "set_product_category_metafields: no definition for '%s'",
                attr_name,
            )
            continue

        # Shopify expects metafield values as valid JSON.
        # List types (e.g. list.taxonomy_value_reference) need a JSON array.
        type_name = defn.get("type", {}).get("name", "") or ""
        is_list = type_name.startswith("list.")

        # When the definition expects a metaobject_reference but the
        # value is a TaxonomyValue GID, resolve it to the matching
        # Metaobject GID so that Shopify accepts the write.
        if "metaobject" in type_name and "TaxonomyValue" in value:
            resolved = _resolve_taxonomy_to_metaobject(defn, value_name)
            if resolved:
                value = resolved
            else:
                _log.warning(
                    "set_product_category_metafields: could not resolve "
                    "TaxonomyValue to metaobject for '%s', skipping",
                    attr_name,
                )
                continue

        if is_list:
            # Value should be a JSON array.  If it already is one, use it;
            # otherwise wrap the single value in an array.
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    json_value = value
                else:
                    json_value = json.dumps([value])
            except (json.JSONDecodeError, TypeError):
                json_value = json.dumps([value])
        else:
            try:
                json.loads(value)
                json_value = value
            except (json.JSONDecodeError, TypeError):
                json_value = json.dumps(value)

        # When the value contains a TaxonomyValue GID, force the correct
        _log.info(
            "set_product_category_metafields: %s → type=%s is_list=%s json_value=%s",
            attr_name, type_name, is_list, json_value,
        )

        metafields_to_set.append({
            "ownerId": product_id,
            "namespace": defn["namespace"],
            "key": defn["key"],
            "type": type_name,
            "value": json_value,
        })

    if not metafields_to_set:
        _log.info("set_product_category_metafields: nothing to set")
        return {"set": 0, "errors": []}

    # 3. Write values via metafieldsSet
    set_mutation = gql("""
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
            metafields {
                key
                namespace
                value
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    try:
        result = __gql_client__.execute(
            set_mutation,
            variable_values={"metafields": metafields_to_set},
        )
        user_errors = (
            result.get("metafieldsSet", {}).get("userErrors", [])
        )
        set_metafields = (
            result.get("metafieldsSet", {}).get("metafields", [])
        )

        errors = (
            [f"{e.get('field', '?')}: {e['message']}" for e in user_errors]
            if user_errors
            else []
        )

        _log.info(
            "set_product_category_metafields: set %d metafield(s), errors=%s",
            len(set_metafields or []),
            errors,
        )
        return {"set": len(set_metafields or []), "errors": errors}
    except Exception as exc:
        _log.exception("set_product_category_metafields: exception")
        return {"set": 0, "errors": [str(exc)]}


# ── Product Options ────────────────────────────────────────────────

# SKU length-letter → human-readable name (shared with add_variants)
_LENGTH_NAMES = {
    "A": "Short",
    "B": "Regular",
    "C": "Long",
    "D": "XLong",
    "U": "Unisex",
}


def _extract_length_letter(sku: str) -> str | None:
    """Return the length letter from the 5th dash-part of the SKU, if any."""
    parts = sku.split("-")
    if len(parts) >= 5:
        size_code = parts[4]  # e.g. "B05"
        if size_code and size_code[0].isalpha():
            return size_code[0].upper()
    return None


def detect_product_options(
    vendor: str,
    variants_data: list[dict],
) -> dict:
    """
    Analyse variant data to determine which product options are needed,
    and look up a reference product from the same vendor to discover
    metafield linking (e.g. Farve → color metaobject).

    Returns::

        {
            "options": [
                {
                    "name": "Farve",
                    "values": ["Olive Green", "Black"],
                    "linked_metafield": {"namespace": "...", "key": "..."},
                    "metaobject_type": "shopify--color-pattern",
                    "resolved_values": {"Olive Green": "gid://..."},
                    "missing_values": ["Black"],
                },
                {
                    "name": "Størrelse",
                    "values": ["S", "M", "L"],
                    "linked_metafield": null,
                    "metaobject_type": null,
                    "resolved_values": {},
                    "missing_values": [],
                },
            ],
            "reference_product_id": "gid://..." or null,
        }
    """
    _log.info(
        "detect_product_options: vendor=%r variants=%d",
        vendor, len(variants_data),
    )

    # ── 1. Collect unique values per option from variant data ──────
    colors: list[str] = []
    sizes: list[str] = []
    length_letters: set[str] = set()

    seen_colors: set[str] = set()
    seen_sizes: set[str] = set()

    for v in variants_data:
        color = (v.get("color") or "").strip()
        if color and color not in seen_colors:
            seen_colors.add(color)
            colors.append(color)

        raw_size = (v.get("size") or "").strip()
        if "/" in raw_size:
            raw_size = raw_size.split("/", 1)[0].strip()
        size = _normalize_size(raw_size)
        if size and size.lower() != "one size" and size not in seen_sizes:
            seen_sizes.add(size)
            sizes.append(size)

        letter = _extract_length_letter(v.get("sku", ""))
        if letter:
            length_letters.add(letter)

    include_length = len(length_letters) > 1
    lengths: list[str] = []
    if include_length:
        lengths = sorted(
            [_LENGTH_NAMES.get(l, l) for l in length_letters],
            key=lambda n: list(_LENGTH_NAMES.values()).index(n)
            if n in _LENGTH_NAMES.values()
            else 99,
        )

    _log.info(
        "detect_product_options: colors=%d sizes=%d lengths=%d",
        len(colors), len(sizes), len(lengths),
    )

    # ── 2. Find a reference product from the same vendor ───────────
    ref_template = _find_reference_option_template(vendor)
    ref_product_id = ref_template.get("reference_product_id")
    ref_options = {o["name"]: o for o in ref_template.get("options", [])}

    # ── 3. Build option list ──────────────────────────────────────
    result_options: list[dict] = []

    if colors:
        ref = ref_options.get("Farve", {})
        linked_mf = ref.get("linked_metafield")
        mo_type = ref.get("metaobject_type")

        resolved: dict[str, str] = {}
        missing: list[str] = []
        if mo_type:
            resolution = _resolve_metaobject_values(mo_type, colors)
            resolved = resolution["resolved"]
            missing = resolution["missing"]
        else:
            missing = []  # unlinked — no resolution needed

        result_options.append({
            "name": "Farve",
            "values": colors,
            "linked_metafield": linked_mf,
            "metaobject_type": mo_type,
            "resolved_values": resolved,
            "missing_values": missing,
        })

    if sizes:
        ref = ref_options.get("Størrelse", {})
        result_options.append({
            "name": "Størrelse",
            "values": sizes,
            "linked_metafield": ref.get("linked_metafield"),
            "metaobject_type": None,
            "resolved_values": {},
            "missing_values": [],
        })

    if lengths:
        ref = ref_options.get("Længde", {})
        result_options.append({
            "name": "Længde",
            "values": lengths,
            "linked_metafield": ref.get("linked_metafield"),
            "metaobject_type": None,
            "resolved_values": {},
            "missing_values": [],
        })

    # ── 4. Fetch available linkable PRODUCT metafield definitions ──
    linkable_defs = _fetch_linkable_metafield_definitions()

    _log.info(
        "detect_product_options: returning %d option(s), ref=%s, linkable_defs=%d",
        len(result_options), ref_product_id, len(linkable_defs),
    )
    return {
        "options": result_options,
        "reference_product_id": ref_product_id,
        "linkable_metafield_definitions": linkable_defs,
    }


def fetch_metaobjects_for_definition(
    namespace: str,
    key: str,
) -> dict:
    """
    Given a PRODUCT metafield definition (namespace + key), discover the
    referenced metaobject type and return **all** metaobjects of that type.

    Returns::

        {
            "metaobject_type": "component_colors--farve",
            "metaobjects": [
                {"gid": "gid://...", "displayName": "Olive Green"},
                ...
            ],
        }

    Returns an empty list when the definition does not reference a metaobject.
    """
    # ── 1. Look up the metafield definition to get its validations ──
    def_query = gql("""
    query metafieldDefs($ownerType: MetafieldOwnerType!, $ns: String!, $key: String!) {
        metafieldDefinitions(
            ownerType: $ownerType,
            first: 5,
            namespace: $ns,
            key: $key,
        ) {
            edges {
                node {
                    namespace
                    key
                    type { name }
                    validations { name value }
                }
            }
        }
    }
    """)
    def_result = __gql_client__.execute(
        def_query,
        variable_values={"ownerType": "PRODUCT", "ns": namespace, "key": key},
    )
    edges = def_result.get("metafieldDefinitions", {}).get("edges", [])
    if not edges:
        _log.warning(
            "fetch_metaobjects_for_definition: no definition for %s.%s",
            namespace, key,
        )
        return {"metaobject_type": None, "metaobjects": []}

    definition = edges[0]["node"]
    type_name = (definition.get("type", {}).get("name") or "").lower()
    if "metaobject_reference" not in type_name:
        _log.info(
            "fetch_metaobjects_for_definition: %s.%s type=%s is not a metaobject ref",
            namespace, key, type_name,
        )
        return {"metaobject_type": None, "metaobjects": []}

    validations = definition.get("validations", [])

    # ── 2. Extract the metaobject_definition_id from validations ───
    import json as _json
    ref_def_gid = None
    for v in validations:
        if v.get("name") == "metaobject_definition_id":
            raw = v.get("value", "")
            try:
                parsed = _json.loads(raw)
                ref_def_gid = parsed if isinstance(parsed, str) else raw
            except (ValueError, TypeError):
                ref_def_gid = raw
            break

    if not ref_def_gid:
        _log.warning(
            "fetch_metaobjects_for_definition: no metaobject_definition_id "
            "in validations for %s.%s",
            namespace, key,
        )
        return {"metaobject_type": None, "metaobjects": []}

    # ── 3. Resolve definition GID → metaobject type string ─────────
    node_query = gql("""
    query nodeQuery($id: ID!) {
        node(id: $id) {
            ... on MetaobjectDefinition { type }
        }
    }
    """)
    node_result = __gql_client__.execute(
        node_query, variable_values={"id": ref_def_gid},
    )
    mo_type = (node_result.get("node") or {}).get("type")
    if not mo_type:
        _log.warning(
            "fetch_metaobjects_for_definition: could not resolve type from %s",
            ref_def_gid,
        )
        return {"metaobject_type": None, "metaobjects": []}

    _log.info(
        "fetch_metaobjects_for_definition: %s.%s → type=%s",
        namespace, key, mo_type,
    )

    # ── 4. Fetch all metaobjects of this type ──────────────────────
    list_query = gql("""
    query metaobjectsByType($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            edges { node { id displayName } }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    metaobjects: list[dict] = []
    after = None
    while True:
        res = __gql_client__.execute(
            list_query, variable_values={"type": mo_type, "after": after},
        )
        for edge in res.get("metaobjects", {}).get("edges", []):
            node = edge["node"]
            metaobjects.append({
                "gid": node["id"],
                "displayName": (node.get("displayName") or "").strip(),
            })
        pi = res.get("metaobjects", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    metaobjects.sort(key=lambda x: x["displayName"].lower())
    _log.info(
        "fetch_metaobjects_for_definition: found %d metaobjects for type %s",
        len(metaobjects), mo_type,
    )
    return {"metaobject_type": mo_type, "metaobjects": metaobjects}


def _fetch_linkable_metafield_definitions() -> list[dict]:
    """
    Return PRODUCT metafield definitions whose type involves a metaobject
    reference — these are the ones that can be linked to product options.

    Returns::

        [
            {
                "namespace": "shopify",
                "key": "color-pattern",
                "name": "Color",
                "type": "list.metaobject_reference",
            },
            ...
        ]
    """
    query = gql("""
    query metafieldDefs($ownerType: MetafieldOwnerType!, $after: String) {
        metafieldDefinitions(ownerType: $ownerType, first: 250, after: $after) {
            edges {
                node {
                    namespace
                    key
                    name
                    type { name }
                }
            }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    defs: list[dict] = []
    after = None
    while True:
        result = __gql_client__.execute(
            query, variable_values={"ownerType": "PRODUCT", "after": after},
        )
        for edge in result.get("metafieldDefinitions", {}).get("edges", []):
            node = edge["node"]
            type_name = (node.get("type", {}).get("name") or "").lower()
            if "metaobject_reference" in type_name:
                defs.append({
                    "namespace": node["namespace"],
                    "key": node["key"],
                    "name": node.get("name", ""),
                    "type": node.get("type", {}).get("name", ""),
                })
        pi = result.get("metafieldDefinitions", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    _log.info(
        "_fetch_linkable_metafield_definitions: found %d definitions",
        len(defs),
    )
    return defs


def _find_reference_option_template(vendor: str) -> dict:
    """
    Query Shopify for a product from *vendor* that has non-default options
    and return its option structure (names + metafield linking + metaobject types).
    """
    query = gql("""
    query findRefProduct($query: String!) {
        products(first: 20, query: $query) {
            edges {
                node {
                    id
                    options {
                        name
                        linkedMetafield { namespace key }
                        optionValues {
                            linkedMetafieldValue
                        }
                    }
                }
            }
        }
    }
    """)

    result = __gql_client__.execute(
        query, variable_values={"query": f'vendor:"{vendor}"'},
    )

    for edge in result.get("products", {}).get("edges", []):
        product = edge["node"]
        options = product.get("options", [])
        non_default = [o for o in options if o["name"] != "Title"]
        if not non_default:
            continue

        template: list[dict] = []
        for opt in non_default:
            linked_metafield = opt.get("linkedMetafield")
            metaobject_type = None

            if linked_metafield:
                # Discover metaobject type from an existing value
                for ov in opt.get("optionValues", []):
                    val = ov.get("linkedMetafieldValue", "")
                    if val and val.startswith("gid://shopify/Metaobject/"):
                        type_query = gql("""
                        query metaobjectType($id: ID!) {
                            metaobject(id: $id) { type }
                        }
                        """)
                        type_result = __gql_client__.execute(
                            type_query, variable_values={"id": val},
                        )
                        metaobject_type = (
                            type_result.get("metaobject", {}).get("type")
                        )
                        break

            template.append({
                "name": opt["name"],
                "linked_metafield": linked_metafield,
                "metaobject_type": metaobject_type,
            })

        _log.info(
            "detect_product_options: ref product %s → %s",
            product["id"],
            [(t["name"], bool(t["linked_metafield"])) for t in template],
        )
        return {"options": template, "reference_product_id": product["id"]}

    _log.info(
        "detect_product_options: no reference product found for vendor=%r",
        vendor,
    )
    return {"options": [], "reference_product_id": None}


def _resolve_metaobject_values(
    metaobject_type: str,
    display_names: list[str],
) -> dict:
    """
    For a metaobject type, resolve display names to metaobject GIDs.

    Returns ``{"resolved": {"name": "gid://..."}, "missing": ["name"]}``
    """
    list_query = gql("""
    query metaobjectsByType($type: String!, $after: String) {
        metaobjects(type: $type, first: 250, after: $after) {
            edges { node { id displayName } }
            pageInfo { hasNextPage endCursor }
        }
    }
    """)

    all_mos: dict[str, str] = {}
    after = None
    while True:
        res = __gql_client__.execute(
            list_query, variable_values={"type": metaobject_type, "after": after},
        )
        for edge in res.get("metaobjects", {}).get("edges", []):
            node = edge["node"]
            dn = (node.get("displayName") or "").strip()
            all_mos[dn] = node["id"]
        pi = res.get("metaobjects", {}).get("pageInfo", {})
        if not pi.get("hasNextPage"):
            break
        after = pi.get("endCursor")

    resolved = {}
    missing = []
    for name in display_names:
        if name in all_mos:
            resolved[name] = all_mos[name]
        else:
            missing.append(name)

    _log.info(
        "_resolve_metaobject_values: type=%s resolved=%d missing=%d",
        metaobject_type, len(resolved), len(missing),
    )
    return {"resolved": resolved, "missing": missing}


def create_product_options(
    product_id: str,
    options: list[dict],
) -> dict:
    """
    Create product options on a product.

    *options* is a list of dicts, each with:
      - ``name`` – option name (e.g. "Farve")
      - ``values`` – list of dicts with ``name`` and optionally
        ``linkedMetafieldValue`` (metaobject GID)
      - ``linked_metafield`` – optional ``{"namespace": ..., "key": ...}``

    For linked options, Shopify requires the product's metafield to already
    contain values before the linked option can be created.  The flow is:

    1. Pre-populate product metafields with metaobject GIDs (``metafieldsSet``)
    2. Create all options via ``productOptionsCreate``
       – linked options reference the metafield (values come from it)
       – unlinked options include their values directly
    3. For linked options, add any extra values via ``productOptionUpdate``

    Returns ``{"options": [...], "errors": [...]}``.
    """
    if not options:
        return {"options": [], "errors": []}

    _log.info(
        "create_product_options: product=%s options=%s",
        product_id,
        [
            {"name": o["name"], "values": len(o.get("values", [])),
             "linked": bool(o.get("linked_metafield"))}
            for o in options
        ],
    )

    # Split options into linked and unlinked
    linked_options: list[dict] = []
    unlinked_options: list[dict] = []
    for opt in options:
        if opt.get("linked_metafield"):
            linked_options.append(opt)
        else:
            unlinked_options.append(opt)

    errors: list[str] = []

    # ── Step 1: Pre-populate product metafields for linked options ──
    # Shopify requires the metafield to have values before a linked
    # option can be created from it.
    # We set each metafield individually so that if one fails (e.g.
    # "Owner subtype does not match the definition's constraints"),
    # only that option is demoted to unlinked — the rest proceed.
    failed_metafield_keys: set[tuple[str, str]] = set()

    if linked_options:
        mf_mutation = gql("""
        mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
            metafieldsSet(metafields: $metafields) {
                metafields { key namespace value }
                userErrors { field message }
            }
        }
        """)
        for opt in linked_options:
            ns = opt["linked_metafield"]["namespace"]
            key = opt["linked_metafield"]["key"]
            gids = [
                v["linkedMetafieldValue"]
                for v in opt.get("values", [])
                if v.get("linkedMetafieldValue")
            ]
            if not gids:
                _log.warning(
                    "create_product_options: linked option '%s' has no "
                    "resolved metaobject GIDs — skipping metafield pre-set",
                    opt["name"],
                )
                continue

            metafield_input = {
                "ownerId": product_id,
                "namespace": ns,
                "key": key,
                "value": json.dumps(gids),
            }

            _log.info(
                "create_product_options: pre-setting metafield %s.%s "
                "(%d GIDs) for option '%s'",
                ns, key, len(gids), opt["name"],
            )
            try:
                mf_result = __gql_client__.execute(
                    mf_mutation,
                    variable_values={"metafields": [metafield_input]},
                )
                mf_errors = (
                    mf_result.get("metafieldsSet", {}).get("userErrors", [])
                )
                if mf_errors:
                    _log.warning(
                        "create_product_options: metafield pre-set failed "
                        "for %s.%s — demoting option '%s' to unlinked: %s",
                        ns, key, opt["name"], mf_errors,
                    )
                    failed_metafield_keys.add((ns, key))
                else:
                    _log.info(
                        "create_product_options: metafield %s.%s pre-set OK",
                        ns, key,
                    )
            except Exception as mf_exc:
                _log.warning(
                    "create_product_options: metafield pre-set exception "
                    "for %s.%s — demoting option '%s' to unlinked: %s",
                    ns, key, opt["name"], mf_exc,
                )
                failed_metafield_keys.add((ns, key))

    # ── Step 2: Create all options via productOptionsCreate ─────────
    # Linked options: linkedMetafield only (values come from the metafield)
    # Unlinked options: plain values
    # Options whose metafield pre-set failed are demoted to unlinked.
    options_input: list[dict] = []
    for opt in options:
        opt_input: dict = {"name": opt["name"]}

        lm = opt.get("linked_metafield")
        demoted = (
            lm and (lm["namespace"], lm["key"]) in failed_metafield_keys
        )
        if lm and not demoted:
            opt_input["linkedMetafield"] = {
                "namespace": lm["namespace"],
                "key": lm["key"],
            }
            # No values — Shopify reads them from the pre-populated metafield
        else:
            if demoted:
                _log.info(
                    "create_product_options: option '%s' demoted to unlinked",
                    opt["name"],
                )
            values_input: list[dict] = []
            for val in opt.get("values", []):
                values_input.append({"name": val.get("name", "")})
            opt_input["values"] = values_input

        options_input.append(opt_input)

    mutation = gql("""
    mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
        productOptionsCreate(productId: $productId, options: $options) {
            product {
                id
                options {
                    id
                    name
                    linkedMetafield { namespace key }
                    optionValues {
                        id
                        name
                        linkedMetafieldValue
                    }
                }
            }
            userErrors { field message }
        }
    }
    """)

    try:
        result = __gql_client__.execute(mutation, variable_values={
            "productId": product_id,
            "options": options_input,
        })
        user_errors = (
            result.get("productOptionsCreate", {}).get("userErrors", [])
        )
        product = result.get("productOptionsCreate", {}).get("product")

        if user_errors:
            errors.extend(
                f"{e.get('field', '?')}: {e['message']}" for e in user_errors
            )

        created_options: list[dict] = []
        if product:
            for opt in product.get("options", []):
                if opt["name"] == "Title":
                    continue
                created_options.append({
                    "id": opt["id"],
                    "name": opt["name"],
                    "linked": bool(opt.get("linkedMetafield")),
                    "values_count": len(opt.get("optionValues", [])),
                })

        _log.info(
            "create_product_options: created %d option(s), errors=%s",
            len(created_options), errors,
        )
        return {"options": created_options, "errors": errors}
    except Exception as exc:
        _log.exception("create_product_options: exception")
        return {"options": [], "errors": [str(exc)]}


# ── Product Creation ───────────────────────────────────────────────

def fetch_all_publications() -> list[dict]:
    """
    Fetch all publications (sales channels) from Shopify.

    Returns [{"id": "gid://shopify/Publication/...", "name": "..."}]
    """
    query = gql("""
    query {
        publications(first: 50) {
            edges {
                node {
                    id
                    name
                }
            }
        }
    }
    """)
    result = __gql_client__.execute(query)
    pubs = []
    for edge in result.get("publications", {}).get("edges", []):
        node = edge["node"]
        pubs.append({"id": node["id"], "name": node.get("name", "")})
    _log.info("fetch_all_publications: found %d publications", len(pubs))
    return pubs


def create_shopify_product(
    title: str,
    vendor: str,
    description_html: str = "",
    category_id: str | None = None,
    tags: list[str] | None = None,
) -> dict:
    """
    Create a new Shopify product in DRAFT status, published to all sales
    channels and markets.

    Returns::
        {"product_id": "gid://...", "errors": [...]}
    """
    _log.info("create_shopify_product: title=%r vendor=%r category=%s tags=%s", title, vendor, category_id, tags)

    # 1. Collect all publication IDs
    publications = fetch_all_publications()
    publication_inputs = [{"publicationId": p["id"]} for p in publications]

    # Build product input
    product_input: dict = {
        "title": title,
        "vendor": vendor,
        "descriptionHtml": description_html,
        "status": "DRAFT",
    }
    if category_id:
        product_input["category"] = category_id
    if tags:
        product_input["tags"] = tags

    create_mutation = gql("""
    mutation productCreate($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
        productCreate(product: $product, media: $media) {
            product {
                id
                title
                handle
                vendor
                status
            }
            userErrors {
                field
                message
            }
        }
    }
    """)

    try:
        result = __gql_client__.execute(create_mutation, variable_values={
            "product": product_input,
            "media": [],
        })
        user_errors = result.get("productCreate", {}).get("userErrors", [])
        product = result.get("productCreate", {}).get("product")

        errors = [f"{e.get('field', '?')}: {e['message']}" for e in user_errors] if user_errors else []

        if not product:
            _log.warning("create_shopify_product: no product returned, errors=%s", errors)
            return {"product_id": None, "errors": errors or ["No product returned"]}

        product_id = product["id"]
        _log.info("create_shopify_product: created product %s", product_id)

        # 2. Publish to all sales channels
        if publication_inputs:
            publish_mutation = gql("""
            mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
                publishablePublish(id: $id, input: $input) {
                    publishable { availablePublicationsCount { count } }
                    userErrors { field message }
                }
            }
            """)
            try:
                pub_result = __gql_client__.execute(publish_mutation, variable_values={
                    "id": product_id,
                    "input": publication_inputs,
                })
                pub_errors = pub_result.get("publishablePublish", {}).get("userErrors", [])
                if pub_errors:
                    for pe in pub_errors:
                        errors.append(f"Publish error: {pe.get('message', '?')}")
                    _log.warning("create_shopify_product: publish errors: %s", pub_errors)
                else:
                    _log.info("create_shopify_product: published to %d channels", len(publication_inputs))
            except Exception as exc:
                _log.exception("create_shopify_product: failed to publish")
                errors.append(f"Failed to publish: {exc}")

        return {
            "product_id": product_id,
            "handle": product.get("handle", ""),
            "errors": errors,
        }
    except Exception as exc:
        _log.exception("create_shopify_product: exception during product creation")
        return {"product_id": None, "errors": [str(exc)]}