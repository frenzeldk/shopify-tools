#!/usr/bin/env python3

"""Sync Helikon-Tex product inventory policy with Shopify using SOAP API described in entirem.wsdl."""

import os
import requests
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError
from zeep import Client as ZeepClient
from zeep.transports import Transport as ZeepTransport

def fetch_helikon_stock():
    """
    Fetch Helikon-Tex stock data from SOAP API using remote WSDL.
    Returns a dict mapping ProductCode (SKU) to OnStock (float).
    """
    wsdl_url = "http://89.171.38.14:8886/BasicApiB2BPartners.asmx?WSDL"
    token = os.environ.get("ENTIREM_TOKEN")
    if not token:
        raise RuntimeError("ENTIREM_TOKEN environment variable not set")

    zeep_client = ZeepClient(wsdl_url, transport=ZeepTransport())
    # Call the ProductStock method with csv=0 to get all products as objects
    stock_list = zeep_client.service.BasicApiB2BPartners_ProductStock(token=token, csv=0)
    stock_dict = {}
    for item in stock_list:
        sku = getattr(item, "ProductCode", None)
        on_stock = float(getattr(item, "OnStock", 0))
        if sku:
            stock_dict[sku] = on_stock
    print(f"Loaded {len(stock_dict)} Helikon-Tex SKUs from SOAP API.")
    return stock_dict

def get_helikon_and_update_shopify():
    """
    Fetch Helikon-Tex stock and update Shopify inventory policy accordingly.
    """
    # Shopify GraphQL setup
    shopify_url = os.environ.get("SHOPIFY_URL")
    shopify_header = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}
    transport = AIOHTTPTransport(url=shopify_url, headers=shopify_header)
    gql_client = Client(transport=transport, fetch_schema_from_transport=True)

    # Fetch Helikon-Tex stock from SOAP API
    helikon_stock = fetch_helikon_stock()

    # Query Shopify for Helikon-Tex products by vendor
    vendor = "Helikon-Tex"
    has_next_page = True
    after_cursor = None

    while has_next_page:
        query = gql("""
        query getProductVariantsByVendor($vendor: String!, $after: String) {
            products(first: 200, query: $vendor, after: $after) {
                edges {
                    node {
                        id
                        title
                        vendor
                        variants(first: 200) {
                            edges {
                                node {
                                    id
                                    sku
                                    inventoryPolicy
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
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
        variables = {"vendor": vendor, "after": after_cursor}
        try:
            result = gql_client.execute(query, variable_values=variables)
            products = result.get("products", {}).get("edges", [])
            for product in products:
                product_node = product["node"]
                product_id = product_node["id"]
                bulk_update_input = []

                for variant in product_node["variants"]["edges"]:
                    variant_node = variant["node"]
                    variant_id = variant_node["id"]
                    variant_sku = variant_node["sku"]
                    current_policy = variant_node["inventoryPolicy"]

                    # Check if the variant exists in the SOAP stock
                    if variant_sku in helikon_stock:
                        expected_policy = "CONTINUE" if helikon_stock[variant_sku] > 0 else "DENY"
                    else:
                        expected_policy = "DENY"  # Default to "DENY" if not in SOAP

                    # Add to bulk update input if the policy doesn't match
                    if current_policy != expected_policy:
                        bulk_update_input.append({
                            "id": variant_id,
                            "inventoryPolicy": expected_policy
                        })

                # Perform bulk update if there are changes
                if bulk_update_input:
                    mutation = gql("""
                    mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                        productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                            productVariants {
                                id
                                inventoryPolicy
                            }
                            userErrors {
                                field
                                message
                            }
                        }
                    }
                    """)
                    variables = {
                        "productId": product_id,
                        "variants": bulk_update_input
                    }
                    try:
                        mutation_result = gql_client.execute(mutation, variable_values=variables)
                        errors = mutation_result.get("productVariantsBulkUpdate", {}).get("userErrors", [])
                        if errors:
                            print(f"Error updating variants for product {product_node['title']}: {errors}")
                        else:
                            print(f"Updated variants for product {product_node['title']}")
                    except TransportQueryError as e:
                        print(f"GraphQL transport error while updating variants for product {product_node['title']}: {e}")
                    except requests.exceptions.RequestException as e:
                        print(f"Network error while updating variants for product {product_node['title']}: {e}")
                    except KeyError as e:
                        print(f"Key error while updating variants for product {product_node['title']}: {e}")

            # Handle pagination for products
            page_info = result.get("products", {}).get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor", None)

        except requests.exceptions.RequestException as e:
            print(f"Network error while fetching product variants for vendor {vendor}: {e}")
            break
        except TransportQueryError as e:
            print(f"GraphQL query error for vendor {vendor}: {e}")
            break
        except (KeyError, ValueError, TypeError) as e:
            print(f"Unexpected error fetching product variants for vendor {vendor}: {e}")
            break

#def main():
#    get_helikon_and_update_shopify()

#if __name__ == "__main__":
#    main()