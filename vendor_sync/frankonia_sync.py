"""Provides the base configuration values for shopify integration"""
import csv
import os
import requests
from gql import gql
from gql import Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportQueryError



def get_vendors_and_product_variants():
    """
    Fetch all vendors from the CSV located at frankonia_url and retrieve all product variants
    for those vendors from Shopify. Check if the variant exists in the CSV and update its
    inventory policy if necessary.
    """
    ## Shopify GraphQL setup
# Select your transport with a defined url endpoint
    shopify_url = os.environ.get("SHOPIFY_URL")
    shopify_header = {"X-Shopify-Access-Token": os.environ.get("SHOPIFY_API_KEY")}
    transport = AIOHTTPTransport(url=shopify_url, headers=shopify_header)
    gql_client = Client(transport=transport, fetch_schema_from_transport=True)


    frankonia_url = "https://www.semtrack.de/e?i=" + os.environ.get("FRANKONIA_SECRET")

    frankonia_id = "id"
    frankonia_availability = "lieferbar"

    # Fetch the CSV feed
    response = requests.get(frankonia_url, timeout=10)
    response.raise_for_status()
    csv_data = response.text.splitlines()

    # Parse the CSV feed into a dictionary for quick lookup
    csv_reader = csv.DictReader(csv_data, delimiter=';')
    csv_variants = {row[frankonia_id]: row[frankonia_availability].lower() for row in csv_reader}

    print(f"Loaded {len(csv_variants)} variants from CSV.")

    # Query Shopify for product variants by vendor with pagination
    for vendor in ["Parforce Traditional Hunting", "Parforce", "Highmoor", "Wald & Forst"]:
        has_next_page = True
        after_cursor = None

        while has_next_page:
            query = gql("""
            query getProductVariantsByVendor($vendor: String!, $after: String) {
                products(first: 50, query: $vendor, after: $after) {
                    edges {
                        node {
                            id
                            title
                            vendor
                            variants(first: 50) {
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

                        # Check if the variant exists in the CSV
                        if variant_sku in csv_variants:
                            expected_policy = "CONTINUE" if csv_variants[variant_sku] == "ja" else "DENY"
                        else:
                            expected_policy = "DENY"  # Default to "DENY" if not in CSV

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

def main():
    """
    Main function to execute the script.
    """
    get_vendors_and_product_variants()

if __name__ == "__main__":
    main()
    # Execute the main function