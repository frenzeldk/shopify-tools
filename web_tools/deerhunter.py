"""
Bulk create Deerhunter products from a CSV file downloaded via FTP.
"""
import os
import csv
import io
import ftplib
from typing import List, Dict

_FTP_HOST = os.environ.get("FTP_HOST")
_FTP_USERNAME = os.environ.get("FTP_USERNAME")
_FTP_PASSWORD = os.environ.get("FTP_PASSWORD")
_FTP_REMOTE_PATH = os.environ.get("FTP_REMOTE_PATH")

def _fetch_csv_from_ftp(host: str, username: str, password: str, remote_path: str) -> List[Dict]:
    """Download a CSV file via FTP and return its contents as a list of dicts."""
    buffer = io.BytesIO()
    with ftplib.FTP(host) as ftp:
        ftp.login(user=username, passwd=password)
        ftp.retrbinary(f"RETR {remote_path}", buffer.write)
    buffer.seek(0)
    lines = buffer.read().decode("utf-8").splitlines(keepends=True)
    reader = csv.DictReader(io.StringIO("".join(lines[1:])), delimiter=";", quotechar='"')
    return list(reader)


def _group_products(rows: List[Dict]) -> Dict:
    """Group flat CSV rows into a nested dict by Product_Number, then Colour_Number, then Size."""
    products = {}

    for row in rows:
        prod_num = row["Product_Number"]
        colour_num = row["Colour_Number"]
        size = row["Size"]

        # Product level
        if prod_num not in products:
            products[prod_num] = {
                "Product_Name": "".join(["Deerhunter - ", row["Product_Name"].strip()]),
                "Composition_Type": row["Composition_Type"],
                "Composition": row["Composition"],
                "Description": row["Description"],
                "Keywords": row["Keywords"],
                "Series": row["Series"],
                "Gender": row["Gender"],
                "Outlet": row["Outlet"],
                "Season": row["Season"],
            }

        # Colour level
        if colour_num not in products[prod_num]:
            products[prod_num][colour_num] = {
                "Colour_Name": row["Colour_Name"],
                "Image1": row["Image1"],
                "Image2": row["Image2"],
                "Image3": row["Image3"],
                "Image4": row["Image4"],
                "Image5": row["Image5"],
                "Image6": row["Image6"],
                "Image7": row["Image7"],
            }

        # Size level
        if row["Lifecycle"].strip().upper() != "UDGÅENDE":
            products[prod_num][colour_num][size] = {
                "EAN": row["EAN"],
                "SKU": "-".join([prod_num, colour_num, size]),
                "Retail_Price": str(int(float(row["Retail_Price"].replace(",", ".")))) if row["Retail_Price"].strip() else "",
                "Wholesale_Price": str(int(float(row["Price_Before_VAT"].replace(",", "."))*0.60)) if row["Price_Before_VAT"].strip() else "",
                "Currency": row["Currency"],
                "Country_of_origin": row["Country_of_origin"],
                "Tariff": row["Tariff"][:6],
                "Weight": row["Weight"],
                "Weight_Unit": row["Weight_Unit"],
                "Stock": row["Stock"],
                "BackInStockDate": row["BackInStockDate"],
                "Lifecycle": row["Lifecycle"],
            }

    # Prune colours with no sizes, then products with no colours
    product_level_keys = {"Product_Name", "Composition_Type", "Composition",
                          "Description", "Keywords", "Series", "Gender",
                          "Outlet", "Season"}
    colour_level_keys = {"Colour_Name", "Image_URL", "Image1", "Image2",
                         "Image3", "Image4", "Image5", "Image6", "Image7"}

    for prod_num in list(products):
        for key in list(products[prod_num]):
            if key in product_level_keys:
                continue
            colour = products[prod_num][key]
            has_sizes = any(k not in colour_level_keys for k in colour)
            if not has_sizes:
                del products[prod_num][key]
        # Remove product if no colours remain
        if not any(k not in product_level_keys for k in products[prod_num]):
            del products[prod_num]

    return products

def dh_fetch_all_products() -> Dict:
    """Fetch the CSV from FTP and return the grouped products dict."""
    rows = _fetch_csv_from_ftp(_FTP_HOST, _FTP_USERNAME, _FTP_PASSWORD, _FTP_REMOTE_PATH)
    return _group_products(rows)


def dh_products_to_vendor_format(products: Dict) -> List[Dict]:
    """Convert Deerhunter grouped products to the flat vendor product list
    format expected by ``compare_vendor_products()``.

    Each variant includes image URLs from its colour group:
      - ``variant_image_url``: Image1 (used as the variant thumbnail)
      - ``color_images``: ordered list of non-empty Image1…Image7 URLs
    """
    product_level_keys = {
        "Product_Name", "Composition_Type", "Composition",
        "Description", "Keywords", "Series", "Gender",
        "Outlet", "Season",
    }
    colour_level_keys = {
        "Colour_Name", "Image1", "Image2", "Image3",
        "Image4", "Image5", "Image6", "Image7",
    }

    vendor_products: List[Dict] = []
    for prod_num, product_data in products.items():
        product_name = product_data["Product_Name"]

        for key in product_data:
            if key in product_level_keys:
                continue
            colour_group = product_data[key]
            colour_name = colour_group["Colour_Name"]

            # Collect non-empty image URLs in order
            color_images: List[str] = []
            for i in range(1, 8):
                img = colour_group.get(f"Image{i}", "").strip()
                if img:
                    color_images.append(img)

            variant_image_url = color_images[0] if color_images else ""

            for size_key in colour_group:
                if size_key in colour_level_keys:
                    continue
                size_data = colour_group[size_key]
                # "På lager" (In stock) → allow overselling (CONTINUE)
                stock_val = (size_data.get("Stock") or "").strip()
                inv_policy = "CONTINUE" if stock_val == "På lager" else "DENY"

                vendor_products.append({
                    "sku": size_data["SKU"],
                    "ean": size_data["EAN"],
                    "hs_code": size_data["Tariff"],
                    "size": size_key,
                    "name": f"{product_name} - {colour_name}",
                    "product_code": prod_num,
                    "base_name": product_name,
                    "color": colour_name,
                    "size_eu": "",
                    "size_usa": "",
                    "price": size_data["Wholesale_Price"],
                    "msrp": size_data["Retail_Price"],
                    "currency": size_data["Currency"],
                    "weight": size_data["Weight"],
                    "weight_unit": size_data["Weight_Unit"],
                    "country_of_origin": size_data["Country_of_origin"],
                    "inventory_policy": inv_policy,
                    "variant_image_url": variant_image_url,
                    "color_images": color_images,
                    # Product-level fields for description generation
                    "dh_composition_type": product_data.get("Composition_Type", ""),
                    "dh_composition": product_data.get("Composition", ""),
                    "dh_description": product_data.get("Description", ""),
                    "dh_keywords": product_data.get("Keywords", ""),
                    "dh_season": product_data.get("Season", ""),
                })

    return vendor_products