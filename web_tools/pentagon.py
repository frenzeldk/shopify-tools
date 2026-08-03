"""Pentagon Tactical catalogue integration.

Pentagon publishes a single XML stock feed — the same one the nightly
availability sync reads (``vendor_sync/pentagon_sync.py``).  It holds one
``<SHOPITEM>`` per product with a ``<COMB>`` element for every
barcode / colour / size / inseam combination:

    <SHOPITEM>
      <PRODUCTNAME>APOLLO SHORTS K10001</PRODUCTNAME>
      <MODEL>K10001</MODEL><B2BPRICE>12.7500</B2BPRICE><MSRP>26.9000</MSRP>
      <DESCRIPTION_EN>…</DESCRIPTION_EN><IMG>…</IMG>
      <OPTIONS>
        <COMB><BARCODE>5207153077704</BARCODE><COLOR>01-Black</COLOR>
              <SIZE>S</SIZE><INSEAM></INSEAM><STOCK>IN STOCK</STOCK>
              <IMAGE>…</IMAGE></COMB>
      </OPTIONS>
    </SHOPITEM>

The feed carries no SKUs, so they are derived as
``MODEL-COLOURCODE-SIZE[-INSEAM]`` (e.g. ``K10001-01-S``, ``K05039-01-38-30``).
Matching against Shopify still happens on the barcode as well, so existing
variants are recognised even if their SKU was keyed differently.

Public surface used by app.py:
  - pt_fetch_all_products()            — download and parse the XML feed
  - pt_products_to_vendor_format(...)  — flatten to the comparison row shape
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests
import xmltodict

logger = logging.getLogger(__name__)

_FEED_URL = "https://b2b.pentagon.com.gr/index.php?route=feed/xml_stock"

# The feed is served by a WAF that rejects non-browser agents.
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"
)

# Pentagon uses these SIZE values for items that have no size at all.
# Normalising them to "One Size" keeps the size option off the product.
_NO_SIZE_VALUES = {"", "ONE SIZE", "PER PIECE"}

# Pentagon quotes B2B prices in euro; the feed has no currency field.
_CURRENCY = "EUR"


def _as_list(value: Any) -> list:
    """Return *value* as a list — xmltodict collapses single children."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _text(value: Any) -> str:
    """Return the stripped text of an xmltodict node (empty tags parse to None)."""
    if isinstance(value, list):
        value = value[0] if value else ""
    if isinstance(value, dict):
        value = value.get("#text", "")
    return (value or "").strip()


def _price(value: str) -> str:
    """Trim the feed's four-decimal prices ("12.7500" → "12.75")."""
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return value


def _split_color(raw: str) -> tuple[str, str]:
    """Split a feed colour into ``(code, display name)``.

    Pentagon prefixes every colour with its own code and writes spaces as
    dashes, so "08WG-Wolf-Grey" → ("08WG", "Wolf Grey").  The display name is
    what reaches Shopify (and the global colour rename map); the code only
    feeds the SKU.
    """
    code, _, name = raw.partition("-")
    if not name:
        return raw, raw
    return code.strip(), name.replace("-", " ").strip()


def _base_name(product_name: str, model: str) -> str:
    """Return the product name without the model code Pentagon appends to it.

    Names are shouted in the feed ("APOLLO SHORTS K10001"); fully uppercase
    ones are title-cased for the Shopify title, leaving short tokens and
    anything containing a digit alone ("M65 2.0 PANTS" → "M65 2.0 Pants").
    """
    name = product_name.strip()
    if model and name.upper().endswith(model.upper()):
        name = name[: -len(model)].strip()
    if not name:
        name = product_name.strip()
    if name and name == name.upper():
        name = " ".join(
            word if len(word) <= 3 or any(c.isdigit() for c in word) else word.title()
            for word in name.split()
        )
    return name


def _sku(model: str, color_code: str, size: str, inseam: str) -> str:
    """Build ``MODEL-COLOURCODE-SIZE[-INSEAM]``; sizeless items drop the size."""
    parts = [model, color_code]
    if size.upper() not in _NO_SIZE_VALUES:
        parts.append(size.upper().replace(" ", ""))
    if inseam:
        # Inseams are quoted in inches ('32"') — keep the digits only.
        parts.append(re.sub(r"\D", "", inseam) or inseam)
    return "-".join(part for part in parts if part)


def pt_fetch_all_products() -> list[dict]:
    """Download the Pentagon XML stock feed and return its ``SHOPITEM`` entries."""
    response = requests.get(
        _FEED_URL, headers={"User-Agent": _USER_AGENT}, timeout=180
    )
    response.raise_for_status()
    shop = xmltodict.parse(response.text).get("SHOP") or {}
    items = _as_list(shop.get("SHOPITEM"))
    logger.info("pentagon: fetched %d products from the stock feed", len(items))
    return items


def pt_products_to_vendor_format(products: list[dict]) -> list[dict]:
    """Flatten Pentagon ``SHOPITEM`` entries into the vendor product list
    format expected by ``compare_vendor_products()``.

    One row per ``<COMB>``, with:
      - ``length``: the inseam ('32"'), which becomes the "Længde" option.
        The key is always present so the Helikon SKU-letter fallback stays
        out of the way — an empty value means the item has no length.
      - ``variant_image_url`` / ``color_images``: the colour's images, falling
        back to the product-level ``IMG``.
      - ``pt_*``: product-level fields used to auto-generate a description.
    """
    vendor_products: list[dict] = []

    for item in products:
        model = _text(item.get("MODEL"))
        combs = _as_list((item.get("OPTIONS") or {}).get("COMB"))
        if not model or not combs:
            continue

        base_name = _base_name(_text(item.get("PRODUCTNAME")), model)
        product_image = _text(item.get("IMG"))
        price = _price(
            _text(item.get("B2BSPECIALPRICE")) or _text(item.get("B2BPRICE"))
        )
        msrp = _price(_text(item.get("MSRP")))
        categories = " > ".join(
            _text(value) for value in (item.get("CATEGORY") or {}).values() if _text(value)
        )

        # Collect each colour's images up front so every variant of a colour
        # carries the full set (the modal offers them when creating variants).
        color_images: dict[str, list[str]] = {}
        for comb in combs:
            code, _name = _split_color(_text(comb.get("COLOR")))
            image = _text(comb.get("IMAGE"))
            images = color_images.setdefault(code, [])
            if image and image not in images:
                images.append(image)

        for comb in combs:
            barcode = _text(comb.get("BARCODE"))
            raw_color = _text(comb.get("COLOR"))
            if not barcode or not raw_color:
                continue

            color_code, color_name = _split_color(raw_color)
            raw_size = _text(comb.get("SIZE"))
            size = "One Size" if raw_size.upper() in _NO_SIZE_VALUES else raw_size
            inseam = _text(comb.get("INSEAM"))
            images = color_images.get(color_code) or (
                [product_image] if product_image else []
            )

            vendor_products.append({
                "sku": _sku(model, color_code, raw_size, inseam),
                "ean": barcode,
                "hs_code": "",
                "size": size,
                "length": inseam,
                "name": f"{base_name} - {color_name}",
                "product_code": model,
                "base_name": base_name,
                "color": color_name,
                "size_eu": "",
                "size_usa": "",
                "price": price,
                "msrp": msrp,
                "currency": _CURRENCY,
                "weight": "",
                "weight_unit": "",
                "country_of_origin": "",
                # "IN STOCK" → allow overselling, mirroring the nightly sync
                "inventory_policy": (
                    "CONTINUE" if _text(comb.get("STOCK")) == "IN STOCK" else "DENY"
                ),
                "variant_image_url": images[0] if images else "",
                "color_images": list(images),
                # Product-level fields for description generation
                "pt_description": _text(item.get("DESCRIPTION_EN")),
                "pt_categories": categories,
                "pt_link": _text(item.get("LINK")),
            })

    return vendor_products
