"""Count sheet assembly for the Counting page.

Turns rows of the local inventory database (:mod:`local_inventory`) into the
bin-by-bin sheet the page prints.  Kept out of the view so the decision that
matters on a shop floor — which SKUs are worth walking to, and which are only
shown when the user asks for them — can be tested without the Flask stack.
"""
from __future__ import annotations

from typing import Any

from local_inventory import SOURCE_BIN_ONLY

#: Why a line is hidden by default.  ``missing`` is a SKU the last inventory
#: fetch could not find in Shopify, so its bin data is stale; ``empty`` is a
#: SKU with no stock.  Neither is worth a walk, but both stay on the sheet as
#: hidden rows: the page's toggle reveals them, and a hidden row can still have
#: its quantity corrected.
HIDDEN_MISSING = "missing"
HIDDEN_EMPTY = "empty"


def bin_index(inventory: dict[str, dict]) -> dict[str, dict]:
    """Project inventory rows into the shape ``find_items_in_bins`` matches on.

    Bins live in the local database alongside the quantities, so bin matching
    needs nothing from Shipmondo at sheet time.
    """
    return {
        sku: {
            "bin": row["bin"],
            "name": row["product_title"],
            "barcode": row["barcode"],
        }
        for sku, row in inventory.items()
    }


def build_count_sheet(
    inventory: dict[str, dict], bin_items: list[dict]
) -> dict[str, Any]:
    """Group ``bin_items`` into one sheet section per bin.

    ``bin_items`` are ``find_items_in_bins`` matches, already in walking order
    (bin, then SKU), and ``inventory`` is the local database keyed by SKU.

    Returns ``{"bins": [{"bin", "lines"}], "totals": {...}}``.  The totals count
    only the countable lines, so they describe the walk itself; the hidden lines
    are carried on the sheet and counted separately.
    """
    groups: list[dict] = []
    by_bin: dict[str, dict] = {}
    total_units = 0
    counted_skus = 0
    missing_in_shopify = 0
    skipped_empty = 0

    for item in bin_items:
        # The match carries the bin; everything printed about the SKU comes
        # from the inventory row the match was built from.
        row = inventory[item["sku"]]

        hidden_reason = None
        # Only a stale bin counts as missing.  A product added by hand is not in
        # Shopify either, but it was typed in here deliberately, so it is
        # countable stock like any other.
        if row["source"] == SOURCE_BIN_ONLY:
            hidden_reason = HIDDEN_MISSING
            missing_in_shopify += 1
        elif row["on_hand"] == 0:
            hidden_reason = HIDDEN_EMPTY
            skipped_empty += 1
        else:
            total_units += row["on_hand"]
            counted_skus += 1

        group = by_bin.get(item["bin"])
        if group is None:
            group = {"bin": item["bin"], "lines": []}
            by_bin[item["bin"]] = group
            groups.append(group)
        group["lines"].append({
            "sku": row["sku"],
            "product_title": row["product_title"],
            "variant_title": row["variant_title"],
            "vendor": row["vendor"],
            "barcode": row["barcode"],
            # Cost rides along for the CSV export; it is not printed on the
            # sheet a counter carries.
            "unit_cost": row["unit_cost"],
            "on_hand": row["on_hand"],
            "available": row["available"],
            "committed": row["committed"],
            "synced_on_hand": row["synced_on_hand"],
            "locally_modified": row["locally_modified"],
            "source": row["source"],
            "hidden": hidden_reason is not None,
            "hidden_reason": hidden_reason,
        })

    return {
        "bins": groups,
        "totals": {
            "bins": len(groups),
            "skus": counted_skus,
            "units": total_units,
            "skipped_empty": skipped_empty,
            "missing_in_shopify": missing_in_shopify,
        },
    }
