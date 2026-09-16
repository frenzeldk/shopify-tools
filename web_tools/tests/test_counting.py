"""Unit tests for the Counting page's moving parts.

The route itself needs the whole Flask/OIDC/Shopify stack, so what is covered
here is the logic it delegates to: turning a free-text bin field into bins,
turning Shopify variants into on-hand quantities, and the local inventory
database the count sheet is built from.

    python -m unittest discover -s web_tools/tests
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import counting_sheet  # noqa: E402
import local_inventory  # noqa: E402
import shipmondo  # noqa: E402
import shopify  # noqa: E402


def _item(bin_name: str, name: str = "", barcode: str = "") -> dict:
    return {"id": 1, "bin": bin_name, "name": name, "barcode": barcode}


class ExpandBinPatterns(unittest.TestCase):
    def test_plain_bins_pass_through_upper_cased(self):
        patterns, errors = shipmondo.expand_bin_patterns("a1-05, B2-07")
        self.assertEqual(patterns, ["A1-05", "B2-07"])
        self.assertEqual(errors, [])

    def test_separators_are_interchangeable(self):
        patterns, _ = shipmondo.expand_bin_patterns("A1\nB2; C3 D4")
        self.assertEqual(patterns, ["A1", "B2", "C3", "D4"])

    def test_numeric_range_expands(self):
        patterns, _ = shipmondo.expand_bin_patterns("A1-[1-4]")
        self.assertEqual(patterns, ["A1-1", "A1-2", "A1-3", "A1-4"])

    def test_padding_is_kept_when_written(self):
        patterns, _ = shipmondo.expand_bin_patterns("A1-[08-11]")
        self.assertEqual(patterns, ["A1-08", "A1-09", "A1-10", "A1-11"])

    def test_letter_range_expands(self):
        patterns, _ = shipmondo.expand_bin_patterns("[A-C]-1")
        self.assertEqual(patterns, ["A-1", "B-1", "C-1"])

    def test_ranges_combine(self):
        patterns, _ = shipmondo.expand_bin_patterns("[A-B]-[1-2]")
        self.assertEqual(patterns, ["A-1", "A-2", "B-1", "B-2"])

    def test_wildcards_survive_expansion(self):
        patterns, _ = shipmondo.expand_bin_patterns("A1-*")
        self.assertEqual(patterns, ["A1-*"])

    def test_duplicates_are_dropped(self):
        patterns, _ = shipmondo.expand_bin_patterns("A1-01 A1-01 a1-01")
        self.assertEqual(patterns, ["A1-01"])

    def test_backwards_range_is_reported_not_expanded(self):
        patterns, errors = shipmondo.expand_bin_patterns("A1-[9-2]")
        self.assertEqual(patterns, [])
        self.assertIn("backwards", errors[0])

    def test_oversized_range_is_refused(self):
        patterns, errors = shipmondo.expand_bin_patterns("A1-[1-99999]")
        self.assertEqual(patterns, [])
        self.assertIn(str(shipmondo.MAX_RANGE_SPAN), errors[0])

    def test_too_many_tokens_is_refused(self):
        raw = " ".join(f"A{i}" for i in range(shipmondo.MAX_BIN_PATTERN_TOKENS + 1))
        patterns, errors = shipmondo.expand_bin_patterns(raw)
        self.assertEqual(patterns, [])
        self.assertIn(str(shipmondo.MAX_BIN_PATTERN_TOKENS), errors[0])

    def test_a_bad_token_does_not_discard_the_good_ones(self):
        patterns, errors = shipmondo.expand_bin_patterns("A1-01 B2-[9-2]")
        self.assertEqual(patterns, ["A1-01"])
        self.assertEqual(len(errors), 1)


class BinSortKey(unittest.TestCase):
    def test_bins_sort_numerically_not_lexically(self):
        bins = ["A1-10", "A1-2", "A1-1", "B1-1"]
        self.assertEqual(
            sorted(bins, key=shipmondo.bin_sort_key),
            ["A1-1", "A1-2", "A1-10", "B1-1"],
        )


class FindItemsInBins(unittest.TestCase):
    items = {
        "SKU-1": _item("A1-01", "Jacket M", "111"),
        "SKU-2": _item("A1-02", "Jacket L"),
        "SKU-3": _item("A1-10", "Boots"),
        "SKU-4": _item("B9-01", "Hat"),
        "SKU-5": _item("", "Unbinned"),
        "SKU-6": _item("a1-03", "Lower-cased bin"),
    }

    def test_wildcard_selects_the_whole_aisle(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-*"])
        self.assertEqual(
            [i["sku"] for i in result["items"]],
            ["SKU-1", "SKU-2", "SKU-6", "SKU-3"],
        )

    def test_literal_patterns_match_case_insensitively(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-03"])
        self.assertEqual([i["sku"] for i in result["items"]], ["SKU-6"])

    def test_items_without_a_bin_are_ignored(self):
        result = shipmondo.find_items_in_bins(self.items, ["*"])
        self.assertNotIn("SKU-5", [i["sku"] for i in result["items"]])

    def test_bins_come_back_in_walking_order(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-*", "B9-01"])
        self.assertEqual(result["bins"], ["A1-01", "A1-02", "a1-03", "A1-10", "B9-01"])

    def test_patterns_matching_nothing_are_reported(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-01", "Z9-99"])
        self.assertEqual(result["unmatched_patterns"], ["Z9-99"])

    def test_a_matched_pattern_is_not_reported(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-*"])
        self.assertEqual(result["unmatched_patterns"], [])

    def test_carried_fields(self):
        result = shipmondo.find_items_in_bins(self.items, ["A1-01"])
        self.assertEqual(result["items"], [{
            "sku": "SKU-1", "bin": "A1-01", "name": "Jacket M", "barcode": "111",
        }])


class FetchOnHandBySkus(unittest.TestCase):
    """`_execute` is stubbed: these assert the request shape and the folding of
    inventory levels, not Shopify's behaviour."""

    def setUp(self):
        self.queries: list[str] = []
        self.addCleanup(setattr, shopify, "_execute", shopify._execute)

    def _stub(self, edges_for):
        def _execute(document, *, variable_values=None):
            self.queries.append(variable_values["query"])
            return {
                "productVariants": {
                    "edges": edges_for(variable_values["query"]),
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            }
        shopify._execute = _execute

    @staticmethod
    def _level(location, on_hand, available, committed):
        return {"node": {
            "location": {"name": location},
            "quantities": [
                {"name": "on_hand", "quantity": on_hand},
                {"name": "available", "quantity": available},
                {"name": "committed", "quantity": committed},
            ],
        }}

    @classmethod
    def _variant(cls, sku, on_hand, available, committed, location="Warehouse"):
        return cls._variant_with_levels(
            sku, [cls._level(location, on_hand, available, committed)]
        )

    @staticmethod
    def _variant_with_levels(sku, levels):
        return {"node": {
            "sku": sku,
            "title": "Green / M",
            "barcode": "570",
            "product": {"title": "Alpha Jacket", "vendor": "ACME"},
            "inventoryItem": {
                "tracked": True,
                "unitCost": {"amount": "149.50"},
                "inventoryLevels": {"edges": levels},
            },
        }}

    def test_skus_are_or_ed_into_the_search_query(self):
        self._stub(lambda q: [])
        shopify.fetch_on_hand_by_skus(["SKU-2", "SKU-1"])
        self.assertEqual(self.queries, ['sku:"SKU-1" OR sku:"SKU-2"'])

    def test_skus_are_batched(self):
        self._stub(lambda q: [])
        skus = [f"SKU-{i:03d}" for i in range(shopify.SKU_QUERY_BATCH + 1)]
        shopify.fetch_on_hand_by_skus(skus)
        self.assertEqual(len(self.queries), 2)

    def test_quotes_and_backslashes_are_escaped(self):
        self.assertEqual(shopify._sku_query_term('A"B\\C'), 'sku:"A\\"B\\\\C"')

    def test_no_skus_makes_no_request(self):
        self._stub(lambda q: [])
        self.assertEqual(shopify.fetch_on_hand_by_skus([""]), {})
        self.assertEqual(self.queries, [])

    def test_quantities_are_summed_across_locations(self):
        self._stub(lambda q: [self._variant_with_levels("SKU-1", [
            self._level("Warehouse", 7, 5, 2),
            self._level("Shop", 3, 3, 0),
        ])])
        result = shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]
        self.assertEqual(
            (result["on_hand"], result["available"], result["committed"]), (10, 8, 2)
        )
        self.assertEqual(result["locations"], [
            {"name": "Warehouse", "on_hand": 7},
            {"name": "Shop", "on_hand": 3},
        ])

    def test_variants_shopify_volunteers_are_dropped(self):
        # Shopify's search is token-based: a query for SKU-1 can also return
        # SKU-10, which must not land on the count sheet.
        self._stub(lambda q: [
            self._variant("SKU-1", 7, 5, 2),
            self._variant("SKU-10", 99, 99, 0),
        ])
        result = shopify.fetch_on_hand_by_skus(["SKU-1"])
        self.assertEqual(list(result), ["SKU-1"])

    def test_placeholder_variant_title_is_dropped(self):
        # A product without options has one variant Shopify names "Default
        # Title"; that is noise on a count sheet.
        variant = self._variant("SKU-1", 7, 5, 2)
        variant["node"]["title"] = "Default Title"
        self._stub(lambda q: [variant])
        self.assertEqual(shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]["variant_title"], "")

    def test_unit_cost_is_read_as_a_number(self):
        self._stub(lambda q: [self._variant("SKU-1", 7, 5, 2)])
        self.assertEqual(
            shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]["unit_cost"], 149.5
        )

    def test_a_missing_unit_cost_is_unknown_not_zero(self):
        # Shopify holds no cost for the item; a cost report must not read that
        # as free.
        variant = self._variant("SKU-1", 7, 5, 2)
        variant["node"]["inventoryItem"]["unitCost"] = None
        self._stub(lambda q: [variant])
        self.assertIsNone(shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]["unit_cost"])

    def test_an_unreadable_unit_cost_is_unknown(self):
        variant = self._variant("SKU-1", 7, 5, 2)
        variant["node"]["inventoryItem"]["unitCost"] = {"amount": "not a number"}
        self._stub(lambda q: [variant])
        self.assertIsNone(shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]["unit_cost"])

    def test_real_variant_titles_are_kept(self):
        self._stub(lambda q: [self._variant("SKU-1", 7, 5, 2)])
        self.assertEqual(
            shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"]["variant_title"], "Green / M"
        )

    def test_unknown_skus_are_simply_absent(self):
        self._stub(lambda q: [self._variant("SKU-1", 7, 5, 2)])
        result = shopify.fetch_on_hand_by_skus(["SKU-1", "SKU-GONE"])
        self.assertEqual(list(result), ["SKU-1"])

    def test_reported_fields(self):
        self._stub(lambda q: [self._variant("SKU-1", 7, 5, 2)])
        self.assertEqual(shopify.fetch_on_hand_by_skus(["SKU-1"])["SKU-1"], {
            "sku": "SKU-1",
            "variant_title": "Green / M",
            "product_title": "Alpha Jacket",
            "vendor": "ACME",
            "barcode": "570",
            "tracked": True,
            "unit_cost": 149.5,
            "on_hand": 7,
            "available": 5,
            "committed": 2,
            "locations": [{"name": "Warehouse", "on_hand": 7}],
        })


class FetchAllOnHand(unittest.TestCase):
    """The full-catalogue fetch that fills the local inventory database."""

    def setUp(self):
        self.pages: list[dict] = []
        self.cursors: list[str | None] = []
        self.addCleanup(setattr, shopify, "_execute", shopify._execute)

    def _stub(self, pages):
        """``pages`` is a list of edge lists, served one call at a time."""
        remaining = list(pages)

        def _execute(document, *, variable_values=None):
            self.cursors.append(variable_values["cursor"])
            edges = remaining.pop(0)
            return {
                "productVariants": {
                    "edges": edges,
                    "pageInfo": {
                        "hasNextPage": bool(remaining),
                        "endCursor": f"cursor-{len(self.cursors)}",
                    },
                }
            }
        shopify._execute = _execute

    def test_every_page_is_walked(self):
        self._stub([
            [FetchOnHandBySkus._variant("SKU-1", 7, 5, 2)],
            [FetchOnHandBySkus._variant("SKU-2", 3, 3, 0)],
        ])
        rows = shopify.fetch_all_on_hand()
        self.assertEqual([row["sku"] for row in rows], ["SKU-1", "SKU-2"])
        self.assertEqual(self.cursors, [None, "cursor-1"])

    def test_variants_without_a_sku_are_skipped(self):
        # The local database is keyed by SKU, so a variant without one cannot be
        # matched to a bin or counted.
        nameless = FetchOnHandBySkus._variant("", 7, 5, 2)
        self._stub([[nameless, FetchOnHandBySkus._variant("SKU-1", 1, 1, 0)]])
        self.assertEqual(
            [row["sku"] for row in shopify.fetch_all_on_hand()], ["SKU-1"]
        )

    def test_rows_have_the_by_sku_shape(self):
        self._stub([[FetchOnHandBySkus._variant("SKU-1", 7, 5, 2)]])
        self.assertEqual(shopify.fetch_all_on_hand()[0], {
            "sku": "SKU-1",
            "variant_title": "Green / M",
            "product_title": "Alpha Jacket",
            "vendor": "ACME",
            "barcode": "570",
            "tracked": True,
            "unit_cost": 149.5,
            "on_hand": 7,
            "available": 5,
            "committed": 2,
            "locations": [{"name": "Warehouse", "on_hand": 7}],
        })


def _variant_row(sku, on_hand=5, **overrides) -> dict:
    row = {
        "sku": sku,
        "variant_title": "Green / M",
        "product_title": "Alpha Jacket",
        "vendor": "ACME",
        "barcode": "570",
        "tracked": True,
        "unit_cost": 149.5,
        "on_hand": on_hand,
        "available": on_hand,
        "committed": 0,
        "locations": [],
    }
    row.update(overrides)
    return row


class MergeRows(unittest.TestCase):
    """Shopify variants + Shipmondo bins → rows of the local table."""

    def test_bins_are_attached_by_sku(self):
        rows = local_inventory.merge_rows(
            [_variant_row("SKU-1")], {"SKU-1": {"bin": "A1-01"}}
        )
        self.assertEqual(rows[0]["bin"], "A1-01")
        self.assertEqual(rows[0]["source"], local_inventory.SOURCE_SHOPIFY)

    def test_a_variant_without_a_bin_is_still_stored(self):
        rows = local_inventory.merge_rows([_variant_row("SKU-1")], {})
        self.assertEqual(rows[0]["bin"], "")

    def test_the_synced_quantity_starts_equal_to_on_hand(self):
        rows = local_inventory.merge_rows([_variant_row("SKU-1", 9)], {})
        self.assertEqual((rows[0]["on_hand"], rows[0]["synced_on_hand"]), (9, 9))

    def test_variants_without_a_sku_are_dropped(self):
        self.assertEqual(local_inventory.merge_rows([_variant_row("")], {}), [])

    def test_the_first_of_two_variants_sharing_a_sku_wins(self):
        rows = local_inventory.merge_rows(
            [_variant_row("SKU-1", 5), _variant_row("SKU-1", 99)], {}
        )
        self.assertEqual([(r["sku"], r["on_hand"]) for r in rows], [("SKU-1", 5)])

    def test_a_binned_sku_shopify_lost_is_kept_as_bin_only(self):
        rows = local_inventory.merge_rows(
            [], {"SKU-GONE": {"bin": "A1-02", "name": "Old hat", "barcode": "9"}}
        )
        self.assertEqual(rows[0]["sku"], "SKU-GONE")
        self.assertEqual(rows[0]["source"], local_inventory.SOURCE_BIN_ONLY)
        self.assertEqual(rows[0]["on_hand"], 0)
        self.assertEqual(rows[0]["product_title"], "Old hat")

    def test_an_unbinned_shipmondo_only_sku_is_ignored(self):
        # Nothing can be counted without a bin to walk to.
        rows = local_inventory.merge_rows([], {"SKU-X": {"bin": "", "name": "Hat"}})
        self.assertEqual(rows, [])

    def test_the_unit_cost_is_carried_over(self):
        rows = local_inventory.merge_rows([_variant_row("SKU-1", unit_cost=12.5)], {})
        self.assertEqual(rows[0]["unit_cost"], 12.5)

    def test_a_sku_shopify_lost_has_no_cost(self):
        rows = local_inventory.merge_rows([], {"SKU-GONE": {"bin": "A1-02"}})
        self.assertIsNone(rows[0]["unit_cost"])

    def test_an_unknown_cost_stays_unknown(self):
        rows = local_inventory.merge_rows([_variant_row("SKU-1", unit_cost=None)], {})
        self.assertIsNone(rows[0]["unit_cost"])

    def test_shopify_barcode_wins_over_shipmondo(self):
        rows = local_inventory.merge_rows(
            [_variant_row("SKU-1", barcode="570")],
            {"SKU-1": {"bin": "A1-01", "barcode": "111"}},
        )
        self.assertEqual(rows[0]["barcode"], "570")

    def test_shipmondo_barcode_fills_a_gap(self):
        rows = local_inventory.merge_rows(
            [_variant_row("SKU-1", barcode="")],
            {"SKU-1": {"bin": "A1-01", "barcode": "111"}},
        )
        self.assertEqual(rows[0]["barcode"], "111")


class LocalInventoryStore(unittest.TestCase):
    """The database itself: replacing it, reading it and editing a quantity."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "inventory.db")
        local_inventory.init_schema(self.db)

    def _store(self, *rows):
        return local_inventory.replace_all(self.db, list(rows))

    def test_an_empty_store_reads_as_empty(self):
        self.assertEqual(local_inventory.load_all(self.db), {})
        self.assertEqual(local_inventory.status(self.db)["total_skus"], 0)
        self.assertIsNone(local_inventory.status(self.db)["last_synced"])

    def test_a_missing_database_reads_as_empty(self):
        # The page must work before the first fetch has ever run.
        missing = str(Path(self.db).parent / "not-created-yet.db")
        self.assertEqual(local_inventory.load_all(missing), {})
        self.assertEqual(local_inventory.status(missing)["total_skus"], 0)

    def test_rows_survive_a_round_trip(self):
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 7)], {"SKU-1": {"bin": "A1-01"}}
        ))
        row = local_inventory.load_all(self.db)["SKU-1"]
        self.assertEqual(row["bin"], "A1-01")
        self.assertEqual(row["on_hand"], 7)
        self.assertTrue(row["tracked"])
        self.assertEqual(row["source"], local_inventory.SOURCE_SHOPIFY)
        self.assertFalse(row["locally_modified"])

    def test_the_unit_cost_survives_a_round_trip(self):
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", unit_cost=12.5), _variant_row("SKU-2", unit_cost=None)],
            {},
        ))
        rows = local_inventory.load_all(self.db)
        self.assertEqual(rows["SKU-1"]["unit_cost"], 12.5)
        self.assertIsNone(rows["SKU-2"]["unit_cost"])

    def test_a_local_quantity_edit_leaves_the_cost_alone(self):
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1", 5, unit_cost=12.5)], {}))
        row = local_inventory.set_on_hand(self.db, "SKU-1", 2)
        self.assertEqual(row["unit_cost"], 12.5)

    def test_replace_all_removes_rows_the_new_fetch_lacks(self):
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1")], {}))
        self._store(*local_inventory.merge_rows([_variant_row("SKU-2")], {}))
        self.assertEqual(list(local_inventory.load_all(self.db)), ["SKU-2"])

    def test_replace_all_discards_local_edits(self):
        # This is the documented cost of the Fetch Full Inventory button.
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1", 5)], {}))
        local_inventory.set_on_hand(self.db, "SKU-1", 3)
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1", 5)], {}))
        row = local_inventory.get(self.db, "SKU-1")
        self.assertEqual(row["on_hand"], 5)
        self.assertFalse(row["locally_modified"])

    def test_replace_all_reports_and_records_the_sync(self):
        result = self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 7), _variant_row("SKU-2", 3)],
            {"SKU-1": {"bin": "A1-01"}},
        ))
        self.assertEqual((result["skus"], result["units"], result["with_bins"]), (2, 10, 1))
        status = local_inventory.status(self.db)
        self.assertEqual(status["last_synced"], result["synced_at"])
        self.assertEqual(status["units"], 10)
        self.assertEqual(status["with_bins"], 1)

    def test_setting_a_quantity_marks_the_row_as_edited(self):
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1", 5)], {}))
        row = local_inventory.set_on_hand(self.db, "SKU-1", 4)
        self.assertEqual(row["on_hand"], 4)
        self.assertEqual(row["synced_on_hand"], 5)
        self.assertTrue(row["locally_modified"])
        self.assertEqual(local_inventory.status(self.db)["locally_modified"], 1)

    def test_available_keeps_shopify_s_invariant(self):
        # on_hand = available + committed, so a corrected count must not imply
        # stock that is already promised to an open order.
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 7, available=5, committed=2)], {}
        ))
        row = local_inventory.set_on_hand(self.db, "SKU-1", 10)
        self.assertEqual((row["available"], row["committed"]), (8, 2))

    def test_setting_the_quantity_back_clears_the_edit_marker(self):
        self._store(*local_inventory.merge_rows([_variant_row("SKU-1", 5)], {}))
        local_inventory.set_on_hand(self.db, "SKU-1", 4)
        row = local_inventory.set_on_hand(self.db, "SKU-1", 5)
        self.assertFalse(row["locally_modified"])
        self.assertEqual(local_inventory.status(self.db)["locally_modified"], 0)

    def test_an_unknown_sku_cannot_be_edited(self):
        self.assertIsNone(local_inventory.set_on_hand(self.db, "SKU-NOPE", 1))

    def test_a_missing_sku_reads_as_none(self):
        self.assertIsNone(local_inventory.get(self.db, "SKU-NOPE"))

    def test_a_local_edit_does_not_move_other_rows(self):
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 5), _variant_row("SKU-2", 8)], {}
        ))
        local_inventory.set_on_hand(self.db, "SKU-1", 0)
        self.assertEqual(local_inventory.get(self.db, "SKU-2")["on_hand"], 8)

    def test_bins_from_the_store_drive_find_items_in_bins(self):
        # The count sheet builds its bin index from these rows, so the two must
        # fit together without the Shipmondo cache being present.
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 5), _variant_row("SKU-2", 1)],
            {"SKU-1": {"bin": "A1-01"}, "SKU-2": {"bin": "B2-01"}},
        ))
        rows = local_inventory.load_all(self.db)
        index = {
            sku: {"bin": row["bin"], "name": row["product_title"], "barcode": row["barcode"]}
            for sku, row in rows.items()
        }
        match = shipmondo.find_items_in_bins(index, ["A1-*"])
        self.assertEqual([item["sku"] for item in match["items"]], ["SKU-1"])



class BuildCountSheet(unittest.TestCase):
    """What lands on the sheet, and what is only there when the toggle is on."""

    @staticmethod
    def _row(sku, bin_name, on_hand, **overrides):
        row = {
            "sku": sku,
            "product_title": "Alpha Jacket",
            "variant_title": "Green / M",
            "vendor": "ACME",
            "barcode": "570",
            "bin": bin_name,
            "tracked": True,
            "source": local_inventory.SOURCE_SHOPIFY,
            "unit_cost": 149.5,
            "on_hand": on_hand,
            "available": on_hand,
            "committed": 0,
            "synced_on_hand": on_hand,
            "locally_modified": False,
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        row.update(overrides)
        return row

    def _sheet(self, rows, patterns=("*",)):
        inventory = {row["sku"]: row for row in rows}
        match = shipmondo.find_items_in_bins(
            counting_sheet.bin_index(inventory), list(patterns)
        )
        return counting_sheet.build_count_sheet(inventory, match["items"])

    def test_one_group_per_bin_in_walking_order(self):
        sheet = self._sheet([
            self._row("SKU-2", "A1-10", 1),
            self._row("SKU-1", "A1-2", 1),
        ])
        self.assertEqual([g["bin"] for g in sheet["bins"]], ["A1-2", "A1-10"])

    def test_countable_lines_are_not_hidden(self):
        sheet = self._sheet([self._row("SKU-1", "A1-01", 4)])
        line = sheet["bins"][0]["lines"][0]
        self.assertFalse(line["hidden"])
        self.assertIsNone(line["hidden_reason"])
        self.assertEqual(sheet["totals"], {
            "bins": 1, "skus": 1, "units": 4,
            "skipped_empty": 0, "missing_in_shopify": 0,
        })

    def test_an_empty_sku_stays_on_the_sheet_as_hidden(self):
        # Hidden, not dropped: the page's toggle reveals it, and its quantity
        # can still be corrected from there.
        sheet = self._sheet([self._row("SKU-1", "A1-01", 0)])
        line = sheet["bins"][0]["lines"][0]
        self.assertTrue(line["hidden"])
        self.assertEqual(line["hidden_reason"], counting_sheet.HIDDEN_EMPTY)
        self.assertEqual(sheet["totals"]["skipped_empty"], 1)

    def test_a_stale_bin_is_hidden_as_missing(self):
        sheet = self._sheet([self._row(
            "SKU-1", "A1-01", 0, source=local_inventory.SOURCE_BIN_ONLY
        )])
        line = sheet["bins"][0]["lines"][0]
        self.assertEqual(line["hidden_reason"], counting_sheet.HIDDEN_MISSING)
        self.assertEqual(sheet["totals"]["missing_in_shopify"], 1)
        self.assertEqual(sheet["totals"]["skipped_empty"], 0)

    def test_a_hand_added_product_is_countable_not_missing(self):
        # It is not in Shopify either, but it was typed in deliberately, so it
        # belongs on the sheet rather than behind the toggle.
        sheet = self._sheet([
            self._row("LOCAL-1", "A1-01", 4, source=local_inventory.SOURCE_LOCAL)
        ])
        line = sheet["bins"][0]["lines"][0]
        self.assertFalse(line["hidden"])
        self.assertEqual(sheet["totals"], {
            "bins": 1, "skus": 1, "units": 4,
            "skipped_empty": 0, "missing_in_shopify": 0,
        })

    def test_a_hand_added_product_with_no_stock_is_hidden_as_empty(self):
        sheet = self._sheet([
            self._row("LOCAL-1", "A1-01", 0, source=local_inventory.SOURCE_LOCAL)
        ])
        self.assertEqual(
            sheet["bins"][0]["lines"][0]["hidden_reason"], counting_sheet.HIDDEN_EMPTY
        )
        self.assertEqual(sheet["totals"]["missing_in_shopify"], 0)

    def test_hidden_lines_are_left_out_of_the_totals(self):
        sheet = self._sheet([
            self._row("SKU-1", "A1-01", 4),
            self._row("SKU-2", "A1-01", 0),
            self._row("SKU-3", "A1-01", 0, source=local_inventory.SOURCE_BIN_ONLY),
        ])
        self.assertEqual(len(sheet["bins"][0]["lines"]), 3)
        self.assertEqual(sheet["totals"], {
            "bins": 1, "skus": 1, "units": 4,
            "skipped_empty": 1, "missing_in_shopify": 1,
        })

    def test_a_bin_of_nothing_but_hidden_skus_still_reaches_the_page(self):
        sheet = self._sheet([self._row("SKU-1", "A1-01", 0)])
        self.assertEqual(sheet["totals"]["bins"], 1)
        self.assertEqual(sheet["totals"]["skus"], 0)

    def test_a_local_edit_is_reported_on_the_line(self):
        sheet = self._sheet([
            self._row("SKU-1", "A1-01", 3, synced_on_hand=5, locally_modified=True)
        ])
        line = sheet["bins"][0]["lines"][0]
        self.assertEqual((line["on_hand"], line["synced_on_hand"]), (3, 5))
        self.assertTrue(line["locally_modified"])
        # The corrected figure is what a counter checks against.
        self.assertEqual(sheet["totals"]["units"], 3)

    def test_the_line_carries_the_cost_the_export_needs(self):
        sheet = self._sheet([self._row("SKU-1", "A1-01", 3, unit_cost=12.5)])
        self.assertEqual(sheet["bins"][0]["lines"][0]["unit_cost"], 12.5)

    def test_a_line_without_a_cost_reports_none(self):
        sheet = self._sheet([self._row("SKU-1", "A1-01", 3, unit_cost=None)])
        self.assertIsNone(sheet["bins"][0]["lines"][0]["unit_cost"])

    def test_the_line_carries_the_row_s_own_barcode(self):
        sheet = self._sheet([self._row("SKU-1", "A1-01", 1, barcode="111")])
        self.assertEqual(sheet["bins"][0]["lines"][0]["barcode"], "111")

    def test_unbinned_rows_never_reach_a_sheet(self):
        sheet = self._sheet([
            self._row("SKU-1", "", 5), self._row("SKU-2", "A1-01", 5)
        ])
        self.assertEqual(
            [line["sku"] for line in sheet["bins"][0]["lines"]], ["SKU-2"]
        )

    def test_only_matching_bins_are_included(self):
        sheet = self._sheet(
            [self._row("SKU-1", "A1-01", 5), self._row("SKU-2", "B2-01", 5)],
            patterns=["A1-*"],
        )
        self.assertEqual([g["bin"] for g in sheet["bins"]], ["A1-01"])



class AddProduct(unittest.TestCase):
    """Hand-typed products: what the store accepts, and what it refuses."""

    VALID = {
        "vendor": "ACME",
        "product_title": "Vinterjakke",
        "variant_title": "Sort / M",
        "sku": "ACME-001",
        "barcode": "5700000000001",
        "on_hand": 7,
        "unit_cost": "149.50",
    }

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "inventory.db")
        local_inventory.init_schema(self.db)

    def _add(self, **overrides):
        fields = dict(self.VALID)
        fields.update(overrides)
        return local_inventory.add_product(self.db, fields)

    def _refusal(self, **overrides):
        with self.assertRaises(local_inventory.ProductError) as caught:
            self._add(**overrides)
        return str(caught.exception)

    def test_a_product_is_stored_and_read_back(self):
        self._add()
        row = local_inventory.get(self.db, "ACME-001")
        self.assertEqual(row["vendor"], "ACME")
        self.assertEqual(row["product_title"], "Vinterjakke")
        self.assertEqual(row["variant_title"], "Sort / M")
        self.assertEqual(row["barcode"], "5700000000001")
        self.assertEqual(row["on_hand"], 7)
        self.assertEqual(row["unit_cost"], 149.5)

    def test_it_is_marked_as_added_here_not_as_a_lost_shopify_sku(self):
        # The distinction decides whether a count sheet hides the row.
        self.assertEqual(self._add()["source"], local_inventory.SOURCE_LOCAL)

    def test_the_whole_quantity_is_available(self):
        # Nothing can be committed to an order for stock Shopify cannot see.
        row = self._add(on_hand=7)
        self.assertEqual((row["available"], row["committed"]), (7, 0))

    def test_it_is_not_counted_as_a_changed_quantity(self):
        # It is reported as added instead, so the two counts do not overlap.
        self._add()
        status = local_inventory.status(self.db)
        self.assertEqual(status["added_locally"], 1)
        self.assertEqual(status["locally_modified"], 0)
        self.assertFalse(local_inventory.get(self.db, "ACME-001")["locally_modified"])

    def test_fields_are_trimmed(self):
        row = self._add(vendor="  ACME  ", sku="  ACME-002  ")
        self.assertEqual((row["vendor"], row["sku"]), ("ACME", "ACME-002"))

    def test_the_barcode_is_optional(self):
        self.assertEqual(self._add(barcode="")["barcode"], "")

    def test_a_bin_may_be_given_so_the_row_reaches_a_count_sheet(self):
        self.assertEqual(self._add(bin="A1-01")["bin"], "A1-01")

    def test_a_product_without_a_bin_is_still_accepted(self):
        self.assertEqual(self._add()["bin"], "")

    def test_zero_stock_is_a_valid_amount(self):
        self.assertEqual(self._add(on_hand=0)["on_hand"], 0)

    def test_a_free_item_is_a_valid_cost(self):
        self.assertEqual(self._add(unit_cost=0)["unit_cost"], 0.0)

    def test_a_comma_decimal_is_read_as_a_decimal(self):
        # What a Danish keyboard produces; the figure is unambiguous.
        self.assertEqual(self._add(unit_cost="149,50")["unit_cost"], 149.5)

    def test_every_mandatory_field_is_required(self):
        for field, label in (
            ("vendor", "Vendor"),
            ("product_title", "Product name"),
            ("variant_title", "Variant"),
            ("sku", "SKU"),
        ):
            with self.subTest(field=field):
                self.assertIn(label, self._refusal(**{field: "   "}))

    def test_an_over_long_field_is_refused(self):
        self.assertIn(
            str(local_inventory.MAX_FIELD_CHARS),
            self._refusal(product_title="x" * (local_inventory.MAX_FIELD_CHARS + 1)),
        )

    def test_a_missing_amount_is_refused(self):
        self.assertIn("Amount", self._refusal(on_hand=""))

    def test_a_fractional_amount_is_refused(self):
        # Truncating it in silence would invent a count.
        self.assertIn("whole number", self._refusal(on_hand=1.5))

    def test_a_negative_amount_is_refused(self):
        self.assertIn("Amount", self._refusal(on_hand=-1))

    def test_an_absurd_amount_is_refused(self):
        self.assertIn(
            "Amount", self._refusal(on_hand=local_inventory.MAX_LOCAL_QUANTITY + 1)
        )

    def test_a_missing_cost_is_refused(self):
        self.assertIn("Cost", self._refusal(unit_cost=""))

    def test_an_unreadable_cost_is_refused(self):
        self.assertIn("Cost", self._refusal(unit_cost="gratis"))

    def test_a_negative_cost_is_refused(self):
        self.assertIn("Cost", self._refusal(unit_cost=-1))

    def test_an_infinite_cost_is_refused(self):
        self.assertIn("Cost", self._refusal(unit_cost="inf"))

    def test_a_duplicate_sku_is_refused(self):
        self._add()
        self.assertIn("already in the local inventory", self._refusal())

    def test_a_refused_product_is_not_stored(self):
        self._refusal(vendor="")
        self.assertEqual(local_inventory.load_all(self.db), {})

    def test_a_hand_added_product_does_not_survive_a_fetch(self):
        # Documented consequence of Fetch Full Inventory: Shopify has no record
        # of the row, so it is deleted rather than refreshed.
        self._add(bin="A1-01")
        local_inventory.replace_all(self.db, local_inventory.merge_rows(
            [_variant_row("SKU-1")], {}
        ))
        self.assertIsNone(local_inventory.get(self.db, "ACME-001"))

    def test_a_hand_added_product_can_be_counted(self):
        self._add(bin="A1-01", on_hand=7)
        row = local_inventory.set_on_hand(self.db, "ACME-001", 5)
        self.assertEqual(row["on_hand"], 5)
        self.assertEqual(row["source"], local_inventory.SOURCE_LOCAL)



class UpdateBins(unittest.TestCase):
    """The bins-only update: Shipmondo owns bins, and nothing else may move."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "inventory.db")
        local_inventory.init_schema(self.db)

    def _store(self, *rows):
        local_inventory.replace_all(self.db, list(rows))

    def _stored(self, bins):
        """Store one Shopify row per SKU with the given bins."""
        self._store(*local_inventory.merge_rows(
            [_variant_row(sku) for sku in bins],
            {sku: {"bin": bin_name} for sku, bin_name in bins.items()},
        ))

    @staticmethod
    def _bins(**bins):
        return {sku: {"bin": bin_name} for sku, bin_name in bins.items()}

    def test_a_moved_bin_is_applied(self):
        self._stored({"SKU-1": "A1-01"})
        result = local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "B2-07"}))
        self.assertEqual(local_inventory.get(self.db, "SKU-1")["bin"], "B2-07")
        self.assertEqual(result["updated"], 1)

    def test_an_unchanged_bin_is_reported_not_rewritten(self):
        self._stored({"SKU-1": "A1-01"})
        before = local_inventory.get(self.db, "SKU-1")["updated_at"]
        result = local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "A1-01"}))
        self.assertEqual((result["updated"], result["unchanged"]), (0, 1))
        self.assertEqual(local_inventory.get(self.db, "SKU-1")["updated_at"], before)

    def test_a_bin_emptied_in_shipmondo_is_cleared_here(self):
        self._stored({"SKU-1": "A1-01"})
        result = local_inventory.update_bins(self.db, self._bins(**{"SKU-1": ""}))
        self.assertEqual(local_inventory.get(self.db, "SKU-1")["bin"], "")
        self.assertEqual((result["cleared"], result["updated"]), (1, 0))

    def test_a_previously_unbinned_sku_gains_its_bin(self):
        self._stored({"SKU-1": ""})
        local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "A1-01"}))
        self.assertEqual(local_inventory.get(self.db, "SKU-1")["bin"], "A1-01")

    def test_nothing_but_the_bin_is_written(self):
        # The whole point: a bin update in the middle of a count must not move
        # a counted quantity, a cost or any product data.
        self._store(*local_inventory.merge_rows(
            [_variant_row("SKU-1", 9, unit_cost=12.5)], {"SKU-1": {"bin": "A1-01"}}
        ))
        local_inventory.set_on_hand(self.db, "SKU-1", 4)
        before = local_inventory.get(self.db, "SKU-1")

        local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "B2-07"}))
        after = local_inventory.get(self.db, "SKU-1")

        self.assertEqual(after["bin"], "B2-07")
        self.assertEqual(
            {k: v for k, v in after.items() if k not in ("bin", "updated_at")},
            {k: v for k, v in before.items() if k not in ("bin", "updated_at")},
        )
        # Including the count itself and its "changed locally" marker.
        self.assertEqual(after["on_hand"], 4)
        self.assertTrue(after["locally_modified"])
        self.assertEqual(local_inventory.status(self.db)["locally_modified"], 1)

    def test_a_hand_added_product_is_left_alone(self):
        # Shipmondo has no record of it, so the update must not clear its bin.
        local_inventory.add_product(self.db, {
            "vendor": "ACME", "product_title": "P", "variant_title": "V",
            "sku": "LOCAL-1", "on_hand": 3, "unit_cost": 1, "bin": "C3-01",
        })
        local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "A1-01"}))
        row = local_inventory.get(self.db, "LOCAL-1")
        self.assertEqual(row["bin"], "C3-01")
        self.assertEqual(row["source"], local_inventory.SOURCE_LOCAL)

    def test_a_hand_added_product_shipmondo_knows_gets_its_bin(self):
        local_inventory.add_product(self.db, {
            "vendor": "ACME", "product_title": "P", "variant_title": "V",
            "sku": "LOCAL-1", "on_hand": 3, "unit_cost": 1,
        })
        local_inventory.update_bins(self.db, self._bins(**{"LOCAL-1": "C3-02"}))
        self.assertEqual(local_inventory.get(self.db, "LOCAL-1")["bin"], "C3-02")

    def test_a_shipmondo_sku_the_database_lacks_is_reported_not_created(self):
        # A row needs product data and quantities a bin update cannot supply.
        self._stored({"SKU-1": "A1-01"})
        result = local_inventory.update_bins(self.db, self._bins(**{"SKU-NEW": "B2-07"}))
        self.assertEqual(result["missing_locally"], 1)
        self.assertEqual(list(local_inventory.load_all(self.db)), ["SKU-1"])

    def test_rows_shipmondo_says_nothing_about_keep_their_bin(self):
        self._stored({"SKU-1": "A1-01", "SKU-2": "A1-02"})
        local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "B2-07"}))
        self.assertEqual(local_inventory.get(self.db, "SKU-2")["bin"], "A1-02")

    def test_the_update_is_recorded_in_the_status(self):
        self._stored({"SKU-1": "A1-01"})
        result = local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "B2-07"}))
        self.assertEqual(
            local_inventory.status(self.db)["bins_updated"], result["updated_at"]
        )

    def test_a_full_fetch_also_counts_as_a_bin_update(self):
        # It brings bins with it, so the panel must not claim they are older.
        stored = local_inventory.replace_all(self.db, local_inventory.merge_rows(
            [_variant_row("SKU-1")], {"SKU-1": {"bin": "A1-01"}}
        ))
        self.assertEqual(
            local_inventory.status(self.db)["bins_updated"], stored["synced_at"]
        )

    def test_an_updated_bin_reaches_the_next_count_sheet(self):
        self._stored({"SKU-1": "A1-01"})
        local_inventory.update_bins(self.db, self._bins(**{"SKU-1": "B2-07"}))
        inventory = local_inventory.load_all(self.db)
        match = shipmondo.find_items_in_bins(
            counting_sheet.bin_index(inventory), ["B2-*"]
        )
        self.assertEqual([item["sku"] for item in match["items"]], ["SKU-1"])



if __name__ == "__main__":
    unittest.main()
