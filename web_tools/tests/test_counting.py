"""Unit tests for the Counting page's two moving parts.

The route itself needs the whole Flask/OIDC/Shopify stack, so what is covered
here is the logic it delegates to: turning a free-text bin field into bins, and
turning bins into on-hand quantities.

    python -m unittest discover -s web_tools/tests
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
            "on_hand": 7,
            "available": 5,
            "committed": 2,
            "locations": [{"name": "Warehouse", "on_hand": 7}],
        })


if __name__ == "__main__":
    unittest.main()
