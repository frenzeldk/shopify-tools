"""Functions for interacting with Shipmondo API."""

import base64
import fnmatch
import os
import re
import requests
from typing import Dict, List, Tuple


def get_shipmondo_headers():
    """Return authorization headers for Shipmondo API."""
    api_user = os.getenv("SHIPMONDO_API_USER")
    api_key = os.getenv("SHIPMONDO_API_KEY")
    auth_string = base64.b64encode(f'{api_user}:{api_key}'.encode()).decode()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_string}"
    }


def fetch_all_shipmondo_items() -> Dict[str, dict]:
    """
    Fetch all items from Shipmondo using pagination.
    Returns a dict mapping SKU to item data.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    url = "https://app.shipmondo.com/api/public/v3/items"
    headers = get_shipmondo_headers()
    all_items = {}
    page = 1
    
    logger.info(f"Starting Shipmondo API fetch from {url}")
    
    while True:
        try:
            logger.debug(f"Fetching page {page}...")
            response = requests.get(
                url,
                headers=headers,
                params={"per_page": 50, "page": page},
                timeout=10
            )
            response.raise_for_status()
            items = response.json()
            
            logger.debug(f"Page {page}: Received {len(items) if items else 0} items")
            
            # If no items returned, we've reached the end
            if not items or len(items) == 0:
                logger.info(f"Pagination complete. Total pages fetched: {page - 1}")
                break
            
            # Store all items
            for item in items:
                if not item.get("sku"):
                    continue
                sku = item.get("sku", "").strip()
                if sku:
                    all_items[sku] = {
                        "id": item.get("id"),
                        "bin": item.get("bin", ""),
                        "name": item.get("name", ""),
                        "sku": sku,
                        "barcode": item.get("barcode", "")
                    }
            
            # Move to next page
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Shipmondo items (page {page}): {e}", exc_info=True)
            # If we got an error on the first page, re-raise it so we know something is wrong
            if page == 1:
                raise
            # Otherwise, return what we've collected so far
            break
    
    logger.info(f"Fetched total of {len(all_items)} items from Shipmondo")
    return all_items


def clear_bin_location(item_id: int, sku: str) -> Tuple[bool, str]:
    """
    Clear the bin location for a Shipmondo item.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    url = f"https://app.shipmondo.com/api/public/v3/items/{item_id}"
    headers = get_shipmondo_headers()
    
    try:
        response = requests.put(
            url,
            headers=headers,
            json={"bin": ""},
            timeout=10
        )
        response.raise_for_status()
        return True, f"Cleared bin location for SKU {sku}"
    except requests.exceptions.RequestException as e:
        return False, f"Error clearing bin for SKU {sku}: {str(e)}"


def update_bin_location(item_id: int, sku: str, new_bin: str) -> Tuple[bool, str]:
    """
    Update the bin location for a Shipmondo item.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    url = f"https://app.shipmondo.com/api/public/v3/items/{item_id}"
    headers = get_shipmondo_headers()
    
    try:
        response = requests.put(
            url,
            headers=headers,
            json={"bin": new_bin},
            timeout=10
        )
        response.raise_for_status()
        return True, f"Updated bin location for SKU {sku} to '{new_bin}'"
    except requests.exceptions.RequestException as e:
        return False, f"Error updating bin for SKU {sku}: {str(e)}"


def update_barcode(item_id: int, sku: str, new_barcode: str) -> Tuple[bool, str]:
    """
    Update the barcode for a Shipmondo item.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    url = f"https://app.shipmondo.com/api/public/v3/items/{item_id}"
    headers = get_shipmondo_headers()
    
    try:
        response = requests.put(
            url,
            headers=headers,
            json={"barcode": new_barcode},
            timeout=10
        )
        response.raise_for_status()
        return True, f"Updated barcode for SKU {sku} to '{new_barcode}'"
    except requests.exceptions.RequestException as e:
        return False, f"Error updating barcode for SKU {sku}: {str(e)}"


def batch_update_bins_with_regex(shipmondo_items: Dict[str, dict], 
                                  regex_pattern: str, 
                                  replacement: str) -> Dict[str, any]:
    """
    Batch update bin locations using regex pattern matching.
    
    Args:
        shipmondo_items: Dict of all Shipmondo items
        regex_pattern: Regex pattern to match bin locations
        replacement: Replacement string (can use \\1, \\2 for capture groups)
    
    Returns:
        Dict with results including matched items, success count, and errors
    """
    try:
        compiled_regex = re.compile(regex_pattern)
    except re.error as e:
        return {"error": f"Invalid regex pattern: {str(e)}"}
    
    # Find matching items (only those with bins)
    matching_items = []
    for sku, item_data in shipmondo_items.items():
        current_bin = item_data.get("bin", "")
        if current_bin and compiled_regex.search(current_bin):
            new_bin = compiled_regex.sub(replacement, current_bin)
            matching_items.append({
                "sku": sku,
                "item_id": item_data.get("id"),
                "current_bin": current_bin,
                "new_bin": new_bin,
                "name": item_data.get("name", "")
            })
    
    return {
        "matching_items": matching_items,
        "count": len(matching_items)
    }


def apply_batch_update(matching_items: List[dict]) -> Dict[str, any]:
    """
    Apply the batch update to Shipmondo.
    
    Args:
        matching_items: List of items to update (from batch_update_bins_with_regex)
    
    Returns:
        Dict with success count and any errors
    """
    success_count = 0
    errors = []
    
    for item in matching_items:
        success, message = update_bin_location(
            item["item_id"], 
            item["sku"], 
            item["new_bin"]
        )
        if success:
            success_count += 1
        else:
            errors.append(message)
    
    return {
        "success_count": success_count,
        "total_count": len(matching_items),
        "errors": errors
    }


# ── Bin patterns ──────────────────────────────────────────────────────────────
#
# The counting sheet asks for bins by pattern rather than one at a time, because
# a physical count is always a whole aisle or shelf.  Tokens are separated by
# newlines, commas, semicolons or spaces, and each token may contain:
#
#   *           any run of characters                A1-*
#   ?           exactly one character                A1-0?
#   [1-12]      an inclusive numeric range           A1-[1-12]  -> A1-1 … A1-12
#   [01-12]     ditto, zero-padded when an endpoint  A1-[01-12] -> A1-01 … A1-12
#               is written padded
#   [A-D]       an inclusive single-letter range     [A-D]-1    -> A-1 … D-1
#
# Ranges are expanded here; the surviving `*`/`?` are matched against the bins
# actually present in Shipmondo, so a pattern never invents a bin that does not
# exist.

#: Tokens accepted in one request, before range expansion.
MAX_BIN_PATTERN_TOKENS = 100
#: Patterns a single request may expand to, so `[1-99999]` cannot blow up memory.
MAX_EXPANDED_BIN_PATTERNS = 2000
#: Widest numeric range one bracket may cover.
MAX_RANGE_SPAN = 500

_TOKEN_SEPARATORS = re.compile(r"[\s,;]+")
_GLOB_CHARS = re.compile(r"[*?\[\]]")
_NUMERIC_RANGE = re.compile(r"\[(\d+)-(\d+)\]")
_LETTER_RANGE = re.compile(r"\[([A-Za-z])-([A-Za-z])\]")


def _expand_ranges(token: str) -> Tuple[List[str], str]:
    """Expand every ``[a-b]`` range in ``token``.

    Returns ``(patterns, error)``; ``patterns`` is empty when ``error`` is set.
    """
    pending = [token]
    expanded: List[str] = []

    while pending:
        current = pending.pop()

        match = _NUMERIC_RANGE.search(current)
        if match:
            start_text, end_text = match.group(1), match.group(2)
            start, end = int(start_text), int(end_text)
            if start > end:
                return [], f"'{match.group(0)}' counts backwards in '{token}'."
            if end - start + 1 > MAX_RANGE_SPAN:
                return [], (
                    f"'{match.group(0)}' in '{token}' covers more than "
                    f"{MAX_RANGE_SPAN} bins."
                )
            # Keep the caller's zero padding: A1-[01-12] means A1-01, not A1-1.
            width = max(len(start_text), len(end_text)) if (
                start_text.startswith("0") or end_text.startswith("0")
            ) else 0
            for value in range(start, end + 1):
                pending.append(
                    current[:match.start()]
                    + str(value).rjust(width, "0")
                    + current[match.end():]
                )
            continue

        match = _LETTER_RANGE.search(current)
        if match:
            start, end = ord(match.group(1).upper()), ord(match.group(2).upper())
            if start > end:
                return [], f"'{match.group(0)}' counts backwards in '{token}'."
            for code in range(start, end + 1):
                pending.append(
                    current[:match.start()] + chr(code) + current[match.end():]
                )
            continue

        expanded.append(current)

        if len(expanded) + len(pending) > MAX_EXPANDED_BIN_PATTERNS:
            return [], (
                f"'{token}' expands to more than {MAX_EXPANDED_BIN_PATTERNS} bins."
            )

    return sorted(expanded), ""


def expand_bin_patterns(raw: str) -> Tuple[List[str], List[str]]:
    """Turn the free-text bin field into a list of match patterns.

    Returns ``(patterns, errors)``.  Patterns are upper-cased (Shipmondo bins
    are matched case-insensitively) and de-duplicated in input order.
    """
    tokens = [t for t in _TOKEN_SEPARATORS.split(raw.strip()) if t]
    if len(tokens) > MAX_BIN_PATTERN_TOKENS:
        return [], [
            f"At most {MAX_BIN_PATTERN_TOKENS} bin patterns may be listed at once "
            f"({len(tokens)} given)."
        ]

    patterns: List[str] = []
    seen = set()
    errors: List[str] = []

    for token in tokens:
        candidates, error = _expand_ranges(token.upper())
        if error:
            errors.append(error)
            continue
        for candidate in candidates:
            if candidate not in seen:
                seen.add(candidate)
                patterns.append(candidate)

    if len(patterns) > MAX_EXPANDED_BIN_PATTERNS:
        errors.append(
            f"The patterns expand to more than {MAX_EXPANDED_BIN_PATTERNS} bins; "
            "narrow the selection."
        )
        return [], errors

    return patterns, errors


def bin_sort_key(bin_name: str) -> List:
    """Sort bins the way a picker walks them: A2-9 before A2-10, not after."""
    parts = re.split(r"(\d+)", bin_name.upper())
    return [(1, int(p), "") if p.isdigit() else (0, 0, p) for p in parts if p != ""]


def find_items_in_bins(
    shipmondo_items: Dict[str, dict],
    patterns: List[str],
) -> Dict[str, any]:
    """Group the Shipmondo items whose bin matches any of ``patterns``.

    Returns ``{"items": [...], "bins": [...], "unmatched_patterns": [...]}`` where
    each item carries its ``sku``, ``bin``, ``name`` and ``barcode``.
    """
    # An expanded range is a plain list of bin names, so the common case is a set
    # lookup; only the patterns still holding a wildcard are matched one by one.
    literals = {p for p in patterns if not _GLOB_CHARS.search(p)}
    globs = [
        (p, re.compile(fnmatch.translate(p))) for p in patterns if p not in literals
    ]

    matched: List[dict] = []
    bins = set()
    patterns_hit = set()

    for sku, item_data in shipmondo_items.items():
        bin_name = (item_data.get("bin") or "").strip()
        if not bin_name:
            continue
        upper_bin = bin_name.upper()
        hits = [p for p, matcher in globs if matcher.match(upper_bin)]
        if upper_bin in literals:
            hits.append(upper_bin)
        if not hits:
            continue
        patterns_hit.update(hits)
        bins.add(bin_name)
        matched.append({
            "sku": sku,
            "bin": bin_name,
            "name": item_data.get("name", ""),
            "barcode": item_data.get("barcode", ""),
        })

    matched.sort(key=lambda item: (bin_sort_key(item["bin"]), item["sku"]))

    return {
        "items": matched,
        "bins": sorted(bins, key=bin_sort_key),
        # Patterns that matched nothing: usually a typo, and worth showing rather
        # than silently handing over a short count sheet.
        "unmatched_patterns": [p for p in patterns if p not in patterns_hit],
    }
