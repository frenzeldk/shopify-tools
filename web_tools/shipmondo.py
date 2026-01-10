"""Functions for interacting with Shipmondo API."""

import os
import re
import base64
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
                        "sku": sku
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
