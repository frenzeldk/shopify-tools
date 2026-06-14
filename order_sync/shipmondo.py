"""Provides function for working with Shipmondo"""
import os
import time
import base64
import requests

API_USER = os.getenv("SHIPMONDO_API_USER")
API_KEY = os.getenv("SHIPMONDO_API_KEY")
AUTH_STRING = base64.b64encode(f'{API_USER}:{API_KEY}'.encode()).decode()

BASE_URL = "https://app.shipmondo.com/api/public/v3/"

# Shipmondo caps sales_orders pagination at 25 entries per page.
_MAX_PER_PAGE = 25

def _get_order_id(sid: str):
    """Fetch orders from Shipmondo API."""
    url = BASE_URL + "sales_orders" + f"?order_id={sid}"
    response = requests.get(url,
                            headers={"Accept": "application/json",
                                     "Authorization": f"Basic {AUTH_STRING}"},
                            timeout=5)
    response.raise_for_status()
    try:
        return response.json()[0].get("id")
    except (IndexError, KeyError):
        return None

def get_sales_order(sid: str):
    """Fetch the full Shipmondo sales order matching the given Shopify order id.

    Args:
        sid: The Shopify order number (without the leading ``#``).

    Returns:
        The first matching sales order dict, or ``None`` when not found.
    """
    url = BASE_URL + "sales_orders" + f"?order_id={sid}"
    response = requests.get(url,
                            headers={"Accept": "application/json",
                                     "Authorization": f"Basic {AUTH_STRING}"},
                            timeout=10)
    response.raise_for_status()
    try:
        return response.json()[0]
    except (IndexError, KeyError):
        return None

def _get_with_retry(url: str, params: dict, retries: int = 5):
    """GET *url* honouring rate limits, retrying on HTTP 429 with backoff."""
    headers = {"Accept": "application/json", "Authorization": f"Basic {AUTH_STRING}"}
    for attempt in range(retries):
        response = requests.get(url, headers=headers, params=params, timeout=20)
        if response.status_code == 429:
            wait = int(response.headers.get("Retry-After", 2 ** attempt))
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response
    response.raise_for_status()
    return response


def get_sales_orders_since(created_at_min: str) -> dict:
    """Fetch all Shipmondo sales orders created since *created_at_min* in one sweep.

    This consolidates what would otherwise be one API request per order into a
    single paginated sweep, keyed by Shopify order id for O(1) lookup.

    Args:
        created_at_min: ISO timestamp lower bound for the order ``created_at``.

    Returns:
        A dict mapping ``order_id`` (the Shopify order number, as a string) to
        the corresponding Shipmondo sales order dict. When several sales orders
        share an order id, the most recently created one wins.
    """
    url = BASE_URL + "sales_orders"
    by_order_id: dict[str, dict] = {}
    page = 1
    while True:
        response = _get_with_retry(
            url, {"per_page": _MAX_PER_PAGE, "page": page, "created_at_min": created_at_min}
        )
        batch = response.json()
        if not batch:
            break
        for sales_order in batch:
            order_id = str(sales_order.get("order_id"))
            by_order_id[order_id] = sales_order
        page += 1
    return by_order_id


def set_order_status(oid: str, status: str):
    """Set the ``order_status`` of a Shipmondo sales order by its internal id.

    Args:
        oid: The Shipmondo internal sales order id.
        status: The target status, e.g. ``"open"`` or ``"on_hold"``.

    Returns:
        The parsed JSON response, or ``None`` when the request failed.
    """
    url = BASE_URL + f"sales_orders/{oid}"
    response = requests.put(url,
                            headers={"Content-Type": "application/json",
                                     "Accept": "application/json",
                                     "Authorization": f"Basic {AUTH_STRING}"},
                            json={"order_status": status},
                            timeout=10)
    if not response.ok:
        print(f"Failed to set order {oid} status to {status}")
        return None
    return response.json()

def pause_order(oid: str):
    """Pause an order in Shipmondo."""
    url = BASE_URL + f"sales_orders/{oid}"
    response = requests.put(url,
                            headers={"Content-Type": "application/json",
                                     "Accept": "application/json",
                                     "Authorization": f"Basic {AUTH_STRING}"},
                            json={"order_status": "on_hold"},
                            timeout=5)
    response.raise_for_status()
    return response.json()

def resume_order(sid: str):
    """Resume an order in Shipmondo."""
    oid = _get_order_id(sid)
    url = BASE_URL + f"sales_orders/{oid}"
    response = requests.put(url,
                            headers={"Content-Type": "application/json",
                                     "Accept": "application/json",
                                     "Authorization": f"Basic {AUTH_STRING}"},
                            json={"order_status": "open"},
                            timeout=5)
    if not response.ok:
        print(f"Failed to resume order: {sid}")
        return None
    return response.json()
