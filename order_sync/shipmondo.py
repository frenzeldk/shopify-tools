"""Provides function for working with Shipmondo"""
import os
import base64
import requests

API_USER = os.getenv("SHIPMONDO_API_USER")
API_KEY = os.getenv("SHIPMONDO_API_KEY")
AUTH_STRING = base64.b64encode(f'{API_USER}:{API_KEY}'.encode()).decode()

BASE_URL = "https://app.shipmondo.com/api/public/v3/"

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
    response.raise_for_status()
    return response.json()
