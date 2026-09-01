#!/opt/shopify-python/bin/python3
"""
Web tools Flask application for managing purchase orders.
Does not push POs to Shopify, as this is not supported by the Shopify API.
"""

from __future__ import annotations

import asyncio
import atexit
import json
import signal
import sqlite3
import os
import sys
import logging
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta, timezone
from waitress import serve
from flask import Flask, Response, current_app, g, jsonify, render_template, request, redirect, url_for, session
from flask_oidc import OpenIDConnect
from flask_session import Session
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from shopify import (
    init_session as init_gql_session,
    shutdown_session as shutdown_gql_session,
    fetch_missing_inventory as fetch_purchase_order_data,
    calculate_brand_inventory_value,
    update_variant_barcode,
    fetch_order_customer,
    fetch_shopify_products_by_vendors,
    compare_vendor_products,
    add_variants_to_shopify_product,
    fetch_color_field_options,
    check_existing_color_metaobjects,
    create_color_metaobject,
    upload_file_to_shopify,
    generate_diagonal_swatch,
    upload_swatch_bytes_to_shopify,
    check_linked_option_values,
    create_option_value_metaobject,
    fetch_shopify_taxonomy,
    fetch_all_product_tags,
    fetch_all_products_lightweight,
    update_product,
    fetch_category_metafields,
    create_shopify_product,
    set_product_category_metafields,
    detect_product_options,
    create_product_options,
    create_staged_uploads,
    staged_upload_with_fallback,
    fetch_metaobject_type_details,
    fetch_metaobjects_for_definition,
    fetch_product_images,
    add_product_images,
    reorder_product_images,
    delete_product_image,
    fetch_locations,
    fetch_on_hand_by_skus,
    create_inventory_transfer,
    set_transfer_items,
    delete_inventory_transfer,
    TransferError,
)
from chatgpt import fetch_and_translate_vendor_page, translate_product_data, translate_plain_text
from deerhunter import dh_fetch_all_products, dh_products_to_vendor_format
from pentagon import pt_fetch_all_products, pt_products_to_vendor_format
import entire_m
from shipmondo import (
    fetch_all_shipmondo_items,
    clear_bin_location,
    batch_update_bins_with_regex,
    apply_batch_update,
    expand_bin_patterns,
    find_items_in_bins,
    update_barcode
)
from microsoft365 import (
    TEMPLATE_VARIABLES,
    render_email_template,
    send_missed_pickup_email,
    send_plaintext_email,
    send_template_email,
)
import netguard
import purchase_order
import security
import shopify as shopify_module
import threading
import yaml
from security import (
    LIMIT_EXPENSIVE,
    LIMIT_MAIL,
    LIMIT_READ,
    LIMIT_WRITE,
    ROLE_CATALOG_WRITE,
    ROLE_CONFIG_ADMIN,
    ROLE_INVENTORY_WRITE,
    ROLE_MAIL_SEND,
    ROLE_PLACE_ORDER,
    ROLE_READ,
    RoutePolicy,
)

# Configure logging to stdout for systemd/journalctl
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "purchase_orders.db"
CACHE_DURATION_MINUTES = 30

# Largest uploaded CSV product_tools_compare will parse.
MAX_CSV_UPLOAD_BYTES = int(os.environ.get("MAX_CSV_UPLOAD_BYTES", 16 * 1024 * 1024))
# Largest base64 swatch payload accepted from the browser.
MAX_INLINE_IMAGE_BYTES = int(os.environ.get("MAX_INLINE_IMAGE_BYTES", 8 * 1024 * 1024))
# Ceilings on caller-supplied list arguments, so one request cannot fan out into
# thousands of Shopify calls.
MAX_BATCH_ITEMS = int(os.environ.get("MAX_BATCH_ITEMS", 500))
# Longest regex a batch bin update may use.
MAX_REGEX_LENGTH = 200
# Longest bin-pattern list the counting sheet accepts, and the most SKUs one
# count sheet may cover — a count sheet longer than this is unusable on paper
# and turns into hundreds of Shopify lookups.
MAX_BIN_INPUT_CHARS = 4000
MAX_COUNT_SHEET_SKUS = 1000

# ── Access policy ─────────────────────────────────────────────────────────────
#
# Every route needs an entry here: create_app() refuses to start when one is
# missing (see security.audit_routes), which is what keeps a newly added
# endpoint from being published anonymously.  `role` is enforced once
# OIDC_REQUIRE_ROLES=1; the authentication gate, CSRF check and rate limits
# always apply.

ROUTE_POLICIES: dict[str, RoutePolicy] = {
    # Pages
    "index": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "purchase_orders": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "inventory_tools": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "barcode_scanner": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "product_tools": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "mail_tools": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    "counting": RoutePolicy(ROLE_READ, LIMIT_READ, html=True),
    # Purchase orders
    "purchase_order_data": RoutePolicy(ROLE_READ, LIMIT_EXPENSIVE),
    "list_configurations": RoutePolicy(ROLE_READ, LIMIT_READ),
    "upsert_configuration": RoutePolicy(ROLE_CONFIG_ADMIN, LIMIT_WRITE),
    "delete_configuration": RoutePolicy(ROLE_CONFIG_ADMIN, LIMIT_WRITE),
    "purchase_orders_order_methods": RoutePolicy(ROLE_READ, LIMIT_READ),
    "purchase_orders_locations": RoutePolicy(ROLE_READ, LIMIT_READ),
    "purchase_orders_place_order": RoutePolicy(ROLE_PLACE_ORDER, LIMIT_EXPENSIVE),
    "get_ordering_template_route": RoutePolicy(ROLE_CONFIG_ADMIN, LIMIT_READ),
    "save_ordering_template_route": RoutePolicy(ROLE_CONFIG_ADMIN, LIMIT_WRITE),
    "delete_ordering_template_route": RoutePolicy(ROLE_CONFIG_ADMIN, LIMIT_WRITE),
    # Inventory tools
    "calculate_brand_value": RoutePolicy(ROLE_READ, LIMIT_EXPENSIVE),
    "shipmondo_cache_status": RoutePolicy(ROLE_READ, LIMIT_READ),
    "refresh_shipmondo_cache": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_EXPENSIVE),
    "cleanup_sold_out_bins": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_EXPENSIVE),
    "preview_batch_update": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_WRITE),
    "apply_batch_update_route": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_EXPENSIVE),
    # Counting
    "counting_count_sheet": RoutePolicy(ROLE_READ, LIMIT_EXPENSIVE),
    # Barcode scanner
    "lookup_barcode": RoutePolicy(ROLE_READ, LIMIT_READ),
    "search_items": RoutePolicy(ROLE_READ, LIMIT_READ),
    "assign_bin": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_WRITE),
    "assign_barcode_to_sku": RoutePolicy(ROLE_INVENTORY_WRITE, LIMIT_WRITE),
    # Product tools
    "product_tools_compare": RoutePolicy(ROLE_READ, LIMIT_EXPENSIVE),
    "product_tools_add_variants": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_color_options": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_check_colors": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_generate_swatch": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    "product_tools_create_color": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_check_linked_options": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_create_option_value": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_taxonomy": RoutePolicy(ROLE_READ, LIMIT_READ),
    "product_tools_tags": RoutePolicy(ROLE_READ, LIMIT_READ),
    "product_tools_category_metafields": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_save_category_metafields": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_translate_description": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    "product_tools_translate_product_data": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    "product_tools_translate_plain_text": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    "product_tools_create_product": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_detect_product_options": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_create_product_options": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_definition_metaobjects": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_metaobject_type_fields": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_get_images": RoutePolicy(ROLE_READ, LIMIT_WRITE),
    "product_tools_add_images": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_reorder_images": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_delete_image": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_stage_uploads": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_WRITE),
    "product_tools_helikon_images": RoutePolicy(ROLE_READ, LIMIT_EXPENSIVE),
    "product_tools_helikon_image_proxy": RoutePolicy(ROLE_READ, LIMIT_READ),
    "product_tools_helikon_stage_images": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    "product_tools_all_products": RoutePolicy(ROLE_READ, LIMIT_READ),
    "product_tools_remap_apply": RoutePolicy(ROLE_CATALOG_WRITE, LIMIT_EXPENSIVE),
    # Mail tools — customer PII and outbound mail are gated behind mail-send.
    "lookup_order": RoutePolicy(ROLE_MAIL_SEND, LIMIT_WRITE),
    "send_missed_pickup": RoutePolicy(ROLE_MAIL_SEND, LIMIT_MAIL),
    "list_email_templates": RoutePolicy(ROLE_MAIL_SEND, LIMIT_READ),
    "save_email_template_route": RoutePolicy(ROLE_MAIL_SEND, LIMIT_WRITE),
    "delete_email_template_route": RoutePolicy(ROLE_MAIL_SEND, LIMIT_WRITE),
    "preview_template": RoutePolicy(ROLE_MAIL_SEND, LIMIT_WRITE),
    "send_template": RoutePolicy(ROLE_MAIL_SEND, LIMIT_MAIL),
}

# Global Shipmondo cache with thread lock
shipmondo_cache = {
    "items": {},
    "last_updated": None,
    "is_refreshing": False
}
shipmondo_lock = threading.Lock()

# Global Shopify taxonomy cache with thread lock
taxonomy_cache = {
    "categories": [],
    "last_updated": None,
    "is_refreshing": False
}
taxonomy_lock = threading.Lock()

# Global product tags cache
tags_cache = {
    "tags": [],
    "last_updated": None,
    "is_refreshing": False
}
tags_lock = threading.Lock()

# Global lightweight products cache (id, title, vendor, tags, category_id)
products_cache = {
    "products": [],
    "last_updated": None,
    "is_refreshing": False
}
products_lock = threading.Lock()

# Global Shopify locations cache (id, name) — used as transfer destinations
locations_cache = {
    "locations": [],
    "last_updated": None,
    "is_refreshing": False
}
locations_lock = threading.Lock()

def fetch_and_cache_shipmondo_items():
    """Fetch all Shipmondo items and update the global cache."""
    
    # Check if already refreshing
    if shipmondo_cache["is_refreshing"]:
        logger.info("Shipmondo cache refresh already in progress, skipping")
        return
    
    try:
        # Set refreshing flag
        shipmondo_cache["is_refreshing"] = True
        
        logger.info(f"Starting Shipmondo items fetch at {datetime.now()}")
        items = fetch_all_shipmondo_items()
        logger.info(f"Fetched {len(items)} Shipmondo items")
        
        if len(items) == 0:
            logger.warning("No items fetched from Shipmondo - this may indicate an API issue")
        
        with shipmondo_lock:
            shipmondo_cache["items"] = items
            shipmondo_cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Successfully cached {len(items)} Shipmondo items")
    except Exception as e:
        logger.error(f"Error fetching Shipmondo items: {e}", exc_info=True)
    finally:
        shipmondo_cache["is_refreshing"] = False


def fetch_and_cache_taxonomy():
    """Fetch the Shopify product taxonomy and update the global cache."""
    if taxonomy_cache["is_refreshing"]:
        logger.info("Taxonomy cache refresh already in progress, skipping")
        return

    try:
        taxonomy_cache["is_refreshing"] = True
        logger.info(f"Starting taxonomy fetch at {datetime.now()}")
        categories = fetch_shopify_taxonomy()
        logger.info(f"Fetched {len(categories)} taxonomy categories")

        with taxonomy_lock:
            taxonomy_cache["categories"] = categories
            taxonomy_cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Successfully cached {len(categories)} taxonomy categories")
    except Exception as e:
        logger.error(f"Error fetching taxonomy: {e}", exc_info=True)
    finally:
        taxonomy_cache["is_refreshing"] = False


def fetch_and_cache_product_tags():
    """Fetch all product tags from Shopify and update the global cache."""
    if tags_cache["is_refreshing"]:
        logger.info("Product tags cache refresh already in progress, skipping")
        return

    try:
        tags_cache["is_refreshing"] = True
        logger.info(f"Starting product tags fetch at {datetime.now()}")
        tags = fetch_all_product_tags()
        logger.info(f"Fetched {len(tags)} unique product tags")

        with tags_lock:
            tags_cache["tags"] = tags
            tags_cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Successfully cached {len(tags)} product tags")
    except Exception as e:
        logger.error(f"Error fetching product tags: {e}", exc_info=True)
    finally:
        tags_cache["is_refreshing"] = False


def fetch_and_cache_all_products():
    """Fetch all products (lightweight) and update the global products cache."""
    if products_cache["is_refreshing"]:
        logger.info("Products cache refresh already in progress, skipping")
        return

    try:
        products_cache["is_refreshing"] = True
        logger.info(f"Starting products fetch at {datetime.now()}")
        products = fetch_all_products_lightweight()
        logger.info(f"Fetched {len(products)} products")

        with products_lock:
            products_cache["products"] = products
            products_cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Successfully cached {len(products)} products")
    except Exception as e:
        logger.error(f"Error fetching products: {e}", exc_info=True)
    finally:
        products_cache["is_refreshing"] = False


def fetch_and_cache_locations():
    """Fetch Shopify locations and update the global cache."""
    if locations_cache["is_refreshing"]:
        logger.info("Locations cache refresh already in progress, skipping")
        return

    try:
        locations_cache["is_refreshing"] = True
        logger.info(f"Starting locations fetch at {datetime.now()}")
        locations = fetch_locations()
        logger.info(f"Fetched {len(locations)} locations")

        with locations_lock:
            locations_cache["locations"] = locations
            locations_cache["last_updated"] = datetime.now(timezone.utc).isoformat()

        logger.info(f"Successfully cached {len(locations)} locations")
    except Exception as e:
        logger.error(f"Error fetching locations: {e}", exc_info=True)
    finally:
        locations_cache["is_refreshing"] = False


def refresh_all_shopify_caches():
    """Run all Shopify-dependent cache refreshes sequentially.

    Shopify's API rate-limits concurrent requests, so we must avoid
    firing multiple heavy fetches in parallel.  This wrapper is used
    both at startup and for the daily scheduled refresh.
    """
    logger.info("refresh_all_shopify_caches: starting sequential refresh")
    fetch_and_cache_taxonomy()
    fetch_and_cache_product_tags()
    fetch_and_cache_all_products()
    fetch_and_cache_locations()
    logger.info("refresh_all_shopify_caches: all Shopify caches refreshed")


def get_db() -> sqlite3.Connection:
    """Return a per-request SQLite connection."""
    if "db" not in g:
        database_path = current_app.config.get("DATABASE", str(DATABASE_PATH))
        g.db = sqlite3.connect(database_path)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(_exception: BaseException | None = None) -> None:
    """Close the database connection at request teardown."""
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    """Ensure the tables required for configuration storage exist."""
    database_path = Path(current_app.config.get("DATABASE", str(DATABASE_PATH)))
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS purchase_order_configurations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                columns TEXT NOT NULL,
                filters TEXT NOT NULL,
                column_labels TEXT NOT NULL DEFAULT '{}',
                sort_model TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        existing_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(purchase_order_configurations)"
            ).fetchall()
        }
        if "column_labels" not in existing_columns:
            conn.execute(
                "ALTER TABLE purchase_order_configurations ADD COLUMN column_labels TEXT NOT NULL DEFAULT '{}'"
            )
        if "sort_model" not in existing_columns:
            conn.execute(
                "ALTER TABLE purchase_order_configurations ADD COLUMN sort_model TEXT NOT NULL DEFAULT '[]'"
            )
        if "custom_columns" not in existing_columns:
            conn.execute(
                "ALTER TABLE purchase_order_configurations ADD COLUMN custom_columns TEXT NOT NULL DEFAULT '[]'"
            )
        if "column_widths" not in existing_columns:
            conn.execute(
                "ALTER TABLE purchase_order_configurations ADD COLUMN column_widths TEXT NOT NULL DEFAULT '{}'"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ordering_templates (
                view_name TEXT PRIMARY KEY,
                template_json TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ordering_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_templates (
                name TEXT PRIMARY KEY,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def load_ordering_templates(db_path: str) -> dict[str, dict]:
    """Return all stored ordering templates as {view_name: template dict}.

    Uses its own short-lived connection so it is safe to call from the
    purchase_order vendors-provider regardless of request context.
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT view_name, template_json FROM ordering_templates"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    finally:
        conn.close()
    out: dict[str, dict] = {}
    for view_name, template_json in rows:
        try:
            out[view_name] = json.loads(template_json)
        except (TypeError, ValueError):
            continue
    return out


def get_ordering_template(db_path: str, name: str) -> dict | None:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT template_json FROM ordering_templates WHERE view_name = ?", (name,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except (TypeError, ValueError):
        return None


def save_ordering_template(db_path: str, name: str, template: dict) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO ordering_templates (view_name, template_json, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(view_name) DO UPDATE SET
                template_json = excluded.template_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (name, json.dumps(template)),
        )
        conn.commit()
    finally:
        conn.close()


def delete_ordering_template(db_path: str, name: str) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("DELETE FROM ordering_templates WHERE view_name = ?", (name,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def load_ordering_defaults(db_path: str) -> dict:
    """Return the global ordering defaults from the DB.

    Falls back to the YAML file's ``defaults`` while a migration has not yet run
    (so the app keeps working before/without migration); the backends apply
    their own sane fallbacks if neither is set.
    """
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM ordering_settings WHERE key = 'defaults'"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if row:
        try:
            data = json.loads(row[0])
            if data:
                return data
        except (TypeError, ValueError):
            pass
    return purchase_order.file_defaults()


def save_ordering_defaults(db_path: str, defaults: dict) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ordering_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.execute(
            """
            INSERT INTO ordering_settings (key, value) VALUES ('defaults', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (json.dumps(defaults),),
        )
        conn.commit()
    finally:
        conn.close()


def load_email_templates(db_path: str) -> list[dict]:
    """Return every saved mail-tools email template, ordered by name."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name, subject, body FROM email_templates ORDER BY name COLLATE NOCASE"
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    return [{"name": name, "subject": subject, "body": body} for name, subject, body in rows]


def get_email_template(db_path: str, name: str) -> dict | None:
    """Return a single saved email template, or ``None`` when it is unknown."""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name, subject, body FROM email_templates WHERE name = ?", (name,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()
    if not row:
        return None
    return {"name": row[0], "subject": row[1], "body": row[2]}


def save_email_template(db_path: str, name: str, subject: str, body: str) -> None:
    """Insert or replace the email template stored under ``name``."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO email_templates (name, subject, body, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(name) DO UPDATE SET
                subject = excluded.subject,
                body = excluded.body,
                updated_at = CURRENT_TIMESTAMP
            """,
            (name, subject, body),
        )
        conn.commit()
    finally:
        conn.close()


def delete_email_template(db_path: str, name: str) -> bool:
    """Delete the named email template; return whether a row was removed."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("DELETE FROM email_templates WHERE name = ?", (name,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


async def _resolve_order_customer(order_number: str) -> tuple[dict | None, Any]:
    """
    Resolve an order number to its customer for the mail tools.

    Returns:
        Tuple of (customer, error_response).  Exactly one is set: on success the
        customer dict, otherwise a ready-to-return (payload, status) tuple.
    """
    if not order_number:
        return None, (jsonify({"error": "Order number is required."}), 400)

    if not order_number.startswith("#"):
        order_number = f"#{order_number}"

    customer = await asyncio.to_thread(fetch_order_customer, order_number)

    if customer is None:
        return None, (
            jsonify({"error": f"Order {order_number} not found or has no customer."}),
            404,
        )

    return customer, None


def _too_many(name: str, value: Any, limit: int = MAX_BATCH_ITEMS) -> Any:
    """Return a 413 response when a caller-supplied list exceeds ``limit``.

    Each entry in these lists costs at least one Shopify API call, so an
    unbounded list is a cheap way to tie up the process and burn the shop's
    rate-limit budget.
    """
    if isinstance(value, (list, tuple)) and len(value) > limit:
        return jsonify({"error": f"'{name}' may contain at most {limit} entries."}), 413
    return None


def _template_variables(customer: dict) -> dict[str, str]:
    """Build the {{ variable }} values a saved email template can reference."""
    return {name: customer.get(name, "") or "" for name in TEMPLATE_VARIABLES}


def _yaml_vendor_to_template(name: str, vcfg: dict, yaml_dir: Path) -> dict:
    """Convert a YAML vendor entry into the storable template shape.

    Email bodies may be inline (``body``) or, for legacy files, read from
    ``body_template_file`` resolved relative to the YAML file or its parent.
    """
    method = (vcfg.get("method") or "").lower()
    out: dict = {"method": method, "label": vcfg.get("label", name)}
    if method == "email":
        email = dict(vcfg.get("email") or {})
        body = email.get("body")
        if not body and email.get("body_template_file"):
            rel = Path(email["body_template_file"])
            candidates = [rel] if rel.is_absolute() else [yaml_dir / rel, yaml_dir.parent / rel]
            for candidate in candidates:
                try:
                    body = candidate.read_text(encoding="utf-8")
                    break
                except OSError:
                    continue
        attachment = email.get("attachment") or {}
        out["email"] = {
            "to": email.get("to", ""),
            "to_env": email.get("to_env", ""),
            "subject": email.get("subject", "Purchase Order {order_number} - {company_name}"),
            "body": body or "",
            "attachment": {
                "enabled": bool(attachment.get("enabled", True)),
                "filename": attachment.get("filename", "PO_{order_number}.xlsx"),
            },
        }
    elif method == "api":
        out["api"] = vcfg.get("api") or {}
    return out


def migrate_yaml_to_db(db_path: str | None = None, yaml_path: str | None = None) -> dict:
    """Migrate ordering config from a YAML file into the database.

    Moves the ``defaults`` block and every ``vendors`` entry (email and API
    alike) into ``ordering_settings`` / ``ordering_templates``. Safe to re-run
    (upserts). Returns a summary; if no YAML file is present nothing is changed.
    """
    db_path = db_path or str(DATABASE_PATH)
    ypath = Path(yaml_path) if yaml_path else Path(purchase_order.config_path())
    summary: dict = {
        "yaml_path": str(ypath),
        "migrated": False,
        "defaults": False,
        "templates": [],
        "skipped": [],
    }
    if not ypath.exists():
        summary["reason"] = "no YAML file present"
        return summary

    with open(ypath, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ordering_templates "
            "(view_name TEXT PRIMARY KEY, template_json TEXT NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ordering_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        conn.commit()
    finally:
        conn.close()

    defaults = data.get("defaults") or {}
    if defaults:
        save_ordering_defaults(db_path, defaults)
        summary["defaults"] = True

    for name, vcfg in (data.get("vendors") or {}).items():
        try:
            template = _yaml_vendor_to_template(name, vcfg or {}, ypath.parent)
            template = purchase_order.validate_template(name, template)
        except Exception as exc:  # noqa: BLE001 — collect, don't abort the batch
            summary["skipped"].append({"name": name, "reason": str(exc)})
            continue
        save_ordering_template(db_path, name, template)
        summary["templates"].append(name)

    summary["migrated"] = True
    return summary


def _safe_delete_transfer(transfer_id: str) -> None:
    """Best-effort delete of an orphaned (empty) transfer after an order failure."""
    try:
        delete_inventory_transfer(transfer_id)
    except Exception:
        logger.warning("Could not delete orphaned transfer %s", transfer_id, exc_info=True)


def create_app() -> Flask:
    """Application factory for the web tools service."""
    application = Flask(__name__, template_folder="templates", static_folder="static")
    application.config.setdefault("DATABASE", str(DATABASE_PATH))
    application.config.setdefault("OIDC_CLIENT_SECRETS", str(BASE_DIR / "client_secrets.json"))
    application.config["SESSION_TYPE"] = "filesystem"
    application.config['SESSION_PERMANENT'] = True
    # Secret-key strength, cookie flags, session lifetime and request-size caps.
    security.configure(application)

    # Configure Flask's logger to use stdout
    if not application.debug:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        application.logger.addHandler(stream_handler)
        application.logger.setLevel(logging.INFO)
    
    Session(application)
    oidc = OpenIDConnect(application)

    # Fail-closed access control for *every* route.  Registered here, right
    # after the OIDC extension, so the gate runs after flask-oidc has refreshed
    # the session token but before any view function.
    security.install(
        application,
        policies=ROUTE_POLICIES,
        is_authenticated=lambda: oidc.user_loggedin,
    )

    with application.app_context():
        init_db()

    # All ordering config (templates + global defaults) lives in the DB and is
    # edited through the UI. Point the purchase_order package at the DB so edits
    # take effect without restarts.
    _ordering_db_path = application.config.get("DATABASE", str(DATABASE_PATH))
    purchase_order.register_vendors_provider(
        lambda: load_ordering_templates(_ordering_db_path)
    )
    purchase_order.register_defaults_provider(
        lambda: load_ordering_defaults(_ordering_db_path)
    )

    # ── GQL async permanent session ───────────────────────────────
    # Open a single persistent GraphQL connection with automatic
    # reconnection.  Must happen before the scheduler fires any
    # Shopify-dependent jobs.
    init_gql_session()

    # Initialize background scheduler for cache updates.
    # IMPORTANT: Shopify rate-limits concurrent API requests, so all
    # Shopify-dependent refreshes are funnelled through a single
    # sequential wrapper (refresh_all_shopify_caches).  Shipmondo is
    # a separate API and can run independently.
    scheduler = BackgroundScheduler()

    # Shipmondo cache (separate API — safe to run independently)
    scheduler.add_job(
        func=fetch_and_cache_shipmondo_items,
        trigger=CronTrigger(hour=4, minute=0),  # Daily at 4:00 UTC
        id='shipmondo_cache_update',
        name='Update Shipmondo cache',
        replace_existing=True
    )
    scheduler.add_job(
        func=fetch_and_cache_shipmondo_items,
        id='shipmondo_initial_fetch',
        name='Initial Shipmondo cache fetch'
    )

    # All Shopify caches — run sequentially to avoid rate-limit denials
    scheduler.add_job(
        func=refresh_all_shopify_caches,
        trigger=CronTrigger(hour=4, minute=5),  # Daily at 4:05 UTC (after Shipmondo)
        id='shopify_cache_update',
        name='Update all Shopify caches (sequential)',
        replace_existing=True
    )
    scheduler.add_job(
        func=refresh_all_shopify_caches,
        id='shopify_initial_fetch',
        name='Initial Shopify cache fetch (sequential)'
    )

    scheduler.start()

    application.teardown_appcontext(close_db)

    def get_user_context() -> dict[str, str]:
        """Extract user information from session for template rendering."""
        user_info = session['oidc_auth_profile']
        user_name = user_info.get('name', user_info.get('preferred_username', 'User'))
        return {'user_name': user_name}

    @application.route("/")
    @oidc.require_login
    def index() -> str:
        return redirect(url_for("purchase_orders"))

    @application.route("/purchase-orders/")
    @oidc.require_login
    def purchase_orders() -> str:
        """Render the purchase orders grid."""
        context = get_user_context()
        return render_template(
            "purchase_orders.html", 
            purchase_orders=None,
            **context,
            active_page='purchase_orders'
        )

    @application.get("/purchase-orders/data/")
    async def purchase_order_data() -> Any:
        """Fetch purchase order data asynchronously with caching."""
        force_refresh = request.args.get('refresh', 'false').lower() == 'true'
        
        # Check cache if not forcing refresh
        if not force_refresh and 'po_data' in session and 'po_data_timestamp' in session:
            cache_time = datetime.fromisoformat(session['po_data_timestamp'])
            cache_age = datetime.now(timezone.utc) - cache_time
            
            # If cache is less than 30 minutes old, return cached data
            if cache_age < timedelta(minutes=CACHE_DURATION_MINUTES):
                current_app.logger.info(f"Returning cached purchase order data (age: {cache_age})")
                return jsonify({
                    "data": session['po_data'],
                    "cached": True,
                    "cache_timestamp": session['po_data_timestamp']
                })
        
        # Fetch fresh data
        try:
            current_app.logger.info("Fetching fresh purchase order data")
            data = await asyncio.to_thread(fetch_purchase_order_data)
            
            # Store in session cache
            session['po_data'] = data
            session['po_data_timestamp'] = datetime.now(timezone.utc).isoformat()
            
            return jsonify({
                "data": data,
                "cached": False,
                "cache_timestamp": session['po_data_timestamp']
            })
        except Exception as exc:  # pragma: no cover - defensive logging
            current_app.logger.exception("Failed to load purchase orders", exc_info=exc)
            return jsonify({"error": "Failed to load purchase orders."}), 500

    @application.get("/purchase-orders/configurations/")
    def list_configurations() -> Any:
        """List saved grid configurations."""
        db = get_db()
        # First check which columns exist
        existing_columns = {
            row[1]
            for row in db.execute(
                "PRAGMA table_info(purchase_order_configurations)"
            ).fetchall()
        }
        
        # Build query based on available columns
        base_columns = "id, name, columns, filters, column_labels, sort_model"
        extra_columns = []
        if "custom_columns" in existing_columns:
            extra_columns.append("custom_columns")
        if "column_widths" in existing_columns:
            extra_columns.append("column_widths")
        
        query_columns = base_columns
        if extra_columns:
            query_columns += ", " + ", ".join(extra_columns)
        
        rows = db.execute(
            f"""
            SELECT {query_columns}
            FROM purchase_order_configurations
            ORDER BY LOWER(name)
            """
        ).fetchall()
        
        configs = []
        for row in rows:
            try:
                config = {
                    "id": row["id"],
                    "name": row["name"],
                    "columns": json.loads(row["columns"]),
                    "filters": json.loads(row["filters"]),
                    "columnLabels": json.loads(row["column_labels"]),
                    "sortModel": json.loads(row["sort_model"]),
                    "customColumns": [],
                    "columnWidths": {},
                }
                # Add optional fields if they exist
                if "custom_columns" in existing_columns:
                    config["customColumns"] = json.loads(row["custom_columns"] or "[]")
                if "column_widths" in existing_columns:
                    config["columnWidths"] = json.loads(row["column_widths"] or "{}")
                configs.append(config)
            except Exception as e:
                current_app.logger.warning(f"Failed to parse configuration: {e}")
                continue
        return jsonify(configs)

    @application.post("/purchase-orders/configurations/")
    def upsert_configuration() -> Any:
        """Create or update a saved grid configuration."""
        payload = request.get_json(silent=True) or {}
        name = str(payload.get("name", "")).strip()
        columns = payload.get("columns")
        filters = payload.get("filters")
        column_labels = payload.get("columnLabels", {})
        sort_model = payload.get("sortModel", [])
        custom_columns = payload.get("customColumns", [])
        column_widths = payload.get("columnWidths", {})

        if not name:
            return jsonify({"error": "Configuration name is required."}), 400
        if not isinstance(columns, list):
            return jsonify({"error": "Columns must be provided as a list."}), 400
        if not isinstance(filters, dict):
            return jsonify({"error": "Filters must be provided as an object."}), 400
        if not isinstance(column_labels, dict):
            return jsonify({"error": "Column labels must be provided as an object."}), 400
        if not isinstance(sort_model, list):
            return jsonify({"error": "Sort model must be provided as a list."}), 400
        if not isinstance(custom_columns, list):
            return jsonify({"error": "Custom columns must be provided as a list."}), 400
        if not isinstance(column_widths, dict):
            return jsonify({"error": "Column widths must be provided as an object."}), 400

        db = get_db()
        
        # Check which columns exist in the database
        existing_columns = {
            row[1]
            for row in db.execute(
                "PRAGMA table_info(purchase_order_configurations)"
            ).fetchall()
        }
        
        # Build query based on available columns
        base_fields = ["name", "columns", "filters", "column_labels", "sort_model"]
        base_values = [name, json.dumps(columns), json.dumps(filters), json.dumps(column_labels), json.dumps(sort_model)]
        
        extra_fields = []
        extra_values = []
        update_fields = ["columns=excluded.columns", "filters=excluded.filters", 
                        "column_labels=excluded.column_labels", "sort_model=excluded.sort_model"]
        
        if "custom_columns" in existing_columns:
            extra_fields.append("custom_columns")
            extra_values.append(json.dumps(custom_columns))
            update_fields.append("custom_columns=excluded.custom_columns")
        
        if "column_widths" in existing_columns:
            extra_fields.append("column_widths")
            extra_values.append(json.dumps(column_widths))
            update_fields.append("column_widths=excluded.column_widths")
        
        all_fields = base_fields + extra_fields
        all_values = base_values + extra_values
        
        placeholders = ", ".join(["?" for _ in all_fields])
        field_names = ", ".join(all_fields)
        update_clause = ", ".join(update_fields)
        
        db.execute(
            f"""
            INSERT INTO purchase_order_configurations ({field_names})
            VALUES ({placeholders})
            ON CONFLICT(name) DO UPDATE SET
                {update_clause},
                created_at=CURRENT_TIMESTAMP
            """,
            tuple(all_values),
        )
        db.commit()

        # Build SELECT query
        select_fields = ", ".join(base_fields + extra_fields)
        row = db.execute(
            f"""
            SELECT id, {select_fields}
            FROM purchase_order_configurations
            WHERE name = ?
            """,
            (name,),
        ).fetchone()

        if row is None:
            return jsonify({"error": "Failed to persist configuration."}), 500

        response_payload = {
            "id": row["id"],
            "name": row["name"],
            "columns": json.loads(row["columns"]),
            "filters": json.loads(row["filters"]),
            "columnLabels": json.loads(row["column_labels"]),
            "sortModel": json.loads(row["sort_model"]),
            "customColumns": [],
            "columnWidths": {},
        }
        
        # Add optional fields if they exist
        if "custom_columns" in existing_columns:
            response_payload["customColumns"] = json.loads(row["custom_columns"] or "[]")
        if "column_widths" in existing_columns:
            response_payload["columnWidths"] = json.loads(row["column_widths"] or "{}")
        
        return jsonify(response_payload), 201

    @application.delete("/purchase-orders/configurations/<int:config_id>/")
    def delete_configuration(config_id: int) -> Any:
        """Delete a saved grid configuration."""
        db = get_db()
        deleted = db.execute(
            """
            DELETE FROM purchase_order_configurations
            WHERE id = ?
            """,
            (config_id,),
        )
        db.commit()
        if deleted.rowcount == 0:
            return jsonify({"error": "Configuration not found."}), 404
        return jsonify({"status": "deleted", "id": config_id})

    @application.get("/purchase-orders/order-methods/")
    def purchase_orders_order_methods() -> Any:
        """Return the ordering method/label for each configured vendor.

        Lets the grid decide which order button to show (and which flow to run)
        purely from the ordering config, keyed by the saved configuration name.
        """
        try:
            return jsonify(purchase_order.list_vendors())
        except Exception as exc:
            current_app.logger.exception("Failed to load ordering configuration", exc_info=exc)
            return jsonify({"error": "Failed to load ordering configuration."}), 500

    @application.get("/purchase-orders/locations/")
    def purchase_orders_locations() -> Any:
        """Return cached Shopify locations (used as transfer destinations)."""
        try:
            with locations_lock:
                locations = list(locations_cache["locations"])
            if not locations:
                # Lazy-fill the cache if the startup refresh hasn't run yet.
                fetch_and_cache_locations()
                with locations_lock:
                    locations = list(locations_cache["locations"])
            return jsonify({"locations": locations})
        except Exception as exc:
            current_app.logger.exception("Failed to load locations", exc_info=exc)
            return jsonify({"error": "Failed to load locations."}), 500

    @application.post("/purchase-orders/place-order/")
    def purchase_orders_place_order() -> Any:
        """Place a purchase order and record it as a Shopify inventory transfer.

        Flow: create an empty transfer into the template's destination location
        → use its name (e.g. "#T0733" → "T0733") as the order number → place the
        vendor order (email or API) → add the actually-ordered items to the
        transfer and mark it in transit → refresh the PO cache for the user.

        Request JSON: {"vendor": <configuration name>, "items": [...],
        "columns": [...]}. On vendor-order failure the empty transfer is cleaned
        up. Validation / business blocks return HTTP 4xx; success returns the
        backend result augmented with transfer_name / order_number / warnings.
        """
        payload = request.get_json(silent=True) or {}
        vendor = (payload.get("vendor") or payload.get("config_name") or "").strip()
        template = purchase_order.get_vendor(vendor)
        if not template:
            return jsonify({"error": f"'{vendor}' has no ordering configuration."}), 400

        items = payload.get("items") or []
        if not isinstance(items, list) or not items:
            return jsonify({"error": "No items provided."}), 400

        destination_location_id = (template.get("location_id") or "").strip()
        if not destination_location_id:
            return jsonify({
                "error": "This ordering template has no destination location. "
                         "Edit the template and choose one before ordering."
            }), 400

        # 1) Create the empty transfer first so its name becomes the order number.
        try:
            transfer = create_inventory_transfer(destination_location_id)
        except TransferError as exc:
            return jsonify({"error": f"Could not create the Shopify transfer: {exc}", "details": exc.errors}), 502
        except Exception as exc:
            current_app.logger.exception("Failed to create transfer for %s", vendor, exc_info=exc)
            return jsonify({"error": f"Could not create the Shopify transfer: {exc}"}), 502

        transfer_id = transfer["id"]
        transfer_name = transfer.get("name") or ""
        transfer_status = transfer.get("status")
        order_number = transfer_name[1:] if transfer_name.startswith("#") else transfer_name

        # 2) Place the vendor order using the transfer-derived order number.
        try:
            result = purchase_order.place_order(
                vendor, items, payload.get("columns"),
                send_email=send_plaintext_email, order_number=order_number,
            )
        except purchase_order.OrderError as exc:
            _safe_delete_transfer(transfer_id)
            body = {"error": str(exc), "details": exc.details}
            body.update(exc.extra)
            return jsonify(body), (exc.status or 500)
        except Exception as exc:
            _safe_delete_transfer(transfer_id)
            current_app.logger.exception("Failed to place order for %s", vendor, exc_info=exc)
            return jsonify({"error": f"Unexpected error: {exc}"}), 500

        # 3) Add the actually-ordered items to the transfer. The transfer is
        #    left as a draft (no shipment) so the user can manually add the
        #    supplier in Shopify before creating the shipment.
        ordered = result.get("ordered") or []
        line_items: list[dict] = []
        skipped_skus: list[str] = []
        for it in ordered:
            inv_id = it.get("inventory_item_id")
            try:
                qty = int(it.get("quantity") or 0)
            except (TypeError, ValueError):
                qty = 0
            if not inv_id or qty <= 0:
                skipped_skus.append(it.get("sku"))
                continue
            line_items.append({"inventoryItemId": inv_id, "quantity": qty})

        warnings: list[str] = []
        if skipped_skus:
            warnings.append(
                f"{len(skipped_skus)} ordered item(s) had no Shopify inventory item id and "
                "were not added to the transfer."
            )
        if line_items:
            try:
                set_transfer_items(transfer_id, line_items)
            except Exception as exc:  # order already placed — surface as warning, not failure
                current_app.logger.exception("Transfer update failed for %s", transfer_name, exc_info=exc)
                warnings.append(f"Order placed, but updating the Shopify transfer failed: {exc}")
        else:
            warnings.append("No items could be added to the Shopify transfer.")

        result["transfer_id"] = transfer_id
        result["transfer_name"] = transfer_name
        result["transfer_status"] = transfer_status
        result["order_number"] = order_number
        if warnings:
            result["warnings"] = warnings
        return jsonify(result)

    @application.get("/purchase-orders/ordering-template/")
    def get_ordering_template_route() -> Any:
        """Return the ordering template for a saved view (for the editor).

        Query: ?name=<view name>. Responds with {exists, view_name, method,
        label} plus either an ``email`` object or an ``api_raw`` YAML string for
        the raw API editor.
        """
        name = (request.args.get("name") or "").strip()
        if not name:
            return jsonify({"error": "A view name is required."}), 400
        template = get_ordering_template(_ordering_db_path, name)
        if not template:
            return jsonify({"exists": False, "view_name": name})
        resp: dict[str, Any] = {
            "exists": True,
            "view_name": name,
            "method": template.get("method"),
            "label": template.get("label", name),
            "location_id": template.get("location_id", ""),
            "location_name": template.get("location_name", ""),
        }
        if template.get("method") == "email":
            resp["email"] = template.get("email") or {}
        else:
            resp["api_raw"] = yaml.safe_dump(
                template.get("api") or {}, sort_keys=False, allow_unicode=True
            )
        return jsonify(resp)

    @application.post("/purchase-orders/ordering-template/")
    def save_ordering_template_route() -> Any:
        """Create or update the ordering template for a saved view.

        Body: {name, method, label, email{...}} for email, or
        {name, method, label, api_raw: <YAML string>} for API.
        """
        payload = request.get_json(silent=True) or {}
        name = (payload.get("name") or payload.get("view_name") or "").strip()
        method = (payload.get("method") or "").lower()
        data: dict[str, Any] = {
            "method": method,
            "label": payload.get("label") or name,
            "location_id": payload.get("location_id"),
            "location_name": payload.get("location_name"),
        }

        if method == "email":
            data["email"] = payload.get("email") or {}
        elif method == "api":
            raw = payload.get("api_raw")
            if isinstance(raw, str):
                try:
                    data["api"] = yaml.safe_load(raw) or {}
                except yaml.YAMLError as exc:
                    return jsonify({"error": f"API config is not valid YAML/JSON: {exc}"}), 400
            else:
                data["api"] = payload.get("api") or {}
        else:
            return jsonify({"error": "Template method must be 'email' or 'api'."}), 400

        try:
            template = purchase_order.validate_template(name, data)
        except purchase_order.OrderError as exc:
            return jsonify({"error": str(exc)}), (exc.status or 400)

        save_ordering_template(_ordering_db_path, name, template)
        return jsonify(
            {
                "status": "saved",
                "view_name": name,
                "method": template["method"],
                "label": template["label"],
            }
        )

    @application.delete("/purchase-orders/ordering-template/")
    def delete_ordering_template_route() -> Any:
        """Delete the ordering template for a saved view."""
        name = (request.args.get("name") or "").strip()
        if not name:
            return jsonify({"error": "A view name is required."}), 400
        if not delete_ordering_template(_ordering_db_path, name):
            return jsonify({"error": "No ordering template exists for that view."}), 404
        return jsonify({"status": "deleted", "view_name": name})

    @application.route("/inventory-tools/")
    @oidc.require_login
    def inventory_tools() -> str:
        """Render the inventory tools page."""
        context = get_user_context()
        return render_template(
            "inventory_tools.html",
            **context,
            active_page='inventory_tools'
        )

    @application.post("/inventory-tools/calculate-brand-value/")
    async def calculate_brand_value() -> Any:
        """Calculate the total inventory value for a specific brand or all inventory."""
        try:
            payload = request.get_json(silent=True) or {}
            brand_name = str(payload.get("brand", "")).strip()
            
            # If no brand provided, calculate total inventory value
            total_value = await asyncio.to_thread(calculate_brand_inventory_value, brand_name or None)
            
            result = {"total_value": total_value}
            if brand_name:
                result["brand"] = brand_name
            
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to calculate brand inventory value", exc_info=exc)
            return jsonify({"error": "Failed to calculate inventory value."}), 500

    @application.get("/inventory-tools/shipmondo-cache-status/")
    def shipmondo_cache_status() -> Any:
        """Get the status of the Shipmondo cache."""
        with shipmondo_lock:
            items_with_bins = sum(1 for item in shipmondo_cache["items"].values() if item.get("bin"))
            return jsonify({
                "total_items": len(shipmondo_cache["items"]),
                "items_with_bins": items_with_bins,
                "last_updated": shipmondo_cache["last_updated"],
                "is_refreshing": shipmondo_cache["is_refreshing"]
            })

    @application.post("/inventory-tools/refresh-shipmondo-cache/")
    def refresh_shipmondo_cache() -> Any:
        """Manually refresh the Shipmondo cache."""
        # Check if already refreshing
        if shipmondo_cache["is_refreshing"]:
            return jsonify({
                "success": False,
                "message": "Cache refresh already in progress",
                "is_refreshing": True
            }), 409  # Conflict status code
        
        try:
            # Schedule the refresh in background (non-blocking)
            from apscheduler.schedulers.background import BackgroundScheduler
            import atexit
            
            # Get or create scheduler
            if not hasattr(application, '_refresh_scheduler'):
                application._refresh_scheduler = BackgroundScheduler()
                application._refresh_scheduler.start()
                atexit.register(lambda: application._refresh_scheduler.shutdown())
            
            # Add one-time job
            application._refresh_scheduler.add_job(
                func=fetch_and_cache_shipmondo_items,
                id=f'manual_refresh_{datetime.now().timestamp()}',
                name='Manual Shipmondo cache refresh'
            )
            
            return jsonify({
                "success": True,
                "message": "Cache refresh started in background",
                "is_refreshing": True
            })
        except Exception as exc:
            current_app.logger.exception("Failed to start Shipmondo cache refresh", exc_info=exc)
            return jsonify({
                "error": "Failed to start cache refresh.",
                "is_refreshing": shipmondo_cache["is_refreshing"]
            }), 500

    @application.post("/inventory-tools/cleanup-sold-out-bins/")
    async def cleanup_sold_out_bins() -> Any:
        """Clean up bin locations for sold-out and archived Shopify variants."""
        try:
            # Fetch sold-out and archived variants from Shopify
            result = await asyncio.to_thread(_fetch_cleanup_variants)
            sold_out_skus = result['sold_out']
            archived_skus = result['archived']
            
            if not sold_out_skus and not archived_skus:
                return jsonify({
                    "success": True,
                    "message": "No sold-out or archived variants found in Shopify",
                    "cleared_count": 0
                })
            
            # Combine both lists
            cleanup_set = set(sold_out_skus + archived_skus)
            cleared_count = 0
            errors = []
            
            for sku, item_data in list(shipmondo_cache["items"].items()):
                if sku in cleanup_set and item_data.get("bin"):
                    item_id = item_data.get("id")
                    success, message = clear_bin_location(item_id, sku)
                    if success:
                        # Update cache
                        shipmondo_cache["items"][sku]["bin"] = ""
                        cleared_count += 1
                    else:
                        errors.append(message)
            
            return jsonify({
                "success": True,
                "sold_out_count": len(sold_out_skus),
                "archived_count": len(archived_skus),
                "cleared_count": cleared_count,
                "errors": errors[:10]  # Limit error messages
            })
        except Exception as exc:
            current_app.logger.exception("Failed to cleanup sold-out bins", exc_info=exc)
            return jsonify({"error": "Failed to cleanup bins."}), 500

    @application.post("/inventory-tools/preview-batch-update/")
    def preview_batch_update() -> Any:
        """Preview regex-based batch update without applying changes."""
        try:
            payload = request.get_json(silent=True) or {}
            regex_pattern = payload.get("regex_pattern", "").strip()
            replacement = payload.get("replacement", "").strip()
            
            if not regex_pattern:
                return jsonify({"error": "Regex pattern is required."}), 400
            if len(regex_pattern) > MAX_REGEX_LENGTH:
                return jsonify({
                    "error": f"The pattern may be at most {MAX_REGEX_LENGTH} characters."
                }), 400

            result = batch_update_bins_with_regex(
                shipmondo_cache["items"],
                regex_pattern,
                replacement
            )
            
            if "error" in result:
                return jsonify(result), 400
            
            # Return preview (limit to first 50 items)
            return jsonify({
                "matching_items": result["matching_items"][:50],
                "total_count": result["count"],
                "showing_count": min(50, result["count"])
            })
        except Exception as exc:
            current_app.logger.exception("Failed to preview batch update", exc_info=exc)
            return jsonify({"error": "Failed to preview batch update."}), 500

    @application.post("/inventory-tools/apply-batch-update/")
    async def apply_batch_update_route() -> Any:
        """Apply regex-based batch update to Shipmondo."""
        try:
            payload = request.get_json(silent=True) or {}
            regex_pattern = payload.get("regex_pattern", "").strip()
            replacement = payload.get("replacement", "").strip()
            
            if not regex_pattern:
                return jsonify({"error": "Regex pattern is required."}), 400
            if len(regex_pattern) > MAX_REGEX_LENGTH:
                return jsonify({
                    "error": f"The pattern may be at most {MAX_REGEX_LENGTH} characters."
                }), 400

            # Get matching items
            match_result = batch_update_bins_with_regex(
                shipmondo_cache["items"],
                regex_pattern,
                replacement
            )
            
            if "error" in match_result:
                return jsonify(match_result), 400
            
            if match_result["count"] == 0:
                return jsonify({
                    "success": True,
                    "message": "No items matched the pattern",
                    "success_count": 0,
                    "total_count": 0
                })
            
            # Apply updates
            result = await asyncio.to_thread(apply_batch_update, match_result["matching_items"])
            
            # Update cache for successful updates
            for item in match_result["matching_items"]:
                sku = item["sku"]
                if sku in shipmondo_cache["items"]:
                    shipmondo_cache["items"][sku]["bin"] = item["new_bin"]
            
            return jsonify({
                "success": True,
                "success_count": result["success_count"],
                "total_count": result["total_count"],
                "errors": result["errors"][:10]  # Limit error messages
            })
        except Exception as exc:
            current_app.logger.exception("Failed to apply batch update", exc_info=exc)
            return jsonify({"error": "Failed to apply batch update."}), 500

    @application.route("/counting/")
    @oidc.require_login
    def counting() -> str:
        """Render the stock counting page."""
        context = get_user_context()
        return render_template(
            "counting.html",
            **context,
            active_page="counting"
        )

    @application.post("/counting/count-sheet/")
    async def counting_count_sheet() -> Any:
        """Build a count sheet for the Shipmondo bins matching the given patterns.

        Bins and their SKUs come from the Shipmondo cache; the expected quantity
        is Shopify's *on-hand*, which is what a counter should physically find in
        the bin (available stock plus stock already committed to open orders).
        """
        payload = request.get_json(silent=True) or {}
        raw_bins = str(payload.get("bins") or "")
        if not raw_bins.strip():
            return jsonify({"error": "Enter at least one bin or bin pattern."}), 400
        if len(raw_bins) > MAX_BIN_INPUT_CHARS:
            return jsonify({
                "error": f"The bin list may be at most {MAX_BIN_INPUT_CHARS} characters."
            }), 400

        patterns, pattern_errors = expand_bin_patterns(raw_bins)
        if not patterns:
            return jsonify({
                "error": pattern_errors[0] if pattern_errors
                else "None of those bin patterns could be read."
            }), 400

        with shipmondo_lock:
            shipmondo_items = dict(shipmondo_cache["items"])
            cache_updated = shipmondo_cache["last_updated"]
        if not shipmondo_items:
            return jsonify({
                "error": "The Shipmondo bin cache is empty. Refresh it on the "
                         "Inventory Tools page and try again."
            }), 503

        match = find_items_in_bins(shipmondo_items, patterns)
        bin_items = match["items"]
        if not bin_items:
            return jsonify({
                "error": "No Shipmondo bins matched those patterns.",
                "unmatched_patterns": match["unmatched_patterns"][:50],
            }), 404
        if len(bin_items) > MAX_COUNT_SHEET_SKUS:
            return jsonify({
                "error": f"Those bins hold {len(bin_items)} SKUs; a count sheet "
                         f"covers at most {MAX_COUNT_SHEET_SKUS}. Narrow the patterns."
            }), 400

        try:
            stock = await asyncio.to_thread(
                fetch_on_hand_by_skus, [item["sku"] for item in bin_items]
            )
        except Exception as exc:
            current_app.logger.exception(
                "Failed to fetch on-hand inventory for the count sheet", exc_info=exc
            )
            return jsonify({"error": "Failed to fetch inventory from Shopify."}), 502

        # One group per bin, in walking order, so the printed sheet follows the
        # shelves.  find_items_in_bins already sorted by bin then SKU.
        groups: list[dict] = []
        by_bin: dict[str, dict] = {}
        total_units = 0
        missing_in_shopify = 0

        for item in bin_items:
            variant = stock.get(item["sku"])
            if variant is None:
                missing_in_shopify += 1
            on_hand = variant["on_hand"] if variant else None
            total_units += on_hand or 0

            group = by_bin.get(item["bin"])
            if group is None:
                group = {"bin": item["bin"], "lines": [], "on_hand": 0}
                by_bin[item["bin"]] = group
                groups.append(group)
            group["on_hand"] += on_hand or 0
            group["lines"].append({
                "sku": item["sku"],
                # Shipmondo's item name is the fallback for SKUs Shopify no
                # longer has, so the line is still countable.
                "product_title": (variant or {}).get("product_title") or item["name"],
                "variant_title": (variant or {}).get("variant_title") or "",
                "vendor": (variant or {}).get("vendor") or "",
                "barcode": (variant or {}).get("barcode") or item["barcode"],
                "on_hand": on_hand,
                "available": (variant or {}).get("available"),
                "committed": (variant or {}).get("committed"),
                "in_shopify": variant is not None,
            })

        return jsonify({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "bins_cache_updated": cache_updated,
            "bins": groups,
            "totals": {
                "bins": len(groups),
                "skus": len(bin_items),
                "units": total_units,
                "missing_in_shopify": missing_in_shopify,
            },
            "patterns": patterns[:50],
            "pattern_count": len(patterns),
            "unmatched_patterns": match["unmatched_patterns"][:50],
            "pattern_errors": pattern_errors[:10],
        })

    @application.route("/barcode-scanner/")
    @oidc.require_login
    def barcode_scanner() -> Any:
        """Barcode scanner page for looking up items in Shipmondo cache."""
        context = get_user_context()
        return render_template(
            "barcode_scanner.html",
            **context,
            active_page="barcode_scanner"
        )

    @application.post("/barcode-scanner/lookup/")
    def lookup_barcode() -> Any:
        """Look up a barcode in the Shipmondo cache."""
        try:
            payload = request.get_json(silent=True) or {}
            barcode = str(payload.get("barcode", "")).strip()
            
            if not barcode:
                return jsonify({"error": "Barcode is required"}), 400
            
            # Search for the item in cache by barcode field
            found_item = None
            for sku, item_data in shipmondo_cache["items"].items():
                if item_data.get("barcode") == barcode:
                    found_item = item_data
                    break
            
            if found_item:
                return jsonify({
                    "found": True,
                    "sku": found_item.get("sku", "Unknown"),
                    "name": found_item.get("name", "Unknown"),
                    "bin": found_item.get("bin", "No bin assigned"),
                    "itemId": found_item.get("id")
                })
            else:
                return jsonify({
                    "found": False,
                    "message": f"No item found with barcode: {barcode}"
                })
        except Exception as exc:
            current_app.logger.exception("Failed to lookup barcode", exc_info=exc)
            return jsonify({"error": "Failed to lookup barcode."}), 500

    @application.post("/barcode-scanner/search-items/")
    def search_items() -> Any:
        """Search for items in Shipmondo cache by SKU or name."""
        try:
            payload = request.get_json(silent=True) or {}
            query = str(payload.get("query", "")).strip().lower()
            
            if not query:
                return jsonify({"items": []})
            
            # Search through cache
            matching_items = []
            with shipmondo_lock:
                for sku, item_data in shipmondo_cache["items"].items():
                    sku_lower = sku.lower()
                    name_lower = item_data.get("name", "").lower()
                    
                    # Match on SKU or name
                    if query in sku_lower or query in name_lower:
                        matching_items.append({
                            "sku": item_data.get("sku", ""),
                            "name": item_data.get("name", ""),
                            "bin": item_data.get("bin", ""),
                            "id": item_data.get("id")
                        })
                        
                        # Limit results to 50 for performance
                        if len(matching_items) >= 50:
                            break
            
            return jsonify({"items": matching_items})
        except Exception as exc:
            current_app.logger.exception("Failed to search items", exc_info=exc)
            return jsonify({"error": "Failed to search items."}), 500

    @application.post("/barcode-scanner/assign-bin/")
    async def assign_bin() -> Any:
        """Assign a bin location to an item."""
        try:
            payload = request.get_json(silent=True) or {}
            sku = str(payload.get("sku", "")).strip()
            bin_code = str(payload.get("bin", "")).strip()
            
            if not sku or not bin_code:
                return jsonify({"error": "SKU and bin code are required"}), 400
            
            # Find item in cache
            item_data = shipmondo_cache["items"].get(sku)
            if not item_data:
                return jsonify({"error": f"Item with SKU {sku} not found in cache"}), 404
            
            item_id = item_data.get("id")
            if not item_id:
                return jsonify({"error": "Item ID not found"}), 500
            
            # Update bin in Shipmondo
            from shipmondo import update_bin_location
            success, message = await asyncio.to_thread(update_bin_location, item_id, sku, bin_code)
            
            if success:
                # Update cache
                with shipmondo_lock:
                    shipmondo_cache["items"][sku]["bin"] = bin_code
                
                return jsonify({
                    "success": True,
                    "message": message,
                    "bin": bin_code
                })
            else:
                return jsonify({"error": message}), 500
                
        except Exception as exc:
            current_app.logger.exception("Failed to assign bin", exc_info=exc)
            return jsonify({"error": "Failed to assign bin."}), 500

    @application.post("/barcode-scanner/assign-barcode/")
    async def assign_barcode_to_sku() -> Any:
        """Assign a barcode to a SKU."""
        try:
            payload = request.get_json(silent=True) or {}
            sku = str(payload.get("sku", "")).strip()
            barcode = str(payload.get("barcode", "")).strip()
            
            current_app.logger.info(f"Assigning barcode {barcode} to SKU {sku}")
            
            if not sku or not barcode:
                return jsonify({"error": "SKU and barcode are required"}), 400
            
            # Find item in cache
            item_data = shipmondo_cache["items"].get(sku)
            if not item_data:
                return jsonify({"error": f"Item with SKU {sku} not found in cache"}), 404
            
            item_id = item_data.get("id")
            if not item_id:
                return jsonify({"error": "Item ID not found"}), 500
            
            # Update barcode in both Shipmondo and Shopify
            current_app.logger.info(f"Updating Shipmondo for SKU {sku}...")
            shipmondo_success, shipmondo_message = await asyncio.to_thread(update_barcode, item_id, sku, barcode)
            current_app.logger.info(f"Shipmondo result: {shipmondo_success} - {shipmondo_message}")
            
            current_app.logger.info(f"Updating Shopify for SKU {sku}...")
            shopify_success, shopify_message = await asyncio.to_thread(update_variant_barcode, sku, barcode)
            current_app.logger.info(f"Shopify result: {shopify_success} - {shopify_message}")
            
            if shipmondo_success and shopify_success:
                # Update cache
                with shipmondo_lock:
                    shipmondo_cache["items"][sku]["barcode"] = barcode
                
                return jsonify({
                    "success": True,
                    "message": f"Updated barcode in both Shipmondo and Shopify for SKU {sku}",
                    "barcode": barcode
                })
            elif shipmondo_success and not shopify_success:
                # Partial success - Shipmondo updated but Shopify failed
                with shipmondo_lock:
                    shipmondo_cache["items"][sku]["barcode"] = barcode
                
                return jsonify({
                    "success": True,
                    "warning": f"Updated in Shipmondo but failed in Shopify: {shopify_message}",
                    "message": f"Barcode updated in Shipmondo. Shopify update failed: {shopify_message}",
                    "barcode": barcode
                }), 207  # Multi-Status
            elif not shipmondo_success and shopify_success:
                # Partial success - Shopify updated but Shipmondo failed
                return jsonify({
                    "success": False,
                    "error": f"Updated in Shopify but failed in Shipmondo: {shipmondo_message}"
                }), 207  # Multi-Status
            else:
                # Both failed
                return jsonify({
                    "error": f"Failed to update barcode. Shipmondo: {shipmondo_message}. Shopify: {shopify_message}"
                }), 500
                
        except Exception as exc:
            current_app.logger.exception("Failed to assign barcode", exc_info=exc)
            return jsonify({"error": "Failed to assign barcode."}), 500

    # ── Product Tools ──────────────────────────────────────────────

    @application.route("/product-tools/")
    @oidc.require_login
    def product_tools() -> str:
        """Render the product tools page."""
        context = get_user_context()
        return render_template(
            "product_tools.html",
            **context,
            active_page="product_tools",
        )

    # Vendor → list of Shopify vendor names to compare against
    VENDOR_SHOPIFY_BRANDS: dict[str, list[str]] = {
        "entirem": ["Helikon-Tex", "Tac Maven"],
        "deerhunter": ["Deerhunter"],
        "pentagon": ["Pentagon Tactical"],
    }

    # Vendors that require a CSV file upload (others fetch data automatically)
    VENDORS_REQUIRING_CSV: set[str] = {"entirem"}

    @application.post("/product-tools/compare/")
    async def product_tools_compare() -> Any:
        """Compare vendor products against Shopify."""
        try:
            vendor = (request.form.get("vendor") or "").strip()

            if not vendor:
                return jsonify({"error": "Vendor is required."}), 400

            if vendor not in VENDOR_SHOPIFY_BRANDS:
                return jsonify({"error": f"Unsupported vendor: {vendor}"}), 400

            # Get vendor products based on source type
            if vendor == "deerhunter":
                dh_products = await asyncio.to_thread(dh_fetch_all_products)
                vendor_products = dh_products_to_vendor_format(dh_products)
                current_app.logger.info(
                    f"Fetched {len(vendor_products)} rows from Deerhunter FTP"
                )
            elif vendor == "pentagon":
                pt_products = await asyncio.to_thread(pt_fetch_all_products)
                vendor_products = pt_products_to_vendor_format(pt_products)
                current_app.logger.info(
                    f"Fetched {len(vendor_products)} rows from the Pentagon XML feed"
                )
            else:
                csv_file = request.files.get("csv_file")
                if not csv_file or csv_file.filename == "":
                    return jsonify({"error": "A CSV file is required."}), 400

                # Read one byte past the limit so an oversized upload is
                # rejected instead of being parsed into memory.
                raw = csv_file.read(MAX_CSV_UPLOAD_BYTES + 1)
                if len(raw) > MAX_CSV_UPLOAD_BYTES:
                    return jsonify({
                        "error": f"The CSV file may be at most "
                                 f"{MAX_CSV_UPLOAD_BYTES // (1024 * 1024)} MB."
                    }), 413
                try:
                    csv_content = raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    return jsonify({"error": "The CSV file must be UTF-8 encoded."}), 400
                vendor_products = entire_m.parse_vendor_csv(csv_content)
                current_app.logger.info(
                    f"Parsed {len(vendor_products)} rows from uploaded CSV"
                )

            if not vendor_products:
                return jsonify({"error": "No valid product rows found. Check the data source."}), 400

            # Fetch matching Shopify products for the vendor's brands
            brands = VENDOR_SHOPIFY_BRANDS[vendor]
            shopify_products = await asyncio.to_thread(
                fetch_shopify_products_by_vendors, brands,
            )
            current_app.logger.info(
                f"Fetched {len(shopify_products)} products from Shopify"
            )

            # Compare
            result = compare_vendor_products(vendor_products, shopify_products)
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to compare products", exc_info=exc
            )
            return jsonify({"error": "Failed to compare products."}), 500

    @application.post("/product-tools/add-variants/")
    async def product_tools_add_variants() -> Any:
        """Add selected new variants to their existing Shopify products."""
        try:
            payload = request.get_json(silent=True) or {}
            variants = payload.get("variants", [])
            color_image_urls = payload.get("color_image_urls", {})

            current_app.logger.info(
                "add-variants: received %d variant(s) in payload, %d product(s) with images",
                len(variants), len(color_image_urls),
            )

            if not variants or not isinstance(variants, list):
                return jsonify({"error": "No variants provided."}), 400
            if (err := _too_many("variants", variants)):
                return err

            # Group variants by Shopify product ID
            by_product: dict[str, list[dict]] = {}
            for v in variants:
                pid = v.get("shopify_product_id")
                if pid:
                    by_product.setdefault(pid, []).append(v)

            current_app.logger.info(
                "add-variants: grouped into %d product(s): %s",
                len(by_product),
                {pid: len(vlist) for pid, vlist in by_product.items()},
            )

            if not by_product:
                return jsonify({"error": "No variants with a valid Shopify product ID found."}), 400

            all_created: list[dict] = []
            all_errors: list[str] = []

            for product_id, product_variants in by_product.items():
                current_app.logger.info(
                    "add-variants: calling mutation for product %s with %d variant(s)",
                    product_id, len(product_variants),
                )
                product_images = color_image_urls.get(product_id, {})
                result = await asyncio.to_thread(
                    add_variants_to_shopify_product, product_id, product_variants, product_images
                )
                current_app.logger.info(
                    "add-variants: result for %s — created=%d errors=%d",
                    product_id,
                    len(result.get("created", [])),
                    len(result.get("errors", [])),
                )
                all_created.extend(result.get("created", []))
                all_errors.extend(result.get("errors", []))

            return jsonify({
                "created": all_created,
                "errors": all_errors,
            })

        except Exception as exc:
            current_app.logger.exception(
                "Failed to add variants", exc_info=exc
            )
            return jsonify({"error": "Failed to add variants."}), 500

    @application.post("/product-tools/color-options/")
    async def product_tools_color_options() -> Any:
        """Fetch the color metaobject field definitions and valid options."""
        try:
            payload = request.get_json(silent=True) or {}
            # Accept both single product_id and list of product_ids
            product_ids = payload.get("product_ids", [])
            if not product_ids:
                pid = payload.get("product_id", "")
                if pid:
                    product_ids = [pid]

            if not product_ids:
                return jsonify({"error": "product_ids is required."}), 400
            if (err := _too_many("product_ids", product_ids)):
                return err

            # Try each product ID until one returns a valid definition
            result = None
            for pid in product_ids:
                result = await asyncio.to_thread(fetch_color_field_options, pid)
                if result.get("metaobject_type"):
                    break

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to fetch color options", exc_info=exc
            )
            return jsonify({"error": "Failed to fetch color options."}), 500

    @application.post("/product-tools/check-colors/")
    async def product_tools_check_colors() -> Any:
        """Check which color names already exist as metaobjects."""
        try:
            payload = request.get_json(silent=True) or {}
            # Accept both single product_id and list of product_ids
            product_ids = payload.get("product_ids", [])
            if not product_ids:
                pid = payload.get("product_id", "")
                if pid:
                    product_ids = [pid]
            color_names = payload.get("color_names", [])

            if not product_ids:
                return jsonify({"error": "product_ids is required."}), 400
            if (err := _too_many("product_ids", product_ids)) or (
                err := _too_many("color_names", color_names)
            ):
                return err
            if not color_names:
                return jsonify({"existing": {}, "missing": [], "on_product": []})

            # Try each product ID until one succeeds (has linked metaobjects)
            result = None
            for pid in product_ids:
                result = await asyncio.to_thread(
                    check_existing_color_metaobjects, pid, color_names
                )
                # If we found any existing colors or the metaobject type was
                # discovered (missing != all), this product worked
                if result.get("existing") or len(result.get("missing", [])) < len(color_names):
                    break

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to check colors", exc_info=exc
            )
            return jsonify({"error": "Failed to check colors."}), 500

    @application.post("/product-tools/generate-swatch/")
    async def product_tools_generate_swatch() -> Any:
        """Generate a 300×300 diagonal-split swatch PNG and return it as a data URI."""
        try:
            import base64
            payload = request.get_json(silent=True) or {}
            top_left = payload.get("top_left", {})
            bottom_right = payload.get("bottom_right", {})

            if not top_left or not bottom_right:
                return jsonify({"error": "top_left and bottom_right are required."}), 400

            png_bytes = await asyncio.to_thread(
                generate_diagonal_swatch, top_left, bottom_right
            )
            b64 = base64.b64encode(png_bytes).decode("ascii")
            data_uri = f"data:image/png;base64,{b64}"

            return jsonify({"data_uri": data_uri})

        except Exception as exc:
            current_app.logger.exception(
                "Failed to generate swatch", exc_info=exc
            )
            return jsonify({"error": f"Failed to generate swatch: {exc}"}), 500

    @application.post("/product-tools/create-color/")
    async def product_tools_create_color() -> Any:
        """Create a new color metaobject."""
        try:
            payload = request.get_json(silent=True) or {}
            metaobject_type = payload.get("metaobject_type", "")
            display_name = payload.get("display_name", "")
            fields = payload.get("fields", {})
            file_fields = payload.get("file_fields", [])  # field keys that need file upload

            if not metaobject_type:
                return jsonify({"error": "metaobject_type is required."}), 400
            if not display_name:
                return jsonify({"error": "display_name is required."}), 400

            # Upload any file_reference fields (e.g. swatch image URL → Shopify file GID)
            for fk in file_fields:
                raw = fields.get(fk, "").strip()
                if raw and raw.startswith("data:"):
                    # Data URI from generated swatch — decode and staged-upload
                    current_app.logger.info(
                        "Uploading data-URI swatch for field '%s'", fk
                    )
                    try:
                        import base64
                        # data:image/png;base64,XXXX
                        _header, b64data = raw.split(",", 1)
                        if len(b64data) > 4 * (MAX_INLINE_IMAGE_BYTES // 3) + 4:
                            return jsonify({
                                "error": f"The swatch image may be at most "
                                         f"{MAX_INLINE_IMAGE_BYTES // (1024 * 1024)} MB."
                            }), 413
                        png_bytes = base64.b64decode(b64data)
                        file_gid = await asyncio.to_thread(
                            upload_swatch_bytes_to_shopify,
                            png_bytes,
                            filename=f"{display_name.replace(' ', '_')}_swatch.png",
                            alt=display_name,
                        )
                        fields[fk] = file_gid
                        current_app.logger.info(
                            "Uploaded swatch for field '%s' → %s", fk, file_gid
                        )
                    except Exception as upload_exc:
                        current_app.logger.exception(
                            "Failed to upload swatch for field '%s'", fk
                        )
                        return jsonify({"error": f"Swatch upload failed for field '{fk}': {upload_exc}"}), 500
                elif raw and raw.startswith("http"):
                    current_app.logger.info(
                        "Uploading file for field '%s': %s", fk, raw
                    )
                    try:
                        file_gid = await asyncio.to_thread(
                            upload_file_to_shopify, raw, alt=display_name
                        )
                        fields[fk] = file_gid
                        current_app.logger.info(
                            "Uploaded file for field '%s' → %s", fk, file_gid
                        )
                    except Exception as upload_exc:
                        current_app.logger.exception(
                            "Failed to upload file for field '%s'", fk
                        )
                        return jsonify({"error": f"File upload failed for field '{fk}': {upload_exc}"}), 500

            result = await asyncio.to_thread(
                create_color_metaobject, metaobject_type, display_name, fields
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to create color metaobject", exc_info=exc
            )
            return jsonify({"error": "Failed to create color."}), 500

    # ── Mail Tools ─────────────────────────────────────────────────

    @application.post("/product-tools/check-linked-options/")
    async def product_tools_check_linked_options() -> Any:
        """Check which linked option values are missing from the metaobject pool."""
        try:
            payload = request.get_json(silent=True) or {}
            product_ids = payload.get("product_ids", [])
            variants = payload.get("variants", [])

            if not product_ids:
                return jsonify({"error": "product_ids is required."}), 400
            if (err := _too_many("product_ids", product_ids)) or (
                err := _too_many("variants", variants)
            ):
                return err
            if not variants:
                return jsonify({"options": {}})

            # Try each product ID until one returns results
            result = {"options": {}}
            for pid in product_ids:
                result = await asyncio.to_thread(
                    check_linked_option_values, pid, variants
                )
                if result.get("options"):
                    break

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to check linked options", exc_info=exc
            )
            return jsonify({"error": "Failed to check linked options."}), 500

    @application.post("/product-tools/create-option-value/")
    async def product_tools_create_option_value() -> Any:
        """Create a simple metaobject for a linked option value (e.g. size)."""
        try:
            payload = request.get_json(silent=True) or {}
            metaobject_type = payload.get("metaobject_type", "")
            display_name = payload.get("display_name", "")

            if not metaobject_type:
                return jsonify({"error": "metaobject_type is required."}), 400
            if not display_name:
                return jsonify({"error": "display_name is required."}), 400

            result = await asyncio.to_thread(
                create_option_value_metaobject, metaobject_type, display_name
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to create option value metaobject", exc_info=exc
            )
            return jsonify({"error": "Failed to create option value."}), 500

    # ── Product Creation Endpoints ────────────────────────────────

    @application.get("/product-tools/taxonomy/")
    def product_tools_taxonomy() -> Any:
        """Return the cached Shopify product taxonomy categories."""
        with taxonomy_lock:
            return jsonify({
                "categories": taxonomy_cache["categories"],
                "last_updated": taxonomy_cache["last_updated"],
            })

    @application.get("/product-tools/tags/")
    def product_tools_tags() -> Any:
        """Return the cached product tags."""
        with tags_lock:
            return jsonify({
                "tags": tags_cache["tags"],
                "last_updated": tags_cache["last_updated"],
            })

    @application.post("/product-tools/category-metafields/")
    async def product_tools_category_metafields() -> Any:
        """Fetch metafield attributes for a given taxonomy category."""
        try:
            payload = request.get_json(silent=True) or {}
            category_id = payload.get("category_id", "").strip()

            if not category_id:
                return jsonify({"error": "category_id is required."}), 400

            metafields = await asyncio.to_thread(fetch_category_metafields, category_id)
            return jsonify({"metafields": metafields})

        except Exception as exc:
            current_app.logger.exception(
                "Failed to fetch category metafields", exc_info=exc
            )
            return jsonify({"error": "Failed to fetch category metafields."}), 500

    @application.post("/product-tools/save-category-metafields/")
    async def product_tools_save_category_metafields() -> Any:
        """Save category metafield values to a product."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            metafield_values = payload.get("metafield_values", [])

            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if (err := _too_many("metafield_values", metafield_values)):
                return err

            result = await asyncio.to_thread(
                set_product_category_metafields, product_id, metafield_values
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to save category metafields", exc_info=exc
            )
            return jsonify({"error": "Failed to save category metafields."}), 500

    @application.post("/product-tools/translate-description/")
    async def product_tools_translate_description() -> Any:
        """Fetch a vendor page URL and translate its description to Danish."""
        try:
            payload = request.get_json(silent=True) or {}
            url = payload.get("url", "").strip()
            product_name = payload.get("product_name", "").strip()

            if not url:
                return jsonify({"error": "url is required."}), 400

            result = await asyncio.to_thread(
                fetch_and_translate_vendor_page, url, product_name
            )

            if result.get("error"):
                return jsonify(result), 500

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to translate description", exc_info=exc
            )
            return jsonify({"error": "Failed to translate description."}), 500

    @application.post("/product-tools/translate-product-data/")
    async def product_tools_translate_product_data() -> Any:
        """Generate a Danish product description from raw vendor product data."""
        try:
            payload = request.get_json(silent=True) or {}
            product_fields = payload.get("product_fields", {})

            if not product_fields or not product_fields.get("product_name"):
                return jsonify({"error": "product_fields with product_name is required."}), 400

            result = await asyncio.to_thread(
                translate_product_data, product_fields
            )

            if result.get("error"):
                return jsonify(result), 500

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to translate product data", exc_info=exc
            )
            return jsonify({"error": "Failed to translate product data."}), 500

    @application.post("/product-tools/translate-plain-text/")
    async def product_tools_translate_plain_text() -> Any:
        """Translate and rephrase plain text product description to Danish HTML."""
        try:
            payload = request.get_json(silent=True) or {}
            text = payload.get("text", "").strip()
            product_name = payload.get("product_name", "").strip()

            if not text:
                return jsonify({"error": "text is required."}), 400

            result = await asyncio.to_thread(
                translate_plain_text, text, product_name
            )

            if result.get("error"):
                return jsonify(result), 500

            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to translate plain text", exc_info=exc
            )
            return jsonify({"error": "Failed to translate plain text."}), 500

    @application.post("/product-tools/create-product/")
    async def product_tools_create_product() -> Any:
        """Create a new Shopify product (draft, published to all channels)."""
        try:
            payload = request.get_json(silent=True) or {}
            title = payload.get("title", "").strip()
            vendor = payload.get("vendor", "").strip()
            description_html = payload.get("description_html", "").strip()
            category_id = payload.get("category_id", "").strip() or None
            tags = payload.get("tags", [])

            if not title:
                return jsonify({"error": "title is required."}), 400
            if not vendor:
                return jsonify({"error": "vendor is required."}), 400

            result = await asyncio.to_thread(
                create_shopify_product, title, vendor, description_html, category_id, tags
            )

            if result.get("product_id"):
                return jsonify(result), 201
            else:
                return jsonify(result), 500

        except Exception as exc:
            current_app.logger.exception(
                "Failed to create product", exc_info=exc
            )
            return jsonify({"error": "Failed to create product."}), 500

    @application.post("/product-tools/detect-product-options/")
    async def product_tools_detect_product_options() -> Any:
        """Detect product options from variant data and a reference product."""
        try:
            payload = request.get_json(silent=True) or {}
            vendor = payload.get("vendor", "").strip()
            variants = payload.get("variants", [])

            if not vendor:
                return jsonify({"error": "vendor is required."}), 400

            result = await asyncio.to_thread(
                detect_product_options, vendor, variants,
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to detect product options", exc_info=exc
            )
            return jsonify({"error": "Failed to detect product options."}), 500

    @application.post("/product-tools/create-product-options/")
    async def product_tools_create_product_options() -> Any:
        """Create product options on a newly created product."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            options = payload.get("options", [])

            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if not options:
                return jsonify({"error": "options is required."}), 400

            result = await asyncio.to_thread(
                create_product_options, product_id, options,
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to create product options", exc_info=exc
            )
            return jsonify({"error": "Failed to create product options."}), 500

    @application.post("/product-tools/definition-metaobjects/")
    @oidc.require_login
    def product_tools_definition_metaobjects() -> Any:
        """Return all metaobjects for a given metafield definition."""
        try:
            payload = request.get_json(silent=True) or {}
            namespace = payload.get("namespace", "").strip()
            key = payload.get("key", "").strip()

            if not namespace or not key:
                return jsonify({"error": "namespace and key are required."}), 400

            result = fetch_metaobjects_for_definition(namespace, key)
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to fetch metaobjects for definition", exc_info=exc
            )
            return jsonify({"error": "Failed to fetch metaobjects."}), 500

    @application.post("/product-tools/metaobject-type-fields/")
    @oidc.require_login
    def product_tools_metaobject_type_fields() -> Any:
        """Return definition (fields + reference options) for a metaobject type."""
        try:
            payload = request.get_json(silent=True) or {}
            metaobject_type = payload.get("metaobject_type", "").strip()
            category_id = payload.get("category_id", "").strip() or None

            if not metaobject_type:
                return jsonify({"error": "metaobject_type is required."}), 400

            result = fetch_metaobject_type_details(
                metaobject_type, category_id=category_id,
            )
            return jsonify(result)

        except Exception as exc:
            current_app.logger.exception(
                "Failed to fetch metaobject type details", exc_info=exc
            )
            return jsonify({"error": "Failed to fetch metaobject type fields."}), 500

    @application.post("/product-tools/product-images/")
    async def product_tools_get_images() -> Any:
        """Fetch all images for a product."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            images = await asyncio.to_thread(fetch_product_images, product_id)
            return jsonify({"images": images})
        except Exception as exc:
            current_app.logger.exception("Failed to fetch product images", exc_info=exc)
            return jsonify({"error": "Failed to fetch product images."}), 500

    @application.post("/product-tools/add-product-images/")
    async def product_tools_add_images() -> Any:
        """Add images to a product by URL."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            image_urls = payload.get("image_urls", [])
            image_alts = payload.get("image_alts", None)
            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if not image_urls:
                return jsonify({"error": "image_urls is required."}), 400
            if (err := _too_many("image_urls", image_urls)):
                return err
            result = await asyncio.to_thread(
                add_product_images, product_id, image_urls, image_alts
            )
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to add product images", exc_info=exc)
            return jsonify({"error": "Failed to add product images."}), 500

    @application.post("/product-tools/reorder-product-images/")
    async def product_tools_reorder_images() -> Any:
        """Reorder product images."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            media_ids = payload.get("media_ids", [])
            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if not media_ids:
                return jsonify({"error": "media_ids is required."}), 400
            if (err := _too_many("media_ids", media_ids)):
                return err
            result = await asyncio.to_thread(reorder_product_images, product_id, media_ids)
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to reorder product images", exc_info=exc)
            return jsonify({"error": "Failed to reorder product images."}), 500

    @application.post("/product-tools/delete-product-image/")
    async def product_tools_delete_image() -> Any:
        """Delete an image from a product."""
        try:
            payload = request.get_json(silent=True) or {}
            product_id = payload.get("product_id", "").strip()
            media_ids = payload.get("media_ids", [])
            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if not media_ids:
                return jsonify({"error": "media_ids is required."}), 400
            if (err := _too_many("media_ids", media_ids)):
                return err
            result = await asyncio.to_thread(delete_product_image, product_id, media_ids)
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to delete product image", exc_info=exc)
            return jsonify({"error": "Failed to delete product image."}), 500

    @application.post("/product-tools/stage-image-uploads/")
    async def product_tools_stage_uploads() -> Any:
        """Create staged upload targets for file-based image uploads."""
        try:
            payload = request.get_json(silent=True) or {}
            files = payload.get("files", [])
            if not files:
                return jsonify({"error": "files is required."}), 400
            if (err := _too_many("files", files)):
                return err
            from shopify import create_staged_uploads
            targets = await asyncio.to_thread(create_staged_uploads, files)
            return jsonify({"targets": targets})
        except Exception as exc:
            current_app.logger.exception("Failed to create staged uploads", exc_info=exc)
            return jsonify({"error": "Failed to create staged uploads."}), 500

    @application.post("/product-tools/helikon-images/")
    async def product_tools_helikon_images() -> Any:
        """Return classified Helikon-Tex images for a product code (fetched from partner portal)."""
        try:
            payload = request.get_json(silent=True) or {}
            product_code = payload.get("product_code", "").strip()
            if not product_code:
                return jsonify({"error": "product_code is required."}), 400
            all_files = await asyncio.to_thread(entire_m.get_helikon_listing)
            result = entire_m.classify_helikon_images(product_code, all_files)
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to fetch Helikon images", exc_info=exc)
            return jsonify({"error": "Failed to fetch Helikon images."}), 500

    @application.get("/product-tools/helikon-image-proxy")
    async def product_tools_helikon_image_proxy() -> Any:
        """Proxy a single Helikon image so the browser can display it without basic-auth."""
        filename = request.args.get("filename", "")
        if not filename or "/" in filename or "\\" in filename or ".." in filename:
            return jsonify({"error": "Invalid filename."}), 400
        try:
            url = entire_m.helikon_image_url(filename)
            auth = entire_m.helikon_image_basic_auth()
            content, content_type = await asyncio.to_thread(
                netguard.fetch_image, url, auth=auth, timeout=15
            )
            return Response(content, content_type=content_type)
        except netguard.UnsafeURLError as exc:
            current_app.logger.warning("Rejected Helikon image fetch: %s", exc)
            return jsonify({"error": "Failed to fetch image."}), 502
        except Exception as exc:
            current_app.logger.exception("Failed to proxy Helikon image", exc_info=exc)
            return jsonify({"error": "Failed to fetch image."}), 500

    @application.post("/product-tools/helikon-stage-images/")
    async def product_tools_helikon_stage_images() -> Any:
        """Download Helikon images server-side and stage-upload them to Shopify.

        Returns a JSON object mapping filename → Shopify resourceUrl.
        """
        try:
            payload = request.get_json(silent=True) or {}
            filenames = payload.get("filenames", [])
            if not filenames:
                return jsonify({"error": "filenames is required."}), 400
            if (err := _too_many("filenames", filenames)):
                return err
            result = await asyncio.to_thread(entire_m.stage_helikon_images, filenames)
            return jsonify(result)
        except Exception as exc:
            current_app.logger.exception("Failed to stage Helikon images", exc_info=exc)
            return jsonify({"error": "Failed to stage Helikon images."}), 500

    @application.get("/product-tools/all-products/")
    def product_tools_all_products() -> Any:
        """Return the cached list of all products (id, title, vendor, tags, category_id)."""
        with products_lock:
            return jsonify({
                "products": products_cache["products"],
                "last_updated": products_cache["last_updated"],
            })

    @application.post("/product-tools/remap-products/apply/")
    async def product_tools_remap_apply() -> Any:
        """Apply tag/category/metafield remapping to a set of products."""
        try:
            payload = request.get_json(silent=True) or {}
            product_ids = payload.get("product_ids", [])
            tags_to_add = payload.get("tags_to_add", [])
            tags_to_remove = payload.get("tags_to_remove", [])
            new_category_id = payload.get("category_id") or None
            metafield_values = payload.get("metafield_values", [])

            if not product_ids:
                return jsonify({"error": "product_ids is required."}), 400
            if (err := _too_many("product_ids", product_ids)) or (
                err := _too_many("metafield_values", metafield_values)
            ):
                return err
            if not tags_to_add and not tags_to_remove and not new_category_id and not metafield_values:
                return jsonify({"error": "At least one of tags_to_add, tags_to_remove, category_id, or metafield_values must be provided."}), 400

            updated = 0
            errors = []
            tag_updates: dict[str, list[str]] = {}
            succeeded_ids: list[str] = []

            for product_id in product_ids:
                with products_lock:
                    cached = next((p for p in products_cache["products"] if p["id"] == product_id), None)
                current_tags: list[str] = list(cached["tags"]) if cached else []

                new_tags = list(current_tags)
                for tag in tags_to_add:
                    if tag not in new_tags:
                        new_tags.append(tag)
                new_tags = [t for t in new_tags if t not in tags_to_remove]

                tags_changed = set(new_tags) != set(current_tags)
                product_updated = False

                if tags_changed or new_category_id:
                    result = await asyncio.to_thread(
                        update_product,
                        product_id,
                        new_tags if tags_changed else None,
                        new_category_id,
                    )
                    if result.get("errors"):
                        errors.extend([f"{product_id}: {e}" for e in result["errors"]])
                    else:
                        product_updated = True
                        tag_updates[product_id] = new_tags
                        with products_lock:
                            if cached:
                                cached["tags"] = new_tags
                                if new_category_id:
                                    cached["category_id"] = new_category_id

                if metafield_values:
                    mf_result = await asyncio.to_thread(
                        set_product_category_metafields, product_id, metafield_values
                    )
                    if mf_result.get("errors"):
                        errors.extend([f"{product_id} metafields: {e}" for e in mf_result["errors"]])
                    else:
                        product_updated = True

                if product_updated:
                    updated += 1
                    succeeded_ids.append(product_id)

            return jsonify({
                "updated": updated,
                "total": len(product_ids),
                "errors": errors,
                "tag_updates": tag_updates,
                "succeeded_ids": succeeded_ids,
            })

        except Exception as exc:
            current_app.logger.exception("Failed to apply product remapping", exc_info=exc)
            return jsonify({"error": "Failed to apply remapping."}), 500

    @application.route("/mail-tools/")
    @oidc.require_login
    def mail_tools() -> str:
        """Render the mail tools page."""
        context = get_user_context()
        return render_template(
            "mail_tools.html",
            **context,
            active_page="mail_tools",
        )

    @application.post("/mail-tools/lookup-order/")
    async def lookup_order() -> Any:
        """Look up a Shopify order by number and return customer info."""
        try:
            payload = request.get_json(silent=True) or {}
            order_number = str(payload.get("order_number", "")).strip()

            customer, error = await _resolve_order_customer(order_number)
            if error:
                return error

            return jsonify(customer)
        except Exception as exc:
            current_app.logger.exception("Failed to look up order", exc_info=exc)
            return jsonify({"error": "Failed to look up order."}), 500

    @application.post("/mail-tools/send-missed-pickup/")
    async def send_missed_pickup() -> Any:
        """Look up the order, then send the missed-pickup email."""
        try:
            payload = request.get_json(silent=True) or {}
            order_number = str(payload.get("order_number", "")).strip()

            customer, error = await _resolve_order_customer(order_number)
            if error:
                return error

            email = customer["email"]

            if not email:
                return jsonify({"error": "Customer has no email address on file."}), 400

            success, message = await asyncio.to_thread(
                send_missed_pickup_email,
                customer["first_name"],
                email,
                customer["order_number"],
            )

            if success:
                return jsonify({"message": message})
            else:
                return jsonify({"error": message}), 500
        except Exception as exc:
            current_app.logger.exception("Failed to send missed-pickup email", exc_info=exc)
            return jsonify({"error": "Failed to send email."}), 500

    @application.get("/mail-tools/templates/")
    def list_email_templates() -> Any:
        """Return every saved email template plus the variables they may use."""
        try:
            return jsonify(
                {
                    "templates": load_email_templates(_ordering_db_path),
                    "variables": list(TEMPLATE_VARIABLES),
                }
            )
        except Exception as exc:
            current_app.logger.exception("Failed to load email templates", exc_info=exc)
            return jsonify({"error": "Failed to load email templates."}), 500

    @application.post("/mail-tools/templates/")
    def save_email_template_route() -> Any:
        """Create or update a named email template."""
        try:
            payload = request.get_json(silent=True) or {}
            name = str(payload.get("name", "")).strip()
            subject = str(payload.get("subject", "")).strip()
            body = str(payload.get("body", ""))

            if not name:
                return jsonify({"error": "A template name is required."}), 400
            if not subject:
                return jsonify({"error": "A subject is required."}), 400
            if not body.strip():
                return jsonify({"error": "A body is required."}), 400

            # Reject unrenderable templates now rather than at send time.
            try:
                render_email_template(
                    subject, body, {name: "" for name in TEMPLATE_VARIABLES}
                )
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400

            save_email_template(_ordering_db_path, name, subject, body)
            return jsonify({"status": "saved", "name": name})
        except Exception as exc:
            current_app.logger.exception("Failed to save email template", exc_info=exc)
            return jsonify({"error": "Failed to save email template."}), 500

    @application.delete("/mail-tools/templates/")
    def delete_email_template_route() -> Any:
        """Delete a named email template."""
        try:
            name = (request.args.get("name") or "").strip()
            if not name:
                return jsonify({"error": "A template name is required."}), 400
            if not delete_email_template(_ordering_db_path, name):
                return jsonify({"error": "No template exists with that name."}), 404
            return jsonify({"status": "deleted", "name": name})
        except Exception as exc:
            current_app.logger.exception("Failed to delete email template", exc_info=exc)
            return jsonify({"error": "Failed to delete email template."}), 500

    @application.post("/mail-tools/preview-template/")
    async def preview_template() -> Any:
        """Render a template against a real order so it can be checked first."""
        try:
            payload = request.get_json(silent=True) or {}
            order_number = str(payload.get("order_number", "")).strip()
            subject = str(payload.get("subject", ""))
            body = str(payload.get("body", ""))

            customer, error = await _resolve_order_customer(order_number)
            if error:
                return error

            try:
                rendered_subject, html_body = render_email_template(
                    subject, body, _template_variables(customer)
                )
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400

            return jsonify(
                {
                    "to": customer["email"],
                    "subject": rendered_subject,
                    "html": html_body,
                }
            )
        except Exception as exc:
            current_app.logger.exception("Failed to preview email template", exc_info=exc)
            return jsonify({"error": "Failed to preview email template."}), 500

    @application.post("/mail-tools/send-template/")
    async def send_template() -> Any:
        """Look up the order, render the template and mail it to the customer."""
        try:
            payload = request.get_json(silent=True) or {}
            order_number = str(payload.get("order_number", "")).strip()
            subject = str(payload.get("subject", "")).strip()
            body = str(payload.get("body", ""))

            if not subject:
                return jsonify({"error": "A subject is required."}), 400
            if not body.strip():
                return jsonify({"error": "A body is required."}), 400

            customer, error = await _resolve_order_customer(order_number)
            if error:
                return error

            variables = _template_variables(customer)

            if not variables["email"]:
                return jsonify({"error": "Customer has no email address on file."}), 400

            success, message = await asyncio.to_thread(
                send_template_email, subject, body, variables
            )

            if success:
                return jsonify({"message": message})
            return jsonify({"error": message}), 500
        except Exception as exc:
            current_app.logger.exception("Failed to send template email", exc_info=exc)
            return jsonify({"error": "Failed to send email."}), 500

    # Every route is now registered: refuse to start if any of them lacks an
    # access policy, so a new endpoint can never ship unprotected.
    security.audit_routes(application, ROUTE_POLICIES)

    return application


def _fetch_cleanup_variants():
    """Fetch sold-out and archived variants from Shopify (helper for async execution)."""
    from gql import gql
    
    _gql_execute = shopify_module._execute
    sold_out_skus = []
    archived_skus = []
    
    # First, fetch active products with sold-out variants
    has_next_page = True
    after_cursor = None

    while has_next_page:
        query = gql("""
        query getActiveProducts($after: String) {
            products(first: 50, query: "status:active", after: $after) {
                edges {
                    node {
                        id
                        variants(first: 100) {
                            edges {
                                node {
                                    sku
                                    inventoryPolicy
                                    inventoryQuantity
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
        
        variables = {"after": after_cursor}
        result = _gql_execute(query, variable_values=variables)
        products = result.get("products", {}).get("edges", [])
        
        for product in products:
            product_node = product["node"]
            product_id = product_node["id"]
            
            # Paginate through variants
            variants_has_next = True
            variants_after = None
            first_page_variants = product_node["variants"]["edges"]
            first_page_info = product_node["variants"]["pageInfo"]
            
            # Process first page of variants
            for variant in first_page_variants:
                variant_node = variant["node"]
                if not variant_node.get("sku"):
                    continue
                sku = variant_node.get("sku", "").strip()
                inventory_policy = variant_node.get("inventoryPolicy")
                inventory_quantity = variant_node.get("inventoryQuantity", 0)
                
                if sku and inventory_policy == "DENY" and inventory_quantity == 0:
                    sold_out_skus.append(sku)
            
            # Fetch additional pages if needed
            variants_has_next = first_page_info.get("hasNextPage", False)
            variants_after = first_page_info.get("endCursor")
            
            while variants_has_next:
                variants_query = gql("""
                query getProductVariants($productId: ID!, $after: String) {
                    product(id: $productId) {
                        variants(first: 100, after: $after) {
                            edges {
                                node {
                                    sku
                                    inventoryPolicy
                                    inventoryQuantity
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                }
                """)
                
                variants_variables = {"productId": product_id, "after": variants_after}
                variants_result = _gql_execute(variants_query, variable_values=variants_variables)
                variant_edges = variants_result.get("product", {}).get("variants", {}).get("edges", [])
                
                for variant in variant_edges:
                    variant_node = variant["node"]
                    if not variant_node.get("sku"):
                        continue
                    sku = variant_node.get("sku", "").strip()
                    inventory_policy = variant_node.get("inventoryPolicy")
                    inventory_quantity = variant_node.get("inventoryQuantity", 0)
                    
                    if sku and inventory_policy == "DENY" and inventory_quantity == 0:
                        sold_out_skus.append(sku)
                
                variants_page_info = variants_result.get("product", {}).get("variants", {}).get("pageInfo", {})
                variants_has_next = variants_page_info.get("hasNextPage", False)
                variants_after = variants_page_info.get("endCursor")
        
        page_info = result.get("products", {}).get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        after_cursor = page_info.get("endCursor", None)
    
    # Now fetch archived products
    has_next_page = True
    after_cursor = None
    
    while has_next_page:
        query = gql("""
        query getArchivedProducts($after: String) {
            products(first: 50, query: "status:archived", after: $after) {
                edges {
                    node {
                        id
                        variants(first: 100) {
                            edges {
                                node {
                                    sku
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
        
        variables = {"after": after_cursor}
        result = _gql_execute(query, variable_values=variables)
        products = result.get("products", {}).get("edges", [])
        
        for product in products:
            product_node = product["node"]
            product_id = product_node["id"]
            
            # Paginate through variants
            variants_has_next = True
            variants_after = None
            first_page_variants = product_node["variants"]["edges"]
            first_page_info = product_node["variants"]["pageInfo"]
            
            # Process first page of variants
            for variant in first_page_variants:
                variant_node = variant["node"]
                if not variant_node.get("sku"):
                    continue
                sku = variant_node.get("sku", "").strip()
                if sku:
                    archived_skus.append(sku)
            
            # Fetch additional pages if needed
            variants_has_next = first_page_info.get("hasNextPage", False)
            variants_after = first_page_info.get("endCursor")
            
            while variants_has_next:
                variants_query = gql("""
                query getProductVariants($productId: ID!, $after: String) {
                    product(id: $productId) {
                        variants(first: 100, after: $after) {
                            edges {
                                node {
                                    sku
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                }
                """)
                
                variants_variables = {"productId": product_id, "after": variants_after}
                variants_result = _gql_execute(variants_query, variable_values=variants_variables)
                variant_edges = variants_result.get("product", {}).get("variants", {}).get("edges", [])
                
                for variant in variant_edges:
                    variant_node = variant["node"]
                    sku = variant_node.get("sku", "").strip()
                    if sku:
                        archived_skus.append(sku)
                
                variants_page_info = variants_result.get("product", {}).get("variants", {}).get("pageInfo", {})
                variants_has_next = variants_page_info.get("hasNextPage", False)
                variants_after = variants_page_info.get("endCursor")
        
        page_info = result.get("products", {}).get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        after_cursor = page_info.get("endCursor", None)
    
    return {
        'sold_out': sold_out_skus,
        'archived': archived_skus
    }

if __name__ == "__main__":
    # One-off migration of YAML ordering config into the DB, without booting the
    # full server:  python app.py migrate-ordering-config [yaml_path]
    if len(sys.argv) > 1 and sys.argv[1] == "migrate-ordering-config":
        result = migrate_yaml_to_db(yaml_path=sys.argv[2] if len(sys.argv) > 2 else None)
        print(json.dumps(result, indent=2))
        sys.exit(0)

    app = create_app()

    # Ensure the GQL session is closed cleanly on interpreter exit
    # (covers normal shutdown and SIGTERM from systemd).
    atexit.register(shutdown_gql_session)

    def _handle_sigterm(signum, frame):
        """Translate SIGTERM into SystemExit so atexit handlers run."""
        logger.info("Received SIGTERM – shutting down")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_sigterm)

    # Bind to loopback by default: the service is reached through the
    # authenticating reverse proxy, never directly.  Set WAITRESS_HOST to
    # override when the proxy lives on another machine.
    serve(
        app,
        host=os.getenv("WAITRESS_HOST", "127.0.0.1"),
        port=int(os.getenv("WAITRESS_PORT", 8000)),
        url_scheme="https",
        max_request_body_size=security.max_request_bytes(),
    )
