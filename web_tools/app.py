#!/opt/shopify-python/bin/python3
"""
Web tools Flask application for managing purchase orders.
Does not push POs to Shopify, as this is not supported by the Shopify API.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import os
import sys
import logging
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta, timezone
from waitress import serve
from flask import Flask, current_app, g, jsonify, render_template, request, redirect, url_for, session
from flask_oidc import OpenIDConnect
from flask_session import Session
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from shopify import (
    fetch_missing_inventory as fetch_purchase_order_data,
    calculate_brand_inventory_value,
    update_variant_barcode,
    fetch_order_customer,
    parse_vendor_csv,
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
    fetch_category_metafields,
    create_shopify_product,
    set_product_category_metafields,
    detect_product_options,
    create_product_options,
    fetch_metaobject_type_details,
    fetch_metaobjects_for_definition,
    fetch_product_images,
    add_product_images,
    reorder_product_images,
    delete_product_image,
)
from chatgpt import fetch_and_translate_vendor_page
from shipmondo import (
    fetch_all_shipmondo_items,
    clear_bin_location,
    batch_update_bins_with_regex,
    apply_batch_update,
    update_barcode
)
from microsoft365 import send_missed_pickup_email
import shopify as shopify_module
import threading

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


def refresh_all_shopify_caches():
    """Run all Shopify-dependent cache refreshes sequentially.

    Shopify's API rate-limits concurrent requests, so we must avoid
    firing multiple heavy fetches in parallel.  This wrapper is used
    both at startup and for the daily scheduled refresh.
    """
    logger.info("refresh_all_shopify_caches: starting sequential refresh")
    fetch_and_cache_taxonomy()
    fetch_and_cache_product_tags()
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
        conn.commit()


def create_app() -> Flask:
    """Application factory for the web tools service."""
    application = Flask(__name__, template_folder="templates", static_folder="static")
    application.config.setdefault("DATABASE", str(DATABASE_PATH))
    application.config.setdefault("OIDC_CLIENT_SECRETS", str(BASE_DIR / "client_secrets.json"))
    application.config['SECRET_KEY'] = os.environ.get("FLASK_SECRET_KEY")
    application.config["SESSION_TYPE"] = "filesystem"
    application.config['SESSION_PERMANENT'] = True
    application.config['SESSION_PERMANENT_LIFETIME'] = timedelta(days=7)
    
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
    
    with application.app_context():
        init_db()
    
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
    }

    @application.post("/product-tools/compare/")
    async def product_tools_compare() -> Any:
        """Compare an uploaded vendor CSV against Shopify."""
        try:
            vendor = (request.form.get("vendor") or "").strip()
            csv_file = request.files.get("csv_file")

            if not vendor:
                return jsonify({"error": "Vendor is required."}), 400

            if vendor not in VENDOR_SHOPIFY_BRANDS:
                return jsonify({"error": f"Unsupported vendor: {vendor}"}), 400

            if not csv_file or csv_file.filename == "":
                return jsonify({"error": "A CSV file is required."}), 400

            # Read and parse CSV
            csv_content = csv_file.read().decode("utf-8-sig")  # handle BOM
            vendor_products = parse_vendor_csv(csv_content)
            current_app.logger.info(
                f"Parsed {len(vendor_products)} rows from uploaded CSV"
            )

            if not vendor_products:
                return jsonify({"error": "No valid rows found in CSV. Check the file format."}), 400

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

            if not metaobject_type:
                return jsonify({"error": "metaobject_type is required."}), 400

            result = fetch_metaobject_type_details(metaobject_type)
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
            if not product_id:
                return jsonify({"error": "product_id is required."}), 400
            if not image_urls:
                return jsonify({"error": "image_urls is required."}), 400
            result = await asyncio.to_thread(add_product_images, product_id, image_urls)
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
            from shopify import create_staged_uploads
            targets = await asyncio.to_thread(create_staged_uploads, files)
            return jsonify({"targets": targets})
        except Exception as exc:
            current_app.logger.exception("Failed to create staged uploads", exc_info=exc)
            return jsonify({"error": "Failed to create staged uploads."}), 500

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

            if not order_number:
                return jsonify({"error": "Order number is required."}), 400

            # Ensure the order number starts with '#'
            if not order_number.startswith("#"):
                order_number = f"#{order_number}"

            customer = await asyncio.to_thread(fetch_order_customer, order_number)

            if customer is None:
                return jsonify({"error": f"Order {order_number} not found or has no customer."}), 404

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

            if not order_number:
                return jsonify({"error": "Order number is required."}), 400

            if not order_number.startswith("#"):
                order_number = f"#{order_number}"

            customer = await asyncio.to_thread(fetch_order_customer, order_number)

            if customer is None:
                return jsonify({"error": f"Order {order_number} not found or has no customer."}), 404

            first_name = customer["first_name"]
            email = customer["email"]

            if not email:
                return jsonify({"error": "Customer has no email address on file."}), 400

            success, message = await asyncio.to_thread(
                send_missed_pickup_email, first_name, email, order_number
            )

            if success:
                return jsonify({"message": message})
            else:
                return jsonify({"error": message}), 500
        except Exception as exc:
            current_app.logger.exception("Failed to send missed-pickup email", exc_info=exc)
            return jsonify({"error": "Failed to send email."}), 500

    return application


def _fetch_cleanup_variants():
    """Fetch sold-out and archived variants from Shopify (helper for async execution)."""
    from gql import gql
    
    gql_client = shopify_module.__gql_client__
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
        result = gql_client.execute(query, variable_values=variables)
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
                variants_result = gql_client.execute(variants_query, variable_values=variants_variables)
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
        result = gql_client.execute(query, variable_values=variables)
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
                variants_result = gql_client.execute(variants_query, variable_values=variants_variables)
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
    app = create_app()
    serve(app, host="0.0.0.0", port=int(os.getenv("WAITRESS_PORT", 8000)), url_scheme='https')
