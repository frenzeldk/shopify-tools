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
from pathlib import Path
from typing import Any
from datetime import datetime, timedelta, timezone
from waitress import serve
from flask import Flask, current_app, g, jsonify, render_template, request, redirect, url_for, session
from flask_oidc import OpenIDConnect
from flask_session import Session
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from shopify import fetch_missing_inventory as fetch_purchase_order_data, calculate_brand_inventory_value
from shipmondo import (
    fetch_all_shipmondo_items,
    clear_bin_location,
    batch_update_bins_with_regex,
    apply_batch_update
)
import shopify as shopify_module
import threading

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


def fetch_and_cache_shipmondo_items():
    """Fetch all Shipmondo items and update the global cache."""
    import logging
    logger = logging.getLogger(__name__)
    
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
    
    Session(application)
    oidc = OpenIDConnect(application)
    
    with application.app_context():
        init_db()
    
    # Initialize background scheduler for Shipmondo cache updates
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=fetch_and_cache_shipmondo_items,
        trigger=CronTrigger(hour=4, minute=0),  # Daily at 4:00 UTC
        id='shipmondo_cache_update',
        name='Update Shipmondo cache',
        replace_existing=True
    )
    
    # Initial fetch on startup (run in background, non-blocking)
    scheduler.add_job(
        func=fetch_and_cache_shipmondo_items,
        id='shipmondo_initial_fetch',
        name='Initial Shipmondo cache fetch'
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
        payload_tuple = (
            name,
            json.dumps(columns),
            json.dumps(filters),
            json.dumps(column_labels),
            json.dumps(sort_model),
            json.dumps(custom_columns),
            json.dumps(column_widths),
        )

        db.execute(
            """
            INSERT INTO purchase_order_configurations (name, columns, filters, column_labels, sort_model, custom_columns, column_widths)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                columns=excluded.columns,
                filters=excluded.filters,
                column_labels=excluded.column_labels,
                sort_model=excluded.sort_model,
                custom_columns=excluded.custom_columns,
                column_widths=excluded.column_widths,
                created_at=CURRENT_TIMESTAMP
            """,
            payload_tuple,
        )
        db.commit()

        row = db.execute(
            """
            SELECT id, name, columns, filters, column_labels, sort_model, custom_columns, column_widths
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
            "customColumns": json.loads(row["custom_columns"]),
            "columnWidths": json.loads(row["column_widths"]),
        }
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
        """Calculate the total inventory value for a specific brand."""
        try:
            payload = request.get_json(silent=True) or {}
            brand_name = str(payload.get("brand", "")).strip()
            
            if not brand_name:
                return jsonify({"error": "Brand name is required."}), 400
            
            total_value = await asyncio.to_thread(calculate_brand_inventory_value, brand_name)
            
            return jsonify({"brand": brand_name, "total_value": total_value})
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
