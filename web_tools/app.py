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
from datetime import timedelta
from waitress import serve
from flask import Flask, current_app, g, jsonify, render_template, request, redirect, url_for
from flask_oidc import OpenIDConnect
from flask_session import Session

from shopify import fetch_missing_inventory as fetch_purchase_order_data, calculate_brand_inventory_value

BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "purchase_orders.db"

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

    application.teardown_appcontext(close_db)

    @application.route("/")
    @oidc.require_login
    def index() -> str:
        return redirect(url_for("purchase_orders"))

    @application.route("/purchase-orders/")
    @oidc.require_login
    def purchase_orders() -> str:
        """Render the purchase orders grid."""
        user_info = oidc.user_getinfo(['name', 'email', 'preferred_username'])
        user_name = user_info.get('name', user_info.get('preferred_username', 'User'))
        return render_template("purchase_orders.html", purchase_orders=None, user_name=user_name)

    @application.get("/purchase-orders/data/")
    async def purchase_order_data() -> Any:
        """Fetch purchase order data asynchronously."""
        try:
            data = await asyncio.to_thread(fetch_purchase_order_data)
        except Exception as exc:  # pragma: no cover - defensive logging
            current_app.logger.exception("Failed to load purchase orders", exc_info=exc)
            return jsonify({"error": "Failed to load purchase orders."}), 500
        return jsonify(data)

    @application.get("/purchase-orders/configurations/")
    def list_configurations() -> Any:
        """List saved grid configurations."""
        db = get_db()
        rows = db.execute(
            """
            SELECT id, name, columns, filters, column_labels, sort_model
            FROM purchase_order_configurations
            ORDER BY LOWER(name)
            """
        ).fetchall()
        configs = [
            {
                "id": row["id"],
                "name": row["name"],
                "columns": json.loads(row["columns"]),
                "filters": json.loads(row["filters"]),
                "columnLabels": json.loads(row["column_labels"]),
                "sortModel": json.loads(row["sort_model"]),
            }
            for row in rows
        ]
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

        db = get_db()
        payload_tuple = (
            name,
            json.dumps(columns),
            json.dumps(filters),
            json.dumps(column_labels),
            json.dumps(sort_model),
        )

        db.execute(
            """
            INSERT INTO purchase_order_configurations (name, columns, filters, column_labels, sort_model)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                columns=excluded.columns,
                filters=excluded.filters,
                column_labels=excluded.column_labels,
                sort_model=excluded.sort_model,
                created_at=CURRENT_TIMESTAMP
            """,
            payload_tuple,
        )
        db.commit()

        row = db.execute(
            """
            SELECT id, name, columns, filters, column_labels, sort_model
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
        user_info = oidc.user_getinfo(['name', 'email', 'preferred_username'])
        user_name = user_info.get('name', user_info.get('preferred_username', 'User'))
        return render_template("inventory_tools.html", user_name=user_name)

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

    return application

if __name__ == "__main__":
    app = create_app()
    serve(app, host="0.0.0.0", port=int(os.getenv("WAITRESS_PORT", 8000)), url_scheme='https')
