"""Local inventory database backing the Counting page.

The counting tool runs off this table and nothing else: a count sheet is built
from local rows, and the quantity on a row can be corrected in place.  Neither
reading nor editing touches Shopify or Shipmondo, so a stock count never waits
on an API and never writes a half-counted figure back to the shop.

The table is filled by one explicit action — the *Fetch Full Inventory* button,
which replaces every row (see :func:`replace_all`).  That is the only moment
this module talks to the rest of the world, and it is never triggered on
startup: an automatic refresh would silently discard counts in progress.

Each row keeps two quantities: ``on_hand`` is the working figure the sheet
shows and the user edits, ``synced_on_hand`` is what the last fetch brought
back.  A row where they differ has been changed locally, which is what the
Counting page reports and what the refresh warning counts.

The store lives in its own SQLite file so a full replace does not block the
configuration database, and runs in WAL mode so a count sheet can still be
built while a fetch is writing.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable

logger = logging.getLogger(__name__)

#: Bound on a manually entered quantity.  A stock count is a shelf count, so
#: anything beyond this is a typo (a stray digit) rather than a real figure.
MAX_LOCAL_QUANTITY = 1_000_000

#: Bound on a manually entered unit cost, for the same reason.
MAX_LOCAL_COST = 1_000_000.0

#: Longest text a hand-typed field may carry.
MAX_FIELD_CHARS = 200

#: Most data rows one CSV import may carry.  An import replaces the whole
#: database, so the ceiling has to clear a full catalogue; the upload size limit
#: bites long before this does.
MAX_IMPORT_ROWS = 50_000

#: Most row errors reported back from an import.  A file with thousands of bad
#: rows has a structural problem, and listing them all helps nobody.
MAX_IMPORT_ERRORS = 50

# Accepted CSV column headings, per field.  Matching is case- and
# punctuation-insensitive (see _normalise_heading), and the names this page's
# own export writes are included so a file derived from it needs no renaming.
_CSV_HEADINGS: dict[str, tuple[str, ...]] = {
    "vendor": ("vendor", "brand"),
    "product_title": ("product", "product name", "product title", "name"),
    "variant_title": ("variant", "variant title", "variation"),
    "sku": ("sku",),
    "barcode": ("barcode", "ean", "gtin"),
    "on_hand": ("amount", "quantity", "qty", "count", "on hand", "stock"),
    "unit_cost": (
        "cost", "cost per item", "cost pr item", "unit cost", "cost price",
    ),
    "bin": ("bin", "bin location", "location"),
}

#: Columns a file must have; the rest may be left out.
_REQUIRED_CSV_FIELDS = (
    "vendor", "product_title", "variant_title", "sku", "on_hand", "unit_cost",
)

#: Delimiters an uploaded file may use.  Danish spreadsheets write `;`, which
#: is also what this page's export uses; `,` and tab come from everything else.
_CSV_DELIMITERS = (";", ",", "\t")

# Where a row came from.  Three origins, because they mean different things on a
# count sheet: SHOPIFY rows are the catalogue, BIN_ONLY rows are bins Shipmondo
# still holds for a SKU Shopify no longer knows (stale data worth reporting),
# and LOCAL rows were typed in here for stock the shop has no record of.
SOURCE_SHOPIFY = "shopify"
SOURCE_BIN_ONLY = "bin_only"
SOURCE_LOCAL = "local"

#: Columns of ``local_inventory``, in insert order.
_COLUMNS = (
    "sku",
    "product_title",
    "variant_title",
    "vendor",
    "barcode",
    "bin",
    "tracked",
    "source",
    "unit_cost",
    "on_hand",
    "available",
    "committed",
    "synced_on_hand",
    "updated_at",
)

# "Edited here" means a fetched quantity that no longer matches what the fetch
# brought back.  A hand-added row is excluded: it has no fetched figure to
# differ from, and it is already reported as added.  Defined once in each
# language so the status summary and the row builder cannot disagree.
_MODIFIED_SQL = f"(on_hand != synced_on_hand AND source != '{SOURCE_LOCAL}')"


def _is_locally_modified(row: sqlite3.Row) -> bool:
    return row["on_hand"] != row["synced_on_hand"] and row["source"] != SOURCE_LOCAL


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # A full replace writes tens of thousands of rows; WAL keeps count sheets
    # readable throughout, and a busy timeout absorbs the commit itself.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(db_path: str) -> None:
    """Create the inventory table and its metadata table if they are absent."""
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS local_inventory (
                    sku TEXT PRIMARY KEY,
                    product_title TEXT NOT NULL DEFAULT '',
                    variant_title TEXT NOT NULL DEFAULT '',
                    vendor TEXT NOT NULL DEFAULT '',
                    barcode TEXT NOT NULL DEFAULT '',
                    bin TEXT NOT NULL DEFAULT '',
                    tracked INTEGER NOT NULL DEFAULT 1,
                    source TEXT NOT NULL DEFAULT 'shopify',
                    unit_cost REAL,
                    on_hand INTEGER NOT NULL DEFAULT 0,
                    available INTEGER NOT NULL DEFAULT 0,
                    committed INTEGER NOT NULL DEFAULT 0,
                    synced_on_hand INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            # Columns added after the first version of this table.
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(local_inventory)")
            }
            # Nullable, so no backfill is needed: the next fetch fills it, and
            # until then a cost export shows the cost as unknown.
            if "unit_cost" not in columns:
                conn.execute("ALTER TABLE local_inventory ADD COLUMN unit_cost REAL")
            if "source" not in columns:
                conn.execute(
                    "ALTER TABLE local_inventory ADD COLUMN source TEXT NOT NULL "
                    f"DEFAULT '{SOURCE_SHOPIFY}'"
                )
                # It replaces an in_shopify flag, whose two states map onto the
                # first two origins.
                if "in_shopify" in columns:
                    conn.execute(
                        "UPDATE local_inventory SET source = ? WHERE in_shopify = 0",
                        (SOURCE_BIN_ONLY,),
                    )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS local_inventory_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
    finally:
        conn.close()


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO local_inventory_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def _meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM local_inventory_meta WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


def merge_rows(
    variants: Iterable[dict], bins: dict[str, dict] | None = None
) -> list[dict]:
    """Fold Shopify variants and Shipmondo bins into rows for the table.

    ``variants`` are :func:`shopify.fetch_all_on_hand` rows; ``bins`` maps SKU to
    ``{"bin", "name", "barcode"}`` from the Shipmondo cache.

    A SKU Shipmondo has a bin for but Shopify no longer knows is kept, with zero
    quantities and source :data:`SOURCE_BIN_ONLY`, so the count sheet can report
    the stale bin instead of quietly dropping the shelf.  Unbinned
    Shipmondo-only SKUs are left out: nothing can be counted without a bin to
    walk to.
    """
    bins = bins or {}
    rows: dict[str, dict] = {}

    for variant in variants:
        sku = str(variant.get("sku") or "").strip()
        # Shopify allows variants without a SKU, and the same SKU on two
        # variants; neither can be counted per-SKU, so the first wins.
        if not sku or sku in rows:
            continue
        bin_data = bins.get(sku) or {}
        on_hand = int(variant.get("on_hand") or 0)
        rows[sku] = {
            "sku": sku,
            "product_title": variant.get("product_title") or "",
            "variant_title": variant.get("variant_title") or "",
            "vendor": variant.get("vendor") or "",
            "barcode": variant.get("barcode") or bin_data.get("barcode") or "",
            "bin": str(bin_data.get("bin") or "").strip(),
            "tracked": bool(variant.get("tracked")),
            "source": SOURCE_SHOPIFY,
            "unit_cost": variant.get("unit_cost"),
            "on_hand": on_hand,
            "available": int(variant.get("available") or 0),
            "committed": int(variant.get("committed") or 0),
            "synced_on_hand": on_hand,
        }

    for sku, bin_data in bins.items():
        sku = str(sku or "").strip()
        if not sku or sku in rows or not str(bin_data.get("bin") or "").strip():
            continue
        rows[sku] = {
            "sku": sku,
            "product_title": bin_data.get("name") or "",
            "variant_title": "",
            "vendor": "",
            "barcode": bin_data.get("barcode") or "",
            "bin": str(bin_data.get("bin")).strip(),
            "tracked": False,
            "source": SOURCE_BIN_ONLY,
            # Shopify no longer knows the item, so there is no cost to report.
            "unit_cost": None,
            "on_hand": 0,
            "available": 0,
            "committed": 0,
            "synced_on_hand": 0,
        }

    return list(rows.values())


def update_bins(db_path: str, bins: dict[str, dict]) -> dict:
    """Apply Shipmondo's bins to rows already in the table, and nothing else.

    Bins live in Shipmondo — Shopify holds none — so they move independently of
    the catalogue: a shelf can be re-organised without any product data
    changing.  This writes the ``bin`` column of rows whose bin actually
    differs and leaves every other column alone, so counted quantities, costs
    and hand-added products survive a bin update untouched.

    ``bins`` maps SKU to ``{"bin": …}``, as :func:`merge_rows` takes.  Only SKUs
    Shipmondo reports are considered: a row Shipmondo knows nothing about keeps
    the bin it has, which is what protects hand-added products.  A SKU whose
    Shipmondo bin has been emptied has its local bin cleared too — that is a
    differing bin like any other — and a SKU Shipmondo has a bin for but the
    table does not is reported rather than half-created, since a row needs
    product data and quantities that only a full fetch can bring.

    Returns ``{"updated", "cleared", "unchanged", "missing_locally",
    "updated_at"}``.
    """
    now = _now()
    conn = _connect(db_path)
    try:
        with conn:
            current = {
                row["sku"]: row["bin"]
                for row in conn.execute("SELECT sku, bin FROM local_inventory")
            }
            changes: list[tuple] = []
            updated = cleared = unchanged = missing_locally = 0

            for sku, bin_data in bins.items():
                sku = str(sku or "").strip()
                if not sku:
                    continue
                if sku not in current:
                    missing_locally += 1
                    continue
                new_bin = str((bin_data or {}).get("bin") or "").strip()
                if new_bin == current[sku]:
                    unchanged += 1
                    continue
                changes.append((new_bin, now, sku))
                if new_bin:
                    updated += 1
                else:
                    cleared += 1

            conn.executemany(
                "UPDATE local_inventory SET bin = ?, updated_at = ? WHERE sku = ?",
                changes,
            )
            _set_meta(conn, "bins_updated_at", now)
    finally:
        conn.close()

    logger.info(
        "local_inventory.update_bins: %d bins changed, %d cleared, %d unchanged, "
        "%d Shipmondo SKUs not in the database",
        updated, cleared, unchanged, missing_locally,
    )
    return {
        "updated": updated,
        "cleared": cleared,
        "unchanged": unchanged,
        "missing_locally": missing_locally,
        "updated_at": now,
    }


def replace_all(db_path: str, rows: Iterable[dict], *, source: str = "shopify") -> dict:
    """Replace every row in the table with ``rows``.

    This is destructive by design: it is what the *Fetch Full Inventory* button
    does, and locally edited quantities are part of what it overwrites.  The
    delete and the inserts share one transaction, so a count sheet built while
    the fetch runs sees either the whole old table or the whole new one.

    Returns ``{"skus", "units", "with_bins", "synced_at"}``.
    """
    now = _now()
    prepared = [
        (
            row["sku"],
            row.get("product_title") or "",
            row.get("variant_title") or "",
            row.get("vendor") or "",
            row.get("barcode") or "",
            row.get("bin") or "",
            1 if row.get("tracked") else 0,
            row.get("source") or SOURCE_SHOPIFY,
            None if row.get("unit_cost") is None else float(row["unit_cost"]),
            int(row.get("on_hand") or 0),
            int(row.get("available") or 0),
            int(row.get("committed") or 0),
            int(row.get("synced_on_hand", row.get("on_hand") or 0)),
            now,
        )
        for row in rows
    ]

    placeholders = ", ".join("?" * len(_COLUMNS))
    conn = _connect(db_path)
    try:
        with conn:
            conn.execute("DELETE FROM local_inventory")
            conn.executemany(
                f"INSERT INTO local_inventory ({', '.join(_COLUMNS)}) "
                f"VALUES ({placeholders})",
                prepared,
            )
            _set_meta(conn, "last_synced", now)
            _set_meta(conn, "last_sync_source", source)
            # The fetch brings bins with it, so it is also the moment the bins
            # were last known to be right.
            _set_meta(conn, "bins_updated_at", now)
    finally:
        conn.close()

    units = sum(row[_COLUMNS.index("on_hand")] for row in prepared)
    with_bins = sum(1 for row in prepared if row[_COLUMNS.index("bin")])
    logger.info(
        "local_inventory.replace_all: stored %d SKUs (%d with bins, %d units)",
        len(prepared), with_bins, units,
    )
    return {
        "skus": len(prepared),
        "units": units,
        "with_bins": with_bins,
        "synced_at": now,
    }


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "sku": row["sku"],
        "product_title": row["product_title"],
        "variant_title": row["variant_title"],
        "vendor": row["vendor"],
        "barcode": row["barcode"],
        "bin": row["bin"],
        "tracked": bool(row["tracked"]),
        "source": row["source"],
        "unit_cost": row["unit_cost"],
        "on_hand": row["on_hand"],
        "available": row["available"],
        "committed": row["committed"],
        "synced_on_hand": row["synced_on_hand"],
        "locally_modified": _is_locally_modified(row),
        "updated_at": row["updated_at"],
    }


def load_all(db_path: str) -> dict[str, dict]:
    """Return every row, keyed by SKU."""
    conn = _connect(db_path)
    try:
        rows = conn.execute("SELECT * FROM local_inventory").fetchall()
    except sqlite3.OperationalError:
        # Table not created yet: an empty store and a missing one mean the same
        # thing to a caller — there is nothing to count until a fetch is run.
        return {}
    finally:
        conn.close()
    return {row["sku"]: _row_to_dict(row) for row in rows}


def get(db_path: str, sku: str) -> dict | None:
    """Return one row, or ``None`` when the SKU is not in the local database."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM local_inventory WHERE sku = ?", (sku,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()
    return _row_to_dict(row) if row else None


def set_on_hand(db_path: str, sku: str, on_hand: int) -> dict | None:
    """Set a row's on-hand quantity locally and return the updated row.

    Local only: Shopify is not told, which is the point — a counter corrects
    the sheet, and reconciling the shop is a separate, deliberate step.
    ``available`` is recomputed as ``on_hand - committed`` to preserve Shopify's
    own invariant, so a corrected count does not imply stock that is already
    promised to an open order.

    Returns ``None`` when the SKU is not in the local database.
    """
    conn = _connect(db_path)
    try:
        with conn:
            row = conn.execute(
                "SELECT * FROM local_inventory WHERE sku = ?", (sku,)
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE local_inventory "
                "SET on_hand = ?, available = ?, updated_at = ? WHERE sku = ?",
                (on_hand, on_hand - row["committed"], _now(), sku),
            )
            updated = conn.execute(
                "SELECT * FROM local_inventory WHERE sku = ?", (sku,)
            ).fetchone()
    finally:
        conn.close()
    return _row_to_dict(updated)


class ProductError(ValueError):
    """A hand-typed product the store refuses, with a message for the user."""


def _required_text(value: Any, label: str) -> str:
    text = ("" if value is None else str(value)).strip()
    if not text:
        raise ProductError(f"{label} is required.")
    if len(text) > MAX_FIELD_CHARS:
        raise ProductError(f"{label} may be at most {MAX_FIELD_CHARS} characters.")
    return text


def _optional_text(value: Any, label: str) -> str:
    text = ("" if value is None else str(value)).strip()
    if len(text) > MAX_FIELD_CHARS:
        raise ProductError(f"{label} may be at most {MAX_FIELD_CHARS} characters.")
    return text


def parse_quantity(value: Any, label: str = "Amount") -> int:
    """Read a hand-entered quantity, or raise :class:`ProductError`.

    Stock counted onto a shelf is a whole number and cannot be negative, so a
    negative figure is refused here; the page clamps its input at zero, so one
    only arrives from a caller that is not the page.

    Raises:
        ProductError: When the value is not a whole number in range.
    """
    # A bool is an int in Python and a float would be truncated in silence;
    # both mean the caller sent something other than a count.
    if isinstance(value, bool) or isinstance(value, float):
        raise ProductError(f"{label} must be a whole number.")
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        raise ProductError(f"{label} must be a whole number.") from None
    if number < 0 or number > MAX_LOCAL_QUANTITY:
        raise ProductError(f"{label} must be between 0 and {MAX_LOCAL_QUANTITY}.")
    return number


def _money(value: Any, label: str) -> float:
    try:
        # A comma decimal is what a Danish keyboard produces; accept it rather
        # than refusing a figure whose meaning is unambiguous.
        number = float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        raise ProductError(f"{label} must be a number.") from None
    if number != number or number in (float("inf"), float("-inf")):
        raise ProductError(f"{label} must be a number.")
    if number < 0 or number > MAX_LOCAL_COST:
        raise ProductError(f"{label} must be between 0 and {MAX_LOCAL_COST:.0f}.")
    return number


def _validated_product(fields: dict) -> dict:
    """Check one hand-supplied product and return the row to store.

    Shared by the add form and the CSV import so a typed product and an
    imported one cannot be held to different rules.

    Raises:
        ProductError: When a field is missing, unreadable or out of range.
    """
    row = {
        "sku": _required_text(fields.get("sku"), "SKU"),
        "vendor": _required_text(fields.get("vendor"), "Vendor"),
        "product_title": _required_text(fields.get("product_title"), "Product name"),
        "variant_title": _required_text(fields.get("variant_title"), "Variant"),
        "barcode": _optional_text(fields.get("barcode"), "Barcode"),
        "bin": _optional_text(fields.get("bin"), "Bin"),
        "on_hand": parse_quantity(fields.get("on_hand"), "Amount"),
        "unit_cost": _money(fields.get("unit_cost"), "Cost"),
        "source": SOURCE_LOCAL,
        # Nothing is committed to an order for stock Shopify does not know
        # about, so the whole quantity is available.
        "tracked": True,
        "synced_on_hand": 0,
    }
    row["available"] = row["on_hand"]
    row["committed"] = 0
    return row


def _insert_product(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        f"INSERT INTO local_inventory ({', '.join(_COLUMNS)}) "
        f"VALUES ({', '.join('?' * len(_COLUMNS))})",
        (
            row["sku"],
            row["product_title"],
            row["variant_title"],
            row["vendor"],
            row["barcode"],
            row["bin"],
            1,
            row["source"],
            row["unit_cost"],
            row["on_hand"],
            row["available"],
            row["committed"],
            row["synced_on_hand"],
            _now(),
        ),
    )


def _duplicate_error(sku: str) -> ProductError:
    return ProductError(
        f"{sku} is already in the local inventory database; correct its "
        "quantity on a count sheet instead."
    )


def add_product(db_path: str, fields: dict) -> dict:
    """Add one hand-typed product to the local database and return its row.

    For stock the shop has no record of — found on a shelf, or not yet in
    Shopify.  The row is marked :data:`SOURCE_LOCAL`, so the count sheet treats
    it as countable rather than as a SKU Shopify lost, and its quantity counts
    as local from the start (``synced_on_hand`` is 0, since no fetch has ever
    confirmed it).

    Local only, like every other edit here: nothing is created in Shopify.  A
    full fetch replaces the whole table, so a row added here does not survive
    one — which is what the page warns about before fetching.

    Raises:
        ProductError: When a field is missing, unreadable or out of range, or
            when the SKU is already in the database.
    """
    row = _validated_product(fields)
    conn = _connect(db_path)
    try:
        with conn:
            existing = conn.execute(
                "SELECT source FROM local_inventory WHERE sku = ?", (row["sku"],)
            ).fetchone()
            if existing is not None:
                raise _duplicate_error(row["sku"])
            _insert_product(conn, row)
            stored = conn.execute(
                "SELECT * FROM local_inventory WHERE sku = ?", (row["sku"],)
            ).fetchone()
    finally:
        conn.close()

    logger.info(
        "local_inventory.add_product: added %s (%d units, bin %r)",
        row["sku"], row["on_hand"], row["bin"],
    )
    return _row_to_dict(stored)


def _normalise_heading(heading: str) -> str:
    """Fold a column heading to its comparable form.

    ``"Cost pr. item"``, ``"cost_pr_item"`` and ``"COST PR ITEM"`` are the same
    heading as far as an import is concerned.
    """
    text = (heading or "").strip().lower().lstrip("\ufeff")
    text = re.sub(r"[._/\\-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _sniff_delimiter(header_line: str) -> str:
    """Pick the delimiter that splits the header into the most columns."""
    return max(_CSV_DELIMITERS, key=header_line.count)


def parse_product_csv(text: str) -> list[dict]:
    """Read a product CSV into one dict of raw field values per data row.

    Field values are returned as written, for :func:`import_products` to
    validate, so a row's complaint can name the line it came from.  Each dict
    carries a ``line`` — the line number in the file, so it matches what the
    user sees in a spreadsheet.

    Raises:
        ProductError: For a problem with the file rather than with a row: no
            header, missing columns, or more rows than an import may carry.
    """
    stripped = text.lstrip("\ufeff").strip()
    if not stripped:
        raise ProductError("That file is empty.")

    delimiter = _sniff_delimiter(stripped.splitlines()[0])
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    try:
        header = next(reader)
    except StopIteration:
        raise ProductError("That file is empty.") from None

    # Heading → field, first column of a repeated heading winning.
    columns: dict[int, str] = {}
    by_heading = {
        heading: field
        for field, headings in _CSV_HEADINGS.items()
        for heading in headings
    }
    for index, heading in enumerate(header):
        field = by_heading.get(_normalise_heading(heading))
        if field and field not in columns.values():
            columns[index] = field

    missing = [f for f in _REQUIRED_CSV_FIELDS if f not in columns.values()]
    if missing:
        labels = {
            "vendor": "Vendor", "product_title": "Product", "variant_title": "Variant",
            "sku": "SKU", "on_hand": "Amount", "unit_cost": "Cost",
        }
        raise ProductError(
            "The file is missing these columns: "
            + ", ".join(labels[f] for f in missing)
            + ". Expected a header row with Vendor, Product, Variant, SKU, "
              "Amount and Cost, plus optional Barcode and Bin."
        )

    rows: list[dict] = []
    for line, values in enumerate(reader, start=2):
        # A spreadsheet's trailing blank lines are not errors.
        if not any((value or "").strip() for value in values):
            continue
        if len(rows) >= MAX_IMPORT_ROWS:
            raise ProductError(
                f"That file holds more than {MAX_IMPORT_ROWS} rows; split it up."
            )
        row = {field: "" for field in _CSV_HEADINGS}
        for index, field in columns.items():
            if index < len(values):
                row[field] = values[index]
        row["line"] = line
        rows.append(row)

    if not rows:
        raise ProductError("That file has a header but no product rows.")
    return rows


def import_products(db_path: str, rows: Iterable[dict]) -> dict:
    """Store the products in a CSV file, replacing the ones whose SKU collides.

    Row by row, keyed on SKU: a SKU the database does not hold is added exactly
    as the add form adds it, and a SKU it does hold is *replaced* by the file's
    version — vendor, product, variant, amount and cost all come from the file,
    including over a quantity counted here.  Rows the file says nothing about
    are left alone, so an import is a targeted overwrite rather than a reload.

    Two fields are treated as "not specified" when the file leaves them blank,
    rather than as "delete this": ``Barcode`` and ``Bin``.  A spreadsheet that
    simply does not carry bins would otherwise empty them, and a row with no
    bin cannot be found by any count sheet.

    A replaced row keeps what the file cannot know: where the SKU came from,
    the stock Shopify has committed to open orders, and the quantity the last
    fetch reported — so ``available`` is recomputed against ``committed`` and
    the row reads as a locally changed quantity, exactly as a count-sheet edit
    would.

    The file is validated in full before anything is written, and a single bad
    row refuses all of it: an import that overwrites products should either
    happen or not, not land halfway.  When that happens nothing is touched and
    the offending lines are reported.

    Returns ``{"rows", "added", "replaced", "units", "without_bin", "errors",
    "error_count"}``, with ``added`` and ``replaced`` both zero when the file
    was refused.
    """
    rows = list(rows)
    validated: list[dict] = []
    errors: list[dict] = []
    seen: dict[str, int | None] = {}

    for raw in rows:
        line = raw.get("line")
        try:
            row = _validated_product(raw)
        except ProductError as exc:
            errors.append({"line": line, "message": str(exc)})
            continue
        first = seen.get(row["sku"])
        if first is not None:
            # Two rows for one SKU have no agreed answer; the file has to say
            # which one is meant.
            errors.append({
                "line": line,
                "message": f"{row['sku']} is already on line {first} of this file.",
            })
            continue
        seen[row["sku"]] = line
        validated.append(row)

    if errors:
        logger.info(
            "local_inventory.import_products: refused a %d-row file with %d bad "
            "rows; nothing was changed",
            len(rows), len(errors),
        )
        return {
            "rows": len(rows),
            "added": 0,
            "replaced": 0,
            "units": 0,
            "without_bin": 0,
            "errors": errors[:MAX_IMPORT_ERRORS],
            "error_count": len(errors),
        }

    added = replaced = units = without_bin = 0
    now = _now()
    conn = _connect(db_path)
    try:
        with conn:
            for row in validated:
                existing = conn.execute(
                    "SELECT bin, barcode, committed FROM local_inventory WHERE sku = ?",
                    (row["sku"],),
                ).fetchone()
                units += row["on_hand"]

                if existing is None:
                    _insert_product(conn, row)
                    added += 1
                    if not row["bin"]:
                        without_bin += 1
                    continue

                bin_name = row["bin"] or existing["bin"]
                conn.execute(
                    "UPDATE local_inventory SET vendor = ?, product_title = ?, "
                    "variant_title = ?, barcode = ?, bin = ?, on_hand = ?, "
                    "available = ?, unit_cost = ?, updated_at = ? WHERE sku = ?",
                    (
                        row["vendor"],
                        row["product_title"],
                        row["variant_title"],
                        row["barcode"] or existing["barcode"],
                        bin_name,
                        row["on_hand"],
                        row["on_hand"] - existing["committed"],
                        row["unit_cost"],
                        now,
                        row["sku"],
                    ),
                )
                replaced += 1
                if not bin_name:
                    without_bin += 1
    finally:
        conn.close()

    logger.info(
        "local_inventory.import_products: %d rows — %d added, %d replaced, "
        "%d with no bin",
        len(rows), added, replaced, without_bin,
    )
    return {
        "rows": len(rows),
        "added": added,
        "replaced": replaced,
        "units": units,
        "without_bin": without_bin,
        "errors": [],
        "error_count": 0,
    }


def status(db_path: str) -> dict:
    """Summarise the local database for the Counting page's status panel."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS skus, "
            "COALESCE(SUM(on_hand), 0) AS units, "
            "COALESCE(SUM(bin != ''), 0) AS with_bins, "
            f"COALESCE(SUM({_MODIFIED_SQL}), 0) AS locally_modified, "
            "COALESCE(SUM(source = ?), 0) AS added_locally "
            "FROM local_inventory",
            (SOURCE_LOCAL,),
        ).fetchone()
        last_synced = _meta(conn, "last_synced")
        source = _meta(conn, "last_sync_source")
        bins_updated = _meta(conn, "bins_updated_at")
    except sqlite3.OperationalError:
        return {
            "total_skus": 0,
            "units": 0,
            "with_bins": 0,
            "locally_modified": 0,
            "added_locally": 0,
            "last_synced": None,
            "last_sync_source": None,
            "bins_updated": None,
        }
    finally:
        conn.close()
    return {
        "total_skus": row["skus"],
        "units": row["units"],
        "with_bins": row["with_bins"],
        "locally_modified": row["locally_modified"],
        "added_locally": row["added_locally"],
        "last_synced": last_synced,
        "last_sync_source": source,
        "bins_updated": bins_updated,
    }
