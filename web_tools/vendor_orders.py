"""
Email-based purchase ordering for vendors that have no ordering API.

Each supported vendor is ordered by sending a plain-text email to the vendor's
ordering address. Both the recipient address and the body template are supplied
through environment variables, the latter falling back to a bundled default
under ./content/<name>.tmpl.

Body templates use ``string.Template`` ($-style) placeholders:
  $order_number   - generated order reference
  $date           - order date (YYYY-MM-DD)
  $order_lines     - formatted, one line per item ("<qty> x <sku> - <title>")
  $line_count     - number of order lines
  $total_quantity - sum of all ordered quantities
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date
from pathlib import Path
from string import Template

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
CONTENT_DIR = BASE_DIR / "content"


class VendorOrderError(Exception):
    """Raised when an email order cannot be prepared or sent."""


# Maps a purchase-order configuration name (as shown in the grid) to the env
# vars that drive its email order. ``template_default`` is used when the
# template env var is unset.
VENDORS: dict[str, dict[str, str]] = {
    "M-Tac": {
        "email_env": "MTAC_ORDER_EMAIL",
        "template_env": "MTAC_ORDER_TEMPLATE",
        "template_default": str(CONTENT_DIR / "mtac_order.tmpl"),
    },
    "Leatherman": {
        "email_env": "LEATHERMAN_ORDER_EMAIL",
        "template_env": "LEATHERMAN_ORDER_TEMPLATE",
        "template_default": str(CONTENT_DIR / "leatherman_order.tmpl"),
    },
}


def is_supported(vendor: str) -> bool:
    """Return True if ``vendor`` can be ordered by email."""
    return vendor in VENDORS


def make_order_number(prefix: str = "WT") -> str:
    """Generate a unique order reference for our side of the order."""
    return f"{prefix}-{int(time.time())}"


def _format_order_lines(items: list[dict]) -> str:
    """Render order items as one human-readable line each."""
    lines = []
    for it in items:
        sku = str(it.get("sku") or "").strip()
        qty = int(it.get("quantity") or 0)
        title = str(it.get("title") or it.get("product_title") or "").strip()
        lines.append(f"{qty} x {sku}" + (f" - {title}" if title else ""))
    return "\n".join(lines)


def _load_template(vendor: str, config: dict[str, str]) -> Template:
    """Resolve and read the body template for ``vendor``."""
    path = os.environ.get(config["template_env"]) or config["template_default"]
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return Template(handle.read())
    except OSError as exc:
        raise VendorOrderError(
            f"Could not read the order email template for {vendor} at '{path}': {exc}"
        ) from exc


def prepare_order_email(vendor: str, items: list[dict]) -> dict:
    """Build the recipient, subject and rendered body for a vendor order.

    Returns a dict with keys: vendor, email, subject, body, order_number,
    line_count, total_quantity. Raises VendorOrderError on misconfiguration.
    """
    config = VENDORS.get(vendor)
    if config is None:
        raise VendorOrderError(f"'{vendor}' has no email ordering configuration.")

    to_email = (os.environ.get(config["email_env"]) or "").strip()
    if not to_email:
        raise VendorOrderError(
            f"No ordering email address configured for {vendor} "
            f"(set the {config['email_env']} environment variable)."
        )

    if not items:
        raise VendorOrderError("No items to order.")

    order_number = make_order_number()
    total_quantity = sum(int(it.get("quantity") or 0) for it in items)
    template = _load_template(vendor, config)
    body = template.safe_substitute(
        order_number=order_number,
        date=date.today().isoformat(),
        order_lines=_format_order_lines(items),
        line_count=len(items),
        total_quantity=total_quantity,
    )

    return {
        "vendor": vendor,
        "email": to_email,
        "subject": f"Purchase Order {order_number} - XtraGrej",
        "body": body,
        "order_number": order_number,
        "line_count": len(items),
        "total_quantity": total_quantity,
    }
