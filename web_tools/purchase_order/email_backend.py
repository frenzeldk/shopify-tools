"""Email-based ordering backend.

Renders a plain-text order email from a ``string.Template`` body and an XLSX
attachment, then hands it to an injected ``send_email`` callable so all
ordering logic stays in this package while the actual transport (Microsoft 365,
SMTP, ...) is owned by the caller.

Body templates use $-style placeholders:
  $order_number $date $order_lines $line_count $total_quantity $company_name
"""
from __future__ import annotations

from datetime import date
from string import Template
from typing import Callable

from . import config as cfg, security
from .common import OrderError, build_order_workbook, make_order_number
from .templates import safe_attachment_name


def _format_order_lines(items: list[dict]) -> str:
    """Render order items as one human-readable line each."""
    lines = []
    for it in items:
        sku = str(it.get("sku") or "").strip()
        qty = int(it.get("quantity") or 0)
        title = str(it.get("title") or it.get("product_title") or "").strip()
        lines.append(f"{qty} x {sku}" + (f" - {title}" if title else ""))
    return "\n".join(lines)


def _load_template(vendor: str, ecfg: dict) -> Template:
    """Build the body template from the template's inline ``body``."""
    body = ecfg.get("body")
    if not body:
        raise OrderError(f"No order email body configured for {vendor}.", status=400)
    return Template(body)


def place_order(
    vendor: str,
    vcfg: dict,
    items: list[dict],
    columns: list[dict] | None,
    *,
    send_email: Callable[..., tuple[bool, str]] | None = None,
    order_number: str | None = None,
) -> dict:
    """Prepare (and, if ``send_email`` is given, send) a vendor order email.

    Returns a result dict with vendor/email/order_number/line_count/
    total_quantity and ``ordered`` (every item — email orders the lot). When
    ``send_email`` is omitted the rendered message is returned under
    ``prepared`` for the caller to send.
    """
    ecfg = vcfg.get("email") or {}
    company = cfg.defaults().get("company_name", "")
    prefix = cfg.defaults().get("order_number_prefix", "WT")

    to_env = ecfg.get("to_env") or ""
    try:
        to_from_env = security.resolve_env(to_env) if to_env else ""
    except security.PolicyError as exc:
        raise OrderError(str(exc), status=exc.status) from exc
    to_email = (to_from_env or ecfg.get("to") or "").strip()
    if not to_email:
        env_hint = f" (set the {ecfg.get('to_env')} environment variable)" if ecfg.get("to_env") else ""
        raise OrderError(f"No ordering email address configured for {vendor}{env_hint}.", status=400)

    order_number = order_number or make_order_number(prefix)
    total_quantity = sum(int(it.get("quantity") or 0) for it in items)
    fmt = {
        "order_number": order_number,
        "company_name": company,
        "date": date.today().isoformat(),
        "line_count": len(items),
        "total_quantity": total_quantity,
    }

    template = _load_template(vendor, ecfg)
    body = template.safe_substitute(order_lines=_format_order_lines(items), **fmt)
    subject = (ecfg.get("subject") or "Purchase Order {order_number} - {company_name}").format(**fmt)

    attachments = None
    att = ecfg.get("attachment") or {}
    if att.get("enabled", True):
        # Re-sanitised after formatting: a placeholder value could otherwise
        # reintroduce a separator into an already-validated template name.
        filename = safe_attachment_name(
            (att.get("filename") or "PO_{order_number}.xlsx").format(**fmt)
        )
        attachments = [(build_order_workbook(items, columns), filename)]

    result = {
        "vendor": vendor,
        "method": "email",
        "email": to_email,
        "order_number": order_number,
        "line_count": len(items),
        "total_quantity": total_quantity,
        "subject": subject,
        "ordered": items,
    }

    if send_email is None:
        result["prepared"] = {
            "to": to_email,
            "subject": subject,
            "body": body,
            "attachments": attachments,
        }
        return result

    success, message = send_email(to_email, subject, body, attachments=attachments)
    if not success:
        raise OrderError(
            f"Failed to send order email to {vendor}: {message}",
            status=502,
            extra={"order_number": order_number},
        )
    return result
