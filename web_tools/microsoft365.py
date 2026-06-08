import os
import shutil
import tempfile
import logging
from html import escape
from O365 import Account
from flask import render_template

logger = logging.getLogger(__name__)

_credentials = (os.getenv('O365_CLIENT_ID'), os.getenv('O365_CLIENT_SECRET'))
_tenant_id = os.getenv('O365_TENANT_ID')


def _get_mailbox():
    account = Account(_credentials, auth_flow_type='credentials', tenant_id=_tenant_id)
    account.authenticate()
    return account.mailbox('info@xtragrej.dk')


def send_missed_pickup_email(first_name: str, email: str, order_number: str) -> tuple[bool, str]:
    """
    Send a missed-pickup notification email to a customer.

    Args:
        first_name: Customer's first name (used in the template).
        email: Customer's email address.
        order_number: The Shopify order name (e.g. "#27542").

    Returns:
        Tuple of (success, message).
    """
    try:
        html_body = render_template(
            "missed_pickup.html",
            first_name=first_name,
            order_number=order_number,
        )

        msg = _get_mailbox().new_message()
        msg.to.add(email)
        msg.subject = f"Vedr. din ordre {order_number}"
        msg.body = html_body
        msg.send()

        logger.info(f"Sent missed-pickup email to {email} for order {order_number}")
        return True, f"Email sent to {email}"
    except Exception as exc:
        logger.exception(f"Failed to send missed-pickup email to {email} for order {order_number}")
        return False, str(exc)


def send_plaintext_email(
    to_email: str,
    subject: str,
    body: str,
    attachments: list[tuple[bytes, str]] | None = None,
) -> tuple[bool, str]:
    """
    Send a plain-text email, preserving the body's line breaks.

    The Graph mailbox renders message bodies as HTML, so the plain-text body is
    HTML-escaped and its newlines are converted to <br> to keep it readable
    (e.g. for line-itemised vendor orders).

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        body: Plain-text email body.
        attachments: Optional list of (content_bytes, filename) tuples to attach.

    Returns:
        Tuple of (success, message).
    """
    tmp_dir = None
    try:
        html_body = (
            '<div style="font-family: sans-serif;">'
            f'{escape(body).replace(chr(10), "<br>")}</div>'
        )

        msg = _get_mailbox().new_message()
        msg.to.add(to_email)
        msg.subject = subject
        msg.body = html_body

        if attachments:
            # O365 attaches by file path, so stage the bytes on disk first.
            tmp_dir = tempfile.mkdtemp(prefix="o365_attach_")
            for content, filename in attachments:
                path = os.path.join(tmp_dir, filename)
                with open(path, "wb") as handle:
                    handle.write(content)
                msg.attachments.add(path)

        msg.send()

        logger.info(f"Sent email '{subject}' to {to_email}")
        return True, f"Email sent to {to_email}"
    except Exception as exc:
        logger.exception(f"Failed to send email '{subject}' to {to_email}")
        return False, str(exc)
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)