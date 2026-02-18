import os
import logging
from O365 import Account
from flask import render_template

logger = logging.getLogger(__name__)

credentials = (os.getenv('O365_CLIENT_ID'), os.getenv('O365_CLIENT_SECRET'))
account = Account(credentials, auth_flow_type='credentials', tenant_id=os.getenv('O365_TENANT_ID'))

if not account.is_authenticated:
    account.authenticate()

mailbox = account.mailbox('info@xtragrej.dk')


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

        msg = mailbox.new_message()
        msg.to.add(email)
        msg.subject = f"Vedr. din ordre {order_number}"
        msg.body = html_body
        msg.send()

        logger.info(f"Sent missed-pickup email to {email} for order {order_number}")
        return True, f"Email sent to {email}"
    except Exception as exc:
        logger.exception(f"Failed to send missed-pickup email to {email} for order {order_number}")
        return False, str(exc)