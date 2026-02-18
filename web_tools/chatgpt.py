"""
ChatGPT integration for translating vendor product descriptions to Danish.

Uses the OpenAI Responses API to translate and rephrase product descriptions
from a vendor/manufacturer perspective to a reseller perspective in Danish.

Environment variables:
    OPENAI_API_KEY  – OpenAI API key
    OPENAI_MODEL    – Model name (default: gpt-4o-mini)
"""

import logging
import os

import requests
from openai import OpenAI, APITimeoutError, APIError

logger = logging.getLogger(__name__)

_OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# Initialise the client once (reads OPENAI_API_KEY from env automatically)
_client: OpenAI | None = None


def _get_client() -> OpenAI:
    """Lazily initialise the OpenAI client."""
    global _client
    if _client is None:
        _client = OpenAI()          # uses OPENAI_API_KEY env var
    return _client


_SYSTEM_PROMPT = """You are a professional translator and copywriter for an online retail store.
Your task is to translate product descriptions to Danish and rephrase them so they read as if
written by a retailer selling the product, NOT the manufacturer. 

Rules:
- Translate the entire description into fluent, natural Danish.
- Rephrase any first-person manufacturer language ("we designed", "our product") into
  third-person or retailer-appropriate language ("produktet er designet", "denne jakke").
- Keep product specifications, materials, and technical details accurate.
- Make sure to use the FULL product description and technical specifications.
- Do not add any extra commentary — return ONLY the translated HTML description.
- If the input is already in Danish, still rephrase manufacturer language to reseller language.
- Include the Material and specification sectino or similar if it exists! This is very important.
"""


def fetch_and_translate_vendor_page(url: str, product_name: str = "") -> dict:
    """
    Fetch a vendor product page URL and translate its description to Danish.

    This fetches the HTML content of the page, then sends to ChatGPT for
    translation. ChatGPT is asked to extract the product description from
    the page content and translate it.

    Args:
        url: URL of the vendor product page.
        product_name: Optional product name for context.

    Returns:
        {"description_html": "...", "error": None} on success.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        return {"description_html": "", "error": "OPENAI_API_KEY is not configured."}

    if not url or not url.strip():
        return {"description_html": "", "error": "No URL provided."}
    
    # Send to ChatGPT with a specialised prompt

    user_message = f"Product: {product_name}\n\nVendor page URL:{url}" if product_name else url

    try:
        response = _get_client().responses.create(
            model=_OPENAI_MODEL,
            instructions=_SYSTEM_PROMPT,
            input=user_message,
            temperature=0.3,
            max_output_tokens=10000,
            store=False,
            timeout=90,
        )

        translated = (response.output_text or "").strip()
        logger.info("fetch_and_translate_vendor_page: received %d chars of translated description", len(translated))

        return {"description_html": translated, "error": None}

    except APITimeoutError:
        return {"description_html": "", "error": "OpenAI API request timed out."}
    except APIError as exc:
        return {"description_html": "", "error": f"OpenAI API error ({exc.status_code}): {exc.message}"}
    except Exception as exc:
        logger.exception("fetch_and_translate_vendor_page: unexpected error")
        return {"description_html": "", "error": str(exc)}
