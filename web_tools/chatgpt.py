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

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

# Initialise the client once (reads OPENAI_API_KEY from env automatically)
_client: OpenAI | None = None


def _get_client() -> OpenAI:
    """Lazily initialise the OpenAI client."""
    global _client
    if _client is None:
        _client = OpenAI()          # uses OPENAI_API_KEY env var
    return _client


SYSTEM_PROMPT = """You are a professional translator and copywriter for an online retail store.
Your task is to translate product descriptions to Danish and rephrase them so they read as if
written by a retailer selling the product, NOT the manufacturer. 

Rules:
- Translate the entire description into fluent, natural Danish.
- Rephrase any first-person manufacturer language ("we designed", "our product") into
  third-person or retailer-appropriate language ("produktet er designet", "denne jakke").
- Keep product specifications, materials, and technical details accurate.
- Preserve any HTML formatting (paragraphs, lists, bold, etc.) in the output.
- Do not add any extra commentary — return ONLY the translated HTML description.
- If the input is already in Danish, still rephrase manufacturer language to reseller language."""


def translate_product_description(source_html: str, product_name: str = "") -> dict:
    """
    Translate a product description from a vendor page to Danish,
    rephrased from a reseller perspective.

    Args:
        source_html: The HTML product description to translate.
        product_name: Optional product name for context.

    Returns:
        {"description_html": "...", "error": None} on success, or
        {"description_html": "", "error": "..."} on failure.
    """
    if not os.environ.get("OPENAI_API_KEY"):
        return {"description_html": "", "error": "OPENAI_API_KEY is not configured."}

    if not source_html or not source_html.strip():
        return {"description_html": "", "error": "No description provided."}

    user_message = f"Product: {product_name}\n\nDescription to translate:\n{source_html}" if product_name else source_html

    try:
        logger.info("translate_product_description: sending request to OpenAI (model=%s)", OPENAI_MODEL)

        response = _get_client().responses.create(
            model=OPENAI_MODEL,
            instructions=SYSTEM_PROMPT,
            input=user_message,
            temperature=0.3,
            max_output_tokens=4000,
            store=False,
            timeout=60,
        )

        translated = (response.output_text or "").strip()
        logger.info("translate_product_description: received %d chars", len(translated))

        return {"description_html": translated, "error": None}

    except APITimeoutError:
        logger.error("translate_product_description: request timed out")
        return {"description_html": "", "error": "OpenAI API request timed out."}
    except APIError as exc:
        logger.error("translate_product_description: API error %s: %s", exc.status_code, exc.message)
        return {"description_html": "", "error": f"OpenAI API error ({exc.status_code}): {exc.message}"}
    except Exception as exc:
        logger.exception("translate_product_description: unexpected error")
        return {"description_html": "", "error": str(exc)}


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

    # Fetch the vendor page
    try:
        logger.info("fetch_and_translate_vendor_page: fetching %s", url)
        resp = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ShopifyTools/1.0)"},
        )
        resp.raise_for_status()
        page_html = resp.text
        logger.info("fetch_and_translate_vendor_page: fetched %d chars", len(page_html))

        # Truncate to avoid token limits — keep body content area
        if len(page_html) > 50000:
            page_html = page_html[:50000]

    except Exception as exc:
        logger.exception("fetch_and_translate_vendor_page: failed to fetch URL")
        return {"description_html": "", "error": f"Failed to fetch vendor page: {exc}"}

    # Send to ChatGPT with a specialised prompt
    extract_and_translate_prompt = """You are a professional translator and copywriter for an online retail store.

Given the HTML of a vendor product page, extract the main product description and translate it to Danish.
Rephrase it so it reads as if written by a retailer selling the product, NOT the manufacturer.

Rules:
- Extract ONLY the product description content (not navigation, headers, footers, etc.).
- Translate into fluent, natural Danish.
- Rephrase manufacturer language to reseller language.
- Keep product specs, materials, and technical details accurate.
- Format the output as clean HTML (paragraphs, lists, bold where appropriate).
- Do not include any commentary — return ONLY the translated HTML description."""

    user_message = f"Product: {product_name}\n\nVendor page HTML:\n{page_html}" if product_name else page_html

    try:
        response = _get_client().responses.create(
            model=OPENAI_MODEL,
            instructions=extract_and_translate_prompt,
            input=user_message,
            temperature=0.3,
            max_output_tokens=4000,
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
