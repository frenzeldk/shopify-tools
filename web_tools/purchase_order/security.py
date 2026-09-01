"""Outbound policy for UI-managed ordering templates.

An ordering template is operator-supplied data stored in the database, so it is
treated as untrusted input rather than configuration: it may not choose which
process environment variable to read, which host to talk to, or how long/large
a request may run.

Everything here is driven by three environment variables so a new vendor can be
onboarded without a code change, while an edited template can never reach a new
destination or a new secret on its own:

``PO_ALLOWED_ENV_VARS``
    Names of the environment variables ordering templates may reference through
    ``{$env: NAME}``, ``base_url_env``, ``token_env``, ``username_env``,
    ``password_env`` and ``to_env``.  Unset means no template may read anything.

``PO_ALLOWED_API_HOSTS``
    Hosts (``host`` or ``host:port``) API templates may send requests to.
    Unset means no API template may make a request.

``PO_MAX_*``
    Ceilings for flow length, item count, pagination, timeout and template size.
"""
from __future__ import annotations

import os
from typing import Any, Iterable

import netguard


class PolicyError(Exception):
    """Raised when a template asks for something the outbound policy forbids.

    Carries ``status`` so the web layer can map it onto a 4xx like
    :class:`~purchase_order.common.OrderError` does.
    """

    def __init__(self, message: str, *, status: int = 400):
        super().__init__(message)
        self.status = status


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name) or default)
    except ValueError:
        return default


def max_flow_steps() -> int:
    return _int_env("PO_MAX_FLOW_STEPS", 8)


def max_items() -> int:
    return _int_env("PO_MAX_ITEMS", 1000)


def max_pages() -> int:
    return _int_env("PO_MAX_PAGES", 50)


def max_timeout() -> int:
    return _int_env("PO_MAX_TIMEOUT", 60)


def max_template_bytes() -> int:
    return _int_env("PO_MAX_TEMPLATE_BYTES", 256 * 1024)


# Nesting depth accepted in a template's request bodies / GraphQL variables.
MAX_VALUE_DEPTH = 20


# ── Environment-variable allowlist ────────────────────────────────────────────


def allowed_env_names() -> set[str]:
    """Environment variables an ordering template is permitted to reference."""
    raw = os.environ.get("PO_ALLOWED_ENV_VARS", "")
    return {name for name in raw.replace(",", " ").split() if name}


def check_env_name(name: str) -> str:
    """Validate that a template may reference ``name``; return it unchanged."""
    name = (name or "").strip()
    if not name:
        raise PolicyError("An environment variable name is required.")
    allowed = allowed_env_names()
    if name not in allowed:
        raise PolicyError(
            f"Ordering templates may not read the environment variable '{name}'. "
            "Add it to PO_ALLOWED_ENV_VARS if this vendor legitimately needs it."
        )
    return name


def resolve_env(name: str, default: Any = "") -> Any:
    """Read an allowlisted environment variable on behalf of a template."""
    return os.environ.get(check_env_name(name), default)


# ── Outbound host allowlist ───────────────────────────────────────────────────


def allowed_hosts() -> list[tuple[str, int | None]]:
    """Hosts API ordering templates may send requests to."""
    return netguard.parse_host_allowlist(os.environ.get("PO_ALLOWED_API_HOSTS", ""))


def check_url(url: str) -> str:
    """Validate an outbound ordering URL against the policy; return it unchanged.

    Enforces HTTPS, the host allowlist, and that no resolved address points at a
    loopback/private/link-local/metadata destination.
    """
    hosts = allowed_hosts()
    if not hosts:
        raise PolicyError(
            "No ordering API hosts are allowlisted. Set PO_ALLOWED_API_HOSTS to the "
            "vendor hostnames this service may order from."
        )
    try:
        netguard.validate_url(url, allowed_hosts=hosts)
    except netguard.UnsafeURLError as exc:
        raise PolicyError(f"Ordering request rejected: {exc}") from exc
    return url


def safe_request(method: str, url: str, **kwargs):
    """Send an ordering request through the outbound guard.

    Validates the destination against the allowlist, refuses redirects and caps
    the response size, translating every guard failure into a
    :class:`PolicyError` the web layer can render.
    """
    hosts = allowed_hosts()
    if not hosts:
        raise PolicyError(
            "No ordering API hosts are allowlisted. Set PO_ALLOWED_API_HOSTS to the "
            "vendor hostnames this service may order from."
        )
    try:
        return netguard.request(method, url, allowed_hosts=hosts, **kwargs)
    except netguard.UnsafeURLError as exc:
        raise PolicyError(f"Ordering request rejected: {exc}") from exc
    except netguard.ResponseTooLargeError as exc:
        raise PolicyError(f"Vendor response was too large: {exc}", status=502) from exc


def clamp_timeout(timeout: Any) -> float:
    """Clamp a template-supplied timeout to the configured maximum."""
    try:
        value = float(timeout)
    except (TypeError, ValueError):
        value = 30.0
    if value <= 0:
        value = 30.0
    return min(value, float(max_timeout()))


def check_item_count(items: Iterable[Any]) -> None:
    """Reject orders with more lines than the policy allows."""
    count = len(list(items))
    if count > max_items():
        raise PolicyError(
            f"An order may contain at most {max_items()} lines (got {count}).",
            status=413,
        )


# ── Whole-template validation (run before a template is stored) ───────────────


def _walk(node: Any, depth: int = 0):
    """Yield every dict in a template body, rejecting absurd nesting."""
    if depth > MAX_VALUE_DEPTH:
        raise PolicyError("Template nesting is too deep.")
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value, depth + 1)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value, depth + 1)


def validate_api_template(api: dict) -> None:
    """Check a stored API template against the outbound policy.

    Called when a template is saved so an operator sees the problem in the
    editor instead of at order time; the same checks are re-applied per request
    because the allowlists can change while templates sit in the database.
    """
    import json

    try:
        size = len(json.dumps(api))
    except (TypeError, ValueError):
        raise PolicyError("API configuration is not serialisable.")
    if size > max_template_bytes():
        raise PolicyError(
            f"API configuration is too large ({size} bytes, max {max_template_bytes()})."
        )

    # Every {$env: NAME} directive and every *_env indirection.
    for node in _walk(api):
        if "$env" in node and set(node) <= {"$env", "$default"}:
            check_env_name(str(node["$env"]))
        for key in ("base_url_env", "token_env", "username_env", "password_env", "to_env"):
            if node.get(key):
                check_env_name(str(node[key]))

    flow = api.get("flow") or []
    if not isinstance(flow, list):
        raise PolicyError("API 'flow' must be a list of steps.")
    if len(flow) > max_flow_steps():
        raise PolicyError(
            f"An ordering flow may have at most {max_flow_steps()} steps (got {len(flow)})."
        )

    # A literal base_url must already satisfy the host allowlist; a base_url_env
    # one can only be checked at request time because the value lives in the
    # process environment.
    base_url = (api.get("base_url") or "").strip()
    if base_url and not api.get("base_url_env"):
        check_url(base_url.rstrip("/") or base_url)
    if not base_url and not api.get("base_url_env"):
        raise PolicyError("API templates need a base_url (or base_url_env).")

    if api.get("timeout") is not None:
        try:
            float(api["timeout"])
        except (TypeError, ValueError):
            raise PolicyError("API 'timeout' must be a number.")
