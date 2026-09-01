"""Application-wide security controls for the web tools service.

The service is an internal admin console: every route mutates or exposes
commercial data, so access control is a property of the *application*, not of a
decorator someone remembered to add.  This module installs the controls once,
in ``create_app()``:

* a fail-closed authentication gate — a request is rejected unless its endpoint
  is explicitly listed as public;
* optional role authorisation driven by a route → role table;
* same-origin enforcement for every state-changing request (CSRF);
* per-user rate limits with a cheaper budget for expensive endpoints;
* hardened session cookies and browser security headers.

The route table lives in :mod:`app` next to the routes themselves; this module
only consumes it and refuses to start when a route is missing from it.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Callable, Iterable, NamedTuple
from urllib.parse import urlsplit

from flask import Flask, jsonify, redirect, request, session, url_for

logger = logging.getLogger(__name__)


# ── Roles ─────────────────────────────────────────────────────────────────────
# Coarse capabilities, mapped to Keycloak roles of the same name.  Enforcement
# is enabled with OIDC_REQUIRE_ROLES=1 once the roles exist in the realm and are
# mapped into the userinfo/ID-token claims.

ROLE_READ = "read"
ROLE_INVENTORY_WRITE = "inventory-write"
ROLE_CATALOG_WRITE = "catalog-write"
ROLE_MAIL_SEND = "mail-send"
ROLE_PLACE_ORDER = "place-order"
ROLE_CONFIG_ADMIN = "config-admin"

ALL_ROLES = frozenset(
    {
        ROLE_READ,
        ROLE_INVENTORY_WRITE,
        ROLE_CATALOG_WRITE,
        ROLE_MAIL_SEND,
        ROLE_PLACE_ORDER,
        ROLE_CONFIG_ADMIN,
    }
)

# Endpoints reachable without a session.  Only the OIDC handshake and static
# assets belong here; everything else is authenticated.
PUBLIC_ENDPOINTS = frozenset(
    {
        "static",
        "oidc_auth.login",
        "oidc_auth.authorize",
        "oidc_auth.logout",
        "legacy_oidc_callback",
    }
)


class Limit(NamedTuple):
    """``count`` requests allowed per ``seconds`` for one caller."""

    count: int
    seconds: int


# Budgets, per authenticated user.  Reads are generous; anything that mutates
# Shopify/Shipmondo, sends mail, spends OpenAI credit or forces a full-catalog
# refresh gets a much smaller allowance.
LIMIT_READ = Limit(300, 60)
LIMIT_WRITE = Limit(60, 60)
LIMIT_EXPENSIVE = Limit(10, 60)
LIMIT_MAIL = Limit(20, 300)


class RoutePolicy(NamedTuple):
    """Access policy for one endpoint."""

    role: str
    limit: Limit = LIMIT_READ
    #: True for endpoints a browser navigates to; they get a login redirect
    #: instead of a 401 JSON body when the caller is anonymous.
    html: bool = False

    def limit_bucket(self) -> str:
        """Endpoints sharing a budget share a bucket, so one user cannot fan
        out an expensive operation across many endpoints."""
        return f"{self.limit.count}/{self.limit.seconds}"


class RateLimiter:
    """Fixed-window-per-caller limiter, in process memory.

    Waitress serves this app from one multi-threaded process, so a shared dict
    guarded by a lock is sufficient and avoids adding a Redis dependency for a
    handful of internal users.
    """

    def __init__(self, max_keys: int = 4096):
        self._hits: dict[tuple[str, str], deque[float]] = {}
        self._lock = threading.Lock()
        self._max_keys = max_keys

    def check(self, caller: str, bucket: str, limit: Limit) -> float | None:
        """Record a hit; return the retry-after seconds when over budget."""
        now = time.monotonic()
        cutoff = now - limit.seconds
        key = (caller, bucket)
        with self._lock:
            if len(self._hits) > self._max_keys:
                self._prune(cutoff)
            hits = self._hits.setdefault(key, deque())
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= limit.count:
                return max(1.0, round(hits[0] + limit.seconds - now, 1))
            hits.append(now)
            return None

    def _prune(self, cutoff: float) -> None:
        for key in [k for k, v in self._hits.items() if not v or v[-1] < cutoff]:
            self._hits.pop(key, None)


# ── Startup configuration ─────────────────────────────────────────────────────


def _flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def configure(app: Flask) -> None:
    """Apply session, cookie and request-size configuration.

    Raises:
        RuntimeError: When ``FLASK_SECRET_KEY`` is missing or too weak to sign
            sessions with.
    """
    from datetime import timedelta

    secret = os.environ.get("FLASK_SECRET_KEY") or ""
    if len(secret) < 32:
        raise RuntimeError(
            "FLASK_SECRET_KEY must be set to at least 32 characters of random data "
            "(e.g. `python -c \"import secrets; print(secrets.token_urlsafe(48))\"`). "
            "Rotate it if it has ever been exposed."
        )
    app.config["SECRET_KEY"] = secret

    app.config["SESSION_COOKIE_SECURE"] = _flag("SESSION_COOKIE_SECURE", True)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
        days=int(os.environ.get("SESSION_LIFETIME_DAYS", 7))
    )
    app.config["PREFERRED_URL_SCHEME"] = "https"

    # Bound what a single request may push into the process. Waitress gets the
    # same ceiling in serve() so oversized bodies are refused before buffering.
    app.config["MAX_CONTENT_LENGTH"] = max_request_bytes()
    app.config["MAX_FORM_MEMORY_SIZE"] = int(
        os.environ.get("MAX_FORM_MEMORY_BYTES", 2 * 1024 * 1024)
    )

    proxies = int(os.environ.get("TRUSTED_PROXY_COUNT", 0))
    if proxies > 0:
        from werkzeug.middleware.proxy_fix import ProxyFix

        app.wsgi_app = ProxyFix(
            app.wsgi_app, x_for=proxies, x_proto=proxies, x_host=proxies
        )


def max_request_bytes() -> int:
    """Largest request body the service will accept."""
    return int(os.environ.get("MAX_REQUEST_BYTES", 32 * 1024 * 1024))


# ── Origin / CSRF ─────────────────────────────────────────────────────────────

_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _trusted_origins() -> set[str]:
    raw = os.environ.get("TRUSTED_ORIGINS", "")
    return {o.strip().rstrip("/").lower() for o in raw.replace(",", " ").split() if o.strip()}


def _request_origin_ok() -> bool:
    """Whether a state-changing request demonstrably comes from our own origin.

    Browsers send ``Origin`` on cross-origin requests and on same-origin
    non-GET requests, so a missing *and* unusable ``Referer`` is treated as a
    failure rather than waved through.
    """
    expected = {request.host_url.rstrip("/").lower()} | _trusted_origins()

    origin = (request.headers.get("Origin") or "").strip().rstrip("/").lower()
    if origin:
        return origin in expected

    referer = (request.headers.get("Referer") or "").strip()
    if referer:
        parts = urlsplit(referer)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}".lower() in expected
    return False


# ── Roles ─────────────────────────────────────────────────────────────────────


def user_roles() -> set[str]:
    """Roles claimed by the signed-in user's OIDC profile.

    Keycloak must be configured to include the roles in the userinfo/ID-token
    claims (``roles``/``groups``, or the standard ``realm_access``/
    ``resource_access`` structures).
    """
    profile = session.get("oidc_auth_profile") or {}
    roles: set[str] = set()

    for key in ("roles", "groups"):
        value = profile.get(key)
        if isinstance(value, str):
            roles.update(value.replace(",", " ").split())
        elif isinstance(value, (list, tuple, set)):
            roles.update(str(v).lstrip("/") for v in value)

    realm = profile.get("realm_access") or {}
    if isinstance(realm, dict):
        roles.update(str(r) for r in (realm.get("roles") or []))

    resources = profile.get("resource_access") or {}
    if isinstance(resources, dict):
        for entry in resources.values():
            if isinstance(entry, dict):
                roles.update(str(r) for r in (entry.get("roles") or []))

    return roles


def _roles_enforced() -> bool:
    return _flag("OIDC_REQUIRE_ROLES", False)


# ── Response headers ──────────────────────────────────────────────────────────

# Frontend libraries are still loaded from public CDNs (see ST-10), so those two
# origins are allowed explicitly; 'unsafe-eval' is required by the full Vue
# builds, which compile in-DOM templates at runtime.
_CDN = "https://cdn.jsdelivr.net https://unpkg.com"
CONTENT_SECURITY_POLICY = "; ".join(
    [
        "default-src 'self'",
        f"script-src 'self' 'unsafe-inline' 'unsafe-eval' {_CDN}",
        f"style-src 'self' 'unsafe-inline' {_CDN}",
        f"font-src 'self' data: {_CDN}",
        "img-src 'self' data: blob: https:",
        # Staged uploads are POSTed straight from the browser to Shopify's
        # upload target, so https: is required here.
        "connect-src 'self' https:",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
        "object-src 'none'",
    ]
)

_STATIC_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    "Cross-Origin-Opener-Policy": "same-origin",
    # The barcode scanner needs the camera; nothing else needs a device.
    "Permissions-Policy": "camera=(self), geolocation=(), microphone=(), payment=()",
}


def apply_security_headers(response):
    """Attach the browser security headers to every response."""
    response.headers.setdefault("Content-Security-Policy", CONTENT_SECURITY_POLICY)
    for name, value in _STATIC_HEADERS.items():
        response.headers.setdefault(name, value)
    if request.is_secure and _flag("ENABLE_HSTS", True):
        response.headers.setdefault(
            "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
        )
    return response


# ── Installation ──────────────────────────────────────────────────────────────


def audit_routes(app: Flask, policies: dict[str, RoutePolicy]) -> None:
    """Fail startup when a registered route has no access policy.

    This is the regression check for ST-01: adding a route without deciding who
    may call it stops the service from booting instead of silently publishing
    it.
    """
    registered = {rule.endpoint for rule in app.url_map.iter_rules()}
    unpoliced = sorted(registered - PUBLIC_ENDPOINTS - set(policies))
    if unpoliced:
        raise RuntimeError(
            "These routes have no entry in ROUTE_POLICIES and are not listed as "
            f"public: {', '.join(unpoliced)}. Add each one to ROUTE_POLICIES (or "
            "to security.PUBLIC_ENDPOINTS if it must be anonymous)."
        )
    unknown_roles = sorted(
        {p.role for p in policies.values()} - ALL_ROLES
    )
    if unknown_roles:
        raise RuntimeError(f"ROUTE_POLICIES references unknown roles: {unknown_roles}")

    stale = sorted(set(policies) - registered)
    if stale:
        logger.warning("ROUTE_POLICIES has entries for unregistered routes: %s", stale)


def install(
    app: Flask,
    *,
    policies: dict[str, RoutePolicy],
    is_authenticated: Callable[[], bool],
    exempt_paths: Iterable[str] = (),
) -> None:
    """Install the request guards and response headers on ``app``.

    Args:
        app: The Flask application.
        policies: Endpoint name → :class:`RoutePolicy`.
        is_authenticated: Callable returning whether the caller has a session.
        exempt_paths: Path prefixes that bypass the gate entirely (health
            checks behind the proxy, for example).
    """
    limiter = RateLimiter()
    exempt = tuple(exempt_paths)
    roles_enforced = _roles_enforced()
    if not roles_enforced:
        logger.info(
            "Role authorisation is not enforced (set OIDC_REQUIRE_ROLES=1 once the "
            "Keycloak roles are mapped into the userinfo claims)."
        )

    @app.before_request
    def _guard():  # noqa: C901 - a single linear gate is clearer than five hooks
        endpoint = request.endpoint
        if endpoint in PUBLIC_ENDPOINTS or request.path.startswith(exempt):
            return None
        if request.method == "OPTIONS":
            return None

        policy = policies.get(endpoint)

        # 1. Authentication — fail closed, including for unmatched paths so the
        #    route table cannot be enumerated anonymously.
        if not is_authenticated():
            if policy is not None and policy.html and request.method == "GET":
                return redirect(url_for("oidc_auth.login", next=request.url))
            return _deny(401, "Authentication required.")

        # 2. Unknown endpoint: authenticated, so a plain 404 is safe.
        if policy is None:
            return None

        # 3. CSRF — every state-changing call must prove same-origin.
        if request.method in _UNSAFE_METHODS and not _request_origin_ok():
            logger.warning(
                "Blocked cross-origin %s %s (Origin=%r Referer=%r)",
                request.method, request.path,
                request.headers.get("Origin"), request.headers.get("Referer"),
            )
            return _deny(403, "Cross-origin requests are not allowed.")

        # 4. Authorisation.
        if roles_enforced and policy.role not in user_roles():
            return _deny(403, f"This action requires the '{policy.role}' role.")

        # 5. Rate limiting, per user and per cost class.
        retry_after = limiter.check(_caller_id(), policy.limit_bucket(), policy.limit)
        if retry_after is not None:
            response = _deny(429, "Too many requests; slow down.")
            response.headers["Retry-After"] = str(int(retry_after))
            return response
        return None

    app.after_request(apply_security_headers)


def _caller_id() -> str:
    """Stable identity for rate limiting: the OIDC subject, else the peer IP."""
    profile = session.get("oidc_auth_profile") or {}
    return str(
        profile.get("sub")
        or profile.get("preferred_username")
        or request.remote_addr
        or "anonymous"
    )


def _deny(status: int, message: str):
    response = jsonify({"error": message})
    response.status_code = status
    return response
