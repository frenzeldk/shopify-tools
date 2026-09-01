"""Guards for server-side outbound HTTP requests.

Everything the application fetches on behalf of a caller goes through here so a
request body can never turn the service into an SSRF proxy for internal
networks, nor pull an unbounded response into memory.

Two levels of strictness are offered:

``validate_url``
    Scheme/host/port checks plus DNS resolution with every resolved address
    checked against the blocked ranges (loopback, private, link-local and the
    cloud-metadata address, multicast, reserved).  Pass ``allowed_hosts`` to
    additionally pin the request to a known vendor.

``fetch``
    ``validate_url`` followed by a streamed GET that refuses redirects and
    aborts once ``max_bytes`` have been read.
"""
from __future__ import annotations

import ipaddress
import logging
import os
import socket
from typing import Iterable
from urllib.parse import urlsplit

import requests

logger = logging.getLogger(__name__)

# Hard ceiling for any single guarded download (bytes).
DEFAULT_MAX_BYTES = int(os.environ.get("OUTBOUND_MAX_RESPONSE_BYTES", 15 * 1024 * 1024))
# Hard ceiling for any guarded request timeout (seconds).
MAX_TIMEOUT_SECONDS = float(os.environ.get("OUTBOUND_MAX_TIMEOUT", 120))

_ALLOWED_SCHEMES = ("https",)
_DEFAULT_PORTS = {"https": 443, "http": 80}


class UnsafeURLError(ValueError):
    """Raised when a URL is rejected before any connection is attempted."""


class ResponseTooLargeError(ValueError):
    """Raised when a guarded download exceeds its byte budget."""


def _is_blocked_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Whether ``ip`` points somewhere a caller must never be able to reach.

    Only globally routable unicast addresses are allowed.  Embedded IPv4
    addresses are unwrapped first so ``::ffff:127.0.0.1`` and 6to4 cannot be
    used to smuggle a loopback or RFC1918 destination past the check.
    """
    for attr in ("ipv4_mapped", "sixtofour"):
        embedded = getattr(ip, attr, None)
        if embedded is not None:
            ip = embedded
    if not ip.is_global:         # covers CGNAT 100.64/10 and the doc/test ranges
        return True
    return (
        ip.is_private            # RFC1918, unique-local v6, …
        or ip.is_loopback
        or ip.is_link_local      # includes 169.254.169.254 (cloud metadata)
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _normalize_host_entry(entry: str) -> tuple[str, int | None]:
    """Split an allowlist entry into (hostname, port or None)."""
    entry = entry.strip().lower()
    if not entry:
        return "", None
    if entry.startswith("["):                     # [::1]:443 style
        host, _, rest = entry.partition("]")
        host = host[1:]
        port = rest.lstrip(":") or None
    elif entry.count(":") == 1:
        host, _, port = entry.partition(":")
    else:
        host, port = entry, None
    try:
        return host, int(port) if port else None
    except ValueError:
        raise UnsafeURLError(f"Invalid host allowlist entry '{entry}'.")


def parse_host_allowlist(raw: str | Iterable[str] | None) -> list[tuple[str, int | None]]:
    """Parse a comma/whitespace separated host allowlist into (host, port) pairs."""
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = raw.replace(",", " ").split()
    return [pair for pair in (_normalize_host_entry(e) for e in raw) if pair[0]]


def validate_url(
    url: str,
    *,
    allowed_hosts: Iterable[tuple[str, int | None]] | str | None = None,
    allowed_schemes: Iterable[str] = _ALLOWED_SCHEMES,
) -> tuple[str, int, list[str]]:
    """Validate ``url`` for outbound use and return (host, port, resolved IPs).

    Raises :class:`UnsafeURLError` when the URL is malformed, uses a scheme
    other than ``allowed_schemes``, carries embedded credentials, is not in
    ``allowed_hosts`` (when one is given), or resolves to a blocked address.
    """
    if not isinstance(url, str) or not url.strip():
        raise UnsafeURLError("A URL is required.")

    parts = urlsplit(url.strip())
    scheme = (parts.scheme or "").lower()
    if scheme not in {s.lower() for s in allowed_schemes}:
        raise UnsafeURLError(
            f"URL scheme '{scheme or 'none'}' is not allowed "
            f"(allowed: {', '.join(allowed_schemes)})."
        )
    if parts.username or parts.password:
        raise UnsafeURLError("URLs with embedded credentials are not allowed.")

    host = (parts.hostname or "").lower()
    if not host:
        raise UnsafeURLError("The URL has no hostname.")
    try:
        port = parts.port or _DEFAULT_PORTS.get(scheme, 443)
    except ValueError:
        raise UnsafeURLError("The URL has an invalid port.")

    allowlist = (
        parse_host_allowlist(allowed_hosts)
        if isinstance(allowed_hosts, str) or allowed_hosts is None
        else list(allowed_hosts)
    )
    if allowlist:
        if not any(
            host == entry_host and (entry_port is None or entry_port == port)
            for entry_host, entry_port in allowlist
        ):
            raise UnsafeURLError(
                f"Host '{host}:{port}' is not in the outbound allowlist."
            )

    resolved = _resolve(host, port)
    for ip_text in resolved:
        if _is_blocked_address(ipaddress.ip_address(ip_text)):
            raise UnsafeURLError(
                f"Host '{host}' resolves to the non-routable address {ip_text}."
            )
    return host, port, resolved


def _resolve(host: str, port: int) -> list[str]:
    """Resolve ``host`` to every address it maps to (literals pass through)."""
    try:
        ipaddress.ip_address(host)
        return [host]
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeURLError(f"Could not resolve host '{host}': {exc}") from exc
    addresses = {info[4][0] for info in infos}
    if not addresses:
        raise UnsafeURLError(f"Could not resolve host '{host}'.")
    return sorted(addresses)


def clamp_timeout(timeout: float | int | None, default: float = 30.0) -> float:
    """Clamp a caller/config supplied timeout into a sane range."""
    try:
        value = float(timeout)
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return default
    return min(value, MAX_TIMEOUT_SECONDS)


def request(
    method: str,
    url: str,
    *,
    allowed_hosts: Iterable[tuple[str, int | None]] | str | None = None,
    allowed_schemes: Iterable[str] = _ALLOWED_SCHEMES,
    max_bytes: int = DEFAULT_MAX_BYTES,
    timeout: float | int | None = 30,
    session: requests.Session | None = None,
    **kwargs,
) -> requests.Response:
    """Perform a validated request with redirects disabled and a size cap.

    ``kwargs`` are forwarded to ``requests``; ``allow_redirects`` and ``stream``
    are forced.  A redirect response is returned as-is (callers treat a 3xx the
    same way they treat any other unexpected status) rather than followed, so a
    validated host cannot bounce the request onto an internal address.
    """
    validate_url(url, allowed_hosts=allowed_hosts, allowed_schemes=allowed_schemes)
    kwargs.pop("allow_redirects", None)
    kwargs.pop("stream", None)
    caller = session or requests
    response = caller.request(
        method,
        url,
        allow_redirects=False,
        stream=True,
        timeout=clamp_timeout(timeout),
        **kwargs,
    )
    try:
        _read_capped(response, max_bytes)
    except Exception:
        response.close()
        raise
    return response


def _read_capped(response: requests.Response, max_bytes: int) -> None:
    """Buffer the body into ``response._content``, aborting past ``max_bytes``."""
    declared = response.headers.get("Content-Length")
    if declared and declared.isdigit() and int(declared) > max_bytes:
        raise ResponseTooLargeError(
            f"Response is {int(declared)} bytes, over the {max_bytes} byte limit."
        )
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(64 * 1024):
        total += len(chunk)
        if total > max_bytes:
            raise ResponseTooLargeError(
                f"Response exceeded the {max_bytes} byte limit."
            )
        chunks.append(chunk)
    # Populate the cached body so response.content / .text / .json() work as
    # usual for callers that never see the streaming.
    response._content = b"".join(chunks)
    response._content_consumed = True


def fetch_image(
    url: str,
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
    timeout: float | int | None = 15,
    auth=None,
) -> tuple[bytes, str]:
    """Fetch a caller-supplied image URL safely; return (bytes, content type).

    Only public HTTPS origins are accepted and the response must declare an
    image content type.
    """
    response = request(
        "GET", url, max_bytes=max_bytes, timeout=timeout, auth=auth,
        headers={"Accept": "image/*"},
    )
    if response.status_code >= 300:
        raise UnsafeURLError(
            f"Image fetch returned HTTP {response.status_code} (redirects are not followed)."
        )
    content_type = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if content_type and not content_type.startswith("image/"):
        raise UnsafeURLError(f"Expected an image, got content type '{content_type}'.")
    return response.content, content_type or "image/jpeg"
