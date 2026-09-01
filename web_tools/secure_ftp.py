"""FTP over TLS for the supplier feeds.

Plain FTP puts the supplier password on the wire in the clear and lets anyone
on the path rewrite the stock/price CSV that drives Shopify catalog and
inventory decisions.  Every download goes through :func:`fetch_bytes`, which
negotiates ``AUTH TLS``, calls ``prot_p()`` so the *data* connection is
encrypted as well as the control connection, validates the server certificate,
and caps the download size.

Certificate validation can be relaxed per call (a supplier with a self-signed
certificate is common) — the transport stays encrypted either way, and the
downgrade is logged so it is visible rather than assumed.
"""
from __future__ import annotations

import ftplib
import logging
import ssl

logger = logging.getLogger(__name__)

DEFAULT_MAX_BYTES = 256 * 1024 * 1024
DEFAULT_TIMEOUT = 120


class FeedTooLargeError(RuntimeError):
    """Raised when a supplier feed exceeds its size budget."""


def _tls_context(verify: bool, host: str) -> ssl.SSLContext:
    context = ssl.create_default_context()
    if not verify:
        logger.warning(
            "FTPS certificate validation is disabled for %s (set the *_FTP_TLS_VERIFY "
            "environment variable to 1 once the supplier presents a trusted certificate).",
            host,
        )
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def fetch_bytes(
    host: str,
    username: str,
    password: str,
    remote_path: str,
    *,
    port: int = 21,
    timeout: int = DEFAULT_TIMEOUT,
    verify: bool = True,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> bytes:
    """Download ``remote_path`` over FTPS and return its bytes.

    Args:
        host: FTP server hostname.
        username: Login user.
        password: Login password.
        remote_path: Path passed to ``RETR``.
        port: Control-connection port (explicit FTPS uses 21).
        timeout: Socket timeout in seconds.
        verify: Whether the server certificate must validate.
        max_bytes: Abort once this many bytes have been received.

    Raises:
        FeedTooLargeError: When the file exceeds ``max_bytes``.
        ftplib.all_errors: On any FTP/TLS failure.
    """
    if not host:
        raise ValueError("An FTP host is required.")

    chunks: list[bytes] = []
    total = 0

    def _collect(chunk: bytes) -> None:
        nonlocal total
        total += len(chunk)
        if total > max_bytes:
            raise FeedTooLargeError(
                f"{host}:{remote_path} exceeded the {max_bytes} byte limit."
            )
        chunks.append(chunk)

    with ftplib.FTP_TLS(context=_tls_context(verify, host), timeout=timeout) as ftp:
        ftp.connect(host, port, timeout)
        # login() issues AUTH TLS before the credentials are sent; prot_p()
        # then switches the data connection to TLS as well.
        ftp.login(user=username, passwd=password)
        ftp.prot_p()
        ftp.retrbinary(f"RETR {remote_path}", _collect)

    return b"".join(chunks)
