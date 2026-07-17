"""Cross-machine HTTP helpers.

Centralizes the timeouts and retry policy that the coordinator and Raft
runtime use when talking to remote nodes. The defaults are tuned for a
LAN deployment: short connect timeout, generous read/write timeout, and a
single retry with exponential backoff for idempotent reads.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Mapping

import httpx


_LOGGER = logging.getLogger(__name__)


def cross_machine_timeout(
    *,
    connect: float = 2.0,
    read: float = 5.0,
    write: float = 5.0,
    pool: float = 5.0,
) -> httpx.Timeout:
    """Default httpx.Timeout for cross-machine HTTP traffic."""
    return httpx.Timeout(connect=connect, read=read, write=write, pool=pool)


def make_cross_machine_client(
    *,
    timeout: httpx.Timeout | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Build a direct cluster client with cross-machine timeout defaults."""
    # Cluster members are trusted, explicitly configured endpoints. Routing
    # them through a workstation HTTP proxy can turn healthy local nodes into
    # misleading 502 responses.
    kwargs.setdefault("trust_env", False)
    return httpx.AsyncClient(
        timeout=timeout if timeout is not None else cross_machine_timeout(),
        **kwargs,
    )


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    retries: int = 1,
    backoff: float = 0.5,
    params: Mapping[str, Any] | None = None,
) -> httpx.Response:
    """GET with a single retry on transport errors.

    Only safe for idempotent reads. ``retries=1`` means one retry after the
    initial attempt (so two tries total).
    """
    last_error: httpx.HTTPError | None = None
    for attempt in range(retries + 1):
        start = time.monotonic()
        try:
            response = await client.get(url, params=params)
            log_http(url, response, start=start)
            response.raise_for_status()
            return response
        except httpx.HTTPError as error:
            last_error = error
            _LOGGER.warning(
                "GET %s failed on attempt %d/%d: %s",
                url, attempt + 1, retries + 1, error,
            )
            if attempt < retries:
                await asyncio.sleep(backoff * (2 ** attempt))
    assert last_error is not None
    raise last_error


def log_http(
    url: str,
    response: httpx.Response,
    *,
    start: float | None = None,
    level: int = logging.INFO,
) -> None:
    """Emit a structured log line ``http <url> -> <status> in <ms>ms``."""
    elapsed_ms = (time.monotonic() - start) * 1000 if start is not None else 0.0
    _LOGGER.log(
        level,
        "http %s -> %d in %.1fms",
        url, response.status_code, elapsed_ms,
    )
