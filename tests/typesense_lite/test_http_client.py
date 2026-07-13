from __future__ import annotations

import asyncio
import logging

import httpx
import pytest

from typesense_lite.http_client import (
    cross_machine_timeout,
    get_with_retry,
    log_http,
    make_cross_machine_client,
)


pytestmark = [pytest.mark.unit, pytest.mark.http]


def test_cross_machine_timeout_has_expected_components() -> None:
    timeout = cross_machine_timeout()

    assert timeout.connect == 2.0
    assert timeout.read == 5.0
    assert timeout.write == 5.0
    assert timeout.pool == 5.0


def test_make_cross_machine_client_applies_default_timeout() -> None:
    client = make_cross_machine_client()

    assert client.timeout.connect == 2.0
    assert client.timeout.read == 5.0


def test_make_cross_machine_client_respects_explicit_timeout() -> None:
    custom = httpx.Timeout(connect=1.0, read=2.0, write=2.0, pool=2.0)
    client = make_cross_machine_client(timeout=custom)

    assert client.timeout == custom


def test_log_http_emits_structured_message(
    caplog: pytest.LogCaptureFixture,
) -> None:
    response = httpx.Response(200, request=httpx.Request("GET", "http://x/y"))
    with caplog.at_level(logging.INFO, logger="typesense_lite.http_client"):
        log_http("http://x/y", response)
    assert any("http://x/y -> 200" in rec.message for rec in caplog.records)


def test_get_with_retry_succeeds_after_transient_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A single 503 followed by 200 should succeed and log a warning."""
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return httpx.Response(503, json={"err": "busy"})
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)

    async def run() -> httpx.Response:
        async with make_cross_machine_client(transport=transport) as client:
            with caplog.at_level(logging.WARNING, logger="typesense_lite.http_client"):
                return await get_with_retry(client, "http://x/y", retries=1, backoff=0.01)

    response = asyncio.run(run())

    assert response.status_code == 200
    assert attempts["count"] == 2
    assert any("failed on attempt 1/2" in rec.message for rec in caplog.records)


def test_get_with_retry_raises_after_exhausting_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    transport = httpx.MockTransport(handler)

    async def run() -> httpx.Response:
        async with make_cross_machine_client(transport=transport) as client:
            return await get_with_retry(client, "http://x/y", retries=1, backoff=0.01)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())


def test_get_with_retry_zero_retries_means_one_attempt() -> None:
    attempts = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        return httpx.Response(503)

    transport = httpx.MockTransport(handler)

    async def run() -> httpx.Response:
        async with make_cross_machine_client(transport=transport) as client:
            return await get_with_retry(client, "http://x/y", retries=0, backoff=0.01)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(run())
    assert attempts["count"] == 1