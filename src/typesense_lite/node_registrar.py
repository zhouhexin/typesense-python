"""Best-effort coordinator registration + heartbeat for a data node.

Data nodes call :meth:`NodeRegistrar.register_once` once at startup and then
:meth:`NodeRegistrar.heartbeat_once` on a fixed interval via
:meth:`NodeRegistrar.start`. Failures are logged but never raise: the data
node must keep working even when the coordinator is temporarily unreachable.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

import httpx

from .http_client import make_cross_machine_client


_LOGGER = logging.getLogger(__name__)


class NodeRegistrar:
    """Background-driven coordinator liveness client."""

    def __init__(
        self,
        *,
        coordinator_url: str,
        node_id: str,
        advertise_host: str,
        advertise_port: int,
        role: str = "node",
        interval: float = 5.0,
        client: httpx.AsyncClient | None = None,
        clock=asyncio.sleep,
    ) -> None:
        if not coordinator_url:
            raise ValueError("coordinator_url is required")
        if not node_id:
            raise ValueError("node_id is required")
        if interval <= 0:
            raise ValueError("interval must be positive")

        self._base = coordinator_url.rstrip("/")
        self._node_id = node_id
        self._advertise_host = advertise_host
        self._advertise_port = int(advertise_port)
        self._role = role
        self._interval = float(interval)
        self._owns_client = client is None
        self._client = client or make_cross_machine_client()
        self._sleep = clock
        self._task: asyncio.Task[None] | None = None

    @property
    def node_id(self) -> str:
        return self._node_id

    async def aclose(self) -> None:
        await self.stop()
        if self._owns_client:
            await self._client.aclose()

    async def start(self) -> None:
        """Spawn the background heartbeat loop."""
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name=f"register-{self._node_id}")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self.register_once()
                backoff = 1.0
                while True:
                    await self._sleep(self._interval)
                    try:
                        await self.heartbeat_once()
                    except httpx.HTTPError as error:
                        _LOGGER.warning(
                            "heartbeat for %s failed: %s", self._node_id, error,
                        )
            except asyncio.CancelledError:
                raise
            except httpx.HTTPError as error:
                _LOGGER.warning(
                    "registration for %s failed (retry in %.1fs): %s",
                    self._node_id, backoff, error,
                )
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def register_once(self) -> dict[str, Any]:
        """POST a single registration. Raises on transport/HTTP errors."""
        response = await self._client.post(
            f"{self._base}/internal/cluster/nodes/register",
            json={
                "node_id": self._node_id,
                "host": self._advertise_host,
                "port": self._advertise_port,
                "role": self._role,
            },
        )
        response.raise_for_status()
        return response.json()

    async def heartbeat_once(self) -> dict[str, Any]:
        """PUT a single heartbeat. Raises on transport/HTTP errors."""
        response = await self._client.put(
            f"{self._base}/internal/cluster/nodes/{self._node_id}/heartbeat",
        )
        response.raise_for_status()
        return response.json()