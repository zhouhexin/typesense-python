"""Coordinator-side Raft leader discovery."""

from __future__ import annotations

from typing import Any

import httpx

from .cluster import ClusterMap
from .http_client import get_with_retry
from .schemas import NodeInfo


class LeaderDirectory:
    """Discovers and caches the current Raft leader for each shard."""

    def __init__(self, cluster: ClusterMap, client: httpx.AsyncClient) -> None:
        self.cluster = cluster
        self.client = client
        self._leaders: dict[int, NodeInfo] = {}

    async def get_leader(self, shard_id: int) -> NodeInfo:
        leader = self._leaders.get(shard_id)
        if leader is not None:
            return leader

        discovered = await self.refresh(shard_id)
        if discovered is None:
            raise RuntimeError(f"no raft leader for shard {shard_id}")
        return discovered

    async def refresh(self, shard_id: int) -> NodeInfo | None:
        voters = {
            node.id: node for node in self.cluster.get_shard_voters(shard_id)
        }
        hinted_leader_id: str | None = None
        for node in voters.values():
            try:
                response = await get_with_retry(
                    self.client,
                    f"{node.url}/internal/raft/{shard_id}/state",
                )
            except httpx.HTTPError:
                continue

            payload: dict[str, Any] = response.json()
            if payload.get("role") == "leader":
                self._leaders[shard_id] = node
                return node
            leader_id = payload.get("leader_id")
            if isinstance(leader_id, str) and leader_id:
                hinted_leader_id = leader_id

        if hinted_leader_id and hinted_leader_id in voters:
            leader = voters[hinted_leader_id]
            if await self._verify_leader(shard_id, leader):
                self._leaders[shard_id] = leader
                return leader

        self._leaders.pop(shard_id, None)
        return None

    def invalidate(self, shard_id: int) -> None:
        self._leaders.pop(shard_id, None)

    async def _verify_leader(self, shard_id: int, node: NodeInfo) -> bool:
        try:
            response = await get_with_retry(
                self.client,
                f"{node.url}/internal/raft/{shard_id}/state",
            )
        except httpx.HTTPError:
            return False

        payload: dict[str, Any] = response.json()
        return payload.get("role") == "leader"
