"""Coordinator-side Raft leader discovery."""

from __future__ import annotations

from typing import Any

import httpx

from .cluster import ClusterMap
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
        hinted_leader_id: str | None = None
        for node in self.cluster.get_shard_voters(shard_id):
            try:
                response = await self.client.get(
                    f"{node.url}/internal/raft/{shard_id}/state"
                )
                response.raise_for_status()
            except httpx.HTTPError:
                continue

            payload: dict[str, Any] = response.json()
            if payload.get("role") == "leader":
                self._leaders[shard_id] = node
                return node
            leader_id = payload.get("leader_id")
            if isinstance(leader_id, str) and leader_id:
                hinted_leader_id = leader_id

        if hinted_leader_id and hinted_leader_id in self.cluster.nodes:
            leader = self.cluster.nodes[hinted_leader_id]
            self._leaders[shard_id] = leader
            return leader

        self._leaders.pop(shard_id, None)
        return None

    def invalidate(self, shard_id: int) -> None:
        self._leaders.pop(shard_id, None)
