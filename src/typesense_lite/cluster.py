"""Static cluster map and shard placement utilities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .schemas import NodeInfo, ShardPlacement

ClusterConfig = dict[str, Any]


class ClusterMap:
    """Static cluster configuration for the local distributed-search demo."""

    def __init__(
        self,
        *,
        coordinator: NodeInfo,
        shard_count: int,
        nodes: dict[str, NodeInfo],
        shards: dict[int, ShardPlacement],
    ) -> None:
        self.coordinator = coordinator
        self.shard_count = shard_count
        self.nodes = nodes
        self.shards = shards

    @classmethod
    def from_file(cls, path: str | Path) -> "ClusterMap":
        with Path(path).open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        return cls.from_dict(config)

    @classmethod
    def from_dict(cls, config: ClusterConfig) -> "ClusterMap":
        coordinator_data = config["coordinator"]
        coordinator = NodeInfo(
            id="coordinator",
            host=coordinator_data["host"],
            port=int(coordinator_data["port"]),
        )
        nodes = {
            node["id"]: NodeInfo(
                id=node["id"],
                host=node["host"],
                port=int(node["port"]),
            )
            for node in config["nodes"]
        }
        shards = {
            int(shard_id): ShardPlacement(
                shard_id=int(shard_id),
                primary=placement["primary"],
                replicas=tuple(placement.get("replicas", [])),
            )
            for shard_id, placement in config["shards"].items()
        }
        shard_count = int(config["shard_count"])
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if len(shards) != shard_count:
            raise ValueError("shard_count must match configured shards")

        cluster = cls(
            coordinator=coordinator,
            shard_count=shard_count,
            nodes=nodes,
            shards=shards,
        )
        cluster._validate()
        return cluster

    def to_dict(self) -> ClusterConfig:
        return {
            "coordinator": {
                "host": self.coordinator.host,
                "port": self.coordinator.port,
            },
            "shard_count": self.shard_count,
            "nodes": [
                {"id": node.id, "host": node.host, "port": node.port}
                for node in self.nodes.values()
            ],
            "shards": {
                str(shard_id): {
                    "primary": placement.primary,
                    "replicas": list(placement.replicas),
                }
                for shard_id, placement in self.shards.items()
            },
        }

    def get_shard_id(self, document_id: str) -> int:
        digest = hashlib.sha256(document_id.encode("utf-8")).hexdigest()
        return int(digest, 16) % self.shard_count

    def get_primary(self, shard_id: int) -> NodeInfo:
        return self.nodes[self.shards[shard_id].primary]

    def get_replicas(self, shard_id: int) -> list[NodeInfo]:
        return [self.nodes[node_id] for node_id in self.shards[shard_id].replicas]

    def get_shard_voters(self, shard_id: int) -> list[NodeInfo]:
        placement = self.shards[shard_id]
        return [
            self.nodes[placement.primary],
            *[self.nodes[node_id] for node_id in placement.replicas],
        ]

    def get_search_candidates(self, shard_id: int) -> list[NodeInfo]:
        return [self.get_primary(shard_id), *self.get_replicas(shard_id)]

    def node_url(self, node_id: str) -> str:
        return self.nodes[node_id].url

    def _validate(self) -> None:
        for shard_id, placement in self.shards.items():
            if shard_id < 0 or shard_id >= self.shard_count:
                raise ValueError(f"invalid shard id {shard_id}")
            if placement.primary not in self.nodes:
                raise ValueError(f"unknown primary node {placement.primary}")
            for replica in placement.replicas:
                if replica not in self.nodes:
                    raise ValueError(f"unknown replica node {replica}")
