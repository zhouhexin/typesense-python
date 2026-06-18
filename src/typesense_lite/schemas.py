"""Shared schemas for the Typesense Lite demo."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

Document = dict[str, Any]
SearchHit = dict[str, Any]


@dataclass(frozen=True)
class NodeInfo:
    id: str
    host: str
    port: int

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


@dataclass(frozen=True)
class ShardPlacement:
    shard_id: int
    primary: str
    replicas: tuple[str, ...]

