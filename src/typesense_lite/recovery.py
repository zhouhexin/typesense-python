"""Recovery state tracking for static Typesense Lite data nodes."""

from __future__ import annotations

from threading import RLock
from typing import Any, Literal

RecoveryStatus = Literal[
    "starting",
    "recovering",
    "rebuilding",
    "catching_up",
    "validating",
    "healthy",
    "failed",
]


class NodeRecoveryState:
    """Thread-safe aggregation of recovery status for one data-node process."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._lock = RLock()
        self._status: RecoveryStatus = "starting"
        self._error: str | None = None
        self._shards: dict[int, dict[str, Any]] = {}

    def set_shard(self, shard_id: int, payload: dict[str, Any]) -> None:
        with self._lock:
            self._shards[shard_id] = dict(payload)
            self._status = _aggregate_status(self._shards.values(), self._error)

    def fail_shard(self, shard_id: int, error: str) -> None:
        with self._lock:
            current = dict(self._shards.get(shard_id, {}))
            current.update({"status": "failed", "ready": False, "error": error})
            self._shards[shard_id] = current
            self._status = "failed"
            self._error = error

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            shards = {
                str(shard_id): dict(payload)
                for shard_id, payload in sorted(self._shards.items())
            }
            ready = bool(shards) and all(shard_is_ready(item) for item in shards.values())
            status = self._status
            if not ready and status == "healthy":
                status = _aggregate_status(shards.values(), self._error)
            return {
                "node_id": self.node_id,
                "status": status,
                "ready": ready and status == "healthy",
                "error": self._error,
                "shards": shards,
            }


def shard_is_ready(payload: dict[str, Any] | None) -> bool:
    return bool(
        payload
        and payload.get("ready") is True
        and payload.get("status") == "healthy"
    )


def node_is_ready(payload: dict[str, Any] | None) -> bool:
    return bool(
        payload
        and payload.get("ready") is True
        and payload.get("status") == "healthy"
    )


def _aggregate_status(shards: Any, error: str | None) -> RecoveryStatus:
    if error:
        return "failed"
    shard_list = list(shards)
    if not shard_list:
        return "starting"

    statuses = {str(shard.get("status", "starting")) for shard in shard_list}
    for status in ("failed", "rebuilding", "validating", "catching_up", "recovering"):
        if status in statuses:
            return status  # type: ignore[return-value]
    if statuses == {"healthy"}:
        return "healthy"
    return "starting"
