"""Coordinator-side liveness tracking for registered nodes.

Each data node registers itself with the coordinator at startup and then
sends periodic heartbeats. The coordinator uses this directory to answer
``GET /cluster/nodes`` and to detect which nodes are reachable so the
operator can see outages before a client request fails.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class NodeLiveness:
    """Snapshot of one node's last contact with the coordinator."""

    node_id: str
    host: str
    port: int
    role: str
    last_seen: float
    registered_at: float

    def age_seconds(self, now: float) -> float:
        return max(0.0, now - self.last_seen)


class NodeDirectory:
    """Thread-safe registry of recently-seen nodes.

    All public methods take a brief lock. The class is small enough that
    fine-grained locking is not needed.
    """

    def __init__(
        self,
        *,
        alive_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if alive_timeout <= 0:
            raise ValueError("alive_timeout must be positive")
        self._lock = threading.Lock()
        self._nodes: dict[str, NodeLiveness] = {}
        self._alive_timeout = float(alive_timeout)
        self._clock = clock

    @property
    def alive_timeout(self) -> float:
        return self._alive_timeout

    # ------------------------------------------------------------------
    # Mutators
    # ------------------------------------------------------------------

    def register(
        self,
        *,
        node_id: str,
        host: str,
        port: int,
        role: str,
    ) -> NodeLiveness:
        """Add or refresh a node entry. Always bumps ``last_seen``."""
        if not node_id:
            raise ValueError("node_id is required")
        now = self._clock()
        with self._lock:
            existing = self._nodes.get(node_id)
            registered_at = existing.registered_at if existing else now
            entry = NodeLiveness(
                node_id=node_id,
                host=host,
                port=port,
                role=role,
                last_seen=now,
                registered_at=registered_at,
            )
            self._nodes[node_id] = entry
            return entry

    def heartbeat(self, node_id: str) -> NodeLiveness | None:
        """Refresh ``last_seen`` for ``node_id``; return None if unknown."""
        now = self._clock()
        with self._lock:
            existing = self._nodes.get(node_id)
            if existing is None:
                return None
            updated = NodeLiveness(
                node_id=existing.node_id,
                host=existing.host,
                port=existing.port,
                role=existing.role,
                last_seen=now,
                registered_at=existing.registered_at,
            )
            self._nodes[node_id] = updated
            return updated

    def cleanup_expired(self) -> list[str]:
        """Drop nodes whose ``last_seen`` is older than ``alive_timeout``."""
        now = self._clock()
        cutoff = now - self._alive_timeout
        expired: list[str] = []
        with self._lock:
            for node_id, entry in list(self._nodes.items()):
                if entry.last_seen < cutoff:
                    del self._nodes[node_id]
                    expired.append(node_id)
        return expired

    def remove(self, node_id: str) -> bool:
        with self._lock:
            return self._nodes.pop(node_id, None) is not None

    # ------------------------------------------------------------------
    # Readers
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a plain-dict snapshot suitable for JSON serialization."""
        now = self._clock()
        with self._lock:
            return {
                node_id: {
                    "node_id": entry.node_id,
                    "host": entry.host,
                    "port": entry.port,
                    "role": entry.role,
                    "last_seen": entry.last_seen,
                    "registered_at": entry.registered_at,
                    "age_seconds": round(entry.age_seconds(now), 3),
                }
                for node_id, entry in self._nodes.items()
            }

    def alive_ids(self) -> set[str]:
        with self._lock:
            return set(self._nodes.keys())