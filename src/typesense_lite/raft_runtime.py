"""Runtime wrapper for one node in one shard Raft group."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from .raft_core import RaftCore
from .raft_storage import RaftStorage
from .raft_types import RaftLogEntry


class RaftRuntime:
    """Coordinates durable Raft state and protocol handlers for one shard."""

    def __init__(
        self,
        *,
        node_id: str,
        shard_id: int,
        members: list[str],
        data_dir: str | Path,
        peer_urls: dict[str, str],
        client: httpx.AsyncClient,
        apply_command: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self.node_id = node_id
        self.shard_id = shard_id
        self.members = members
        self.peer_urls = peer_urls
        self.client = client
        self.apply_command = apply_command
        self.storage = RaftStorage(data_dir, node_id=node_id, shard_id=shard_id)
        self.core = RaftCore(
            node_id=node_id,
            members=members,
            state=self.storage.load_state(),
            log=self.storage.load_log(),
        )

    def state(self) -> dict[str, Any]:
        return {
            "node": self.node_id,
            "shard_id": self.shard_id,
            "role": self.core.role.value,
            "current_term": self.core.state.current_term,
            "voted_for": self.core.state.voted_for,
            "leader_id": self.core.leader_id,
            "commit_index": self.core.state.commit_index,
            "last_applied": self.core.state.last_applied,
            "last_log_index": self.core.last_log_index,
            "last_log_term": self.core.last_log_term,
            "members": self.members,
        }

    async def handle_request_vote(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.core.handle_request_vote(
            term=int(payload["term"]),
            candidate_id=str(payload["candidate_id"]),
            last_log_index=int(payload["last_log_index"]),
            last_log_term=int(payload["last_log_term"]),
        )
        self.storage.save_state(self.core.state)
        return result

    async def handle_append_entries(self, payload: dict[str, Any]) -> dict[str, Any]:
        entries = [
            RaftLogEntry(
                index=int(entry["index"]),
                term=int(entry["term"]),
                command=entry["command"],
            )
            for entry in payload.get("entries", [])
        ]
        result = self.core.handle_append_entries(
            term=int(payload["term"]),
            leader_id=str(payload["leader_id"]),
            prev_log_index=int(payload["prev_log_index"]),
            prev_log_term=int(payload["prev_log_term"]),
            entries=entries,
            leader_commit=int(payload["leader_commit"]),
        )
        self.storage.save_state(self.core.state)
        self.storage.replace_log(self.core.log)
        return result
