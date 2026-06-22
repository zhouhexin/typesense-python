"""Runtime wrapper for one node in one shard Raft group."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .raft_core import RaftCore
from .raft_storage import RaftStorage
from .raft_types import RaftLogEntry, RaftRole


@dataclass
class PeerProgress:
    next_index: int
    match_index: int = 0


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
        election_timeout: float | None = None,
        heartbeat_interval: float = 0.2,
    ) -> None:
        self.node_id = node_id
        self.shard_id = shard_id
        self.members = members
        self.peer_urls = peer_urls
        self.client = client
        self.apply_command = apply_command
        member_index = members.index(node_id) if node_id in members else len(members)
        self.election_timeout = (
            election_timeout if election_timeout is not None else 0.5 + member_index * 0.3
        )
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat_at = time.monotonic()
        self._tasks: list[asyncio.Task[None]] = []
        self.peer_progress: dict[str, PeerProgress] = {}
        self.storage = RaftStorage(data_dir, node_id=node_id, shard_id=shard_id)
        self.core = RaftCore(
            node_id=node_id,
            members=members,
            state=self.storage.load_state(),
            log=self.storage.load_log(),
        )

    def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(self._run_election_loop()),
            asyncio.create_task(self._run_heartbeat_loop()),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks = []

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
        if result.get("vote_granted") is True:
            self.last_heartbeat_at = time.monotonic()
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
        self.storage.replace_log(self.core.log)
        if result.get("success") is True:
            self.last_heartbeat_at = time.monotonic()
            self._apply_committed_entries()
        self.storage.save_state(self.core.state)
        return result

    async def start_election(self) -> None:
        self.core.role = RaftRole.CANDIDATE
        self.core.state.current_term += 1
        self.core.state.voted_for = self.node_id
        self.core.leader_id = None
        self.storage.save_state(self.core.state)

        votes = 1
        request = {
            "term": self.core.state.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self.core.last_log_index,
            "last_log_term": self.core.last_log_term,
        }
        for peer_url in self.peer_urls.values():
            try:
                response = await self.client.post(
                    f"{peer_url}/internal/raft/{self.shard_id}/request_vote",
                    json=request,
                )
                response.raise_for_status()
            except httpx.HTTPError:
                continue

            payload = response.json()
            peer_term = int(payload["term"])
            if peer_term > self.core.state.current_term:
                self.core.state.current_term = peer_term
                self.core.state.voted_for = None
                self.core.role = RaftRole.FOLLOWER
                self.storage.save_state(self.core.state)
                return
            if payload.get("vote_granted") is True:
                votes += 1

        if votes >= self._majority():
            self._become_leader()

    def _majority(self) -> int:
        return len(self.members) // 2 + 1

    async def send_heartbeat(self) -> None:
        if self.core.role is not RaftRole.LEADER:
            return

        for peer_id, peer_url in self.peer_urls.items():
            await self._replicate_to_peer(peer_id, peer_url)

    async def submit_command(self, command: dict[str, Any]) -> dict[str, Any]:
        if self.core.role is not RaftRole.LEADER:
            return {
                "ok": False,
                "error": "not leader",
                "leader_id": self.core.leader_id,
                "term": self.core.state.current_term,
            }

        entry = RaftLogEntry(
            index=self.core.last_log_index + 1,
            term=self.core.state.current_term,
            command=command,
        )
        self.core.log.append(entry)
        self.storage.replace_log(self.core.log)

        replicated = 1
        for peer_id, peer_url in self.peer_urls.items():
            if await self._replicate_to_peer(peer_id, peer_url):
                if self.peer_progress[peer_id].match_index < entry.index:
                    continue
                replicated += 1

        if replicated < self._majority():
            return {
                "ok": False,
                "error": "no raft majority available",
                "leader": self.node_id,
                "term": self.core.state.current_term,
            }

        self.core.state.commit_index = entry.index
        result = self._apply_committed_entries()
        self.storage.save_state(self.core.state)
        return {
            "ok": True,
            "shard_id": self.shard_id,
            "leader": self.node_id,
            "term": self.core.state.current_term,
            "commit_index": self.core.state.commit_index,
            "result": result,
        }

    def _log_term(self, index: int) -> int:
        if index == 0:
            return 0
        for entry in self.core.log:
            if entry.index == index:
                return entry.term
        return 0

    def _entries_from(self, start_index: int) -> list[RaftLogEntry]:
        return [entry for entry in self.core.log if entry.index >= start_index]

    async def _replicate_to_peer(self, peer_id: str, peer_url: str) -> bool:
        progress = self.peer_progress.setdefault(
            peer_id,
            PeerProgress(next_index=self.core.last_log_index + 1),
        )

        while progress.next_index >= 1:
            prev_log_index = progress.next_index - 1
            payload = {
                "term": self.core.state.current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": self._log_term(prev_log_index),
                "entries": [
                    {
                        "index": entry.index,
                        "term": entry.term,
                        "command": entry.command,
                    }
                    for entry in self._entries_from(progress.next_index)
                ],
                "leader_commit": self.core.state.commit_index,
            }

            try:
                response = await self.client.post(
                    f"{peer_url}/internal/raft/{self.shard_id}/append_entries",
                    json=payload,
                )
                response.raise_for_status()
            except httpx.HTTPError:
                return False

            result = response.json()
            peer_term = int(result["term"])
            if peer_term > self.core.state.current_term:
                self.core.state.current_term = peer_term
                self.core.state.voted_for = None
                self.core.role = RaftRole.FOLLOWER
                self.core.leader_id = None
                self.storage.save_state(self.core.state)
                return False

            if result.get("success") is True:
                match_index = int(result["match_index"])
                progress.match_index = match_index
                progress.next_index = match_index + 1
                return True

            progress.next_index = max(1, progress.next_index - 1)

        return False

    def _become_leader(self) -> None:
        self.core.role = RaftRole.LEADER
        self.core.leader_id = self.node_id
        next_index = self.core.last_log_index + 1
        self.peer_progress = {
            peer_id: PeerProgress(next_index=next_index)
            for peer_id in self.peer_urls
        }

    def _apply_committed_entries(self) -> dict[str, Any] | None:
        result = None
        for entry in sorted(self.core.log, key=lambda item: item.index):
            if entry.index <= self.core.state.last_applied:
                continue
            if entry.index > self.core.state.commit_index:
                break
            result = self.apply_command(entry.command)
            self.core.state.last_applied = entry.index
        return result

    async def _run_election_loop(self) -> None:
        while True:
            await asyncio.sleep(self.election_timeout)
            if self.core.role is RaftRole.LEADER:
                continue
            if time.monotonic() - self.last_heartbeat_at >= self.election_timeout:
                await self.start_election()

    async def _run_heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            await self.send_heartbeat()
