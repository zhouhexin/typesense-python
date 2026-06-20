"""Durable local storage for per-shard Raft state."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .raft_types import RaftLogEntry, RaftPersistentState


class RaftStorage:
    """Store one node's durable Raft state for one shard."""

    def __init__(self, data_dir: str | Path, *, node_id: str, shard_id: int) -> None:
        self.root = Path(data_dir) / "raft" / node_id / f"shard-{shard_id}"
        self.state_path = self.root / "state.json"
        self.log_path = self.root / "log.jsonl"

    def load_state(self) -> RaftPersistentState:
        if not self.state_path.exists():
            return RaftPersistentState()

        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        return RaftPersistentState(
            current_term=int(payload.get("current_term", 0)),
            voted_for=payload.get("voted_for"),
            commit_index=int(payload.get("commit_index", 0)),
            last_applied=int(payload.get("last_applied", 0)),
        )

    def save_state(self, state: RaftPersistentState) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(asdict(state), ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )

    def load_log(self) -> list[RaftLogEntry]:
        if not self.log_path.exists():
            return []

        entries: list[RaftLogEntry] = []
        for line in self.log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            entries.append(
                RaftLogEntry(
                    index=int(payload["index"]),
                    term=int(payload["term"]),
                    command=payload["command"],
                )
            )
        return entries

    def replace_log(self, entries: list[RaftLogEntry]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        lines = [
            json.dumps(asdict(entry), ensure_ascii=False, sort_keys=True)
            for entry in entries
        ]
        self.log_path.write_text(
            "\n".join(lines) + ("\n" if lines else ""),
            encoding="utf-8",
        )

    def append_entries(self, entries: list[RaftLogEntry]) -> None:
        if not entries:
            return

        current = self.load_log()
        current.extend(entries)
        self.replace_log(current)
