"""Shared Raft data types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, TypedDict


class RaftRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftCommand(TypedDict, total=False):
    type: Literal["add_document", "update_document", "delete_document"]
    collection: str
    document: dict[str, Any]
    document_id: str
    changes: dict[str, Any]


@dataclass(frozen=True)
class RaftLogEntry:
    index: int
    term: int
    command: RaftCommand


@dataclass
class RaftPersistentState:
    current_term: int = 0
    voted_for: str | None = None
    commit_index: int = 0
    last_applied: int = 0
