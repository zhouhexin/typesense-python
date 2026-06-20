"""Pure Raft state machine logic."""

from __future__ import annotations

from .raft_types import RaftLogEntry, RaftPersistentState, RaftRole


class RaftCore:
    """In-memory Raft state for one node in one shard group."""

    def __init__(
        self,
        *,
        node_id: str,
        members: list[str],
        state: RaftPersistentState | None = None,
        log: list[RaftLogEntry] | None = None,
    ) -> None:
        self.node_id = node_id
        self.members = list(members)
        self.state = state or RaftPersistentState()
        self.log = list(log or [])
        self.role = RaftRole.FOLLOWER
        self.leader_id: str | None = None

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def handle_request_vote(
        self,
        *,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> dict[str, int | bool]:
        if term < self.state.current_term:
            return {"term": self.state.current_term, "vote_granted": False}

        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self.role = RaftRole.FOLLOWER
            self.leader_id = None

        log_is_up_to_date = (
            last_log_term > self.last_log_term
            or (
                last_log_term == self.last_log_term
                and last_log_index >= self.last_log_index
            )
        )
        can_vote = self.state.voted_for in {None, candidate_id}
        vote_granted = can_vote and log_is_up_to_date and candidate_id in self.members

        if vote_granted:
            self.state.voted_for = candidate_id

        return {"term": self.state.current_term, "vote_granted": vote_granted}

    def handle_append_entries(
        self,
        *,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[RaftLogEntry],
        leader_commit: int,
    ) -> dict[str, int | bool]:
        if term < self.state.current_term:
            return {
                "term": self.state.current_term,
                "success": False,
                "match_index": self.last_log_index,
            }

        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None

        self.role = RaftRole.FOLLOWER
        self.leader_id = leader_id

        if prev_log_index > 0:
            previous = self._entry_at(prev_log_index)
            if previous is None or previous.term != prev_log_term:
                return {
                    "term": self.state.current_term,
                    "success": False,
                    "match_index": self.last_log_index,
                }

        for entry in entries:
            existing = self._entry_at(entry.index)
            if existing is not None and existing.term != entry.term:
                self.log = [item for item in self.log if item.index < entry.index]
            if self._entry_at(entry.index) is None:
                self.log.append(entry)

        if leader_commit > self.state.commit_index:
            self.state.commit_index = min(leader_commit, self.last_log_index)

        return {
            "term": self.state.current_term,
            "success": True,
            "match_index": self.last_log_index,
        }

    def _entry_at(self, index: int) -> RaftLogEntry | None:
        for entry in self.log:
            if entry.index == index:
                return entry
        return None
