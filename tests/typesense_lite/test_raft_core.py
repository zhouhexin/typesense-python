from typesense_lite.raft_types import RaftLogEntry, RaftRole

from typesense_lite.raft_core import RaftCore


def test_raft_role_values() -> None:
    assert RaftRole.FOLLOWER.value == "follower"
    assert RaftRole.CANDIDATE.value == "candidate"
    assert RaftRole.LEADER.value == "leader"


def test_raft_log_entry_shape() -> None:
    entry = RaftLogEntry(
        index=1,
        term=2,
        command={
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        },
    )

    assert entry.index == 1
    assert entry.term == 2
    assert entry.command["type"] == "add_document"


def test_request_vote_grants_vote_for_newer_term() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2"])

    result = core.handle_request_vote(
        term=1,
        candidate_id="node-2",
        last_log_index=0,
        last_log_term=0,
    )

    assert result == {"term": 1, "vote_granted": True}
    assert core.state.current_term == 1
    assert core.state.voted_for == "node-2"


def test_request_vote_rejects_second_candidate_in_same_term() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2", "node-3"])
    core.handle_request_vote(
        term=1,
        candidate_id="node-2",
        last_log_index=0,
        last_log_term=0,
    )

    result = core.handle_request_vote(
        term=1,
        candidate_id="node-3",
        last_log_index=0,
        last_log_term=0,
    )

    assert result == {"term": 1, "vote_granted": False}
    assert core.state.voted_for == "node-2"


def test_append_entries_accepts_heartbeat_from_new_leader() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2"])

    result = core.handle_append_entries(
        term=1,
        leader_id="node-2",
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )

    assert result == {"term": 1, "success": True, "match_index": 0}
    assert core.leader_id == "node-2"
    assert core.role == RaftRole.FOLLOWER


def test_append_entries_rejects_when_previous_log_does_not_match() -> None:
    core = RaftCore(
        node_id="node-1",
        members=["node-1", "node-2"],
        log=[
            RaftLogEntry(
                index=1,
                term=1,
                command={
                    "type": "add_document",
                    "collection": "books",
                    "document": {"id": "a"},
                },
            )
        ],
    )

    result = core.handle_append_entries(
        term=2,
        leader_id="node-2",
        prev_log_index=1,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    assert result == {"term": 2, "success": False, "match_index": 1}


def test_append_entries_replaces_conflicting_entries() -> None:
    original_entry = RaftLogEntry(
        index=1,
        term=1,
        command={
            "type": "add_document",
            "collection": "books",
            "document": {"id": "a"},
        },
    )
    core = RaftCore(
        node_id="node-1",
        members=["node-1", "node-2"],
        log=[
            original_entry,
            RaftLogEntry(
                index=2,
                term=1,
                command={
                    "type": "add_document",
                    "collection": "books",
                    "document": {"id": "b"},
                },
            ),
        ],
    )
    new_entry = RaftLogEntry(
        index=2,
        term=2,
        command={
            "type": "add_document",
            "collection": "books",
            "document": {"id": "c"},
        },
    )

    result = core.handle_append_entries(
        term=2,
        leader_id="node-2",
        prev_log_index=1,
        prev_log_term=1,
        entries=[new_entry],
        leader_commit=2,
    )

    assert result == {"term": 2, "success": True, "match_index": 2}
    assert core.log == [original_entry, new_entry]
    assert core.state.commit_index == 2
