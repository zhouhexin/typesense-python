from typesense_lite.raft_types import RaftLogEntry, RaftRole


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
