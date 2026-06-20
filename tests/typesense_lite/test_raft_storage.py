from typesense_lite.raft_storage import RaftStorage
from typesense_lite.raft_types import RaftLogEntry, RaftPersistentState


def test_storage_loads_default_state(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)

    state = storage.load_state()

    assert state == RaftPersistentState()


def test_storage_saves_and_loads_state(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)

    storage.save_state(
        RaftPersistentState(
            current_term=3,
            voted_for="node-2",
            commit_index=7,
            last_applied=6,
        )
    )

    assert storage.load_state() == RaftPersistentState(
        current_term=3,
        voted_for="node-2",
        commit_index=7,
        last_applied=6,
    )


def test_storage_appends_and_loads_log_entries(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)
    entry = RaftLogEntry(
        index=1,
        term=1,
        command={
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        },
    )

    storage.append_entries([entry])

    assert storage.load_log() == [entry]
