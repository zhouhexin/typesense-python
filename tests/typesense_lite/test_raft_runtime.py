import httpx
import pytest
import anyio

from typesense_lite.raft_types import RaftLogEntry
from typesense_lite.raft_runtime import RaftRuntime


@pytest.mark.asyncio
async def test_runtime_exposes_request_vote(tmp_path) -> None:
    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: {"ok": True},
    )

    result = await runtime.handle_request_vote(
        {
            "term": 1,
            "candidate_id": "node-2",
            "last_log_index": 0,
            "last_log_term": 0,
        }
    )

    assert result == {"term": 1, "vote_granted": True}


@pytest.mark.asyncio
async def test_runtime_exposes_append_entries(tmp_path) -> None:
    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: {"ok": True},
    )

    result = await runtime.handle_append_entries(
        {
            "term": 1,
            "leader_id": "node-2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }
    )

    assert result == {"term": 1, "success": True, "match_index": 0}


@pytest.mark.asyncio
async def test_runtime_starts_election_and_becomes_leader_with_majority(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/raft/0/request_vote"
        return httpx.Response(200, json={"term": 1, "vote_granted": True})

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )

    await runtime.start_election()

    assert runtime.state()["role"] == "leader"
    assert runtime.state()["current_term"] == 1
    assert runtime.state()["leader_id"] == "node-1"


@pytest.mark.asyncio
async def test_leader_sends_append_entries_heartbeat(tmp_path) -> None:
    seen_paths = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        return httpx.Response(
            200,
            json={"term": 1, "success": True, "match_index": 0},
        )

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )
    await runtime.start_election()

    await runtime.send_heartbeat()

    assert seen_paths == [
        "/internal/raft/0/request_vote",
        "/internal/raft/0/append_entries",
    ]


@pytest.mark.asyncio
async def test_leader_commits_command_after_majority_replication(tmp_path) -> None:
    applied = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        return httpx.Response(
            200,
            json={"term": 1, "success": True, "match_index": 1},
        )

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: applied.append(command)
        or {"id": command["document"]["id"]},
    )
    await runtime.start_election()

    result = await runtime.submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    )

    assert result["ok"] is True
    assert result["commit_index"] == 1
    assert applied == [
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    ]


@pytest.mark.asyncio
async def test_follower_applies_committed_append_entries(tmp_path) -> None:
    applied = []
    runtime = RaftRuntime(
        node_id="node-2",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-1": "http://node-1"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: applied.append(command) or {"ok": True},
    )

    result = await runtime.handle_append_entries(
        {
            "term": 1,
            "leader_id": "node-1",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [
                {
                    "index": 1,
                    "term": 1,
                    "command": {
                        "type": "add_document",
                        "collection": "books",
                        "document": {"id": "book-1"},
                    },
                }
            ],
            "leader_commit": 1,
        }
    )

    assert result == {"term": 1, "success": True, "match_index": 1}
    assert runtime.state()["last_applied"] == 1
    assert applied == [
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    ]


@pytest.mark.asyncio
async def test_follower_applies_existing_log_when_heartbeat_advances_commit(
    tmp_path,
) -> None:
    applied = []
    runtime = RaftRuntime(
        node_id="node-2",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-1": "http://node-1"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: applied.append(command) or {"ok": True},
    )
    runtime.core.log = [
        RaftLogEntry(
            index=1,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-1"},
            },
        )
    ]
    runtime.storage.replace_log(runtime.core.log)

    result = await runtime.handle_append_entries(
        {
            "term": 1,
            "leader_id": "node-1",
            "prev_log_index": 1,
            "prev_log_term": 1,
            "entries": [],
            "leader_commit": 1,
        }
    )

    assert result == {"term": 1, "success": True, "match_index": 1}
    assert runtime.state()["last_applied"] == 1
    assert applied == [
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    ]


@pytest.mark.asyncio
async def test_runtime_background_loop_elects_leader_and_sends_heartbeat(tmp_path) -> None:
    seen_paths = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        return httpx.Response(200, json={"term": 1, "success": True, "match_index": 0})

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
        election_timeout=0.01,
        heartbeat_interval=0.01,
    )

    runtime.start()
    await anyio.sleep(0.05)
    await runtime.stop()

    assert runtime.state()["role"] == "leader"
    assert "/internal/raft/0/request_vote" in seen_paths
    assert "/internal/raft/0/append_entries" in seen_paths
