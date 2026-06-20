import httpx
import pytest

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
