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
