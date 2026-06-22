import json

import httpx
import pytest

from typesense_lite.raft_runtime import RaftRuntime


@pytest.mark.asyncio
async def test_new_leader_elected_after_leader_stops(tmp_path) -> None:
    runtimes: dict[str, RaftRuntime] = {}
    stopped_nodes: set[str] = set()

    async def handler(request: httpx.Request) -> httpx.Response:
        target_node = request.url.host or ""
        if target_node in stopped_nodes:
            return httpx.Response(503)

        payload = json.loads(request.content.decode("utf-8"))
        runtime = runtimes[target_node]
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json=await runtime.handle_request_vote(payload))
        if request.url.path.endswith("/append_entries"):
            return httpx.Response(200, json=await runtime.handle_append_entries(payload))
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    members = ["node-1", "node-2", "node-3"]
    for node_id in members:
        runtimes[node_id] = RaftRuntime(
            node_id=node_id,
            shard_id=0,
            members=members,
            data_dir=tmp_path,
            peer_urls={
                peer_id: f"http://{peer_id}"
                for peer_id in members
                if peer_id != node_id
            },
            client=client,
            apply_command=lambda command: {"ok": True},
        )

    await runtimes["node-1"].start_election()
    old_leader_state = runtimes["node-1"].state()

    stopped_nodes.add("node-1")
    await runtimes["node-2"].start_election()
    new_leader_state = runtimes["node-2"].state()

    assert old_leader_state["role"] == "leader"
    assert old_leader_state["leader_id"] == "node-1"
    assert new_leader_state["role"] == "leader"
    assert new_leader_state["leader_id"] == "node-2"
    assert new_leader_state["leader_id"] != old_leader_state["leader_id"]
    assert new_leader_state["current_term"] > old_leader_state["current_term"]
    assert runtimes["node-3"].state()["voted_for"] == "node-2"


@pytest.mark.asyncio
async def test_recovered_follower_catches_up_after_missing_committed_writes(
    tmp_path,
) -> None:
    runtimes: dict[str, RaftRuntime] = {}
    applied: dict[str, list[dict]] = {
        "node-1": [],
        "node-2": [],
        "node-3": [],
    }
    stopped_nodes: set[str] = set()

    async def handler(request: httpx.Request) -> httpx.Response:
        target_node = request.url.host or ""
        if target_node in stopped_nodes:
            return httpx.Response(503)

        payload = json.loads(request.content.decode("utf-8"))
        runtime = runtimes[target_node]
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json=await runtime.handle_request_vote(payload))
        if request.url.path.endswith("/append_entries"):
            return httpx.Response(200, json=await runtime.handle_append_entries(payload))
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    members = ["node-1", "node-2", "node-3"]
    for node_id in members:
        runtimes[node_id] = RaftRuntime(
            node_id=node_id,
            shard_id=0,
            members=members,
            data_dir=tmp_path / node_id,
            peer_urls={
                peer_id: f"http://{peer_id}"
                for peer_id in members
                if peer_id != node_id
            },
            client=client,
            apply_command=lambda command, node_id=node_id: applied[node_id].append(
                command
            )
            or {"ok": True},
        )

    await runtimes["node-1"].start_election()
    stopped_nodes.add("node-3")

    first = await runtimes["node-1"].submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    )
    second = await runtimes["node-1"].submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-2"},
        }
    )

    assert first["ok"] is True
    assert second["ok"] is True
    assert runtimes["node-3"].state()["last_log_index"] == 0

    stopped_nodes.remove("node-3")
    await runtimes["node-1"].send_heartbeat()

    assert runtimes["node-3"].state()["last_log_index"] == 2
    assert runtimes["node-3"].state()["commit_index"] == 2
    assert runtimes["node-3"].state()["last_applied"] == 2
    assert [command["document"]["id"] for command in applied["node-3"]] == [
        "book-1",
        "book-2",
    ]
