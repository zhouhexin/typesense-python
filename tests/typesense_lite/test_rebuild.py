import json

import httpx
import pytest

from typesense_lite.cluster import ClusterMap
from typesense_lite.coordinator import Coordinator
from typesense_lite.node import SearchNode
from typesense_lite.raft_runtime import RaftRuntime
from typesense_lite.rebuild import build_snapshot_payload, validate_snapshot_payload
from typesense_lite.server import _rebuild_shard_from_leader


def test_snapshot_validation_rejects_checksum_and_wrong_shard() -> None:
    snapshot = build_snapshot_payload(
        shard_id=0,
        source_node_id="node-1",
        source_role="leader",
        term=2,
        snapshot_index=1,
        commit_index=1,
        collections={"books": [{"id": "doc-1", "title": "Leader"}]},
        log_entries=[],
    )

    with pytest.raises(ValueError, match="shard id mismatch"):
        validate_snapshot_payload(
            snapshot, expected_shard_id=1, expected_source_node_id="node-1"
        )

    snapshot["manifest"]["checksum"] = "invalid"
    with pytest.raises(ValueError, match="checksum mismatch"):
        validate_snapshot_payload(
            snapshot, expected_shard_id=0, expected_source_node_id="node-1"
        )


def test_install_snapshot_replaces_storage_and_rebuilds_index(tmp_path) -> None:
    node = SearchNode("node-1", tmp_path)
    node.add_document(0, "books", {"id": "old", "title": "Old content"})

    node.install_shard_snapshot(
        0,
        {
            "books": [{"id": "doc-1", "title": "Leader snapshot"}],
            "articles": [{"id": "article-1", "body": "Searchable article"}],
        },
    )

    assert node.list_document_ids(0, "books") == ["doc-1"]
    assert node.search_query(0, "books", "leader")["hits"][0]["id"] == "doc-1"
    reloaded = SearchNode("node-1", tmp_path)
    assert reloaded.list_document_ids(0, "books") == ["doc-1"]
    assert reloaded.list_document_ids(0, "articles") == ["article-1"]


@pytest.mark.asyncio
async def test_rebuild_uses_current_leader_and_catches_up_after_snapshot(tmp_path) -> None:
    node, runtime, cluster, client, seen = _rebuild_fixture(tmp_path)

    result = await _rebuild_shard_from_leader(
        node=node, runtime=runtime, cluster=cluster, client=client
    )

    assert result["source_leader"] == "node-2"
    assert seen == ["node-2"]
    assert node.list_document_ids(0, "books") == ["doc-1", "doc-2"]
    assert runtime.recovery_state()["status"] == "catching_up"

    await runtime.handle_append_entries(
        {
            "term": 2,
            "leader_id": "node-2",
            "prev_log_index": 2,
            "prev_log_term": 2,
            "entries": [
                {
                    "index": 3,
                    "term": 2,
                    "command": {
                        "type": "add_document",
                        "collection": "books",
                        "document": {"id": "doc-3", "title": "After snapshot"},
                    },
                }
            ],
            "leader_commit": 3,
        }
    )
    await runtime.handle_append_entries(
        {
            "term": 2,
            "leader_id": "node-2",
            "prev_log_index": 3,
            "prev_log_term": 2,
            "entries": [],
            "leader_commit": 3,
        }
    )

    assert node.get_document(0, "books", "doc-3")["title"] == "After snapshot"
    assert runtime.recovery_state()["status"] == "healthy"
    await client.aclose()


@pytest.mark.asyncio
async def test_invalid_snapshot_does_not_replace_existing_replica(tmp_path) -> None:
    node, runtime, cluster, client, _ = _rebuild_fixture(tmp_path, bad_checksum=True)
    node.add_document(0, "books", {"id": "safe", "title": "Keep"})

    with pytest.raises(ValueError, match="checksum mismatch"):
        await _rebuild_shard_from_leader(
            node=node, runtime=runtime, cluster=cluster, client=client
        )

    assert node.get_document(0, "books", "safe")["title"] == "Keep"
    await client.aclose()


@pytest.mark.asyncio
async def test_source_losing_leadership_rejects_snapshot(tmp_path) -> None:
    node, runtime, cluster, client, _ = _rebuild_fixture(
        tmp_path, lose_leadership=True
    )

    with pytest.raises(RuntimeError, match="lost leadership"):
        await _rebuild_shard_from_leader(
            node=node, runtime=runtime, cluster=cluster, client=client
        )
    await client.aclose()


@pytest.mark.asyncio
async def test_coordinator_rebuilds_only_non_leader_voter() -> None:
    cluster = _cluster()
    rebuilt: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/internal/raft/0/state":
            node_id = request.url.host or ""
            role = "leader" if node_id == "node-2" else "follower"
            return httpx.Response(
                200,
                json={
                    "node": node_id,
                    "role": role,
                    "current_term": 2,
                    "leader_id": "node-2",
                },
            )
        if request.url.path == "/internal/shards/0/rebuild":
            rebuilt.append(request.url.host or "")
            return httpx.Response(200, json={"status": "completed"})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster, client=client)
        result = await coordinator.rebuild_shard_member(0, "node-3")
        with pytest.raises(ValueError, match="leader cannot be rebuilt"):
            await coordinator.rebuild_shard_member(0, "node-2")

    assert result["status"] == "completed"
    assert rebuilt == ["node-3"]


def _rebuild_fixture(tmp_path, *, bad_checksum=False, lose_leadership=False):
    cluster = _cluster()
    node = SearchNode("node-3", tmp_path)
    runtime = RaftRuntime(
        node_id="node-3",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-1": "http://node-1", "node-2": "http://node-2"},
        client=httpx.AsyncClient(),
        apply_command=lambda command: node.apply_raft_command(0, command),
    )
    snapshot = build_snapshot_payload(
        shard_id=0,
        source_node_id="node-2",
        source_role="leader",
        term=2,
        snapshot_index=2,
        commit_index=2,
        collections={
            "books": [
                {"id": "doc-1", "title": "Leader"},
                {"id": "doc-2", "title": "Fresh"},
            ]
        },
        log_entries=[
            {
                "index": index,
                "term": index,
                "command": {
                    "type": "add_document",
                    "collection": "books",
                    "document": {"id": f"doc-{index}", "title": title},
                },
            }
            for index, title in ((1, "Leader"), (2, "Fresh"))
        ],
    )
    if bad_checksum:
        snapshot["manifest"]["checksum"] = "invalid"
    exports: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        node_id = request.url.host or ""
        if request.url.path == "/internal/raft/0/state":
            role = "leader" if node_id == "node-2" else "follower"
            if lose_leadership and exports:
                role = "follower"
            return httpx.Response(
                200,
                json={
                    "node": node_id,
                    "role": role,
                    "current_term": 2,
                    "leader_id": "node-2",
                    "commit_index": 2,
                    "last_applied": 2,
                    "last_log_index": 2,
                },
            )
        if request.url.path == "/internal/shards/0/snapshot/manifest":
            return httpx.Response(200, json=snapshot["manifest"])
        if request.url.path == "/internal/shards/0/snapshot/export":
            exports.append(node_id)
            return httpx.Response(200, json=snapshot)
        return httpx.Response(404, content=json.dumps({"detail": "not found"}))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return node, runtime, cluster, client, exports


def _cluster() -> ClusterMap:
    return ClusterMap.from_dict(
        {
            "coordinator": {"host": "127.0.0.1", "port": 9100},
            "shard_count": 1,
            "nodes": [
                {"id": "node-1", "host": "node-1", "port": 80},
                {"id": "node-2", "host": "node-2", "port": 80},
                {"id": "node-3", "host": "node-3", "port": 80},
            ],
            "shards": {
                "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]}
            },
        }
    )
