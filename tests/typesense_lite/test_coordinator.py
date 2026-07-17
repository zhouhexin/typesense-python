import httpx
import pytest

from typesense_lite.cluster import ClusterMap
from typesense_lite.coordinator import Coordinator


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 2,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2"]},
        "1": {"primary": "node-2", "replicas": ["node-1"]},
    },
}


@pytest.mark.asyncio
async def test_coordinator_writes_to_raft_leader() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    shard_id = cluster.get_shard_id("doc-1")
    calls: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path == f"/internal/raft/{shard_id}/state":
            return httpx.Response(
                200,
                json={
                    "node": "node-2",
                    "role": "leader",
                    "current_term": 1,
                    "leader_id": "node-2",
                },
            )
        if request.url.path == f"/internal/raft/{shard_id}/commands":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "leader": "node-2",
                    "term": 1,
                    "commit_index": 1,
                    "result": {"id": "doc-1"},
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.add_document(
            "books",
            {"id": "doc-1", "title": "Distributed search"},
        )

    assert result["ok"] is True
    assert ("POST", f"/internal/raft/{shard_id}/commands") in calls
    assert result["leader"] == "node-2"
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_add_document_routes_to_raft_leader() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    shard_id = cluster.get_shard_id("doc-1")
    leader_port = 9102
    requests: list[tuple[str, int | None, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.port, request.url.path))
        if request.url.path == f"/internal/raft/{shard_id}/state":
            role = "leader" if request.url.port == leader_port else "follower"
            node = "node-2" if request.url.port == 9102 else "node-1"
            return httpx.Response(
                200,
                json={
                    "node": node,
                    "role": role,
                    "current_term": 1,
                    "leader_id": "node-2",
                },
            )
        if request.url.path == f"/internal/raft/{shard_id}/commands":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "leader": "node-2",
                    "term": 1,
                    "commit_index": 1,
                    "result": {"id": "doc-1"},
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.add_document(
            "books",
            {"id": "doc-1", "title": "Raft"},
        )

    assert result["ok"] is True
    assert result["leader"] == "node-2"
    assert ("POST", leader_port, f"/internal/raft/{shard_id}/commands") in requests


@pytest.mark.asyncio
async def test_add_document_retries_after_cached_leader_goes_down() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    shard_id = cluster.get_shard_id("doc-1")
    failed_old_leader = False
    requests: list[tuple[str, int | None, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal failed_old_leader

        requests.append((request.method, request.url.port, request.url.path))
        if request.url.path == f"/internal/raft/{shard_id}/state":
            if request.url.port == 9101:
                role = "leader" if failed_old_leader else "follower"
                leader_id = "node-1" if failed_old_leader else "node-2"
                return httpx.Response(
                    200,
                    json={
                        "node": "node-1",
                        "role": role,
                        "current_term": 2 if failed_old_leader else 1,
                        "leader_id": leader_id,
                    },
                )
            if failed_old_leader:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "node": "node-2",
                    "role": "leader",
                    "current_term": 1,
                    "leader_id": "node-2",
                },
            )

        if request.url.path == f"/internal/raft/{shard_id}/commands":
            if request.url.port == 9102:
                failed_old_leader = True
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "leader": "node-1",
                    "term": 2,
                    "commit_index": 2,
                    "result": {"id": "doc-1"},
                },
            )

        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        coordinator.RAFT_SUBMIT_RETRY_DELAY_SECONDS = 0
        result = await coordinator.add_document(
            "books",
            {"id": "doc-1", "title": "Leader failover"},
        )

    assert result["ok"] is True
    assert result["leader"] == "node-1"
    assert ("POST", 9102, f"/internal/raft/{shard_id}/commands") in requests
    assert ("POST", 9101, f"/internal/raft/{shard_id}/commands") in requests


@pytest.mark.asyncio
async def test_coordinator_search_merges_hits_by_score() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    search_params: list[dict[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/search"):
            search_params.append(dict(request.url.params))
        if request.url.port == 9101:
            hits = [{"id": "doc-1", "score": 1.0, "document": {"id": "doc-1"}}]
        else:
            hits = [{"id": "doc-2", "score": 3.0, "document": {"id": "doc-2"}}]
        return httpx.Response(200, json={"found": len(hits), "hits": hits})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.search("books", "search", limit=10)

    assert [hit["id"] for hit in result["hits"]] == ["doc-2", "doc-1"]
    assert result["found"] == 2
    assert result["warnings"] == []
    assert search_params
    assert all(params == {"q": "search", "limit": "10"} for params in search_params)


@pytest.mark.asyncio
async def test_coordinator_search_forwards_and_globally_applies_advanced_params() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    seen_params: list[dict[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/search"):
            seen_params.append(dict(request.url.params))
        popularity = 20 if request.url.port == 9101 else 5
        hit_id = "doc-1" if request.url.port == 9101 else "doc-2"
        return httpx.Response(
            200,
            json={
                "found": 1,
                "hits": [
                    {
                        "id": hit_id,
                        "score": 1.0,
                        "document": {
                            "id": hit_id,
                            "category": "tech",
                            "popularity": popularity,
                        },
                    }
                ],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.search(
            "books",
            "search",
            query_by="title,body",
            query_by_weights="3,1",
            prefix=True,
            num_typos=1,
            filter_by="category:tech",
            sort_by="popularity:asc",
            facet_by="category",
            highlight_fields="title",
            page=2,
            per_page=1,
        )

    assert all(params["query_by"] == "title,body" for params in seen_params)
    assert all(params["prefix"] == "true" for params in seen_params)
    assert all(params["num_typos"] == "1" for params in seen_params)
    assert all(params["all_results"] == "true" for params in seen_params)
    assert result["found"] == 2
    assert result["page"] == 2
    assert [hit["id"] for hit in result["hits"]] == ["doc-1"]
    assert result["facet_counts"] == [
        {
            "field_name": "category",
            "counts": [{"value": "tech", "count": 2}],
        }
    ]


@pytest.mark.asyncio
async def test_coordinator_search_falls_back_to_replica() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    failed_primary_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9101:
            failed_primary_urls.append(str(request.url))
            return httpx.Response(503, json={"message": "down"})
        hits = [{"id": "doc-2", "score": 2.0, "document": {"id": "doc-2"}}]
        return httpx.Response(200, json={"found": len(hits), "hits": hits})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.search("books", "search", limit=10)

    assert failed_primary_urls
    assert result["hits"]
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_coordinator_lists_collections_from_all_nodes() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/collections"):
            return httpx.Response(200, json={"collections": ["books", "movies"]})
        raise AssertionError(f"unexpected request {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.list_collections()

    assert result == {"collections": ["books", "movies"]}


@pytest.mark.asyncio
async def test_coordinator_lists_documents_across_shards() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        if "/shards/0/" in request.url.path:
            return httpx.Response(200, json={"documents": [{"id": "doc-1"}]})
        if "/shards/1/" in request.url.path:
            return httpx.Response(200, json={"documents": [{"id": "doc-2"}]})
        raise AssertionError(f"unexpected request {request.url}")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.list_documents("books")

    assert result == {"documents": [{"id": "doc-1"}, {"id": "doc-2"}]}


@pytest.mark.asyncio
async def test_coordinator_retrieves_document_by_routed_shard() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    document_id = "doc-1"
    expected_shard = cluster.get_shard_id(document_id)
    seen_urls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_urls.append(str(request.url))
        return httpx.Response(200, json={"id": document_id, "title": "Search"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.get_document("books", document_id)

    assert result["id"] == document_id
    assert f"/shards/{expected_shard}/" in seen_urls[0]


@pytest.mark.asyncio
async def test_coordinator_updates_raft_leader() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    shard_id = cluster.get_shard_id("doc-1")
    calls: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path == f"/internal/raft/{shard_id}/state":
            return httpx.Response(
                200,
                json={
                    "node": "node-2",
                    "role": "leader",
                    "current_term": 1,
                    "leader_id": "node-2",
                },
            )
        if request.url.path == f"/internal/raft/{shard_id}/commands":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "leader": "node-2",
                    "term": 1,
                    "commit_index": 1,
                    "result": {"id": "doc-1", "title": "Updated"},
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.update_document(
            "books",
            "doc-1",
            {"title": "Updated"},
        )

    assert result["ok"] is True
    assert result["id"] == "doc-1"
    assert ("POST", f"/internal/raft/{shard_id}/commands") in calls
    assert result["leader"] == "node-2"
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_coordinator_deletes_raft_leader() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    shard_id = cluster.get_shard_id("doc-1")
    calls: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.url.path == f"/internal/raft/{shard_id}/state":
            return httpx.Response(
                200,
                json={
                    "node": "node-2",
                    "role": "leader",
                    "current_term": 1,
                    "leader_id": "node-2",
                },
            )
        if request.url.path == f"/internal/raft/{shard_id}/commands":
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "leader": "node-2",
                    "term": 1,
                    "commit_index": 1,
                    "result": {"id": "doc-1", "title": "Deleted"},
                },
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.delete_document("books", "doc-1")

    assert result["ok"] is True
    assert ("POST", f"/internal/raft/{shard_id}/commands") in calls
    assert result["leader"] == "node-2"
    assert result["warnings"] == []
