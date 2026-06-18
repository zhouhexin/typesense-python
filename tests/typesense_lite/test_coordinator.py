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
async def test_coordinator_writes_primary_and_replica() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.add_document(
            "books",
            {"id": "doc-1", "title": "Distributed search"},
        )

    assert result["ok"] is True
    assert len(calls) == 2
    assert result["primary"] in {"node-1", "node-2"}
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_coordinator_search_merges_hits_by_score() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
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
async def test_coordinator_updates_primary_and_replica() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    calls: list[tuple[str, dict]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append((str(request.url), dict(request.headers)))
        return httpx.Response(200, json={"id": "doc-1", "title": "Updated"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.update_document(
            "books",
            "doc-1",
            {"title": "Updated"},
        )

    assert result["ok"] is True
    assert result["id"] == "doc-1"
    assert len(calls) == 2
    assert result["warnings"] == []


@pytest.mark.asyncio
async def test_coordinator_deletes_primary_and_replica() -> None:
    cluster = ClusterMap.from_dict(CONFIG)
    methods: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        return httpx.Response(200, json={"id": "doc-1", "title": "Deleted"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        coordinator = Coordinator(cluster=cluster, client=client)
        result = await coordinator.delete_document("books", "doc-1")

    assert result["ok"] is True
    assert methods == ["DELETE", "DELETE"]
    assert result["warnings"] == []
