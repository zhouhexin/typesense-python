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
