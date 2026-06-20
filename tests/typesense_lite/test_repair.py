"""Tests for replica consistency checking and repair."""

import httpx
import pytest

from typesense_lite.repair import check_consistency, repair_collection


@pytest.mark.asyncio
async def test_check_consistency_reports_missing_documents() -> None:
    """When replica is missing documents, reports them in missing_on_replica."""
    cluster = _MockCluster()
    documents = {
        8101: ["doc-1", "doc-2"],
        8102: ["doc-1"],  # Missing doc-2
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/document_ids"):
            port = request.url.port
            ids = documents.get(port, [])
            return httpx.Response(200, json={"ids": ids})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await check_consistency(cluster, "books", client)

    assert result["collection"] == "books"
    assert "shards" in result
    shard = result["shards"]["0"]
    assert shard["primary"] == "node-1"
    assert "node-2" in shard["replicas"]
    assert shard["replicas"]["node-2"]["missing_on_replica"] == ["doc-2"]
    assert shard["replicas"]["node-2"]["extra_on_replica"] == []


@pytest.mark.asyncio
async def test_check_consistency_reports_extra_documents() -> None:
    """When replica has extra documents, reports them in extra_on_replica."""
    cluster = _MockCluster()
    documents = {
        8101: ["doc-1"],
        8102: ["doc-1", "doc-2"],  # Extra doc-2
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/document_ids"):
            port = request.url.port
            ids = documents.get(port, [])
            return httpx.Response(200, json={"ids": ids})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await check_consistency(cluster, "books", client)

    assert result["collection"] == "books"
    shard = result["shards"]["0"]
    assert shard["replicas"]["node-2"]["missing_on_replica"] == []
    assert shard["replicas"]["node-2"]["extra_on_replica"] == ["doc-2"]


@pytest.mark.asyncio
async def test_check_consistency_reports_replica_error() -> None:
    """When replica is unavailable, reports error instead of missing/extra."""
    cluster = _MockCluster()

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/document_ids"):
            port = request.url.port
            if port == 8101:
                return httpx.Response(200, json={"ids": ["doc-1"]})
            # node-2 returns error
            return httpx.Response(500, json={"error": "connection refused"})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await check_consistency(cluster, "books", client)

    shard = result["shards"]["0"]
    assert "error" in shard["replicas"]["node-2"]
    assert "replica unavailable" in shard["replicas"]["node-2"]["error"]


@pytest.mark.asyncio
async def test_check_consistency_empty_when_no_replicas() -> None:
    """When there are no replicas, returns empty replica info."""
    cluster = _MockClusterNoReplicas()

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/document_ids"):
            return httpx.Response(200, json={"ids": ["doc-1", "doc-2"]})
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await check_consistency(cluster, "books", client)

    assert result["collection"] == "books"
    shard = result["shards"]["0"]
    assert shard["primary"] == "node-1"
    assert shard["replicas"] == {}


@pytest.mark.asyncio
async def test_repair_collection_copies_missing_documents_to_replica() -> None:
    """When a replica is missing a document, repair copies it from primary."""
    cluster = _MockCluster()
    documents = {
        8101: {
            "doc-1": {"id": "doc-1", "title": "First"},
            "doc-2": {"id": "doc-2", "title": "Second"},
        },
        8102: {
            "doc-1": {"id": "doc-1", "title": "First"},
        },
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        port = request.url.port
        path = request.url.path

        if path.endswith("/document_ids"):
            ids = sorted(documents.get(port, {}))
            return httpx.Response(200, json={"ids": ids})

        prefix = "/internal/shards/0/collections/books/documents/"
        if path.startswith(prefix) and request.method == "GET":
            document_id = path.removeprefix(prefix)
            document = documents.get(port, {}).get(document_id)
            if document is None:
                return httpx.Response(404, json={"detail": "document not found"})
            return httpx.Response(200, json=document)

        if path == "/internal/shards/0/collections/books/documents" and request.method == "POST":
            document = request.read()
            stored = httpx.Response(200, content=document).json()
            documents.setdefault(port, {})[stored["id"]] = stored
            return httpx.Response(200, json=stored)

        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await repair_collection(cluster, "books", client)

    assert result["collection"] == "books"
    assert result["repaired"] == 1
    assert result["shards"]["0"]["repaired"]["node-2"] == ["doc-2"]
    assert result["shards"]["0"]["failed"] == {}
    assert documents[8102]["doc-2"] == {"id": "doc-2", "title": "Second"}


@pytest.mark.asyncio
async def test_repair_collection_marks_primary_missing_document_as_failed() -> None:
    """If a primary ID cannot be read, repair reports that ID as failed."""
    cluster = _MockCluster()

    async def handler(request: httpx.Request) -> httpx.Response:
        port = request.url.port
        path = request.url.path

        if path.endswith("/document_ids"):
            if port == 8101:
                return httpx.Response(200, json={"ids": ["doc-1"]})
            return httpx.Response(200, json={"ids": []})

        if path.endswith("/documents/doc-1") and request.method == "GET":
            return httpx.Response(404, json={"detail": "document not found"})

        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await repair_collection(cluster, "books", client)

    assert result["repaired"] == 0
    assert result["shards"]["0"]["repaired"] == {}
    assert result["shards"]["0"]["failed"]["node-2"] == ["doc-1"]


@pytest.mark.asyncio
async def test_repair_collection_marks_replica_write_failure_as_failed() -> None:
    """If replica write fails, repair reports that ID as failed."""
    cluster = _MockCluster()

    async def handler(request: httpx.Request) -> httpx.Response:
        port = request.url.port
        path = request.url.path

        if path.endswith("/document_ids"):
            if port == 8101:
                return httpx.Response(200, json={"ids": ["doc-1"]})
            return httpx.Response(200, json={"ids": []})

        if path.endswith("/documents/doc-1") and request.method == "GET":
            return httpx.Response(200, json={"id": "doc-1", "title": "First"})

        if path == "/internal/shards/0/collections/books/documents" and request.method == "POST":
            return httpx.Response(500, json={"detail": "write failed"})

        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await repair_collection(cluster, "books", client)

    assert result["repaired"] == 0
    assert result["shards"]["0"]["repaired"] == {}
    assert result["shards"]["0"]["failed"]["node-2"] == ["doc-1"]


class _MockNode:
    def __init__(self, id: str, host: str = "127.0.0.1", port: int = 8101) -> None:
        self.id = id
        self.host = host
        self.port = port

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"


class _MockPlacement:
    def __init__(self, primary: str, replicas: list[str]) -> None:
        self.primary = primary
        self.replicas = replicas


class _MockCluster:
    def __init__(self) -> None:
        self.shard_count = 1
        self.nodes = {
            "node-1": _MockNode("node-1", port=8101),
            "node-2": _MockNode("node-2", port=8102),
        }
        self.shards = {
            0: _MockPlacement("node-1", ["node-2"]),
        }

    def get_shard_id(self, document_id: str) -> int:
        return 0


class _MockClusterNoReplicas:
    def __init__(self) -> None:
        self.shard_count = 1
        self.nodes = {
            "node-1": _MockNode("node-1", port=8101),
        }
        self.shards = {
            0: _MockPlacement("node-1", []),
        }

    def get_shard_id(self, document_id: str) -> int:
        return 0
