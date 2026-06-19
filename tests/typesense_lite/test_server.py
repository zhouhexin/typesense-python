import json

import httpx
from fastapi.testclient import TestClient

from typesense_lite.server import create_app


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 1,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": []},
    },
}


def test_data_node_health() -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1")
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["role"] == "node"


def test_data_node_internal_write_and_search(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)

    write = client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-1", "title": "Distributed search"},
    )
    search = client.get(
        "/internal/shards/0/collections/books/search",
        params={"q": "distributed", "limit": 10},
    )

    assert write.status_code == 200
    assert write.json()["id"] == "doc-1"
    assert search.status_code == 200
    assert search.json()["hits"][0]["id"] == "doc-1"


def test_data_node_internal_document_management(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)
    client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-1", "title": "Old search", "body": "before"},
    )

    listed = client.get("/internal/shards/0/collections/books/documents")
    retrieved = client.get("/internal/shards/0/collections/books/documents/doc-1")
    updated = client.patch(
        "/internal/shards/0/collections/books/documents/doc-1",
        json={"title": "New search"},
    )
    search_old = client.get(
        "/internal/shards/0/collections/books/search",
        params={"q": "old", "limit": 10},
    )
    search_new = client.get(
        "/internal/shards/0/collections/books/search",
        params={"q": "new", "limit": 10},
    )
    deleted = client.delete("/internal/shards/0/collections/books/documents/doc-1")
    missing = client.get("/internal/shards/0/collections/books/documents/doc-1")

    assert listed.status_code == 200
    assert listed.json()["documents"][0]["id"] == "doc-1"
    assert retrieved.status_code == 200
    assert retrieved.json()["title"] == "Old search"
    assert updated.status_code == 200
    assert updated.json()["title"] == "New search"
    assert search_old.json()["hits"] == []
    assert search_new.json()["hits"][0]["id"] == "doc-1"
    assert deleted.status_code == 200
    assert deleted.json()["id"] == "doc-1"
    assert missing.status_code == 404


def test_data_node_internal_update_rejects_id_change(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)
    client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-1", "title": "Search"},
    )

    response = client.patch(
        "/internal/shards/0/collections/books/documents/doc-1",
        json={"id": "doc-2"},
    )

    assert response.status_code == 400


def test_data_node_internal_lists_collections(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)
    client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-1", "title": "Book"},
    )

    response = client.get("/internal/collections")

    assert response.status_code == 200
    assert response.json() == {"collections": ["books"]}


def test_coordinator_public_document_routes_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if path == "/internal/collections":
            collections = ["books"] if documents else []
            return httpx.Response(200, json={"collections": collections})

        if path == "/internal/shards/0/collections/books/documents":
            if request.method == "POST":
                document = json.loads(request.content.decode("utf-8"))
                documents[document["id"]] = document
                return httpx.Response(200, json=document)
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={"documents": [documents[key] for key in sorted(documents)]},
                )

        if path == "/internal/shards/0/collections/books/documents/doc-1":
            if request.method == "GET":
                if "doc-1" not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                return httpx.Response(200, json=documents["doc-1"])
            if request.method == "PATCH":
                if "doc-1" not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                changes = json.loads(request.content.decode("utf-8"))
                documents["doc-1"] = {**documents["doc-1"], **changes, "id": "doc-1"}
                return httpx.Response(200, json=documents["doc-1"])
            if request.method == "DELETE":
                if "doc-1" not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                return httpx.Response(200, json=documents.pop("doc-1"))

        raise AssertionError(f"unexpected request {request.method} {request.url}")

    coordinator_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=coordinator_client,
    )
    client = TestClient(app)

    created = client.post(
        "/collections/books/documents",
        json={"id": "doc-1", "title": "Old search", "body": "before"},
    )
    collections = client.get("/collections")
    listed = client.get("/collections/books/documents")
    retrieved = client.get("/collections/books/documents/doc-1")
    updated = client.patch(
        "/collections/books/documents/doc-1",
        json={"title": "New search"},
    )
    deleted = client.delete("/collections/books/documents/doc-1")
    missing = client.get("/collections/books/documents/doc-1")

    assert created.status_code == 200
    assert collections.status_code == 200
    assert collections.json() == {"collections": ["books"]}
    assert listed.status_code == 200
    assert listed.json()["documents"][0]["id"] == "doc-1"
    assert retrieved.status_code == 200
    assert retrieved.json()["title"] == "Old search"
    assert updated.status_code == 200
    assert updated.json()["document"]["title"] == "New search"
    assert deleted.status_code == 200
    assert deleted.json()["id"] == "doc-1"
    assert missing.status_code == 404


def test_coordinator_serves_search_page_at_root() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Typesense Lite Search" in response.text
    assert "Search Documents" in response.text
    assert "Add Document" not in response.text


def test_coordinator_serves_search_page_at_search_path() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/search")

    assert response.status_code == 200
    assert "Typesense Lite Search" in response.text


def test_coordinator_serves_admin_page() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "Typesense Lite Admin" in response.text
    assert "Cluster" in response.text
    assert "Add Document" in response.text
    assert "Collections" in response.text
    assert "Documents" in response.text
    assert "fetch('/cluster')" in response.text
    assert "fetch('/collections')" in response.text
    assert 'method: "PATCH"' in response.text
    assert 'method: "DELETE"' in response.text


def test_admin_page_does_not_shadow_dom_document_when_rendering_documents() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "documents.forEach((document)" not in response.text


def test_data_node_does_not_serve_web_pages() -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1")
    client = TestClient(app)

    assert client.get("/").status_code == 404
    assert client.get("/search").status_code == 404
    assert client.get("/admin").status_code == 404
