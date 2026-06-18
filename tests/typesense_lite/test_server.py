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


def test_coordinator_serves_web_console() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Typesense Lite Console" in response.text
    assert "fetch('/cluster')" in response.text


def test_data_node_does_not_serve_web_console() -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1")
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 404
