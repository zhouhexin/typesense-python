import json
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest
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


def mock_single_node_client(documents: dict[str, dict]) -> httpx.AsyncClient:
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

        prefix = "/internal/shards/0/collections/books/documents/"
        if path.startswith(prefix):
            document_id = path.removeprefix(prefix)
            if request.method == "GET":
                if document_id not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                return httpx.Response(200, json=documents[document_id])
            if request.method == "PATCH":
                if document_id not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                changes = json.loads(request.content.decode("utf-8"))
                documents[document_id] = {
                    **documents[document_id],
                    **changes,
                    "id": document_id,
                }
                return httpx.Response(200, json=documents[document_id])
            if request.method == "DELETE":
                if document_id not in documents:
                    return httpx.Response(404, json={"detail": "document not found"})
                return httpx.Response(200, json=documents.pop(document_id))

        raise AssertionError(f"unexpected request {request.method} {request.url}")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


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


def test_data_node_internal_lists_document_ids(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)
    client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-3", "title": "Third"},
    )
    client.post(
        "/internal/shards/0/collections/books/documents",
        json={"id": "doc-1", "title": "First"},
    )

    response = client.get("/internal/shards/0/collections/books/document_ids")

    assert response.status_code == 200
    assert response.json() == {"ids": ["doc-1", "doc-3"]}


def test_data_node_exposes_raft_state(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)

    response = client.get("/internal/raft/0/state")

    assert response.status_code == 200
    assert response.json()["node"] == "node-1"
    assert response.json()["shard_id"] == 0
    assert response.json()["role"] == "follower"


def test_data_node_exposes_request_vote(tmp_path) -> None:
    app = create_app(
        role="node",
        cluster_config=CONFIG,
        node_id="node-1",
        data_dir=tmp_path,
    )
    client = TestClient(app)

    response = client.post(
        "/internal/raft/0/request_vote",
        json={
            "term": 1,
            "candidate_id": "node-1",
            "last_log_index": 0,
            "last_log_term": 0,
        },
    )

    assert response.status_code == 200
    assert response.json()["vote_granted"] is True


def test_coordinator_public_document_routes_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
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


def test_coordinator_imports_multiple_documents_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/import",
        json=[
            {"id": "doc-1", "title": "A"},
            {"id": "doc-2", "title": "B"},
        ],
    )

    assert response.status_code == 200
    assert response.json()["succeeded"] == 2
    assert response.json()["failed"] == 0
    assert [result["id"] for result in response.json()["results"]] == ["doc-1", "doc-2"]
    assert sorted(documents) == ["doc-1", "doc-2"]


def test_coordinator_import_reports_invalid_documents_without_stopping(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/import",
        json=[
            {"id": "doc-1", "title": "A"},
            {"title": "Missing id"},
        ],
    )

    assert response.status_code == 200
    assert response.json()["ok"] is False
    assert response.json()["succeeded"] == 1
    assert response.json()["failed"] == 1
    assert response.json()["results"][1]["ok"] is False
    assert sorted(documents) == ["doc-1"]


def test_coordinator_uploads_txt_file_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/upload",
        files={"file": ("notes.txt", b"searchable uploaded text", "text/plain")},
    )

    assert response.status_code == 200
    assert response.json()["succeeded"] == 1
    assert response.json()["results"][0]["id"] == "notes-txt"
    assert documents["notes-txt"]["body"] == "searchable uploaded text"


def test_coordinator_uploads_docx_file_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/upload",
        files={
            "file": (
                "plan.docx",
                make_docx("word searchable text"),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        },
    )

    assert response.status_code == 200
    assert response.json()["results"][0]["id"] == "plan-docx"
    assert documents["plan-docx"]["body"] == "word searchable text"


def test_coordinator_uploads_pdf_file_with_single_node(tmp_path) -> None:
    documents: dict[str, dict] = {}
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client(documents),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/upload",
        files={"file": ("report.pdf", make_pdf("pdf searchable text"), "application/pdf")},
    )

    assert response.status_code == 200
    assert response.json()["results"][0]["id"] == "report-pdf"
    assert documents["report-pdf"]["body"] == "pdf searchable text"


def test_coordinator_upload_rejects_unsupported_file_type(tmp_path) -> None:
    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=mock_single_node_client({}),
    )
    client = TestClient(app)

    response = client.post(
        "/collections/books/documents/upload",
        files={"file": ("books.csv", b"id,title\n1,A\n", "text/csv")},
    )

    assert response.status_code == 400


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
    assert "Import Documents" in response.text
    assert "Collections" in response.text
    assert "Documents" in response.text
    assert 'type="file"' in response.text
    assert ".pdf,.docx" in response.text
    assert "fetch('/cluster')" in response.text
    assert "fetch('/collections')" in response.text
    assert "/documents/import" in response.text
    assert "/documents/upload" in response.text
    assert 'method: "PATCH"' in response.text
    assert 'method: "DELETE"' in response.text


def test_coordinator_serves_cluster_console_page() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/cluster-console")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Typesense Lite Cluster Console" in response.text
    assert "Cluster Health" in response.text
    assert "Election Status" in response.text
    assert "Consistency & Repair" in response.text
    assert "fetch('/cluster')" in response.text
    assert "fetch('/cluster/health')" in response.text
    assert "/consistency" in response.text
    assert "/repair" in response.text
    assert "static primary mode" in response.text
    assert 'id="check-consistency"' in response.text
    assert 'id="repair-collection"' in response.text


def test_admin_page_includes_consistency_and_repair_controls() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "Consistency & Repair" in response.text
    assert 'id="check-consistency"' in response.text
    assert 'id="repair-collection"' in response.text
    assert 'id="consistency-results"' in response.text
    assert "/consistency" in response.text
    assert "/repair" in response.text
    assert "missing_on_replica" in response.text
    assert "extra_on_replica" in response.text
    assert "repaired" in response.text
    assert "failed" in response.text


def test_admin_page_does_not_shadow_dom_document_when_rendering_documents() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "documents.forEach((document)" not in response.text


def test_admin_page_includes_cluster_health_panel() -> None:
    """Admin page includes Cluster Health panel with refresh button."""
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "Cluster Health" in response.text
    assert 'id="refresh-health"' in response.text
    assert "fetch('/cluster/health')" in response.text
    assert 'id="health-nodes"' in response.text
    assert 'id="health-shards"' in response.text


def test_admin_page_maps_unavailable_shards_to_unavailable_status() -> None:
    app = create_app(role="coordinator", cluster_config=CONFIG)
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert 'status.status === "unavailable"' in response.text
    assert 'status-unavailable' in response.text


def test_data_node_does_not_serve_web_pages() -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1")
    client = TestClient(app)

    assert client.get("/").status_code == 404
    assert client.get("/search").status_code == 404
    assert client.get("/admin").status_code == 404
    assert client.get("/cluster-console").status_code == 404


def test_data_node_does_not_expose_cluster_health() -> None:
    """Data node should not expose /cluster/health endpoint."""
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1")
    client = TestClient(app)

    assert client.get("/cluster/health").status_code == 404


@pytest.mark.asyncio
async def test_coordinator_cluster_health_endpoint(tmp_path) -> None:
    """Coordinator /cluster/health endpoint returns node and shard status."""
    health_config = {
        "coordinator": {"host": "127.0.0.1", "port": 9100},
        "shard_count": 1,
        "nodes": [
            {"id": "node-1", "host": "127.0.0.1", "port": 9101},
            {"id": "node-2", "host": "127.0.0.1", "port": 9102},
        ],
        "shards": {
            "0": {"primary": "node-1", "replicas": ["node-2"]},
        },
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "role": "node", "node_id": request.url.host})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        app = create_app(
            role="coordinator",
            cluster_config=health_config,
            data_dir=tmp_path,
            coordinator_client=client,
        )
        test_client = TestClient(app)
        response = test_client.get("/cluster/health")

    assert response.status_code == 200
    payload = response.json()

    # Should have nodes and shards
    assert "nodes" in payload
    assert "shards" in payload

    # Should have both nodes
    assert "node-1" in payload["nodes"]
    assert "node-2" in payload["nodes"]

    # Both nodes should be healthy
    assert payload["nodes"]["node-1"]["ok"] is True
    assert payload["nodes"]["node-2"]["ok"] is True

    # Should have shard 0
    assert "0" in payload["shards"]

    # Shard should be healthy
    assert payload["shards"]["0"]["status"] == "healthy"


def test_coordinator_consistency_endpoint_reports_replica_drift(tmp_path) -> None:
    config = {
        "coordinator": {"host": "127.0.0.1", "port": 9100},
        "shard_count": 1,
        "nodes": [
            {"id": "node-1", "host": "127.0.0.1", "port": 9101},
            {"id": "node-2", "host": "127.0.0.1", "port": 9102},
        ],
        "shards": {
            "0": {"primary": "node-1", "replicas": ["node-2"]},
        },
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/document_ids"):
            if request.url.port == 9101:
                return httpx.Response(200, json={"ids": ["doc-1", "doc-2"]})
            return httpx.Response(200, json={"ids": ["doc-1"]})
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        app = create_app(
            role="coordinator",
            cluster_config=config,
            data_dir=tmp_path,
            coordinator_client=async_client,
        )
        client = TestClient(app)
        response = client.get("/collections/books/consistency")
    finally:
        import anyio

        anyio.run(async_client.aclose)

    assert response.status_code == 200
    payload = response.json()
    assert payload["collection"] == "books"
    assert payload["shards"]["0"]["replicas"]["node-2"]["missing_on_replica"] == ["doc-2"]


def test_coordinator_repair_endpoint_repairs_missing_replica_document(tmp_path) -> None:
    config = {
        "coordinator": {"host": "127.0.0.1", "port": 9100},
        "shard_count": 1,
        "nodes": [
            {"id": "node-1", "host": "127.0.0.1", "port": 9101},
            {"id": "node-2", "host": "127.0.0.1", "port": 9102},
        ],
        "shards": {
            "0": {"primary": "node-1", "replicas": ["node-2"]},
        },
    }
    documents = {
        9101: {
            "doc-1": {"id": "doc-1", "title": "First"},
            "doc-2": {"id": "doc-2", "title": "Second"},
        },
        9102: {
            "doc-1": {"id": "doc-1", "title": "First"},
        },
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        port = request.url.port
        path = request.url.path

        if path.endswith("/document_ids"):
            return httpx.Response(200, json={"ids": sorted(documents.get(port, {}))})

        prefix = "/internal/shards/0/collections/books/documents/"
        if path.startswith(prefix) and request.method == "GET":
            document_id = path.removeprefix(prefix)
            return httpx.Response(200, json=documents[port][document_id])

        if path == "/internal/shards/0/collections/books/documents" and request.method == "POST":
            document = json.loads(request.content.decode("utf-8"))
            documents.setdefault(port, {})[document["id"]] = document
            return httpx.Response(200, json=document)

        raise AssertionError(f"unexpected request {request.method} {request.url}")

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        app = create_app(
            role="coordinator",
            cluster_config=config,
            data_dir=tmp_path,
            coordinator_client=async_client,
        )
        client = TestClient(app)
        response = client.post("/collections/books/repair")
    finally:
        import anyio

        anyio.run(async_client.aclose)

    assert response.status_code == 200
    payload = response.json()
    assert payload["repaired"] == 1
    assert payload["shards"]["0"]["repaired"]["node-2"] == ["doc-2"]
    assert payload["shards"]["0"]["failed"] == {}
    assert documents[9102]["doc-2"] == {"id": "doc-2", "title": "Second"}


def make_docx(*paragraphs: str) -> bytes:
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        "<w:body>"
        + "".join(
            f"<w:p><w:r><w:t>{paragraph}</w:t></w:r></w:p>"
            for paragraph in paragraphs
        )
        + "</w:body></w:document>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    relationships = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/>'
        "</Relationships>"
    )

    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", relationships)
        archive.writestr("word/document.xml", document_xml)
    return output.getvalue()


def make_pdf(text: str) -> bytes:
    stream = f"BT /F1 24 Tf 72 720 Td ({text}) Tj ET"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 4 0 R >> >> "
        b"/MediaBox [0 0 612 792] /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream".encode(),
    ]
    pdf = BytesIO()
    pdf.write(b"%PDF-1.4\n")
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(pdf.tell())
        pdf.write(f"{index} 0 obj\n".encode())
        pdf.write(obj)
        pdf.write(b"\nendobj\n")
    xref_offset = pdf.tell()
    pdf.write(f"xref\n0 {len(objects) + 1}\n".encode())
    pdf.write(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.write(f"{offset:010d} 00000 n \n".encode())
    pdf.write(
        (
            "trailer\n"
            f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n"
            "startxref\n"
            f"{xref_offset}\n"
            "%%EOF\n"
        ).encode()
    )
    return pdf.getvalue()
