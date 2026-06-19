import json
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

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
