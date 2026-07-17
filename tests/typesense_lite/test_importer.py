from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from typesense_lite.importer import parse_upload


def test_parse_txt_upload_as_single_document() -> None:
    documents = parse_upload("Search Notes.txt", b"hello distributed search")

    assert documents == [
        {
            "id": "search-notes-txt",
            "title": "Search Notes.txt",
            "body": "hello distributed search",
            "source": "Search Notes.txt",
        }
    ]


def test_parse_md_upload_as_single_document() -> None:
    documents = parse_upload("README.md", b"# Demo\nkeyword search")

    assert documents == [
        {
            "id": "readme-md",
            "title": "README.md",
            "body": "# Demo\nkeyword search",
            "source": "README.md",
        }
    ]


def test_parse_docx_upload_as_single_document() -> None:
    documents = parse_upload(
        "Project Plan.docx",
        make_docx("Word searchable body", "second paragraph"),
    )

    assert documents == [
        {
            "id": "project-plan-docx",
            "title": "Project Plan.docx",
            "body": "Word searchable body\nsecond paragraph",
            "source": "Project Plan.docx",
        }
    ]


def test_parse_docx_upload_preserves_chinese_filename_in_document_id() -> None:
    documents = parse_upload(
        "操作系统.docx",
        make_docx("操作系统负责管理计算机硬件和软件资源"),
    )

    assert documents == [
        {
            "id": "操作系统-docx",
            "title": "操作系统.docx",
            "body": "操作系统负责管理计算机硬件和软件资源",
            "source": "操作系统.docx",
        }
    ]


def test_parse_pdf_upload_as_single_document() -> None:
    documents = parse_upload("Report.pdf", make_pdf("PDF searchable body"))

    assert documents == [
        {
            "id": "report-pdf",
            "title": "Report.pdf",
            "body": "PDF searchable body",
            "source": "Report.pdf",
        }
    ]


def test_parse_json_object_upload() -> None:
    documents = parse_upload("book.json", b'{"id": "doc-1", "title": "Book"}')

    assert documents == [{"id": "doc-1", "title": "Book"}]


def test_parse_json_array_upload() -> None:
    documents = parse_upload(
        "books.json",
        b'[{"id": "doc-1", "title": "A"}, {"id": "doc-2", "title": "B"}]',
    )

    assert [document["id"] for document in documents] == ["doc-1", "doc-2"]


def test_parse_jsonl_upload() -> None:
    documents = parse_upload(
        "books.jsonl",
        b'{"id": "doc-1", "title": "A"}\n{"id": "doc-2", "title": "B"}\n',
    )

    assert [document["id"] for document in documents] == ["doc-1", "doc-2"]


def test_parse_jsonl_skips_blank_lines() -> None:
    documents = parse_upload(
        "books.jsonl",
        b'\n{"id": "doc-1", "title": "A"}\n\n{"id": "doc-2", "title": "B"}\n',
    )

    assert [document["id"] for document in documents] == ["doc-1", "doc-2"]


def test_parse_upload_rejects_unsupported_suffix() -> None:
    with pytest.raises(ValueError, match="unsupported upload type"):
        parse_upload("books.csv", b"id,title\n1,A\n")


def test_parse_upload_reports_invalid_json_filename() -> None:
    with pytest.raises(ValueError, match="broken.json"):
        parse_upload("broken.json", b"{")


def test_parse_upload_reports_invalid_jsonl_line_number() -> None:
    with pytest.raises(ValueError, match="broken.jsonl line 2"):
        parse_upload("broken.jsonl", b'{"id": "doc-1"}\nnot-json\n')


def test_parse_upload_generates_missing_json_ids() -> None:
    documents = parse_upload("books.json", b'[{"title": "A"}, {"title": "B"}]')

    assert [document["id"] for document in documents] == ["books-json-1", "books-json-2"]


def test_parse_upload_generates_missing_jsonl_ids_from_line_number() -> None:
    documents = parse_upload("books.jsonl", b'{"title": "A"}\n\n{"title": "B"}\n')

    assert [document["id"] for document in documents] == ["books-jsonl-1", "books-jsonl-3"]


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
