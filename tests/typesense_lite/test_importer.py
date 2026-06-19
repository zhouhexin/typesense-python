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
