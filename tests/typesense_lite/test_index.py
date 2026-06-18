import pytest

from typesense_lite.index import InvertedIndex


def test_index_searches_string_fields_by_term_frequency() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "title": "Search search engine"})
    index.add_document({"id": "doc-2", "title": "Distributed engine"})

    hits = index.search("search engine", limit=10)

    assert [hit["id"] for hit in hits] == ["doc-1", "doc-2"]
    assert hits[0]["score"] == 3.0
    assert hits[1]["score"] == 1.0


def test_index_replaces_existing_document() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "title": "old term"})
    index.add_document({"id": "doc-1", "title": "new term"})

    assert index.search("old", limit=10) == []
    assert index.search("new", limit=10)[0]["id"] == "doc-1"


def test_index_requires_string_document_id() -> None:
    index = InvertedIndex()

    with pytest.raises(ValueError, match="document id"):
        index.add_document({"id": 123, "title": "invalid"})
