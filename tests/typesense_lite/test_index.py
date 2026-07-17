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


def test_index_searches_chinese_text() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "body": "这是一个分布式中文搜索引擎"})

    hits = index.search("中文搜索", limit=10)

    assert [hit["id"] for hit in hits] == ["doc-1"]
    assert hits[0]["score"] > 0


def test_index_requires_string_document_id() -> None:
    index = InvertedIndex()

    with pytest.raises(ValueError, match="document id"):
        index.add_document({"id": 123, "title": "invalid"})


def test_index_gets_and_lists_documents_by_id() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-2", "title": "Second"})
    index.add_document({"id": "doc-1", "title": "First"})

    assert index.get_document("doc-1") == {"id": "doc-1", "title": "First"}
    assert index.list_documents() == [
        {"id": "doc-1", "title": "First"},
        {"id": "doc-2", "title": "Second"},
    ]


def test_index_get_missing_document_raises_key_error() -> None:
    index = InvertedIndex()

    with pytest.raises(KeyError):
        index.get_document("missing")


def test_index_deletes_document_and_terms() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "title": "old term"})
    index.add_document({"id": "doc-2", "title": "old term"})

    deleted = index.delete_document("doc-1")

    assert deleted == {"id": "doc-1", "title": "old term"}
    assert [hit["id"] for hit in index.search("old", limit=10)] == ["doc-2"]
    with pytest.raises(KeyError):
        index.get_document("doc-1")


def test_search_query_supports_weights_filter_facet_highlight_and_pagination() -> None:
    index = InvertedIndex()
    index.add_document(
        {
            "id": "doc-1",
            "title": "Distributed search",
            "body": "Search engines split documents across shards",
            "category": "tech",
            "popularity": 20,
        }
    )
    index.add_document(
        {
            "id": "doc-2",
            "title": "Search basics",
            "body": "Distributed systems search",
            "category": "tech",
            "popularity": 5,
        }
    )
    index.add_document(
        {
            "id": "doc-3",
            "title": "Cooking tips",
            "body": "Distributed recipes",
            "category": "food",
            "popularity": 1,
        }
    )

    result = index.search_query(
        "distributed search",
        query_by="title,body",
        query_by_weights="3,1",
        filter_by="category:tech",
        facet_by="category",
        highlight_fields="title,body",
        page=1,
        per_page=1,
    )

    assert result["found"] == 2
    assert [hit["id"] for hit in result["hits"]] == ["doc-1"]
    assert "<mark>Distributed</mark>" in result["hits"][0]["highlight"]
    assert result["facet_counts"] == [
        {
            "field_name": "category",
            "counts": [{"value": "tech", "count": 2}],
        }
    ]

    second_page = index.search_query(
        "distributed search",
        query_by="title,body",
        query_by_weights="3,1",
        filter_by="category:tech",
        page=2,
        per_page=1,
    )
    assert [hit["id"] for hit in second_page["hits"]] == ["doc-2"]


def test_search_query_supports_prefix_fuzzy_sort_and_chinese() -> None:
    index = InvertedIndex()
    index.add_document(
        {"id": "doc-1", "title": "Distributed search", "popularity": 10}
    )
    index.add_document(
        {"id": "doc-2", "title": "Search systems", "popularity": 20}
    )
    index.add_document({"id": "doc-cn", "title": "中文分布式搜索引擎"})

    prefix_result = index.search_query("dist", query_by="title", prefix=True)
    fuzzy_result = index.search_query(
        "distributd", query_by="title", num_typos=1
    )
    sorted_result = index.search_query(
        "search", query_by="title", sort_by="popularity:desc"
    )
    chinese_result = index.search_query("分布式搜索", query_by="title")

    assert [hit["id"] for hit in prefix_result["hits"]] == ["doc-1"]
    assert [hit["id"] for hit in fuzzy_result["hits"]] == ["doc-1"]
    assert [hit["id"] for hit in sorted_result["hits"]] == ["doc-2", "doc-1"]
    assert [hit["id"] for hit in chinese_result["hits"]] == ["doc-cn"]
