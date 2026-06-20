import json

from typesense_lite.node import SearchNode


def test_node_persists_reloads_and_searches_documents(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-1", "title": "Distributed search"})

    reloaded = SearchNode(node_id="node-1", data_dir=tmp_path)
    hits = reloaded.search(0, "books", "distributed", limit=10)

    assert hits[0]["id"] == "doc-1"
    assert hits[0]["document"]["title"] == "Distributed search"
    assert hits[0]["shard_id"] == 0
    assert hits[0]["node_id"] == "node-1"


def test_node_keeps_collections_isolated(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-1", "title": "Search book"})
    node.add_document(0, "movies", {"id": "doc-2", "title": "Search movie"})

    assert node.search(0, "books", "movie", limit=10) == []
    assert node.search(0, "movies", "movie", limit=10)[0]["id"] == "doc-2"


def test_node_writes_upsert_envelopes(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)

    node.add_document(0, "books", {"id": "doc-1", "title": "Search book"})

    path = (
        tmp_path
        / "node-1"
        / "shard-0"
        / "collections"
        / "books"
        / "documents.jsonl"
    )
    line = json.loads(path.read_text(encoding="utf-8").strip())
    assert line == {
        "op": "upsert",
        "document": {"id": "doc-1", "title": "Search book"},
    }


def test_node_loads_legacy_plain_document_jsonl(tmp_path) -> None:
    path = (
        tmp_path
        / "node-1"
        / "shard-0"
        / "collections"
        / "books"
        / "documents.jsonl"
    )
    path.parent.mkdir(parents=True)
    path.write_text('{"id": "doc-1", "title": "Legacy search"}\n', encoding="utf-8")

    node = SearchNode(node_id="node-1", data_dir=tmp_path)

    assert node.search(0, "books", "legacy", limit=10)[0]["id"] == "doc-1"


def test_node_deletes_document_and_persists_tombstone(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-1", "title": "Deleted search"})
    node.add_document(0, "books", {"id": "doc-2", "title": "Kept search"})

    deleted = node.delete_document(0, "books", "doc-1")
    reloaded = SearchNode(node_id="node-1", data_dir=tmp_path)

    assert deleted["id"] == "doc-1"
    assert [hit["id"] for hit in reloaded.search(0, "books", "search", limit=10)] == [
        "doc-2"
    ]

    path = (
        tmp_path
        / "node-1"
        / "shard-0"
        / "collections"
        / "books"
        / "documents.jsonl"
    )
    records = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    assert records[-1] == {"op": "delete", "id": "doc-1"}


def test_node_lists_collections(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-1", "title": "Book"})
    node.add_document(1, "movies", {"id": "doc-2", "title": "Movie"})
    node.add_document(1, "books", {"id": "doc-3", "title": "Another book"})

    assert node.list_collections() == ["books", "movies"]


def test_node_lists_document_ids(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-3", "title": "Third"})
    node.add_document(0, "books", {"id": "doc-1", "title": "First"})
    node.add_document(0, "books", {"id": "doc-2", "title": "Second"})

    ids = node.list_document_ids(0, "books")

    assert ids == ["doc-1", "doc-2", "doc-3"]


def test_node_lists_document_ids_empty_collection(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)

    ids = node.list_document_ids(0, "books")

    assert ids == []


def test_search_node_applies_add_document_raft_command(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)

    result = node.apply_raft_command(
        0,
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1", "title": "Raft"},
        },
    )

    assert result["id"] == "book-1"
    assert node.get_document(0, "books", "book-1")["title"] == "Raft"


def test_search_node_applies_delete_document_raft_command(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "book-1", "title": "Raft"})

    result = node.apply_raft_command(
        0,
        {
            "type": "delete_document",
            "collection": "books",
            "document_id": "book-1",
        },
    )

    assert result["id"] == "book-1"
