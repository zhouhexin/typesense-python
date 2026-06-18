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
