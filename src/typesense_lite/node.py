"""Data-node search and storage operations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .index import InvertedIndex
from .storage import DocumentStorage

Document = dict[str, Any]
SearchHit = dict[str, Any]


class SearchNode:
    """Owns local indexes for one data-node process."""

    def __init__(self, node_id: str, data_dir: str | Path) -> None:
        self.node_id = node_id
        self.storage = DocumentStorage(node_id=node_id, data_dir=data_dir)
        self._indexes: dict[tuple[int, str], InvertedIndex] = {}
        self._reload()

    def add_document(
        self,
        shard_id: int,
        collection: str,
        document: Document,
    ) -> Document:
        index = self._index_for(shard_id, collection)
        index.add_document(document)
        self.storage.append_document(shard_id, collection, document)
        return document

    def search(
        self,
        shard_id: int,
        collection: str,
        query: str,
        limit: int = 10,
    ) -> list[SearchHit]:
        index = self._index_for(shard_id, collection)
        hits = index.search(query, limit)
        for hit in hits:
            hit["shard_id"] = shard_id
            hit["node_id"] = self.node_id
        return hits

    def _reload(self) -> None:
        for shard_id, collection in self.storage.iter_collection_paths():
            index = self._index_for(shard_id, collection)
            for document in self.storage.iter_documents(shard_id, collection):
                index.add_document(document)

    def _index_for(self, shard_id: int, collection: str) -> InvertedIndex:
        key = (shard_id, collection)
        if key not in self._indexes:
            self._indexes[key] = InvertedIndex()
        return self._indexes[key]

