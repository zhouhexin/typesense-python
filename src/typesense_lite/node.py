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

    def get_document(self, shard_id: int, collection: str, document_id: str) -> Document:
        return self._index_for(shard_id, collection).get_document(document_id)

    def list_documents(self, shard_id: int, collection: str) -> list[Document]:
        return self._index_for(shard_id, collection).list_documents()

    def update_document(
        self,
        shard_id: int,
        collection: str,
        document_id: str,
        changes: Document,
    ) -> Document:
        if "id" in changes and changes["id"] != document_id:
            raise ValueError("document id cannot be changed")
        current = self.get_document(shard_id, collection, document_id)
        updated = {**current, **changes, "id": document_id}
        return self.add_document(shard_id, collection, updated)

    def delete_document(
        self,
        shard_id: int,
        collection: str,
        document_id: str,
    ) -> Document:
        index = self._index_for(shard_id, collection)
        deleted = index.delete_document(document_id)
        self.storage.append_delete(shard_id, collection, document_id)
        return deleted

    def list_collections(self) -> list[str]:
        collections = {collection for _, collection in self.storage.iter_collection_paths()}
        collections.update(collection for _, collection in self._indexes)
        return sorted(collections)

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
            for record in self.storage.iter_records(shard_id, collection):
                if record["op"] == "upsert":
                    index.add_document(record["document"])
                elif record["op"] == "delete":
                    try:
                        index.delete_document(record["id"])
                    except KeyError:
                        continue

    def _index_for(self, shard_id: int, collection: str) -> InvertedIndex:
        key = (shard_id, collection)
        if key not in self._indexes:
            self._indexes[key] = InvertedIndex()
        return self._indexes[key]
