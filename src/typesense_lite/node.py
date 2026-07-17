"""Data-node search and storage operations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .index import InvertedIndex
from .rebuild import ReplicaIntegrityResult, snapshot_checksum
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

    def list_document_ids(self, shard_id: int, collection: str) -> list[str]:
        """Return sorted list of document IDs for a collection."""
        index = self._index_for(shard_id, collection)
        return index.list_document_ids()

    def export_shard_collections(self, shard_id: int) -> dict[str, list[Document]]:
        names = set(self.storage.iter_shard_collections(shard_id))
        names.update(
            collection
            for index_shard_id, collection in self._indexes
            if index_shard_id == shard_id
        )
        return {
            collection: self.list_documents(shard_id, collection)
            for collection in sorted(names)
        }

    def install_shard_snapshot(
        self, shard_id: int, collections: dict[str, list[Document]]
    ) -> None:
        self.storage.replace_shard_documents(shard_id, collections)
        for key in list(self._indexes):
            if key[0] == shard_id:
                self._indexes.pop(key, None)
        for collection, documents in collections.items():
            index = self._index_for(shard_id, collection)
            for document in documents:
                index.add_document(document)

    def check_replica_integrity(
        self, shard_id: int, committed_commands: list[dict[str, Any]]
    ) -> ReplicaIntegrityResult:
        try:
            expected = _collections_from_commands(committed_commands)
            current = self.export_shard_collections(shard_id)
            if committed_commands and not self.storage.shard_has_documents(shard_id):
                return ReplicaIntegrityResult(False, True, "document_store_missing")
            if snapshot_checksum(expected) != snapshot_checksum(current):
                return ReplicaIntegrityResult(
                    False, True, "state_machine_checksum_mismatch"
                )
        except Exception as error:
            return ReplicaIntegrityResult(False, True, f"integrity_check_failed: {error}")
        return ReplicaIntegrityResult(True, False)

    def apply_raft_command(self, shard_id: int, command: dict[str, Any]) -> Document:
        command_type = command.get("type")
        collection = str(command.get("collection") or "")

        if command_type == "add_document":
            return self.add_document(shard_id, collection, command["document"])
        if command_type == "update_document":
            return self.update_document(
                shard_id,
                collection,
                str(command["document_id"]),
                command["changes"],
            )
        if command_type == "delete_document":
            return self.delete_document(
                shard_id,
                collection,
                str(command["document_id"]),
            )

        raise ValueError(f"unknown raft command type {command_type!r}")

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

    def search_query(
        self,
        shard_id: int,
        collection: str,
        query: str,
        *,
        query_by: str | None = None,
        query_by_weights: str | None = None,
        prefix: bool = False,
        num_typos: int = 0,
        filter_by: str | None = None,
        sort_by: str | None = None,
        facet_by: str | None = None,
        page: int = 1,
        per_page: int | None = 10,
        highlight_fields: str | None = None,
    ) -> dict[str, Any]:
        result = self._index_for(shard_id, collection).search_query(
            query,
            query_by=query_by,
            query_by_weights=query_by_weights,
            prefix=prefix,
            num_typos=num_typos,
            filter_by=filter_by,
            sort_by=sort_by,
            facet_by=facet_by,
            page=page,
            per_page=per_page,
            highlight_fields=highlight_fields,
        )
        for hit in result["hits"]:
            hit["shard_id"] = shard_id
            hit["node_id"] = self.node_id
        return result

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


def _collections_from_commands(
    commands: list[dict[str, Any]],
) -> dict[str, list[Document]]:
    collections: dict[str, dict[str, Document]] = {}
    for command in commands:
        collection = str(command.get("collection") or "")
        if not collection:
            continue
        documents = collections.setdefault(collection, {})
        command_type = command.get("type")
        if command_type == "add_document":
            document = dict(command["document"])
            documents[str(document["id"])] = document
        elif command_type == "update_document":
            document_id = str(command["document_id"])
            if document_id in documents:
                documents[document_id] = {
                    **documents[document_id],
                    **command["changes"],
                    "id": document_id,
                }
        elif command_type == "delete_document":
            documents.pop(str(command["document_id"]), None)
    return {
        collection: [documents[item] for item in sorted(documents)]
        for collection, documents in sorted(collections.items())
        if documents
    }
