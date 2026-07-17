"""JSONL document persistence for Typesense Lite data nodes."""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any, Iterator

Document = dict[str, Any]
StorageRecord = dict[str, Any]


class DocumentStorage:
    """Append-only document storage scoped to one data node."""

    def __init__(self, node_id: str, data_dir: str | Path) -> None:
        self.node_id = node_id
        self.data_dir = Path(data_dir)

    def append_document(
        self,
        shard_id: int,
        collection: str,
        document: Document,
    ) -> None:
        self.append_upsert(shard_id, collection, document)

    def append_upsert(
        self,
        shard_id: int,
        collection: str,
        document: Document,
    ) -> None:
        self._append_record(shard_id, collection, {"op": "upsert", "document": document})

    def append_delete(
        self,
        shard_id: int,
        collection: str,
        document_id: str,
    ) -> None:
        self._append_record(shard_id, collection, {"op": "delete", "id": document_id})

    def iter_records(self, shard_id: int, collection: str) -> Iterator[StorageRecord]:
        path = self.documents_path(shard_id, collection)
        if not path.exists():
            return

        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("op") in {"upsert", "delete"}:
                    yield record
                else:
                    yield {"op": "upsert", "document": record}

    def _append_record(
        self,
        shard_id: int,
        collection: str,
        record: StorageRecord,
    ) -> None:
        path = self.documents_path(shard_id, collection)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")

    def iter_documents(self, shard_id: int, collection: str) -> Iterator[Document]:
        documents: dict[str, Document] = {}
        for record in self.iter_records(shard_id, collection):
            if record["op"] == "upsert":
                document = record["document"]
                documents[document["id"]] = document
            elif record["op"] == "delete":
                documents.pop(record["id"], None)
        for document_id in sorted(documents):
            yield documents[document_id]

    def iter_collection_paths(self) -> Iterator[tuple[int, str]]:
        node_root = self.data_dir / self.node_id
        if not node_root.exists():
            return

        for shard_dir in sorted(node_root.glob("shard-*")):
            shard_id_text = shard_dir.name.removeprefix("shard-")
            if not shard_id_text.isdigit():
                continue
            shard_id = int(shard_id_text)
            collections_dir = shard_dir / "collections"
            if not collections_dir.exists():
                continue
            for collection_dir in sorted(collections_dir.iterdir()):
                if collection_dir.is_dir():
                    yield shard_id, collection_dir.name

    def iter_shard_collections(self, shard_id: int) -> Iterator[str]:
        collections_dir = self.shard_path(shard_id) / "collections"
        if not collections_dir.exists():
            return
        for collection_dir in sorted(collections_dir.iterdir()):
            if collection_dir.is_dir():
                yield collection_dir.name

    def shard_path(self, shard_id: int) -> Path:
        return self.data_dir / self.node_id / f"shard-{shard_id}"

    def shard_has_documents(self, shard_id: int) -> bool:
        return any(True for _ in self.iter_shard_collections(shard_id))

    def replace_shard_documents(
        self, shard_id: int, collections: dict[str, list[Document]]
    ) -> None:
        node_root = self.data_dir / self.node_id
        node_root.mkdir(parents=True, exist_ok=True)
        shard_root = self.shard_path(shard_id)
        suffix = str(time.time_ns())
        rebuilding_root = node_root / f"shard-{shard_id}.rebuilding-{suffix}"
        backup_root = node_root / f"shard-{shard_id}.backup-{suffix}"

        try:
            rebuilding_root.mkdir(parents=True, exist_ok=False)
            for collection, documents in collections.items():
                path = rebuilding_root / "collections" / collection / "documents.jsonl"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w", encoding="utf-8") as handle:
                    for document in sorted(
                        documents, key=lambda item: str(item.get("id", ""))
                    ):
                        handle.write(
                            json.dumps(
                                {"op": "upsert", "document": document},
                                ensure_ascii=False,
                                sort_keys=True,
                            )
                        )
                        handle.write("\n")
            if shard_root.exists():
                shard_root.rename(backup_root)
            rebuilding_root.rename(shard_root)
            if backup_root.exists():
                shutil.rmtree(backup_root)
        except Exception:
            if rebuilding_root.exists():
                shutil.rmtree(rebuilding_root)
            if backup_root.exists() and not shard_root.exists():
                backup_root.rename(shard_root)
            raise

    def documents_path(self, shard_id: int, collection: str) -> Path:
        return (
            self.data_dir
            / self.node_id
            / f"shard-{shard_id}"
            / "collections"
            / collection
            / "documents.jsonl"
        )
