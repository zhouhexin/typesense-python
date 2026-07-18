"""Leader-aware shard snapshot helpers for replica rebuild."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

Document = dict[str, Any]


@dataclass(frozen=True)
class ReplicaIntegrityResult:
    valid: bool
    requires_rebuild: bool
    reason: str | None = None


def snapshot_checksum(collections: dict[str, list[Document]]) -> str:
    payload = {
        collection: sorted(documents, key=lambda item: str(item.get("id", "")))
        for collection, documents in sorted(collections.items())
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_snapshot_payload(
    *,
    shard_id: int,
    source_node_id: str,
    source_role: str,
    term: int,
    snapshot_index: int,
    commit_index: int,
    collections: dict[str, list[Document]],
    log_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest = {
        "shard_id": shard_id,
        "source_node_id": source_node_id,
        "source_role": source_role,
        "term": term,
        "snapshot_index": snapshot_index,
        "commit_index": commit_index,
        "collections": {
            collection: {
                "document_count": len(documents),
                "document_ids": [
                    str(document["id"])
                    for document in sorted(
                        documents, key=lambda item: str(item.get("id", ""))
                    )
                ],
            }
            for collection, documents in sorted(collections.items())
        },
        "document_count": sum(len(items) for items in collections.values()),
        "checksum": snapshot_checksum(collections),
        "created_at": time.time(),
    }
    return {
        "manifest": manifest,
        "collections": collections,
        "log_entries": log_entries,
    }


def validate_snapshot_payload(
    payload: dict[str, Any],
    *,
    expected_shard_id: int,
    expected_source_node_id: str,
    expected_term: int | None = None,
    expected_snapshot_index: int | None = None,
    expected_commit_index: int | None = None,
) -> dict[str, Any]:
    manifest = payload.get("manifest")
    collections = payload.get("collections")
    if not isinstance(manifest, dict):
        raise ValueError("snapshot manifest is missing")
    if not isinstance(collections, dict):
        raise ValueError("snapshot collections are missing")
    if int(manifest.get("shard_id", -1)) != expected_shard_id:
        raise ValueError("snapshot shard id mismatch")
    if manifest.get("source_node_id") != expected_source_node_id:
        raise ValueError("snapshot source leader mismatch")
    if manifest.get("source_role") != "leader":
        raise ValueError("snapshot source is not leader")

    term = int(manifest.get("term", -1))
    snapshot_index = int(manifest.get("snapshot_index", -1))
    commit_index = int(manifest.get("commit_index", -1))
    if term < 0 or snapshot_index < 0 or commit_index < 0:
        raise ValueError("snapshot index or term is invalid")
    if snapshot_index > commit_index:
        raise ValueError("snapshot index is ahead of commit index")
    expected_values = (
        (expected_term, term, "snapshot term changed"),
        (expected_snapshot_index, snapshot_index, "snapshot index changed"),
        (expected_commit_index, commit_index, "snapshot commit index changed"),
    )
    for expected, actual, message in expected_values:
        if expected is not None and actual != expected:
            raise ValueError(message)

    normalized: dict[str, list[Document]] = {}
    for collection, documents in collections.items():
        if not isinstance(collection, str) or not isinstance(documents, list):
            raise ValueError("snapshot collection format is invalid")
        normalized[collection] = []
        for document in documents:
            if not isinstance(document, dict) or not isinstance(document.get("id"), str):
                raise ValueError("snapshot document format is invalid")
            normalized[collection].append(dict(document))
    if snapshot_checksum(normalized) != manifest.get("checksum"):
        raise ValueError("snapshot checksum mismatch")

    return {
        "manifest": dict(manifest),
        "collections": normalized,
        "log_entries": list(payload.get("log_entries", [])),
    }
