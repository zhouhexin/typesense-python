"""Replica consistency checking and repair."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class ConsistencyResult:
    """Result of consistency check for one shard."""

    shard_id: int
    primary: str
    missing_on_replica: dict[str, list[str]]
    extra_on_replica: dict[str, list[str]]
    errors: dict[str, str]


@dataclass
class RepairResult:
    """Result of repair operation for one shard."""

    shard_id: int
    repaired: dict[str, list[str]]
    failed: dict[str, list[str]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "repaired": self.repaired,
            "failed": self.failed,
        }


async def check_consistency(
    cluster: Any,
    collection: str,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Check consistency between primary and replicas for a collection.

    Returns a dict with collection name and per-shard consistency info.
    """
    shards_result: dict[str, Any] = {}

    for shard_id in range(cluster.shard_count):
        placement = cluster.shards.get(shard_id)
        if placement is None:
            continue

        primary_node = cluster.nodes.get(placement.primary)
        if primary_node is None:
            continue

        consistency = await _check_shard_consistency(
            primary_node,
            shard_id,
            collection,
            placement.replicas,
            cluster,
            client,
        )
        shards_result[str(shard_id)] = consistency

    return {
        "collection": collection,
        "shards": shards_result,
    }


async def _check_shard_consistency(
    primary: Any,
    shard_id: int,
    collection: str,
    replicas: list[str],
    cluster: Any,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    """Check consistency for a single shard."""
    # Get primary document IDs
    try:
        primary_response = await client.get(
            f"{primary.url}/internal/shards/{shard_id}/collections/{collection}/document_ids"
        )
        primary_response.raise_for_status()
        primary_ids = set(primary_response.json().get("ids", []))
    except httpx.HTTPError as e:
        return {
            "primary": primary.id,
            "error": f"primary unavailable: {e}",
            "replicas": {},
        }

    replica_results: dict[str, Any] = {}

    for replica_id in replicas:
        replica_node = cluster.nodes.get(replica_id)
        if replica_node is None:
            continue

        try:
            replica_response = await client.get(
                f"{replica_node.url}/internal/shards/{shard_id}/collections/{collection}/document_ids"
            )
            replica_response.raise_for_status()
            replica_ids = set(replica_response.json().get("ids", []))
        except httpx.HTTPError as e:
            replica_results[replica_id] = {
                "error": f"replica unavailable: {e}",
            }
            continue

        missing = sorted(primary_ids - replica_ids)
        extra = sorted(replica_ids - primary_ids)

        replica_results[replica_id] = {
            "missing_on_replica": missing,
            "extra_on_replica": extra,
        }

    return {
        "primary": primary.id,
        "replicas": replica_results,
    }


async def repair_collection(
    cluster: Any,
    collection: str,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    """
    Repair replica consistency by copying missing documents from primary.

    Returns a dict with collection name and per-shard repair info.
    """
    # First check consistency
    consistency = await check_consistency(cluster, collection, client)

    shards_result: dict[str, Any] = {}
    total_repaired = 0

    for shard_id_str, shard_info in consistency.get("shards", {}).items():
        shard_id = int(shard_id_str)
        placement = cluster.shards.get(shard_id)
        if placement is None:
            continue

        primary_node = cluster.nodes.get(placement.primary)
        if primary_node is None:
            continue

        repair_result = await _repair_shard(
            primary_node,
            shard_id,
            collection,
            shard_info.get("replicas", {}),
            cluster,
            client,
        )
        shards_result[shard_id_str] = repair_result.to_dict()
        total_repaired += sum(len(ids) for ids in repair_result.repaired.values())

    return {
        "collection": collection,
        "repaired": total_repaired,
        "shards": shards_result,
    }


async def _repair_shard(
    primary: Any,
    shard_id: int,
    collection: str,
    replicas: dict[str, Any],
    cluster: Any,
    client: httpx.AsyncClient,
) -> RepairResult:
    """Repair a single shard by copying missing documents from primary to replicas."""
    repaired: dict[str, list[str]] = {}
    failed: dict[str, list[str]] = {}

    for replica_id, info in replicas.items():
        if "error" in info:
            continue

        missing_ids = info.get("missing_on_replica", [])
        if not missing_ids:
            continue

        replica_node = cluster.nodes.get(replica_id)
        if replica_node is None:
            continue

        replica_repaired: list[str] = []
        replica_failed: list[str] = []

        for doc_id in missing_ids:
            try:
                # Get document from primary
                doc_response = await client.get(
                    f"{primary.url}/internal/shards/{shard_id}/collections/{collection}/documents/{doc_id}"
                )
                doc_response.raise_for_status()
                document = doc_response.json()

                # Write to replica
                write_response = await client.post(
                    f"{replica_node.url}/internal/shards/{shard_id}/collections/{collection}/documents",
                    json=document,
                )
                write_response.raise_for_status()
                replica_repaired.append(doc_id)
            except httpx.HTTPError:
                replica_failed.append(doc_id)

        if replica_repaired:
            repaired[replica_id] = replica_repaired
        if replica_failed:
            failed[replica_id] = replica_failed

    return RepairResult(
        shard_id=shard_id,
        repaired=repaired,
        failed=failed,
    )
