"""Coordinator for shard routing, replication, and distributed search."""

from __future__ import annotations

from typing import Any

import httpx

from .cluster import ClusterMap
from .schemas import Document, NodeInfo, SearchHit


class Coordinator:
    """Routes writes and searches across a static Typesense Lite cluster."""

    def __init__(
        self,
        cluster: ClusterMap,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.cluster = cluster
        self._client = client or httpx.AsyncClient(timeout=3.0)
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def add_document(self, collection: str, document: Document) -> dict[str, Any]:
        document_id = document.get("id")
        if not isinstance(document_id, str) or not document_id:
            raise ValueError("document id must be a non-empty string")

        shard_id = self.cluster.get_shard_id(document_id)
        primary = self.cluster.get_primary(shard_id)
        replicas = self.cluster.get_replicas(shard_id)

        await self._post_document(primary, shard_id, collection, document)

        replica_results: list[dict[str, Any]] = []
        warnings: list[str] = []
        for replica in replicas:
            try:
                await self._post_document(replica, shard_id, collection, document)
                replica_results.append({"node": replica.id, "ok": True})
            except httpx.HTTPError as error:
                replica_results.append({"node": replica.id, "ok": False})
                warnings.append(f"replica {replica.id} write failed: {error}")

        return {
            "ok": True,
            "id": document_id,
            "shard_id": shard_id,
            "primary": primary.id,
            "replicas": replica_results,
            "warnings": warnings,
        }

    async def search(
        self,
        collection: str,
        query: str,
        limit: int = 10,
    ) -> dict[str, Any]:
        hits: list[SearchHit] = []
        warnings: list[str] = []

        for shard_id in range(self.cluster.shard_count):
            shard_hits, warning = await self._search_shard(
                shard_id,
                collection,
                query,
                limit,
            )
            hits.extend(shard_hits)
            if warning:
                warnings.append(warning)

        hits.sort(key=lambda hit: (-float(hit["score"]), str(hit["id"])))
        hits = hits[:limit]
        return {"found": len(hits), "hits": hits, "warnings": warnings}

    async def _post_document(
        self,
        node: NodeInfo,
        shard_id: int,
        collection: str,
        document: Document,
    ) -> None:
        response = await self._client.post(
            self._document_url(node, shard_id, collection),
            json=document,
        )
        response.raise_for_status()

    async def _search_shard(
        self,
        shard_id: int,
        collection: str,
        query: str,
        limit: int,
    ) -> tuple[list[SearchHit], str | None]:
        errors: list[str] = []
        for node in self.cluster.get_search_candidates(shard_id):
            try:
                response = await self._client.get(
                    self._search_url(node, shard_id, collection),
                    params={"q": query, "limit": limit},
                )
                response.raise_for_status()
                payload = response.json()
                return payload.get("hits", []), None
            except httpx.HTTPError as error:
                errors.append(f"{node.id}: {error}")
        return [], f"shard {shard_id} unavailable: {'; '.join(errors)}"

    @staticmethod
    def _document_url(node: NodeInfo, shard_id: int, collection: str) -> str:
        return (
            f"{node.url}/internal/shards/{shard_id}"
            f"/collections/{collection}/documents"
        )

    @staticmethod
    def _search_url(node: NodeInfo, shard_id: int, collection: str) -> str:
        return (
            f"{node.url}/internal/shards/{shard_id}"
            f"/collections/{collection}/search"
        )

