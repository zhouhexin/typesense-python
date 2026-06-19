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

    async def import_documents(
        self,
        collection: str,
        documents: list[Document],
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        succeeded = 0

        for document in documents:
            document_id = document.get("id")
            try:
                result = await self.add_document(collection, document)
            except (ValueError, httpx.HTTPError) as error:
                results.append(
                    {
                        "id": document_id if isinstance(document_id, str) else None,
                        "ok": False,
                        "error": str(error),
                        "warnings": [],
                    }
                )
                continue

            succeeded += 1
            results.append(
                {
                    "id": result["id"],
                    "ok": True,
                    "shard_id": result["shard_id"],
                    "primary": result["primary"],
                    "replicas": result["replicas"],
                    "warnings": result["warnings"],
                }
            )

        failed = len(documents) - succeeded
        return {
            "ok": failed == 0,
            "total": len(documents),
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }

    async def list_collections(self) -> dict[str, list[str]]:
        collections: set[str] = set()
        for node in self.cluster.nodes.values():
            try:
                response = await self._client.get(f"{node.url}/internal/collections")
                response.raise_for_status()
            except httpx.HTTPError:
                continue
            collections.update(response.json().get("collections", []))
        return {"collections": sorted(collections)}

    async def list_documents(self, collection: str) -> dict[str, list[Document]]:
        documents: list[Document] = []
        for shard_id in range(self.cluster.shard_count):
            response = await self._client.get(
                self._documents_url(self.cluster.get_primary(shard_id), shard_id, collection)
            )
            response.raise_for_status()
            documents.extend(response.json().get("documents", []))
        documents.sort(key=lambda document: str(document["id"]))
        return {"documents": documents}

    async def get_document(self, collection: str, document_id: str) -> Document:
        shard_id = self.cluster.get_shard_id(document_id)
        response = await self._client.get(
            self._single_document_url(
                self.cluster.get_primary(shard_id),
                shard_id,
                collection,
                document_id,
            )
        )
        response.raise_for_status()
        return response.json()

    async def update_document(
        self,
        collection: str,
        document_id: str,
        changes: Document,
    ) -> dict[str, Any]:
        shard_id = self.cluster.get_shard_id(document_id)
        primary = self.cluster.get_primary(shard_id)
        replicas = self.cluster.get_replicas(shard_id)

        response = await self._client.patch(
            self._single_document_url(primary, shard_id, collection, document_id),
            json=changes,
        )
        response.raise_for_status()

        replica_results: list[dict[str, Any]] = []
        warnings: list[str] = []
        for replica in replicas:
            try:
                replica_response = await self._client.patch(
                    self._single_document_url(
                        replica,
                        shard_id,
                        collection,
                        document_id,
                    ),
                    json=changes,
                )
                replica_response.raise_for_status()
                replica_results.append({"node": replica.id, "ok": True})
            except httpx.HTTPError as error:
                replica_results.append({"node": replica.id, "ok": False})
                warnings.append(f"replica {replica.id} update failed: {error}")

        return {
            "ok": True,
            "id": document_id,
            "document": response.json(),
            "shard_id": shard_id,
            "primary": primary.id,
            "replicas": replica_results,
            "warnings": warnings,
        }

    async def delete_document(
        self,
        collection: str,
        document_id: str,
    ) -> dict[str, Any]:
        shard_id = self.cluster.get_shard_id(document_id)
        primary = self.cluster.get_primary(shard_id)
        replicas = self.cluster.get_replicas(shard_id)

        response = await self._client.delete(
            self._single_document_url(primary, shard_id, collection, document_id)
        )
        response.raise_for_status()

        replica_results: list[dict[str, Any]] = []
        warnings: list[str] = []
        for replica in replicas:
            try:
                replica_response = await self._client.delete(
                    self._single_document_url(
                        replica,
                        shard_id,
                        collection,
                        document_id,
                    )
                )
                replica_response.raise_for_status()
                replica_results.append({"node": replica.id, "ok": True})
            except httpx.HTTPError as error:
                replica_results.append({"node": replica.id, "ok": False})
                warnings.append(f"replica {replica.id} delete failed: {error}")

        return {
            "ok": True,
            "id": document_id,
            "document": response.json(),
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
    def _documents_url(node: NodeInfo, shard_id: int, collection: str) -> str:
        return (
            f"{node.url}/internal/shards/{shard_id}"
            f"/collections/{collection}/documents"
        )

    @staticmethod
    def _single_document_url(
        node: NodeInfo,
        shard_id: int,
        collection: str,
        document_id: str,
    ) -> str:
        return (
            f"{node.url}/internal/shards/{shard_id}"
            f"/collections/{collection}/documents/{document_id}"
        )

    @staticmethod
    def _search_url(node: NodeInfo, shard_id: int, collection: str) -> str:
        return (
            f"{node.url}/internal/shards/{shard_id}"
            f"/collections/{collection}/search"
        )
