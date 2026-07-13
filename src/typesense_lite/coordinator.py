"""Coordinator for shard routing, replication, and distributed search."""

from __future__ import annotations

from typing import Any

import httpx

from .cluster import ClusterMap
from .health import collect_cluster_health
from .http_client import make_cross_machine_client
from .leader_directory import LeaderDirectory
from .node_directory import NodeDirectory
from .repair import check_consistency as _check_consistency, repair_collection as _repair_collection
from .schemas import Document, NodeInfo, SearchHit


class Coordinator:
    """Routes writes and searches across a static Typesense Lite cluster."""

    def __init__(
        self,
        cluster: ClusterMap,
        client: httpx.AsyncClient | None = None,
        *,
        node_directory: NodeDirectory | None = None,
        node_alive_timeout: float = 30.0,
    ) -> None:
        self.cluster = cluster
        self._client = client or make_cross_machine_client()
        self._owns_client = client is None
        self._leaders = LeaderDirectory(cluster, self._client)
        self.nodes = node_directory or NodeDirectory(alive_timeout=node_alive_timeout)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def add_document(self, collection: str, document: Document) -> dict[str, Any]:
        document_id = document.get("id")
        if not isinstance(document_id, str) or not document_id:
            raise ValueError("document id must be a non-empty string")

        shard_id = self.cluster.get_shard_id(document_id)
        result = await self._submit_raft_command(
            shard_id,
            {
                "type": "add_document",
                "collection": collection,
                "document": document,
            },
        )

        return {
            "ok": True,
            "id": document_id,
            "shard_id": shard_id,
            "primary": result["leader"],
            "leader": result["leader"],
            "term": result.get("term"),
            "commit_index": result.get("commit_index"),
            "replicas": [],
            "warnings": [],
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
        result = await self._submit_raft_command(
            shard_id,
            {
                "type": "update_document",
                "collection": collection,
                "document_id": document_id,
                "changes": changes,
            },
        )

        return {
            "ok": True,
            "id": document_id,
            "document": result.get("result", {}),
            "shard_id": shard_id,
            "primary": result["leader"],
            "leader": result["leader"],
            "term": result.get("term"),
            "commit_index": result.get("commit_index"),
            "replicas": [],
            "warnings": [],
        }

    async def delete_document(
        self,
        collection: str,
        document_id: str,
    ) -> dict[str, Any]:
        shard_id = self.cluster.get_shard_id(document_id)
        result = await self._submit_raft_command(
            shard_id,
            {
                "type": "delete_document",
                "collection": collection,
                "document_id": document_id,
            },
        )

        return {
            "ok": True,
            "id": document_id,
            "document": result.get("result", {}),
            "shard_id": shard_id,
            "primary": result["leader"],
            "leader": result["leader"],
            "term": result.get("term"),
            "commit_index": result.get("commit_index"),
            "replicas": [],
            "warnings": [],
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

    async def _submit_raft_command(
        self,
        shard_id: int,
        command: dict[str, Any],
    ) -> dict[str, Any]:
        leader = await self._leaders.get_leader(shard_id)
        response = await self._client.post(
            f"{leader.url}/internal/raft/{shard_id}/commands",
            json=command,
        )
        if response.status_code == 409:
            self._leaders.invalidate(shard_id)
            leader = await self._leaders.get_leader(shard_id)
            response = await self._client.post(
                f"{leader.url}/internal/raft/{shard_id}/commands",
                json=command,
            )
        response.raise_for_status()
        payload = response.json()
        if payload.get("ok") is not True:
            raise httpx.HTTPStatusError(
                str(payload.get("error") or "raft command failed"),
                request=response.request,
                response=response,
            )
        return payload

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

    async def cluster_health(self) -> dict[str, Any]:
        """Collect health status from all nodes in the cluster."""
        report = await collect_cluster_health(self.cluster, self._client)
        return report.to_dict()

    async def raft_status(self) -> dict[str, Any]:
        shards: dict[str, Any] = {}
        for shard_id in range(self.cluster.shard_count):
            members: dict[str, Any] = {}
            leader: str | None = None
            term = 0

            for node in self.cluster.get_shard_voters(shard_id):
                try:
                    response = await self._client.get(
                        f"{node.url}/internal/raft/{shard_id}/state"
                    )
                    response.raise_for_status()
                except httpx.HTTPError as error:
                    members[node.id] = {
                        "role": "unavailable",
                        "term": None,
                        "commit_index": None,
                        "last_applied": None,
                        "error": str(error),
                    }
                    continue

                payload = response.json()
                role = payload.get("role")
                node_term = int(payload.get("current_term", 0))
                if node_term > term:
                    term = node_term
                if role == "leader":
                    leader = node.id
                members[node.id] = {
                    "role": role,
                    "term": node_term,
                    "commit_index": payload.get("commit_index"),
                    "last_applied": payload.get("last_applied"),
                    "leader_id": payload.get("leader_id"),
                }

            shards[str(shard_id)] = {
                "leader": leader,
                "term": term,
                "members": members,
            }

        return {"shards": shards}

    async def check_consistency(self, collection: str) -> dict[str, Any]:
        """Check consistency between primary and replicas for a collection."""
        return await _check_consistency(self.cluster, collection, self._client)

    async def repair_collection(self, collection: str) -> dict[str, Any]:
        """Repair replica consistency by copying missing documents from primary."""
        return await _repair_collection(self.cluster, collection, self._client)

    # ------------------------------------------------------------------
    # Node discovery (register / heartbeat / alive snapshot)
    # ------------------------------------------------------------------

    def register_node(
        self,
        *,
        node_id: str,
        host: str,
        port: int,
        role: str,
    ) -> dict[str, Any]:
        """Add or refresh a node's liveness entry. Returns the new entry."""
        entry = self.nodes.register(
            node_id=node_id,
            host=host,
            port=port,
            role=role,
        )
        return self._entry_to_dict(entry)

    def heartbeat_node(self, node_id: str) -> dict[str, Any] | None:
        """Refresh a node's last-seen timestamp. Returns None if unknown."""
        entry = self.nodes.heartbeat(node_id)
        if entry is None:
            return None
        return self._entry_to_dict(entry)

    def cluster_nodes(self) -> dict[str, Any]:
        """Return static topology plus currently alive (registered) nodes."""
        self.nodes.cleanup_expired()
        static = [
            {"id": node.id, "host": node.host, "port": node.port}
            for node in self.cluster.nodes.values()
        ]
        alive = list(self.nodes.snapshot().values())
        alive.sort(key=lambda entry: entry["node_id"])
        return {
            "static": static,
            "alive": alive,
            "alive_timeout_seconds": self.nodes.alive_timeout,
        }

    @staticmethod
    def _entry_to_dict(entry: Any) -> dict[str, Any]:
        return {
            "node_id": entry.node_id,
            "host": entry.host,
            "port": entry.port,
            "role": entry.role,
            "last_seen": entry.last_seen,
            "registered_at": entry.registered_at,
        }
