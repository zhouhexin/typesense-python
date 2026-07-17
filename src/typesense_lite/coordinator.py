"""Coordinator for shard routing, replication, and distributed search."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from .cluster import ClusterMap
from .health import collect_cluster_health
from .http_client import get_with_retry, make_cross_machine_client
from .index import _facet_counts_from_hits, _parse_sort_by, _sort_hits, _split_csv
from .leader_directory import LeaderDirectory
from .node_directory import NodeDirectory
from .repair import check_consistency as _check_consistency, repair_collection as _repair_collection
from .recovery import shard_is_ready
from .schemas import Document, NodeInfo, SearchHit


class Coordinator:
    """Routes writes and searches across a static Typesense Lite cluster."""

    RAFT_SUBMIT_ATTEMPTS = 8
    RAFT_SUBMIT_RETRY_DELAY_SECONDS = 0.25

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
                response = await get_with_retry(
                    self._client, f"{node.url}/internal/collections"
                )
            except httpx.HTTPError:
                continue
            collections.update(response.json().get("collections", []))
        return {"collections": sorted(collections)}

    async def list_documents(self, collection: str) -> dict[str, list[Document]]:
        documents: list[Document] = []
        for shard_id in range(self.cluster.shard_count):
            response = await get_with_retry(
                self._client,
                self._documents_url(self.cluster.get_primary(shard_id), shard_id, collection),
            )
            documents.extend(response.json().get("documents", []))
        documents.sort(key=lambda document: str(document["id"]))
        return {"documents": documents}

    async def get_document(self, collection: str, document_id: str) -> Document:
        shard_id = self.cluster.get_shard_id(document_id)
        response = await get_with_retry(
            self._client,
            self._single_document_url(
                self.cluster.get_primary(shard_id),
                shard_id,
                collection,
                document_id,
            ),
        )
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
        *,
        query_by: str | None = None,
        query_by_weights: str | None = None,
        prefix: bool = False,
        num_typos: int = 0,
        filter_by: str | None = None,
        sort_by: str | None = None,
        facet_by: str | None = None,
        page: int = 1,
        per_page: int | None = None,
        highlight_fields: str | None = None,
    ) -> dict[str, Any]:
        hits: list[SearchHit] = []
        warnings: list[str] = []
        request_per_page = per_page if per_page is not None else limit
        advanced_query = any(
            value is not None
            for value in (
                query_by,
                query_by_weights,
                filter_by,
                sort_by,
                facet_by,
                per_page,
                highlight_fields,
            )
        ) or prefix or num_typos > 0 or page != 1

        for shard_id in range(self.cluster.shard_count):
            shard_hits, warning = await self._search_shard(
                shard_id,
                collection,
                query,
                limit=None if advanced_query else limit,
                query_by=query_by,
                query_by_weights=query_by_weights,
                prefix=prefix,
                num_typos=num_typos,
                filter_by=filter_by,
                sort_by=sort_by,
                facet_by=facet_by,
                highlight_fields=highlight_fields,
            )
            hits.extend(shard_hits)
            if warning:
                warnings.append(warning)

        _sort_hits(hits, _parse_sort_by(sort_by))
        total = len(hits)
        facet_counts = _facet_counts_from_hits(hits, _split_csv(facet_by))
        start = max(page - 1, 0) * request_per_page
        hits = hits[start : start + request_per_page]
        return {
            "found": total,
            "out_of": total,
            "page": page,
            "per_page": request_per_page,
            "hits": hits,
            "facet_counts": facet_counts,
            "warnings": warnings,
        }

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
        last_error: Exception | None = None

        for attempt in range(self.RAFT_SUBMIT_ATTEMPTS):
            try:
                leader = await self._leaders.get_leader(shard_id)
            except RuntimeError as error:
                last_error = error
                await self._sleep_before_raft_retry(attempt)
                continue

            try:
                response = await self._client.post(
                    f"{leader.url}/internal/raft/{shard_id}/commands",
                    json=command,
                )
            except httpx.HTTPError as error:
                self._leaders.invalidate(shard_id)
                last_error = error
                await self._sleep_before_raft_retry(attempt)
                continue

            if response.status_code == 409:
                self._leaders.invalidate(shard_id)
                last_error = httpx.HTTPStatusError(
                    self._response_error_message(response),
                    request=response.request,
                    response=response,
                )
                await self._sleep_before_raft_retry(attempt)
                continue

            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                self._leaders.invalidate(shard_id)
                last_error = error
                if response.status_code >= 500:
                    await self._sleep_before_raft_retry(attempt)
                    continue
                raise

            payload = response.json()
            if payload.get("ok") is True:
                return payload

            self._leaders.invalidate(shard_id)
            last_error = httpx.HTTPStatusError(
                str(payload.get("error") or "raft command failed"),
                request=response.request,
                response=response,
            )
            await self._sleep_before_raft_retry(attempt)

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"no raft leader for shard {shard_id}")

    async def _sleep_before_raft_retry(self, attempt: int) -> None:
        if attempt + 1 < self.RAFT_SUBMIT_ATTEMPTS:
            await asyncio.sleep(self.RAFT_SUBMIT_RETRY_DELAY_SECONDS)

    @staticmethod
    def _response_error_message(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text or "raft command failed"

        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, dict):
                return str(detail.get("error") or detail)
            if detail is not None:
                return str(detail)
            if payload.get("error") is not None:
                return str(payload["error"])
        return "raft command failed"

    async def _search_shard(
        self,
        shard_id: int,
        collection: str,
        query: str,
        *,
        limit: int | None = None,
        query_by: str | None = None,
        query_by_weights: str | None = None,
        prefix: bool = False,
        num_typos: int = 0,
        filter_by: str | None = None,
        sort_by: str | None = None,
        facet_by: str | None = None,
        highlight_fields: str | None = None,
    ) -> tuple[list[SearchHit], str | None]:
        params: dict[str, Any] = {"q": query}
        if limit is not None:
            params["limit"] = limit
        else:
            params.update(
                {"prefix": prefix, "num_typos": num_typos, "all_results": True}
            )
        optional_params = {
            "query_by": query_by,
            "query_by_weights": query_by_weights,
            "filter_by": filter_by,
            "sort_by": sort_by,
            "facet_by": facet_by,
            "highlight_fields": highlight_fields,
        }
        params.update({key: value for key, value in optional_params.items() if value})
        errors: list[str] = []
        for node in self.cluster.get_search_candidates(shard_id):
            if not await self._node_shard_ready(node, shard_id):
                errors.append(f"{node.id}: not ready")
                continue
            try:
                response = await get_with_retry(
                    self._client,
                    self._search_url(node, shard_id, collection),
                    params=params,
                )
                payload = response.json()
                return payload.get("hits", []), None
            except httpx.HTTPError as error:
                errors.append(f"{node.id}: {error}")
        return [], f"shard {shard_id} unavailable: {'; '.join(errors)}"

    async def _node_shard_ready(self, node: NodeInfo, shard_id: int) -> bool:
        try:
            response = await get_with_retry(
                self._client, f"{node.url}/internal/recovery/state"
            )
        except httpx.HTTPStatusError as error:
            return error.response.status_code == 404
        except httpx.HTTPError:
            return False
        except Exception:
            # Compatibility for custom transports that predate recovery endpoints.
            return True
        payload = response.json()
        if "shards" not in payload:
            return True
        return shard_is_ready(payload.get("shards", {}).get(str(shard_id)))

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
                    response = await get_with_retry(
                        self._client,
                        f"{node.url}/internal/raft/{shard_id}/state",
                    )
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
                recovery = await self._get_node_shard_recovery(node, shard_id)
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
                    "last_log_index": payload.get("last_log_index"),
                    "leader_id": payload.get("leader_id"),
                    "recovery_status": (recovery or {}).get("status"),
                    "ready": (recovery or {}).get("ready"),
                    "recovery_error": (recovery or {}).get("error"),
                    "rebuild": (recovery or {}).get("rebuild"),
                    "leader_last_log_index": (recovery or {}).get(
                        "leader_last_log_index"
                    ),
                    "leader_commit_index": (recovery or {}).get(
                        "leader_commit_index"
                    ),
                    "recovery_events": (recovery or {}).get("events", []),
                }

            shards[str(shard_id)] = {
                "leader": leader,
                "term": term,
                "members": members,
            }

        return {"shards": shards}

    async def _get_node_shard_recovery(
        self, node: NodeInfo, shard_id: int
    ) -> dict[str, Any] | None:
        try:
            response = await get_with_retry(
                self._client, f"{node.url}/internal/recovery/state"
            )
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 404:
                return None
            return {"status": "failed", "ready": False, "error": str(error)}
        except httpx.HTTPError as error:
            return {"status": "unavailable", "ready": False, "error": str(error)}
        except Exception:
            return None
        return response.json().get("shards", {}).get(str(shard_id))

    async def check_consistency(self, collection: str) -> dict[str, Any]:
        """Check consistency between primary and replicas for a collection."""
        return await _check_consistency(self.cluster, collection, self._client)

    async def repair_collection(self, collection: str) -> dict[str, Any]:
        """Repair replica consistency by copying missing documents from primary."""
        return await _repair_collection(self.cluster, collection, self._client)

    async def rebuild_shard_member(
        self, shard_id: int, member_id: str
    ) -> dict[str, Any]:
        if shard_id not in self.cluster.shards:
            raise ValueError(f"unknown shard {shard_id}")
        voters = {node.id: node for node in self.cluster.get_shard_voters(shard_id)}
        if member_id not in voters:
            raise ValueError(f"node {member_id!r} is not a voter for shard {shard_id}")
        leader = await self._leaders.get_leader(shard_id)
        if leader.id == member_id:
            raise ValueError("current leader cannot be rebuilt")
        response = await self._client.post(
            f"{voters[member_id].url}/internal/shards/{shard_id}/rebuild"
        )
        response.raise_for_status()
        return response.json()

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
