"""FastAPI app factory for Typesense Lite coordinator and data nodes."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Optional, Union

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse

from .cluster import ClusterConfig, ClusterMap
from .coordinator import Coordinator
from .http_client import cross_machine_timeout, get_with_retry, make_cross_machine_client
from .importer import parse_upload
from .node import SearchNode
from .node_registrar import NodeRegistrar
from .raft_runtime import RaftRuntime
from .rebuild import build_snapshot_payload, validate_snapshot_payload
from .recovery import NodeRecoveryState, shard_is_ready
from .schemas import Document

ClusterInput = Union[dict[str, Any], str, Path, ClusterMap]
WEB_DIR = Path(__file__).parent / "web"
SEARCH_HTML = WEB_DIR / "search.html"
ADMIN_HTML = WEB_DIR / "admin.html"
CLUSTER_HTML = WEB_DIR / "cluster.html"
UPLOAD_FILE = File(...)


def create_app(
    *,
    role: str,
    cluster_config: ClusterInput,
    node_id: str | None = None,
    data_dir: str | Path | None = None,
    bind_host: str | None = None,
    bind_port: int | None = None,
    coordinator_url: str | None = None,
    coordinator_instance: Coordinator | None = None,
    coordinator_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    cluster = _load_cluster(cluster_config)
    app = FastAPI(title="Typesense Lite")

    if role == "node":
        if node_id is None:
            raise ValueError("node_id is required for data-node role")
        node = SearchNode(node_id=node_id, data_dir=data_dir or ".data/typesense_lite")
        recovery_state = NodeRecoveryState(node_id)
        raft_client = make_cross_machine_client()
        rebuild_tasks: list[asyncio.Task[None]] = []

        def make_apply_command(shard_id: int):
            return lambda command: node.apply_raft_command(shard_id, command)

        raft_runtimes = {
            shard_id: RaftRuntime(
                node_id=node_id,
                shard_id=shard_id,
                members=[member.id for member in cluster.get_shard_voters(shard_id)],
                data_dir=data_dir or ".data/typesense_lite",
                peer_urls={
                    member.id: member.url
                    for member in cluster.get_shard_voters(shard_id)
                    if member.id != node_id
                },
                client=raft_client,
                apply_command=make_apply_command(shard_id),
            )
            for shard_id, placement in cluster.shards.items()
            if node_id == placement.primary or node_id in placement.replicas
        }

        @app.on_event("startup")
        async def start_raft_runtimes() -> None:
            for runtime in raft_runtimes.values():
                runtime.start()
                if len(runtime.members) > 1:
                    rebuild_tasks.append(
                        asyncio.create_task(
                            _auto_rebuild_shard(
                                node=node,
                                runtime=runtime,
                                cluster=cluster,
                                client=raft_client,
                            )
                        )
                    )
            if coordinator_url:
                advertise_host = os.environ.get(
                    "NODE_ADVERTISE_HOST", bind_host or "",
                )
                advertise_port = int(
                    os.environ.get("NODE_ADVERTISE_PORT", str(bind_port or 0)),
                )
                registrar = NodeRegistrar(
                    coordinator_url=coordinator_url,
                    node_id=node_id,
                    advertise_host=advertise_host,
                    advertise_port=advertise_port,
                    role="node",
                )
                await registrar.start()
                app.state.registrar = registrar

        @app.on_event("shutdown")
        async def close_raft_client() -> None:
            for task in rebuild_tasks:
                task.cancel()
            await asyncio.gather(*rebuild_tasks, return_exceptions=True)
            for runtime in raft_runtimes.values():
                await runtime.stop()
            registrar = getattr(app.state, "registrar", None)
            if registrar is not None:
                await registrar.aclose()
            await raft_client.aclose()

        @app.get("/health")
        def node_health() -> dict[str, Any]:
            recovery = _refresh_recovery_state(recovery_state, raft_runtimes)
            return {
                "ok": True,
                "role": "node",
                "node_id": node_id,
                "status": recovery["status"],
                "ready": recovery["ready"],
                "error": recovery["error"],
            }

        @app.get("/internal/recovery/state")
        def get_recovery_state() -> dict[str, Any]:
            return _refresh_recovery_state(recovery_state, raft_runtimes)

        def ensure_shard_ready(shard_id: int) -> None:
            shard_state = _refresh_recovery_state(
                recovery_state, raft_runtimes
            )["shards"].get(str(shard_id))
            if not shard_is_ready(shard_state):
                raise HTTPException(status_code=503, detail="shard is not ready")

        @app.get("/internal/collections")
        def list_node_collections() -> dict[str, list[str]]:
            return {"collections": node.list_collections()}

        @app.post("/internal/shards/{shard_id}/collections/{collection}/documents")
        def add_node_document(
            shard_id: int,
            collection: str,
            document: Document,
        ) -> Document:
            try:
                return node.add_document(shard_id, collection, document)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error

        @app.get("/internal/shards/{shard_id}/collections/{collection}/documents")
        def list_node_documents(shard_id: int, collection: str) -> dict[str, Any]:
            ensure_shard_ready(shard_id)
            return {"documents": node.list_documents(shard_id, collection)}

        @app.get(
            "/internal/shards/{shard_id}/collections/{collection}"
            "/documents/{document_id}"
        )
        def get_node_document(
            shard_id: int,
            collection: str,
            document_id: str,
        ) -> Document:
            ensure_shard_ready(shard_id)
            try:
                return node.get_document(shard_id, collection, document_id)
            except KeyError as error:
                raise HTTPException(status_code=404, detail="document not found") from error

        @app.patch(
            "/internal/shards/{shard_id}/collections/{collection}"
            "/documents/{document_id}"
        )
        def update_node_document(
            shard_id: int,
            collection: str,
            document_id: str,
            document: Document,
        ) -> Document:
            try:
                return node.update_document(shard_id, collection, document_id, document)
            except KeyError as error:
                raise HTTPException(status_code=404, detail="document not found") from error
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error

        @app.delete(
            "/internal/shards/{shard_id}/collections/{collection}"
            "/documents/{document_id}"
        )
        def delete_node_document(
            shard_id: int,
            collection: str,
            document_id: str,
        ) -> Document:
            try:
                return node.delete_document(shard_id, collection, document_id)
            except KeyError as error:
                raise HTTPException(status_code=404, detail="document not found") from error

        @app.get("/internal/shards/{shard_id}/collections/{collection}/document_ids")
        def list_node_document_ids(shard_id: int, collection: str) -> dict[str, list[str]]:
            ensure_shard_ready(shard_id)
            return {"ids": node.list_document_ids(shard_id, collection)}

        @app.get("/internal/raft/{shard_id}/state")
        def get_raft_state(shard_id: int) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            return raft_runtimes[shard_id].state()

        @app.get("/internal/shards/{shard_id}/snapshot/manifest")
        async def get_snapshot_manifest(shard_id: int) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            async with raft_runtimes[shard_id].rebuild_lock:
                return _build_local_snapshot(node, raft_runtimes[shard_id])["manifest"]

        @app.get("/internal/shards/{shard_id}/snapshot/export")
        async def export_snapshot(shard_id: int) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            async with raft_runtimes[shard_id].rebuild_lock:
                return _build_local_snapshot(node, raft_runtimes[shard_id])

        @app.post("/internal/shards/{shard_id}/rebuild")
        async def rebuild_shard(shard_id: int) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            try:
                return await _rebuild_shard_from_leader(
                    node=node,
                    runtime=raft_runtimes[shard_id],
                    cluster=cluster,
                    client=raft_client,
                )
            except (RuntimeError, ValueError, httpx.HTTPError) as error:
                raft_runtimes[shard_id].set_recovery_override(
                    "failed", error=str(error)
                )
                raise HTTPException(status_code=409, detail=str(error)) from error

        @app.post("/internal/raft/{shard_id}/request_vote")
        async def request_vote(shard_id: int, payload: dict[str, Any]) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            return await raft_runtimes[shard_id].handle_request_vote(payload)

        @app.post("/internal/raft/{shard_id}/append_entries")
        async def append_entries(shard_id: int, payload: dict[str, Any]) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            return await raft_runtimes[shard_id].handle_append_entries(payload)

        @app.post("/internal/raft/{shard_id}/commands")
        async def submit_raft_command(shard_id: int, payload: dict[str, Any]) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            runtime = raft_runtimes[shard_id]
            if len(runtime.members) == 1 and runtime.state()["role"] != "leader":
                await runtime.start_election()
            result = await runtime.submit_command(payload)
            if result.get("ok") is not True:
                raise HTTPException(status_code=409, detail=result)
            return result

        @app.get("/internal/shards/{shard_id}/collections/{collection}/search")
        def search_node_documents(
            shard_id: int,
            collection: str,
            q: str = Query(..., min_length=1),
            query_by: Optional[str] = Query(None),
            query_by_weights: Optional[str] = Query(None),
            prefix: bool = Query(False),
            num_typos: int = Query(0, ge=0, le=2),
            filter_by: Optional[str] = Query(None),
            sort_by: Optional[str] = Query(None),
            facet_by: Optional[str] = Query(None),
            page: int = Query(1, ge=1),
            per_page: Optional[int] = Query(None, ge=1, le=100),
            limit: Optional[int] = Query(None, ge=1, le=100),
            highlight_fields: Optional[str] = Query(None),
            all_results: bool = Query(False),
        ) -> dict[str, Any]:
            ensure_shard_ready(shard_id)
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
            ) or prefix or num_typos > 0 or page != 1 or all_results
            if not advanced_query:
                hits = node.search(shard_id, collection, q, limit or 10)
                return {"found": len(hits), "hits": hits}
            effective_per_page = per_page if per_page is not None else limit
            return node.search_query(
                shard_id,
                collection,
                q,
                query_by=query_by,
                query_by_weights=query_by_weights,
                prefix=prefix,
                num_typos=num_typos,
                filter_by=filter_by,
                sort_by=sort_by,
                facet_by=facet_by,
                page=page,
                per_page=effective_per_page,
                highlight_fields=highlight_fields,
            )

        return app

    if role == "coordinator":
        coordinator = coordinator_instance or Coordinator(
            cluster, client=coordinator_client,
        )
        app.state.coordinator = coordinator

        @app.on_event("shutdown")
        async def close_coordinator() -> None:
            await coordinator.aclose()

        @app.get("/", response_class=HTMLResponse)
        @app.get("/search", response_class=HTMLResponse)
        def search_page() -> str:
            return SEARCH_HTML.read_text(encoding="utf-8")

        @app.get("/admin", response_class=HTMLResponse)
        def admin_page() -> str:
            return ADMIN_HTML.read_text(encoding="utf-8")

        @app.get("/cluster-console", response_class=HTMLResponse)
        def cluster_console_page() -> str:
            return CLUSTER_HTML.read_text(encoding="utf-8")

        @app.get("/health")
        def coordinator_health() -> dict[str, Any]:
            return {"ok": True, "role": "coordinator"}

        @app.get("/cluster/health")
        async def cluster_health() -> dict[str, Any]:
            return await coordinator.cluster_health()

        @app.get("/cluster/raft")
        async def cluster_raft() -> dict[str, Any]:
            return await coordinator.raft_status()

        @app.post("/cluster/shards/{shard_id}/members/{member_id}/rebuild")
        async def rebuild_cluster_member(
            shard_id: int, member_id: str
        ) -> dict[str, Any]:
            try:
                return await coordinator.rebuild_shard_member(shard_id, member_id)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error
            except httpx.HTTPStatusError as error:
                detail = error.response.text or str(error)
                raise HTTPException(
                    status_code=error.response.status_code, detail=detail
                ) from error

        @app.get("/cluster/nodes")
        def cluster_nodes() -> dict[str, Any]:
            return coordinator.cluster_nodes()

        @app.post("/internal/cluster/nodes/register")
        def register_node(payload: dict[str, Any]) -> dict[str, Any]:
            try:
                node_id = str(payload["node_id"])
                host = str(payload["host"])
                port = int(payload["port"])
                role = str(payload.get("role", "node"))
            except (KeyError, TypeError, ValueError) as error:
                raise HTTPException(
                    status_code=400,
                    detail=f"invalid registration payload: {error}",
                ) from error
            return coordinator.register_node(
                node_id=node_id,
                host=host,
                port=port,
                role=role,
            )

        @app.put("/internal/cluster/nodes/{node_id}/heartbeat")
        def heartbeat_node(node_id: str) -> dict[str, Any]:
            entry = coordinator.heartbeat_node(node_id)
            if entry is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"node {node_id!r} not registered",
                )
            return entry

        @app.get("/internal/cluster/config")
        def get_internal_cluster_config() -> dict[str, Any]:
            return cluster.to_dict()

        @app.get("/cluster")
        def get_cluster() -> dict[str, Any]:
            return cluster.to_dict()

        @app.get("/collections")
        async def list_collections() -> dict[str, list[str]]:
            return await coordinator.list_collections()

        @app.post("/collections/{collection}/documents")
        async def add_document(collection: str, document: Document) -> dict[str, Any]:
            try:
                return await coordinator.add_document(collection, document)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error

        @app.post("/collections/{collection}/documents/import")
        async def import_documents(
            collection: str,
            documents: list[Document],
        ) -> dict[str, Any]:
            return await coordinator.import_documents(collection, documents)

        @app.post("/collections/{collection}/documents/upload")
        async def upload_documents(
            collection: str,
            file: UploadFile = UPLOAD_FILE,
        ) -> dict[str, Any]:
            try:
                content = await file.read()
                documents = parse_upload(file.filename or "upload", content)
            except (UnicodeDecodeError, ValueError) as error:
                raise HTTPException(status_code=400, detail=str(error)) from error
            return await coordinator.import_documents(collection, documents)

        @app.get("/collections/{collection}/documents")
        async def list_documents(collection: str) -> dict[str, list[Document]]:
            return await coordinator.list_documents(collection)

        @app.get("/collections/{collection}/documents/search")
        async def search_documents(
            collection: str,
            q: str = Query(..., min_length=1),
            query_by: Optional[str] = Query(None),
            query_by_weights: Optional[str] = Query(None),
            prefix: bool = Query(False),
            num_typos: int = Query(0, ge=0, le=2),
            filter_by: Optional[str] = Query(None),
            sort_by: Optional[str] = Query(None),
            facet_by: Optional[str] = Query(None),
            page: int = Query(1, ge=1),
            per_page: Optional[int] = Query(None, ge=1, le=100),
            limit: int = Query(10, ge=1, le=100),
            highlight_fields: Optional[str] = Query(None),
        ) -> dict[str, Any]:
            return await coordinator.search(
                collection,
                q,
                limit=limit,
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

        @app.get("/collections/{collection}/documents/{document_id}")
        async def get_document(collection: str, document_id: str) -> Document:
            try:
                return await coordinator.get_document(collection, document_id)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 404:
                    raise HTTPException(
                        status_code=404,
                        detail="document not found",
                    ) from error
                raise

        @app.patch("/collections/{collection}/documents/{document_id}")
        async def update_document(
            collection: str,
            document_id: str,
            document: Document,
        ) -> dict[str, Any]:
            try:
                return await coordinator.update_document(collection, document_id, document)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error
            except httpx.HTTPStatusError as error:
                if error.response.status_code in {400, 404}:
                    raise HTTPException(
                        status_code=error.response.status_code,
                        detail=error.response.text,
                    ) from error
                raise

        @app.delete("/collections/{collection}/documents/{document_id}")
        async def delete_document(collection: str, document_id: str) -> dict[str, Any]:
            try:
                return await coordinator.delete_document(collection, document_id)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 404:
                    raise HTTPException(
                        status_code=404,
                        detail="document not found",
                    ) from error
                raise

        @app.get("/collections/{collection}/consistency")
        async def check_collection_consistency(collection: str) -> dict[str, Any]:
            return await coordinator.check_consistency(collection)

        @app.post("/collections/{collection}/repair")
        async def repair_collection_endpoint(collection: str) -> dict[str, Any]:
            return await coordinator.repair_collection(collection)

        return app

    raise ValueError(f"unknown role {role!r}")


def _refresh_recovery_state(
    recovery_state: NodeRecoveryState,
    raft_runtimes: dict[int, RaftRuntime],
) -> dict[str, Any]:
    for shard_id, runtime in raft_runtimes.items():
        recovery_state.set_shard(shard_id, runtime.recovery_state())
    return recovery_state.to_dict()


def _build_local_snapshot(node: SearchNode, runtime: RaftRuntime) -> dict[str, Any]:
    state = runtime.state()
    if state["role"] != "leader":
        raise HTTPException(status_code=409, detail="snapshot source is not leader")
    if state["last_applied"] < state["commit_index"]:
        raise HTTPException(status_code=409, detail="leader state machine is not applied")
    return build_snapshot_payload(
        shard_id=int(state["shard_id"]),
        source_node_id=str(state["node"]),
        source_role=str(state["role"]),
        term=int(state["current_term"]),
        snapshot_index=int(state["commit_index"]),
        commit_index=int(state["commit_index"]),
        collections=node.export_shard_collections(int(state["shard_id"])),
        log_entries=runtime.committed_log_entries(),
    )


async def _auto_rebuild_shard(
    *,
    node: SearchNode,
    runtime: RaftRuntime,
    cluster: ClusterMap,
    client: httpx.AsyncClient,
) -> None:
    for attempt in range(10):
        await asyncio.sleep(min(0.2 * (attempt + 1), 1.0))
        try:
            if runtime.state()["role"] == "leader":
                return
            leader = await _discover_current_leader(runtime.shard_id, cluster, client)
            if leader is None:
                continue
            integrity = node.check_replica_integrity(
                runtime.shard_id, runtime.committed_commands()
            )
            manifest_response = await get_with_retry(
                client,
                f"{leader.url}/internal/shards/{runtime.shard_id}/snapshot/manifest",
            )
            manifest = manifest_response.json()
            needs_snapshot = (
                integrity.requires_rebuild
                or (
                    runtime.state()["last_log_index"] == 0
                    and int(manifest.get("snapshot_index", 0)) > 0
                )
            )
            if needs_snapshot:
                await _rebuild_shard_from_leader(
                    node=node,
                    runtime=runtime,
                    cluster=cluster,
                    client=client,
                )
            return
        except (RuntimeError, ValueError, httpx.HTTPError) as error:
            if attempt == 9:
                runtime.set_recovery_override("failed", error=str(error))


async def _discover_current_leader(
    shard_id: int, cluster: ClusterMap, client: httpx.AsyncClient
) -> Any | None:
    hinted_leader_id: str | None = None
    for candidate in cluster.get_shard_voters(shard_id):
        try:
            response = await get_with_retry(
                client, f"{candidate.url}/internal/raft/{shard_id}/state"
            )
        except httpx.HTTPError:
            continue
        payload = response.json()
        if payload.get("role") == "leader":
            return candidate
        if isinstance(payload.get("leader_id"), str):
            hinted_leader_id = payload["leader_id"]
    if hinted_leader_id in cluster.nodes:
        candidate = cluster.nodes[hinted_leader_id]
        try:
            response = await get_with_retry(
                client, f"{candidate.url}/internal/raft/{shard_id}/state"
            )
        except httpx.HTTPError:
            return None
        if response.json().get("role") == "leader":
            return candidate
    return None


async def _rebuild_shard_from_leader(
    *,
    node: SearchNode,
    runtime: RaftRuntime,
    cluster: ClusterMap,
    client: httpx.AsyncClient,
) -> dict[str, Any]:
    async with runtime.rebuild_lock:
        if runtime.state()["role"] == "leader":
            raise RuntimeError("current leader cannot be rebuilt")
        runtime.set_recovery_override("rebuilding", summary={"attempt": "snapshot"})
        leader = await _discover_current_leader(runtime.shard_id, cluster, client)
        if leader is None:
            raise RuntimeError(f"no leader found for shard {runtime.shard_id}")

        manifest_response = await get_with_retry(
            client,
            f"{leader.url}/internal/shards/{runtime.shard_id}/snapshot/manifest",
        )
        expected = manifest_response.json()
        export_response = await get_with_retry(
            client,
            f"{leader.url}/internal/shards/{runtime.shard_id}/snapshot/export",
        )
        snapshot = validate_snapshot_payload(
            export_response.json(),
            expected_shard_id=runtime.shard_id,
            expected_source_node_id=leader.id,
            expected_term=int(expected["term"]),
            expected_snapshot_index=int(expected["snapshot_index"]),
            expected_commit_index=int(expected["commit_index"]),
        )
        manifest = snapshot["manifest"]
        leader_state_response = await get_with_retry(
            client, f"{leader.url}/internal/raft/{runtime.shard_id}/state"
        )
        leader_state = leader_state_response.json()
        if leader_state.get("role") != "leader":
            raise RuntimeError("snapshot source lost leadership")
        if int(leader_state.get("current_term", -1)) != int(manifest["term"]):
            raise RuntimeError("snapshot source term changed")
        if int(manifest["snapshot_index"]) > int(leader_state.get("commit_index", -1)):
            raise RuntimeError("snapshot index is ahead of current leader commit")

        summary = {
            "status": "completed",
            "source_leader": leader.id,
            "snapshot_index": manifest["snapshot_index"],
            "checksum_verified": True,
            "document_count": manifest["document_count"],
        }
        runtime.set_recovery_override("validating", summary=summary)
        node.install_shard_snapshot(runtime.shard_id, snapshot["collections"])
        runtime.install_snapshot_metadata(
            snapshot_index=int(manifest["snapshot_index"]),
            term=int(manifest["term"]),
            log_entries=snapshot["log_entries"],
            leader_id=leader.id,
            summary=summary,
        )
        runtime.set_recovery_override("catching_up", summary=summary)
        return summary


def _load_cluster(cluster_config: ClusterInput) -> ClusterMap:
    if isinstance(cluster_config, ClusterMap):
        return cluster_config
    if isinstance(cluster_config, (str, Path)):
        return ClusterMap.from_file(cluster_config)
    return ClusterMap.from_dict(cluster_config)


def _fetch_cluster_config(coordinator_url: str) -> ClusterConfig:
    url = f"{coordinator_url.rstrip('/')}/internal/cluster/config"
    with httpx.Client(timeout=cross_machine_timeout()) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


def _create_app_from_env() -> FastAPI:
    role = os.environ.get("ROLE", "coordinator")
    cluster_config: ClusterInput | None = os.environ.get("CLUSTER_CONFIG")
    coordinator_url = os.environ.get("COORDINATOR_URL")
    if cluster_config is None:
        if role == "node" and coordinator_url:
            cluster_config = _fetch_cluster_config(coordinator_url)
        else:
            raise RuntimeError("CLUSTER_CONFIG environment variable is required")
    port_env = os.environ.get("PORT")
    return create_app(
        role=role,
        cluster_config=cluster_config,
        node_id=os.environ.get("NODE_ID"),
        data_dir=os.environ.get("DATA_DIR", ".data/typesense_lite"),
        bind_host=os.environ.get("HOST"),
        bind_port=int(port_env) if port_env else None,
        coordinator_url=coordinator_url,
    )


if os.environ.get("CLUSTER_CONFIG") or (
    os.environ.get("ROLE") == "node" and os.environ.get("COORDINATOR_URL")
):
    app = _create_app_from_env()
else:
    app = FastAPI(title="Typesense Lite")

    @app.get("/health")
    def missing_config_health() -> dict[str, Any]:
        return {
            "ok": False,
            "error": "CLUSTER_CONFIG environment variable is required",
        }
