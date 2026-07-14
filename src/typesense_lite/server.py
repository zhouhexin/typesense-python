"""FastAPI app factory for Typesense Lite coordinator and data nodes."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Union

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse

from .cluster import ClusterConfig, ClusterMap
from .coordinator import Coordinator
from .http_client import cross_machine_timeout, make_cross_machine_client
from .importer import parse_upload
from .node import SearchNode
from .node_registrar import NodeRegistrar
from .raft_runtime import RaftRuntime
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
        raft_client = make_cross_machine_client()

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
            for runtime in raft_runtimes.values():
                await runtime.stop()
            registrar = getattr(app.state, "registrar", None)
            if registrar is not None:
                await registrar.aclose()
            await raft_client.aclose()

        @app.get("/health")
        def node_health() -> dict[str, Any]:
            return {"ok": True, "role": "node", "node_id": node_id}

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
            return {"ids": node.list_document_ids(shard_id, collection)}

        @app.get("/internal/raft/{shard_id}/state")
        def get_raft_state(shard_id: int) -> dict[str, Any]:
            if shard_id not in raft_runtimes:
                raise HTTPException(status_code=404, detail="raft shard not found")
            return raft_runtimes[shard_id].state()

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
            limit: int = Query(10, ge=1, le=100),
        ) -> dict[str, Any]:
            hits = node.search(shard_id, collection, q, limit)
            return {"found": len(hits), "hits": hits}

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
            limit: int = Query(10, ge=1, le=100),
        ) -> dict[str, Any]:
            return await coordinator.search(collection, q, limit)

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
