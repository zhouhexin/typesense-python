"""FastAPI app factory for Typesense Lite coordinator and data nodes."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Union

from fastapi import FastAPI, HTTPException, Query

from .cluster import ClusterMap
from .coordinator import Coordinator
from .node import SearchNode
from .schemas import Document

ClusterInput = Union[dict[str, Any], str, Path, ClusterMap]


def create_app(
    *,
    role: str,
    cluster_config: ClusterInput,
    node_id: str | None = None,
    data_dir: str | Path | None = None,
) -> FastAPI:
    cluster = _load_cluster(cluster_config)
    app = FastAPI(title="Typesense Lite")

    if role == "node":
        if node_id is None:
            raise ValueError("node_id is required for data-node role")
        node = SearchNode(node_id=node_id, data_dir=data_dir or ".data/typesense_lite")

        @app.get("/health")
        def node_health() -> dict[str, Any]:
            return {"ok": True, "role": "node", "node_id": node_id}

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
        coordinator = Coordinator(cluster)

        @app.on_event("shutdown")
        async def close_coordinator() -> None:
            await coordinator.aclose()

        @app.get("/health")
        def coordinator_health() -> dict[str, Any]:
            return {"ok": True, "role": "coordinator"}

        @app.get("/cluster")
        def get_cluster() -> dict[str, Any]:
            return {
                "coordinator": {
                    "host": cluster.coordinator.host,
                    "port": cluster.coordinator.port,
                },
                "shard_count": cluster.shard_count,
                "nodes": [
                    {"id": node.id, "host": node.host, "port": node.port}
                    for node in cluster.nodes.values()
                ],
                "shards": {
                    str(shard_id): {
                        "primary": placement.primary,
                        "replicas": list(placement.replicas),
                    }
                    for shard_id, placement in cluster.shards.items()
                },
            }

        @app.post("/collections/{collection}/documents")
        async def add_document(collection: str, document: Document) -> dict[str, Any]:
            try:
                return await coordinator.add_document(collection, document)
            except ValueError as error:
                raise HTTPException(status_code=400, detail=str(error)) from error

        @app.get("/collections/{collection}/documents/search")
        async def search_documents(
            collection: str,
            q: str = Query(..., min_length=1),
            limit: int = Query(10, ge=1, le=100),
        ) -> dict[str, Any]:
            return await coordinator.search(collection, q, limit)

        return app

    raise ValueError(f"unknown role {role!r}")


def _load_cluster(cluster_config: ClusterInput) -> ClusterMap:
    if isinstance(cluster_config, ClusterMap):
        return cluster_config
    if isinstance(cluster_config, (str, Path)):
        return ClusterMap.from_file(cluster_config)
    return ClusterMap.from_dict(cluster_config)


def _create_app_from_env() -> FastAPI:
    role = os.environ.get("ROLE", "coordinator")
    cluster_config = os.environ.get("CLUSTER_CONFIG")
    if cluster_config is None:
        raise RuntimeError("CLUSTER_CONFIG environment variable is required")
    return create_app(
        role=role,
        cluster_config=cluster_config,
        node_id=os.environ.get("NODE_ID"),
        data_dir=os.environ.get("DATA_DIR", ".data/typesense_lite"),
    )


if os.environ.get("CLUSTER_CONFIG"):
    app = _create_app_from_env()
else:
    app = FastAPI(title="Typesense Lite")

    @app.get("/health")
    def missing_config_health() -> dict[str, Any]:
        return {
            "ok": False,
            "error": "CLUSTER_CONFIG environment variable is required",
        }
