from __future__ import annotations

import asyncio
import socket
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from typesense_lite.cluster import ClusterMap
from typesense_lite.coordinator import Coordinator
from typesense_lite.node_directory import NodeDirectory
from typesense_lite.node_registrar import NodeRegistrar
from typesense_lite.server import create_app


pytestmark = [pytest.mark.unit, pytest.mark.discovery]


CLUSTER_CONFIG: dict[str, Any] = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 1,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2"]},
    },
}


def test_node_directory_register_and_heartbeat() -> None:
    fake_now = {"t": 1000.0}
    directory = NodeDirectory(alive_timeout=30.0, clock=lambda: fake_now["t"])

    entry = directory.register(
        node_id="node-1", host="10.0.0.1", port=9101, role="node",
    )
    assert entry.last_seen == 1000.0
    assert entry.registered_at == 1000.0

    fake_now["t"] = 1005.0
    refreshed = directory.heartbeat("node-1")
    assert refreshed is not None
    assert refreshed.last_seen == 1005.0
    assert refreshed.registered_at == 1000.0  # preserved


def test_node_directory_register_replaces_existing_metadata() -> None:
    fake_now = {"t": 1000.0}
    directory = NodeDirectory(clock=lambda: fake_now["t"])
    directory.register(node_id="n1", host="10.0.0.1", port=9101, role="node")
    fake_now["t"] = 1001.0
    updated = directory.register(node_id="n1", host="10.0.0.2", port=9202, role="node")

    snap = directory.snapshot()
    assert snap["n1"]["host"] == "10.0.0.2"
    assert snap["n1"]["port"] == 9202
    # Re-register preserves the original registered_at and bumps last_seen.
    assert updated.registered_at == 1000.0
    assert updated.last_seen == 1001.0


def test_node_directory_heartbeat_unknown_returns_none() -> None:
    directory = NodeDirectory()

    assert directory.heartbeat("ghost") is None


def test_node_directory_cleanup_expired_drops_stale_entries() -> None:
    fake_now = {"t": 0.0}
    directory = NodeDirectory(alive_timeout=10.0, clock=lambda: fake_now["t"])
    directory.register(node_id="alive", host="10.0.0.1", port=9101, role="node")

    fake_now["t"] = 5.0
    directory.register(node_id="stale", host="10.0.0.2", port=9101, role="node")

    # Heartbeat the alive node at t=8 so it stays fresh.
    fake_now["t"] = 8.0
    directory.heartbeat("alive")

    fake_now["t"] = 16.0
    expired = directory.cleanup_expired()

    assert expired == ["stale"]
    assert directory.alive_ids() == {"alive"}


def test_node_directory_snapshot_contains_age() -> None:
    fake_now = {"t": 100.0}
    directory = NodeDirectory(clock=lambda: fake_now["t"])
    directory.register(node_id="n1", host="10.0.0.1", port=9101, role="node")
    fake_now["t"] = 107.5

    snap = directory.snapshot()

    assert snap["n1"]["age_seconds"] == 7.5
    assert snap["n1"]["role"] == "node"


def test_node_directory_rejects_non_positive_timeout() -> None:
    with pytest.raises(ValueError):
        NodeDirectory(alive_timeout=0)

    with pytest.raises(ValueError):
        NodeDirectory(alive_timeout=-1)


def test_node_directory_remove_returns_bool() -> None:
    directory = NodeDirectory()
    directory.register(node_id="n1", host="10.0.0.1", port=9101, role="node")

    assert directory.remove("n1") is True
    assert directory.remove("n1") is False
    assert directory.alive_ids() == set()


def test_coordinator_register_and_cluster_nodes() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)

    coordinator.register_node(
        node_id="node-1", host="10.0.0.1", port=9101, role="node",
    )

    snapshot = coordinator.cluster_nodes()

    assert {n["id"] for n in snapshot["static"]} == {"node-1", "node-2"}
    assert [entry["node_id"] for entry in snapshot["alive"]] == ["node-1"]
    assert snapshot["alive"][0]["host"] == "10.0.0.1"
    assert snapshot["alive_timeout_seconds"] == 30.0


def test_coordinator_heartbeat_returns_none_for_unknown() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)

    assert coordinator.heartbeat_node("ghost") is None


def test_coordinator_cleanup_runs_during_snapshot() -> None:
    fake_now = {"t": 0.0}
    directory = NodeDirectory(alive_timeout=5.0, clock=lambda: fake_now["t"])
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster, node_directory=directory)

    coordinator.register_node(
        node_id="node-1", host="10.0.0.1", port=9101, role="node",
    )

    fake_now["t"] = 100.0
    snapshot = coordinator.cluster_nodes()

    assert snapshot["alive"] == []


def _build_coordinator_app(coordinator: Coordinator) -> TestClient:
    app = create_app(
        role="coordinator",
        cluster_config=CLUSTER_CONFIG,
        coordinator_instance=coordinator,
    )
    return TestClient(app)


def test_cluster_nodes_endpoint_returns_static_and_alive() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)
    coordinator.register_node(
        node_id="node-1", host="10.0.0.1", port=9101, role="node",
    )

    with _build_coordinator_app(coordinator) as client:
        response = client.get("/cluster/nodes")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert {n["id"] for n in payload["static"]} == {"node-1", "node-2"}
    assert [entry["node_id"] for entry in payload["alive"]] == ["node-1"]


def test_internal_cluster_config_endpoint_returns_static_topology() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)

    with _build_coordinator_app(coordinator) as client:
        response = client.get("/internal/cluster/config")

    assert response.status_code == 200, response.text
    assert response.json() == cluster.to_dict()


    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)
    with _build_coordinator_app(coordinator) as client:
        response = client.post(
            "/internal/cluster/nodes/register",
            json={
                "node_id": "node-1",
                "host": "10.0.0.1",
                "port": 9101,
                "role": "node",
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["node_id"] == "node-1"

        # Heartbeat for the registered node.
        response = client.put("/internal/cluster/nodes/node-1/heartbeat")
        assert response.status_code == 200, response.text
        assert response.json()["node_id"] == "node-1"

        # Heartbeat for an unknown node returns 404.
        response = client.put("/internal/cluster/nodes/ghost/heartbeat")
        assert response.status_code == 404

        # Cluster snapshot shows the registered node.
        response = client.get("/cluster/nodes")
        assert response.status_code == 200, response.text
        payload = response.json()
        assert {n["id"] for n in payload["static"]} == {"node-1", "node-2"}
        assert [entry["node_id"] for entry in payload["alive"]] == ["node-1"]


def test_register_endpoint_rejects_bad_payload() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)
    with _build_coordinator_app(coordinator) as client:
        response = client.post(
            "/internal/cluster/nodes/register",
            json={"host": "10.0.0.1"},  # missing node_id, port
        )
        assert response.status_code == 400


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_node_registrar_register_once_round_trip() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)
    app = create_app(
        role="coordinator",
        cluster_config=CLUSTER_CONFIG,
        coordinator_instance=coordinator,
    )

    coordinator_url = "http://testserver"

    async def run() -> dict[str, Any]:
        registrar = NodeRegistrar(
            coordinator_url=coordinator_url,
            node_id="node-1",
            advertise_host="10.0.0.1",
            advertise_port=9101,
            role="node",
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url=coordinator_url,
            ),
        )
        try:
            return await registrar.register_once()
        finally:
            await registrar.aclose()

    with TestClient(app):
        result = asyncio.run(run())

    assert result["node_id"] == "node-1"
    assert coordinator.cluster_nodes()["alive"][0]["host"] == "10.0.0.1"


def test_node_registrar_heartbeat_once_returns_registered_entry() -> None:
    cluster = ClusterMap.from_dict(CLUSTER_CONFIG)
    coordinator = Coordinator(cluster)
    coordinator.register_node(
        node_id="node-1", host="10.0.0.1", port=9101, role="node",
    )
    app = create_app(
        role="coordinator",
        cluster_config=CLUSTER_CONFIG,
        coordinator_instance=coordinator,
    )

    coordinator_url = "http://testserver"

    async def run() -> dict[str, Any]:
        registrar = NodeRegistrar(
            coordinator_url=coordinator_url,
            node_id="node-1",
            advertise_host="10.0.0.1",
            advertise_port=9101,
            role="node",
            client=httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url=coordinator_url,
            ),
        )
        try:
            return await registrar.heartbeat_once()
        finally:
            await registrar.aclose()

    with TestClient(app):
        result = asyncio.run(run())

    assert result["node_id"] == "node-1"