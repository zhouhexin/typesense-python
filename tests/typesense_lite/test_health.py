"""Tests for the health module."""

import httpx
import pytest

from typesense_lite.cluster import ClusterMap
from typesense_lite.health import NodeHealth, ShardHealth, ClusterHealthReport, collect_cluster_health


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 2,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2"]},
        "1": {"primary": "node-2", "replicas": ["node-1"]},
    },
}


@pytest.mark.asyncio
async def test_all_nodes_healthy_shards_healthy() -> None:
    """All nodes healthy -> all shards status is 'healthy'."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "role": "node", "node_id": request.url.host})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    # All nodes are healthy
    assert report.nodes["node-1"].ok is True
    assert report.nodes["node-2"].ok is True

    # Both shards are healthy (primary available)
    assert report.shards[0].status == "healthy"
    assert report.shards[1].status == "healthy"

    # Shard 0 primary is node-1, shard 1 primary is node-2
    assert report.shards[0].primary.node == "node-1"
    assert report.shards[0].primary.ok is True
    assert report.shards[1].primary.node == "node-2"
    assert report.shards[1].primary.ok is True


@pytest.mark.asyncio
async def test_primary_failed_replica_ok_degraded() -> None:
    """Primary failed but replica healthy -> shard status is 'degraded'."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9101:
            return httpx.Response(500, json={"error": "internal error"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    # node-1 (primary of shard 0) is down, node-2 is up
    assert report.nodes["node-1"].ok is False
    assert report.nodes["node-2"].ok is True

    # Shard 0 is degraded (primary down, replica up)
    assert report.shards[0].status == "degraded"
    assert report.shards[0].primary.ok is False
    assert report.shards[0].replicas[0].node == "node-2"
    assert report.shards[0].replicas[0].ok is True

    # Shard 1 is still healthy (primary up)
    assert report.shards[1].status == "healthy"


@pytest.mark.asyncio
async def test_primary_and_replica_both_failed_unavailable() -> None:
    """Both primary and replica failed -> shard status is 'unavailable'."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "node down"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    # Both nodes are down
    assert report.nodes["node-1"].ok is False
    assert report.nodes["node-2"].ok is False

    # Both shards are unavailable
    assert report.shards[0].status == "unavailable"
    assert report.shards[1].status == "unavailable"


@pytest.mark.asyncio
async def test_node_errors_are_recorded() -> None:
    """Failed node should have error message in response."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "connection refused"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    assert report.nodes["node-1"].ok is False
    assert report.nodes["node-1"].error is not None
    assert "500" in report.nodes["node-1"].error


@pytest.mark.asyncio
async def test_health_response_ok_false_marks_node_down() -> None:
    """HTTP 200 with ok:false should still mark the node as down."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9101:
            return httpx.Response(200, json={"ok": False, "error": "not ready"})
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    assert report.nodes["node-1"].ok is False
    assert report.nodes["node-1"].error == "not ready"
    assert report.shards[0].status == "degraded"


@pytest.mark.asyncio
async def test_cluster_health_report_to_dict() -> None:
    """ClusterHealthReport.to_dict() produces valid JSON-serializable output."""
    cluster = ClusterMap.from_dict(CONFIG)

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    result = report.to_dict()

    # Should have nodes and shards keys
    assert "nodes" in result
    assert "shards" in result

    # Nodes should have node-1 and node-2
    assert "node-1" in result["nodes"]
    assert "node-2" in result["nodes"]
    assert result["nodes"]["node-1"]["ok"] is True
    assert result["nodes"]["node-2"]["ok"] is True

    # Shards should have "0" and "1" (to_dict converts int keys to strings for JSON)
    assert "0" in result["shards"]
    assert "1" in result["shards"]
    assert result["shards"]["0"]["status"] == "healthy"
    assert result["shards"]["0"]["primary"]["node"] == "node-1"


@pytest.mark.asyncio
async def test_replica_becomes_unavailable_when_node_down() -> None:
    """When a replica node is down, that replica should show as unavailable."""
    config = {
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
    cluster = ClusterMap.from_dict(config)

    # node-1 is up, node-2 is down
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9101:
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(500, json={"error": "connection refused"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        report = await collect_cluster_health(cluster, client)

    # node-1 is healthy, node-2 is down
    assert report.nodes["node-1"].ok is True
    assert report.nodes["node-2"].ok is False

    # Shard 0 has node-2 as replica which is down
    assert len(report.shards[0].replicas) == 1
    assert report.shards[0].replicas[0].node == "node-2"
    assert report.shards[0].replicas[0].ok is False

    # Shard is still healthy because primary is up
    assert report.shards[0].status == "healthy"


def test_node_health_dataclass() -> None:
    """NodeHealth dataclass stores values correctly."""
    health = NodeHealth(node="test-node", ok=True, url="http://127.0.0.1:9000", error=None)
    assert health.node == "test-node"
    assert health.ok is True
    assert health.error is None


def test_node_health_with_error() -> None:
    """NodeHealth with error stores the error message."""
    health = NodeHealth(node="test-node", ok=False, url="http://127.0.0.1:9000", error="connect failed")
    assert health.ok is False
    assert health.error == "connect failed"


def test_shard_health_dataclass() -> None:
    """ShardHealth dataclass stores values correctly."""
    primary = NodeHealth(node="primary-node", ok=True, url="http://127.0.0.1:9001", error=None)
    replica = NodeHealth(node="replica-node", ok=True, url="http://127.0.0.1:9002", error=None)
    health = ShardHealth(shard_id=0, status="healthy", primary=primary, replicas=[replica])

    assert health.shard_id == 0
    assert health.status == "healthy"
    assert health.primary.node == "primary-node"
    assert len(health.replicas) == 1


def test_cluster_health_report_to_dict_structure() -> None:
    """ClusterHealthReport.to_dict() returns expected structure."""
    report = ClusterHealthReport(
        nodes={
            "node-1": NodeHealth(node="node-1", ok=True, url="http://127.0.0.1:9001", error=None),
        },
        shards={
            0: ShardHealth(
                shard_id=0,
                status="healthy",
                primary=NodeHealth(node="node-1", ok=True, url="http://127.0.0.1:9001", error=None),
                replicas=[],
            ),
        },
    )

    result = report.to_dict()

    # Check nodes structure
    assert "node-1" in result["nodes"]
    node_data = result["nodes"]["node-1"]
    assert "ok" in node_data
    assert "url" in node_data
    assert "error" in node_data

    # Check shards structure (key is converted to string for JSON)
    assert "0" in result["shards"]
    shard_data = result["shards"]["0"]
    assert "status" in shard_data
    assert "primary" in shard_data
    assert "replicas" in shard_data
    assert shard_data["primary"]["node"] == "node-1"
    assert shard_data["primary"]["ok"] is True
