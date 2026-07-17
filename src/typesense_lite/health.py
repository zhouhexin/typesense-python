"""Health status aggregation for the distributed cluster."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from .http_client import get_with_retry


@dataclass
class NodeHealth:
    """Health status for a single node."""

    node: str
    ok: bool
    url: str
    error: str | None = None
    status: str = "unavailable"
    ready: bool = False
    recovery: dict[str, Any] | None = None
    live: bool = False


@dataclass
class ShardHealth:
    """Health status for a single shard."""

    shard_id: int
    status: str  # "healthy", "degraded", "unavailable"
    primary: NodeHealth
    replicas: list[NodeHealth] = field(default_factory=list)


@dataclass
class ClusterHealthReport:
    """Aggregated health report for the entire cluster."""

    nodes: dict[str, NodeHealth]
    shards: dict[int, ShardHealth]

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        status = _cluster_status(self.shards.values())
        return {
            "status": status,
            "ok": status == "healthy",
            "nodes": {
                node_id: {
                    "ok": node.ok,
                    "url": node.url,
                    "error": node.error,
                    "status": node.status,
                    "ready": node.ready,
                    "recovery": node.recovery,
                }
                for node_id, node in self.nodes.items()
            },
            "shards": {
                str(shard_id): {
                    "status": shard.status,
                    "primary": {
                        "node": shard.primary.node,
                        "ok": shard.primary.ok,
                        "status": shard.primary.status,
                        "ready": shard.primary.ready,
                    },
                    "replicas": [
                        {
                            "node": replica.node,
                            "ok": replica.ok,
                            "status": replica.status,
                            "ready": replica.ready,
                        }
                        for replica in shard.replicas
                    ],
                }
                for shard_id, shard in self.shards.items()
            },
        }


async def collect_cluster_health(
    cluster: Any,
    client: httpx.AsyncClient,
) -> ClusterHealthReport:
    """
    Collect health status from all nodes in the cluster.

    Args:
        cluster: ClusterMap instance with nodes and shard placement info.
        client: Async HTTP client for making requests.

    Returns:
        ClusterHealthReport with node and shard health status.
    """
    nodes: dict[str, NodeHealth] = {}
    shard_healths: dict[int, ShardHealth] = {}

    # Check health of each node
    for node_id, node in cluster.nodes.items():
        try:
            response = await get_with_retry(client, f"{node.url}/health")
            health_payload = response.json()
            if health_payload.get("ok") is not True:
                nodes[node_id] = NodeHealth(
                    node=node_id,
                    ok=False,
                    url=node.url,
                    error=str(
                        health_payload.get("error")
                        or health_payload.get("detail")
                        or "health check returned ok=false"
                    ),
                    status=str(health_payload.get("status") or "unavailable"),
                    ready=False,
                    live=True,
                )
                continue

            recovery_payload = await _get_recovery_state(node.url, client)
            if recovery_payload is not None and "shards" not in recovery_payload:
                recovery_payload = None
            status = str(
                (recovery_payload or {}).get("status")
                or health_payload.get("status")
                or "healthy"
            )
            ready = bool(
                (recovery_payload or {}).get("ready")
                if recovery_payload is not None
                else health_payload.get("ready", True)
            )
            error = health_payload.get("error") or (recovery_payload or {}).get(
                "error"
            )
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=ready and status == "healthy",
                url=node.url,
                error=str(error) if error else None,
                status=status,
                ready=ready,
                recovery=recovery_payload,
                live=True,
            )
        except httpx.HTTPError as error:
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=False,
                url=node.url,
                error=str(error),
                status="unavailable",
                ready=False,
                live=False,
            )
        except Exception as error:
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=False,
                url=node.url,
                error=str(error),
                status="failed",
                ready=False,
                live=False,
            )

    # Determine shard status based on node health
    for shard_id in range(cluster.shard_count):
        placement = cluster.shards[shard_id]

        # Primary status
        primary_node_id = placement.primary
        primary_health = _shard_member_health(
            nodes.get(
                primary_node_id,
                NodeHealth(
                    node=primary_node_id,
                    ok=False,
                    url="unknown",
                    error="node not found in cluster config",
                ),
            ),
            shard_id,
        )

        # Replica statuses
        replica_healths: list[NodeHealth] = []
        for replica_node_id in placement.replicas:
            replica_health = _shard_member_health(
                nodes.get(
                    replica_node_id,
                    NodeHealth(
                        node=replica_node_id,
                        ok=False,
                        url="unknown",
                        error="node not found in cluster config",
                    ),
                ),
                shard_id,
            )
            replica_healths.append(replica_health)

        voters = [primary_health, *replica_healths]
        ready_voters = sum(1 for voter in voters if voter.ready)
        majority = len(voters) // 2 + 1

        if voters and ready_voters == len(voters):
            shard_status = "healthy"
        elif ready_voters >= majority:
            shard_status = "degraded"
        else:
            shard_status = "unavailable"

        shard_healths[shard_id] = ShardHealth(
            shard_id=shard_id,
            status=shard_status,
            primary=primary_health,
            replicas=replica_healths,
        )

    return ClusterHealthReport(
        nodes=nodes,
        shards=shard_healths,
    )


async def _get_recovery_state(
    node_url: str,
    client: httpx.AsyncClient,
) -> dict[str, Any] | None:
    try:
        response = await client.get(
            f"{node_url}/internal/recovery/state",
            timeout=2.0,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        if error.response.status_code == 404:
            return None
        raise
    return response.json()


def _shard_member_health(node: NodeHealth, shard_id: int) -> NodeHealth:
    if node.recovery is None:
        return node

    shard = node.recovery.get("shards", {}).get(str(shard_id))
    if not isinstance(shard, dict):
        return NodeHealth(
            node=node.node,
            ok=False,
            url=node.url,
            error=node.error or f"recovery state missing shard {shard_id}",
            status="starting",
            ready=False,
            recovery=node.recovery,
            live=node.live,
        )

    status = str(shard.get("status") or "starting")
    ready = shard.get("ready") is True and status == "healthy"
    error = shard.get("error") or node.error
    return NodeHealth(
        node=node.node,
        ok=node.live and ready,
        url=node.url,
        error=str(error) if error else None,
        status=status,
        ready=ready,
        recovery=node.recovery,
        live=node.live,
    )


def _cluster_status(shards: Any) -> str:
    shard_statuses = [shard.status for shard in shards]
    if not shard_statuses:
        return "unavailable"
    if any(status == "unavailable" for status in shard_statuses):
        return "unavailable"
    if all(status == "healthy" for status in shard_statuses):
        return "healthy"
    return "degraded"
