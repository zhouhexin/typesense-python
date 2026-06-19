"""Health status aggregation for the distributed cluster."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass
class NodeHealth:
    """Health status for a single node."""

    node: str
    ok: bool
    url: str
    error: str | None = None


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
        return {
            "nodes": {
                node_id: {
                    "ok": node.ok,
                    "url": node.url,
                    "error": node.error,
                }
                for node_id, node in self.nodes.items()
            },
            "shards": {
                str(shard_id): {
                    "status": shard.status,
                    "primary": {
                        "node": shard.primary.node,
                        "ok": shard.primary.ok,
                    },
                    "replicas": [
                        {
                            "node": replica.node,
                            "ok": replica.ok,
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
            response = await client.get(f"{node.url}/health", timeout=2.0)
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok") is not True:
                nodes[node_id] = NodeHealth(
                    node=node_id,
                    ok=False,
                    url=node.url,
                    error=str(payload.get("error") or payload.get("detail") or "health check returned ok=false"),
                )
                continue
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=True,
                url=node.url,
                error=None,
            )
        except httpx.HTTPError as error:
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=False,
                url=node.url,
                error=str(error),
            )
        except Exception as error:
            nodes[node_id] = NodeHealth(
                node=node_id,
                ok=False,
                url=node.url,
                error=str(error),
            )

    # Determine shard status based on node health
    for shard_id in range(cluster.shard_count):
        placement = cluster.shards[shard_id]

        # Primary status
        primary_node_id = placement.primary
        primary_health = nodes.get(
            primary_node_id,
            NodeHealth(
                node=primary_node_id,
                ok=False,
                url="unknown",
                error="node not found in cluster config",
            ),
        )

        # Replica statuses
        replica_healths: list[NodeHealth] = []
        for replica_node_id in placement.replicas:
            replica_health = nodes.get(
                replica_node_id,
                NodeHealth(
                    node=replica_node_id,
                    ok=False,
                    url="unknown",
                    error="node not found in cluster config",
                ),
            )
            replica_healths.append(replica_health)

        # Determine shard status
        if primary_health.ok:
            shard_status = "healthy"
        elif any(r.ok for r in replica_healths):
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
