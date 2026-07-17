import httpx
import pytest

from typesense_lite.cluster import ClusterMap
from typesense_lite.leader_directory import LeaderDirectory


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 1,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {"0": {"primary": "node-1", "replicas": ["node-2"]}},
}


@pytest.mark.asyncio
async def test_discovers_shard_leader() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9102:
            return httpx.Response(
                200,
                json={"node": "node-2", "role": "leader", "current_term": 3},
            )
        return httpx.Response(
            200,
            json={
                "node": "node-1",
                "role": "follower",
                "current_term": 3,
                "leader_id": "node-2",
            },
        )

    cluster = ClusterMap.from_dict(CONFIG)
    directory = LeaderDirectory(
        cluster,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    leader = await directory.get_leader(0)

    assert leader.id == "node-2"


@pytest.mark.asyncio
async def test_ignores_unavailable_hinted_leader() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9102:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "node": "node-1",
                "role": "follower",
                "current_term": 3,
                "leader_id": "node-2",
            },
        )

    cluster = ClusterMap.from_dict(CONFIG)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        directory = LeaderDirectory(cluster, client)
        leader = await directory.refresh(0)

    assert leader is None
