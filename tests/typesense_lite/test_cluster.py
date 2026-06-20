from typesense_lite.cluster import ClusterMap


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 3,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2"]},
        "1": {"primary": "node-2", "replicas": ["node-1"]},
        "2": {"primary": "node-1", "replicas": ["node-2"]},
    },
}


def test_cluster_map_returns_placements() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    assert cluster.get_primary(1).id == "node-2"
    assert [node.id for node in cluster.get_replicas(1)] == ["node-1"]
    assert cluster.get_search_candidates(1)[0].id == "node-2"
    assert cluster.node_url("node-2") == "http://127.0.0.1:9102"


def test_get_shard_voters_returns_primary_then_replicas() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    voters = cluster.get_shard_voters(1)

    assert [node.id for node in voters] == ["node-2", "node-1"]


def test_cluster_map_shard_id_is_stable() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    assert cluster.get_shard_id("doc-1") == cluster.get_shard_id("doc-1")
    assert 0 <= cluster.get_shard_id("doc-1") < 3
