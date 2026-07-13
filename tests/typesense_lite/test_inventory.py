from __future__ import annotations

import json
from pathlib import Path

import pytest

from typesense_lite.inventory import (
    DEFAULT_DATA_DIR,
    Inventory,
    InventoryHost,
    InventoryNode,
    InventoryShard,
    InventorySSH,
)


pytestmark = [pytest.mark.unit, pytest.mark.inventory]


def _inventory(**overrides) -> Inventory:
    base: dict = dict(
        ssh=InventorySSH(user="ubuntu", identity_file="~/.ssh/id_rsa"),
        coordinator=InventoryHost(host="192.168.1.10", port=9100, data_dir="/data/coordinator"),
        nodes={
            "node-1": InventoryNode(
                id="node-1", host="192.168.1.11", port=9101, data_dir="/data/node-1",
            ),
            "node-2": InventoryNode(
                id="node-2", host="192.168.1.12", port=9101, data_dir="/data/node-2",
            ),
            "node-3": InventoryNode(
                id="node-3", host="192.168.1.13", port=9101, data_dir="/data/node-3",
            ),
        },
        shards={
            0: InventoryShard(primary="node-1", replicas=("node-2", "node-3")),
            1: InventoryShard(primary="node-2", replicas=("node-3", "node-1")),
            2: InventoryShard(primary="node-3", replicas=("node-1", "node-2")),
        },
        shard_count=3,
    )
    base.update(overrides)
    return Inventory(**base)


def test_roundtrip_inventory_to_cluster_config_and_back() -> None:
    inv = _inventory()

    config = inv.to_cluster_config()
    rebuilt = Inventory.from_cluster_config(
        config,
        ssh=inv.ssh,
        remote_project_root=inv.remote_project_root,
        remote_venv_python=inv.remote_venv_python,
        remote_config_path=inv.remote_config_path,
        log_dir=inv.log_dir,
    )

    assert rebuilt.to_cluster_config() == config


def test_to_cluster_config_has_expected_shape() -> None:
    config = _inventory().to_cluster_config()

    assert config["coordinator"] == {"host": "192.168.1.10", "port": 9100}
    assert config["shard_count"] == 3
    assert {n["id"] for n in config["nodes"]} == {"node-1", "node-2", "node-3"}
    assert config["shards"]["0"] == {"primary": "node-1", "replicas": ["node-2", "node-3"]}


def test_from_dict_accepts_minimal_payload() -> None:
    payload = {
        "coordinator": {"host": "10.0.0.1", "port": 9100},
        "nodes": [{"id": "n1", "host": "10.0.0.2", "port": 9101}],
        "shards": {"0": {"primary": "n1", "replicas": []}},
        "shard_count": 1,
    }
    inv = Inventory.from_dict(payload)

    assert inv.coordinator.host == "10.0.0.1"
    assert "n1" in inv.nodes
    assert inv.ssh.user == ""  # default


def test_from_file_round_trip(tmp_path: Path) -> None:
    inv = _inventory()
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(_to_plain(inv)), encoding="utf-8")

    loaded = Inventory.from_file(path)

    assert loaded.to_cluster_config() == inv.to_cluster_config()


def _to_plain(inv: Inventory) -> dict:
    return {
        "ssh": {
            "user": inv.ssh.user,
            "identity_file": inv.ssh.identity_file,
            "options": list(inv.ssh.options),
        },
        "coordinator": {
            "host": inv.coordinator.host,
            "port": inv.coordinator.port,
            "data_dir": inv.coordinator.data_dir,
        },
        "nodes": [
            {
                "id": n.id,
                "host": n.host,
                "port": n.port,
                "data_dir": n.data_dir,
            }
            for n in inv.nodes.values()
        ],
        "shards": {
            str(sid): {"primary": p.primary, "replicas": list(p.replicas)}
            for sid, p in inv.shards.items()
        },
        "shard_count": inv.shard_count,
        "remote_project_root": inv.remote_project_root,
        "remote_venv_python": inv.remote_venv_python,
        "remote_config_path": inv.remote_config_path,
        "log_dir": inv.log_dir,
        "sync_mode": inv.sync_mode,
        "local_simulation": inv.local_simulation,
    }


def test_validate_rejects_unknown_primary_node() -> None:
    with pytest.raises(ValueError, match="primary"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="ghost", replicas=("n1",))},
            shard_count=1,
        )


def test_validate_rejects_duplicate_primary_in_replicas() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
                "n2": InventoryNode(id="n2", host="10.0.0.3", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=("n1", "n2"))},
            shard_count=1,
        )


def test_validate_rejects_shard_count_mismatch() -> None:
    with pytest.raises(ValueError, match="shard_count"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=())},
            shard_count=2,  # declared 2 shards but only 1 in shards map
        )


def test_validate_rejects_unknown_replica() -> None:
    with pytest.raises(ValueError, match="replica"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=("ghost",))},
            shard_count=1,
        )


def test_validate_rejects_bad_ip() -> None:
    with pytest.raises(ValueError, match="host"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="not!a!host", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=())},
            shard_count=1,
        )


def test_validate_rejects_bad_port() -> None:
    with pytest.raises(ValueError, match="port"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=99999, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=())},
            shard_count=1,
        )


def test_validate_rejects_bad_sync_mode() -> None:
    with pytest.raises(ValueError, match="sync_mode"):
        Inventory(
            ssh=InventorySSH(),
            coordinator=InventoryHost(host="10.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="10.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=())},
            shard_count=1,
            sync_mode="ftp",
        )


def test_validate_rejects_local_simulation_with_ssh_user() -> None:
    with pytest.raises(ValueError, match="local_simulation"):
        Inventory(
            ssh=InventorySSH(user="ubuntu"),
            coordinator=InventoryHost(host="127.0.0.1", port=9100, data_dir="x"),
            nodes={
                "n1": InventoryNode(id="n1", host="127.0.0.2", port=9101, data_dir="x"),
            },
            shards={0: InventoryShard(primary="n1", replicas=())},
            shard_count=1,
            local_simulation=True,
        )


def test_hostnames_are_accepted() -> None:
    inv = Inventory(
        ssh=InventorySSH(),
        coordinator=InventoryHost(host="coord.internal", port=9100, data_dir="x"),
        nodes={
            "n1": InventoryNode(id="n1", host="node-1.internal", port=9101, data_dir="x"),
        },
        shards={0: InventoryShard(primary="n1", replicas=())},
        shard_count=1,
    )
    assert inv.coordinator.host == "coord.internal"


def test_all_hosts_returns_sorted_unique_hosts() -> None:
    inv = _inventory()
    hosts = inv.all_hosts()

    assert hosts == sorted({"192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13"})
    assert len(hosts) == len(set(hosts))


def test_coordinator_url_value_uses_override_when_provided() -> None:
    inv = _inventory(coordinator_url="http://lb.example.com:9100")

    assert inv.coordinator_url_value() == "http://lb.example.com:9100"


def test_coordinator_url_value_defaults_to_inventory_host() -> None:
    inv = _inventory()

    assert inv.coordinator_url_value() == "http://192.168.1.10:9100"


def test_from_cluster_config_fills_data_dir_template() -> None:
    config = {
        "coordinator": {"host": "10.0.0.1", "port": 9100},
        "nodes": [
            {"id": "n1", "host": "10.0.0.2", "port": 9101},
            {"id": "n2", "host": "10.0.0.3", "port": 9101},
        ],
        "shards": {
            "0": {"primary": "n1", "replicas": ["n2"]},
            "1": {"primary": "n2", "replicas": ["n1"]},
        },
        "shard_count": 2,
    }

    inv = Inventory.from_cluster_config(config, data_dir_template="/srv/data")

    assert inv.coordinator.data_dir == "/srv/data/coordinator"
    assert inv.nodes["n1"].data_dir == "/srv/data/n1"
    assert inv.nodes["n2"].data_dir == "/srv/data/n2"


def test_from_cluster_config_roundtrip_with_defaults() -> None:
    config = {
        "coordinator": {"host": "10.0.0.1", "port": 9100},
        "nodes": [{"id": "n1", "host": "10.0.0.2", "port": 9101}],
        "shards": {"0": {"primary": "n1", "replicas": []}},
        "shard_count": 1,
    }
    inv = Inventory.from_cluster_config(config)

    assert inv.to_cluster_config() == config
    assert inv.ssh.user == ""
    assert inv.sync_mode == "rsync"
    assert inv.coordinator.data_dir == f"{DEFAULT_DATA_DIR}/coordinator"
    assert inv.nodes["n1"].data_dir == f"{DEFAULT_DATA_DIR}/n1"