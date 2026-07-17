"""Inventory schema and conversion to/from :class:`typesense_lite.cluster.ClusterMap`.

An inventory file is the multi-machine counterpart to ``cluster_config.json``:
it describes the hosts, the SSH credentials to reach them, where the project
tree lives on each host, and which data directory each node should write to.
The deploy script reads an inventory, derives a ``cluster_config.json`` from
it, ships that JSON to every host, and starts one process per host.

A roundtrip ``Inventory.from_cluster_config(...) -> to_cluster_config()``
must reproduce the original config (modulo inventory-only fields like
``data_dir``).
"""

from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


DEFAULT_DATA_DIR = ".data/typesense_lite"
DEFAULT_LOG_DIR = "./logs"
DEFAULT_REMOTE_CONFIG_PATH = "/var/lib/typesense_lite/cluster_config.json"
DEFAULT_SYNC_MODE = "rsync"


@dataclass(frozen=True)
class InventorySSH:
    """SSH credentials used by the deploy script to talk to remote hosts."""

    user: str = ""
    identity_file: str | None = None
    options: tuple[str, ...] = ("-o", "StrictHostKeyChecking=accept-new")


@dataclass(frozen=True)
class InventoryHost:
    """Network coordinates and on-disk directory for a single process."""

    host: str
    port: int
    data_dir: str


@dataclass(frozen=True)
class InventoryNode(InventoryHost):
    """A data-node entry. Carries a stable id used by raft members."""

    id: str


@dataclass(frozen=True)
class InventoryShard:
    """Per-shard placement: primary node id plus ordered replicas."""

    primary: str
    replicas: tuple[str, ...] = ()


@dataclass(frozen=True)
class Inventory:
    """Full multi-machine deployment descriptor."""

    ssh: InventorySSH
    coordinator: InventoryHost
    nodes: dict[str, InventoryNode]
    shards: dict[int, InventoryShard]
    shard_count: int
    remote_project_root: str = "."
    remote_venv_python: str = ".venv/bin/python"
    remote_config_path: str = DEFAULT_REMOTE_CONFIG_PATH
    log_dir: str = DEFAULT_LOG_DIR
    sync_mode: str = DEFAULT_SYNC_MODE
    local_simulation: bool = False
    coordinator_url: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: str | Path) -> "Inventory":
        with Path(path).open("r", encoding="utf-8") as handle:
            return cls.from_dict(json.load(handle))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Inventory":
        if not isinstance(data, Mapping):
            raise TypeError("inventory must be a JSON object")

        ssh_data = data.get("ssh", {}) or {}
        ssh = InventorySSH(
            user=str(ssh_data.get("user", "")),
            identity_file=ssh_data.get("identity_file"),
            options=tuple(ssh_data.get("options", ("-o", "StrictHostKeyChecking=accept-new"))),
        )

        coordinator_data = data.get("coordinator") or {}
        if "host" not in coordinator_data or "port" not in coordinator_data:
            raise ValueError("inventory.coordinator requires host and port")
        coordinator = InventoryHost(
            host=str(coordinator_data["host"]),
            port=int(coordinator_data["port"]),
            data_dir=str(coordinator_data.get("data_dir") or DEFAULT_DATA_DIR),
        )

        nodes_data = data.get("nodes") or []
        if not isinstance(nodes_data, list) or not nodes_data:
            raise ValueError("inventory.nodes must be a non-empty list")
        nodes: dict[str, InventoryNode] = {}
        for entry in nodes_data:
            if not isinstance(entry, Mapping):
                raise ValueError("each inventory.nodes entry must be an object")
            node_id = str(entry.get("id") or "")
            if not node_id:
                raise ValueError("inventory.nodes[].id is required")
            if node_id in nodes:
                raise ValueError(f"duplicate node id {node_id!r}")
            nodes[node_id] = InventoryNode(
                id=node_id,
                host=str(entry["host"]),
                port=int(entry["port"]),
                data_dir=str(entry.get("data_dir") or DEFAULT_DATA_DIR),
            )

        shards_data = data.get("shards") or {}
        if not isinstance(shards_data, Mapping):
            raise ValueError("inventory.shards must be an object")
        shards: dict[int, InventoryShard] = {}
        for shard_key, placement in shards_data.items():
            shard_id = int(shard_key)
            if not isinstance(placement, Mapping):
                raise ValueError(f"shards[{shard_id}] must be an object")
            shards[shard_id] = InventoryShard(
                primary=str(placement["primary"]),
                replicas=tuple(str(replica) for replica in placement.get("replicas", []) or ()),
            )

        shard_count = int(data.get("shard_count", len(shards)))

        inventory = cls(
            ssh=ssh,
            coordinator=coordinator,
            nodes=nodes,
            shards=shards,
            shard_count=shard_count,
            remote_project_root=str(data.get("remote_project_root", ".")),
            remote_venv_python=str(data.get("remote_venv_python", ".venv/bin/python")),
            remote_config_path=str(data.get("remote_config_path", DEFAULT_REMOTE_CONFIG_PATH)),
            log_dir=str(data.get("log_dir", DEFAULT_LOG_DIR)),
            sync_mode=str(data.get("sync_mode", DEFAULT_SYNC_MODE)),
            local_simulation=bool(data.get("local_simulation", False)),
            coordinator_url=data.get("coordinator_url"),
        )
        inventory.validate()
        return inventory

    # ------------------------------------------------------------------
    # Roundtrip with cluster_config
    # ------------------------------------------------------------------

    def to_cluster_config(self) -> dict[str, Any]:
        """Return a ``cluster_config.json``-shaped dict derived from this inventory."""
        nodes_list = [
            {"id": node.id, "host": node.host, "port": node.port}
            for node in self.nodes.values()
        ]
        shards_dict = {
            str(shard_id): {
                "primary": placement.primary,
                "replicas": list(placement.replicas),
            }
            for shard_id, placement in sorted(self.shards.items())
        }
        return {
            "coordinator": {
                "host": self.coordinator.host,
                "port": self.coordinator.port,
            },
            "shard_count": self.shard_count,
            "nodes": nodes_list,
            "shards": shards_dict,
        }

    @classmethod
    def from_cluster_config(
        cls,
        config: Mapping[str, Any],
        *,
        ssh: InventorySSH | None = None,
        remote_project_root: str = ".",
        remote_venv_python: str = ".venv/bin/python",
        remote_config_path: str = DEFAULT_REMOTE_CONFIG_PATH,
        log_dir: str = DEFAULT_LOG_DIR,
        sync_mode: str = DEFAULT_SYNC_MODE,
        data_dir_template: str = DEFAULT_DATA_DIR,
        coordinator_data_dir: str | None = None,
        local_simulation: bool = False,
        coordinator_url: str | None = None,
    ) -> "Inventory":
        """Build an inventory by deriving defaults from an existing cluster config."""
        coordinator_data = config["coordinator"]
        nodes: dict[str, InventoryNode] = {}
        for entry in config.get("nodes", []):
            node_id = str(entry["id"])
            nodes[node_id] = InventoryNode(
                id=node_id,
                host=str(entry["host"]),
                port=int(entry["port"]),
                data_dir=f"{data_dir_template.rstrip('/')}/{node_id}",
            )
        shards: dict[int, InventoryShard] = {}
        for shard_key, placement in config.get("shards", {}).items():
            shard_id = int(shard_key)
            shards[shard_id] = InventoryShard(
                primary=str(placement["primary"]),
                replicas=tuple(str(r) for r in placement.get("replicas", []) or ()),
            )

        inventory = cls(
            ssh=ssh or InventorySSH(),
            coordinator=InventoryHost(
                host=str(coordinator_data["host"]),
                port=int(coordinator_data["port"]),
                data_dir=coordinator_data_dir or f"{data_dir_template.rstrip('/')}/coordinator",
            ),
            nodes=nodes,
            shards=shards,
            shard_count=int(config.get("shard_count", len(shards))),
            remote_project_root=remote_project_root,
            remote_venv_python=remote_venv_python,
            remote_config_path=remote_config_path,
            log_dir=log_dir,
            sync_mode=sync_mode,
            local_simulation=local_simulation,
            coordinator_url=coordinator_url,
        )
        inventory.validate()
        return inventory

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        if self.shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if len(self.shards) != self.shard_count:
            raise ValueError(
                f"shard_count ({self.shard_count}) must match number of shards "
                f"({len(self.shards)})"
            )
        _validate_host_port(self.coordinator.host, self.coordinator.port, label="coordinator")
        for node_id, node in self.nodes.items():
            _validate_host_port(node.host, node.port, label=f"node {node_id!r}")
        if not self.nodes:
            raise ValueError("inventory must declare at least one node")
        for shard_id, placement in self.shards.items():
            if shard_id < 0 or shard_id >= self.shard_count:
                raise ValueError(f"shard id {shard_id} outside [0, {self.shard_count})")
            if placement.primary not in self.nodes:
                raise ValueError(
                    f"shard {shard_id} primary {placement.primary!r} not in nodes"
                )
            for replica in placement.replicas:
                if replica not in self.nodes:
                    raise ValueError(
                        f"shard {shard_id} replica {replica!r} not in nodes"
                    )
            seen = {placement.primary, *placement.replicas}
            if len(seen) != 1 + len(placement.replicas):
                raise ValueError(
                    f"shard {shard_id} has duplicate primary/replica node ids"
                )
        if self.sync_mode not in {"rsync", "git"}:
            raise ValueError(
                f"sync_mode must be 'rsync' or 'git', got {self.sync_mode!r}"
            )
        if self.local_simulation and self.ssh.user:
            # In local-simulation mode SSH is not used; warn-shape via raise to
            # catch authoring mistakes early. The deploy script can still force
            # skip this check by clearing ssh.user in code.
            raise ValueError(
                "local_simulation=true requires ssh.user to be empty "
                "(SSH is skipped in this mode)"
            )

    # ------------------------------------------------------------------
    # Convenience accessors used by deploy script
    # ------------------------------------------------------------------

    def all_hosts(self) -> list[str]:
        """Return the de-duplicated list of host strings used by this inventory."""
        hosts = {self.coordinator.host}
        for node in self.nodes.values():
            hosts.add(node.host)
        return sorted(hosts)

    def coordinator_url_value(self) -> str:
        """Best-effort ``http://host:port`` for the coordinator."""
        if self.coordinator_url:
            return self.coordinator_url
        return f"http://{self.coordinator.host}:{self.coordinator.port}"


def _validate_host_port(host: str, port: int, *, label: str) -> None:
    if not isinstance(host, str) or not host:
        raise ValueError(f"{label}.host must be a non-empty string")
    if not (1 <= int(port) <= 65535):
        raise ValueError(f"{label}.port must be in [1, 65535], got {port}")
    try:
        # ip_address accepts both IPv4 and IPv6. Hostnames are also legal at
        # this layer (deploy script will resolve them later).
        ipaddress.ip_address(host)
    except ValueError:
        # Allow hostnames by leaving them un-validated here.
        if not _looks_like_hostname(host):
            raise ValueError(
                f"{label}.host {host!r} is neither a valid IP nor a plausible hostname"
            ) from None


def _looks_like_hostname(host: str) -> bool:
    if len(host) > 253:
        return False
    labels = host.split(".")
    if not labels or any(not label for label in labels):
        return False
    for label in labels:
        if not all(ch.isalnum() or ch == "-" for ch in label):
            return False
        if label.startswith("-") or label.endswith("-"):
            return False
    return True
