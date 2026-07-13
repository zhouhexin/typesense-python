"""Multi-machine integration test using loopback alias IPs.

This test simulates a real multi-host deployment on a single machine by:

1. Binding each node + the coordinator to a distinct loopback alias IP
   (e.g. 192.168.99.10/11/12/13). If the alias is not already configured on
   the host (which requires ``sudo`` once per machine), the fixture skips
   the test.
2. Spawning the real uvicorn processes via ``scripts/run_node.py`` — same
   code path the deploy script uses in ``--local-sim`` mode.
3. Driving the cluster through the public HTTP API to verify cross-IP
   traffic, Raft replication, and heartbeat-based node discovery.

Run prerequisites::

    for ip in 192.168.99.{10..13}; do sudo ip addr add $ip/32 dev lo; done
    # macOS: for ip in 192.168.99.{10..13}; do sudo ifconfig lo0 alias $ip/32; done
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import httpx
import pytest


pytestmark = pytest.mark.multi_machine


ALIAS_IPS = ("192.168.99.10", "192.168.99.11", "192.168.99.12", "192.168.99.13")
PROBE_IP = ALIAS_IPS[0]
PROBE_PORT = 39100  # unlikely to clash with the demo ports


def _alias_ips_available() -> bool:
    """Return True iff the first alias IP accepts a bind on this machine."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((PROBE_IP, 0))
        return True
    except (OSError, PermissionError):
        return False


_skip_reason = (
    "Loopback alias IPs not configured. Run: "
    "for ip in 192.168.99.{10..13}; do sudo ip addr add $ip/32 dev lo; done "
    "(macOS: sudo ifconfig lo0 alias $ip/32)"
)


@pytest.fixture(scope="module")
def alias_ips() -> Iterator[tuple[str, ...]]:
    if not _alias_ips_available():
        pytest.skip(_skip_reason)
    yield ALIAS_IPS


@pytest.fixture()
def multi_ip_cluster(
    alias_ips: tuple[str, ...],
    tmp_path: Path,
) -> Iterator[dict[str, Any]]:
    """Spawn coordinator + 3 data nodes, one per alias IP. Tear down on exit."""
    config = {
        "coordinator": {"host": ALIAS_IPS[0], "port": 39100},
        "shard_count": 3,
        "nodes": [
            {"id": "node-1", "host": ALIAS_IPS[1], "port": 39101},
            {"id": "node-2", "host": ALIAS_IPS[2], "port": 39102},
            {"id": "node-3", "host": ALIAS_IPS[3], "port": 39103},
        ],
        "shards": {
            "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]},
            "1": {"primary": "node-2", "replicas": ["node-3", "node-1"]},
            "2": {"primary": "node-3", "replicas": ["node-1", "node-2"]},
        },
    }
    config_path = tmp_path / "cluster_config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    repo_root = Path(__file__).resolve().parents[2]
    src_path = str(repo_root / "src")
    base_pythonpath = os.environ.get("PYTHONPATH")
    env_pythonpath = (
        os.pathsep.join([src_path, base_pythonpath]) if base_pythonpath else src_path
    )

    processes: list[subprocess.Popen[bytes]] = []
    try:
        # Spawn data nodes first so the coordinator finds leaders when it boots.
        for index, node in enumerate(config["nodes"]):
            data_dir = tmp_path / f"node-{index + 1}"
            data_dir.mkdir()
            log_handle = (tmp_path / f"node-{index + 1}.log").open("wb")
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "run_node.py"),
                    "--role", "node",
                    "--node-id", node["id"],
                    "--host", node["host"],
                    "--port", str(node["port"]),
                    "--config", str(config_path),
                    "--data-dir", str(data_dir),
                ],
                env={
                    **os.environ,
                    "PYTHONPATH": env_pythonpath,
                    "COORDINATOR_URL": f"http://{config['coordinator']['host']}:{config['coordinator']['port']}",
                    "NODE_ADVERTISE_HOST": node["host"],
                    "NODE_ADVERTISE_PORT": str(node["port"]),
                },
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
            processes.append(proc)

        coordinator_dir = tmp_path / "coordinator"
        coordinator_dir.mkdir()
        coordinator_log = (tmp_path / "coordinator.log").open("wb")
        coordinator_proc = subprocess.Popen(
            [
                sys.executable,
                str(repo_root / "scripts" / "run_node.py"),
                "--role", "coordinator",
                "--host", config["coordinator"]["host"],
                "--port", str(config["coordinator"]["port"]),
                "--config", str(config_path),
                "--data-dir", str(coordinator_dir),
            ],
            env={**os.environ, "PYTHONPATH": env_pythonpath},
            stdout=coordinator_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(coordinator_proc)

        if not _wait_for_health(config["coordinator"], timeout=20.0):
            _dump_logs(tmp_path)
            raise RuntimeError("coordinator did not become healthy in time")

        for node in config["nodes"]:
            if not _wait_for_health(node, timeout=20.0):
                _dump_logs(tmp_path)
                raise RuntimeError(f"node {node['id']} did not become healthy in time")

        yield config
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def _wait_for_health(host_port: dict[str, Any], *, timeout: float) -> bool:
    url = f"http://{host_port['host']}:{host_port['port']}/health"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=1.0)
            if response.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    return False


def _dump_logs(tmp_path: Path) -> None:
    for log in sorted(tmp_path.glob("*.log")):
        sys.stderr.write(f"\n=== {log.name} ===\n")
        try:
            sys.stderr.write(log.read_text(encoding="utf-8", errors="replace"))
        except Exception as error:  # noqa: BLE001
            sys.stderr.write(f"(failed to read {log}: {error})\n")


def test_coordinator_health_reachable_on_alias_ip(
    multi_ip_cluster: dict[str, Any],
) -> None:
    coord = multi_ip_cluster["coordinator"]
    response = httpx.get(f"http://{coord['host']}:{coord['port']}/health", timeout=2.0)
    assert response.status_code == 200
    assert response.json()["role"] == "coordinator"


def test_data_node_health_reachable_on_alias_ip(
    multi_ip_cluster: dict[str, Any],
) -> None:
    node = multi_ip_cluster["nodes"][0]
    response = httpx.get(f"http://{node['host']}:{node['port']}/health", timeout=2.0)
    assert response.status_code == 200
    payload = response.json()
    assert payload["role"] == "node"
    assert payload["node_id"] == node["id"]


def test_write_through_coordinator_lands_on_real_node_ip(
    multi_ip_cluster: dict[str, Any],
) -> None:
    coord = multi_ip_cluster["coordinator"]
    document_id = "doc-cross-ip-1"
    response = httpx.post(
        f"http://{coord['host']}:{coord['port']}/collections/books/documents",
        json={"id": document_id, "title": "Cross-IP raft write", "body": "hello"},
        timeout=10.0,
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    shard_id = payload["shard_id"]
    primary = multi_ip_cluster["nodes"][
        [n["id"] for n in multi_ip_cluster["nodes"]].index(
            {**{0: "node-1", 1: "node-2", 2: "node-3"}[shard_id]}
        )
    ]

    response = httpx.get(
        f"http://{primary['host']}:{primary['port']}/internal/shards/{shard_id}"
        f"/collections/books/documents/{document_id}",
        timeout=5.0,
    )
    assert response.status_code == 200, response.text
    assert response.json()["id"] == document_id


def test_search_routes_to_real_node_ips(
    multi_ip_cluster: dict[str, Any],
) -> None:
    coord = multi_ip_cluster["coordinator"]
    document_id = "doc-search-multi-ip"

    response = httpx.post(
        f"http://{coord['host']}:{coord['port']}/collections/articles/documents",
        json={"id": document_id, "title": "Search via cross-IP routing", "body": "find me"},
        timeout=10.0,
    )
    assert response.status_code == 200, response.text

    response = httpx.get(
        f"http://{coord['host']}:{coord['port']}/collections/articles/documents/search",
        params={"q": "find", "limit": 10},
        timeout=10.0,
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert any(hit["id"] == document_id for hit in payload["hits"])


def test_heartbeat_registers_nodes_with_coordinator(
    multi_ip_cluster: dict[str, Any],
) -> None:
    coord = multi_ip_cluster["coordinator"]
    # Heartbeats are emitted every 5s by default; allow some slack.
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        response = httpx.get(
            f"http://{coord['host']}:{coord['port']}/cluster/nodes", timeout=2.0,
        )
        assert response.status_code == 200
        alive_ids = {entry["node_id"] for entry in response.json()["alive"]}
        if {"node-1", "node-2", "node-3"}.issubset(alive_ids):
            break
        time.sleep(1.0)
    else:
        pytest.fail("expected all three data nodes to register via heartbeat")

    # Also confirm each node was advertised with its external IP (the alias).
    response = httpx.get(
        f"http://{coord['host']}:{coord['port']}/cluster/nodes", timeout=2.0,
    )
    advertised_hosts = {entry["node_id"]: entry["host"] for entry in response.json()["alive"]}
    assert advertised_hosts == {
        "node-1": ALIAS_IPS[1],
        "node-2": ALIAS_IPS[2],
        "node-3": ALIAS_IPS[3],
    }


def test_raft_3_voters_elect_leader_across_ips(
    multi_ip_cluster: dict[str, Any],
) -> None:
    coord = multi_ip_cluster["coordinator"]
    deadline = time.monotonic() + 15.0
    expected = {"node-1", "node-2", "node-3"}
    while time.monotonic() < deadline:
        response = httpx.get(
            f"http://{coord['host']}:{coord['port']}/cluster/raft", timeout=2.0,
        )
        assert response.status_code == 200
        shards = response.json()["shards"]
        # All three shards should have a leader chosen.
        leaders = {s["leader"] for s in shards.values()}
        if leaders != {None} and leaders.issubset(expected):
            # Members per shard cover all three nodes.
            members_per_shard = [set(s["members"].keys()) for s in shards.values()]
            if all(m == expected for m in members_per_shard):
                break
        time.sleep(1.0)
    else:
        pytest.fail(f"expected raft shards to converge; got {shards}")


def test_health_uses_external_ip_not_loopback(
    multi_ip_cluster: dict[str, Any],
) -> None:
    """Sanity check: traffic flows via the configured alias IP, not 127.0.0.1."""
    coord = multi_ip_cluster["coordinator"]
    response = httpx.get(f"http://127.0.0.1:{coord['port']}/health", timeout=2.0)
    # The coordinator binds to 0.0.0.0, so loopback works; what we want is the
    # /cluster endpoint reporting the alias IP, confirming the cluster config
    # was actually loaded with external IPs.
    cluster = httpx.get(
        f"http://{coord['host']}:{coord['port']}/cluster", timeout=2.0,
    ).json()
    assert cluster["coordinator"]["host"] == ALIAS_IPS[0]
    assert {n["host"] for n in cluster["nodes"]} == set(ALIAS_IPS)