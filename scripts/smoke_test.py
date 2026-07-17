"""End-to-end smoke test for the multi-machine Typesense Lite deployment.

What it does
============

1. Probes whether loopback alias IPs are configured (``192.168.99.10/11/12/13``).
   If not, prints instructions and exits 2.
2. Writes a ``cluster_config.json`` and spawns one coordinator + three data
   node processes via ``scripts/run_node.py``, each bound to a distinct alias
   IP. Logs go to ``<data_root>/logs/`` and PIDs to ``<data_root>/<role>/server.pid``.
3. Polls every node until ``/health`` returns 200 (or fails the smoke).
4. Runs the full verification matrix:

   - per-node ``/health``
   - cross-IP document write + read-through
   - coordinator fan-out search
   - Raft shard convergence (one leader per shard, members == expected set)
   - heartbeat-based node discovery (``/cluster/nodes`` ``alive`` set)
   - replica read directly from a non-primary node

5. Tears everything down (SIGTERM, then SIGKILL after a grace period) and
   exits 0 if every check passed, 1 otherwise.

Usage
=====

::

    python scripts/smoke_test.py
    python scripts/smoke_test.py --data-root /tmp/ts-smoke
    python scripts/smoke_test.py --alias-prefix 10.0.99  # use 10.0.99.10..13
    python scripts/smoke_test.py --skip-setup            # assume cluster is up

It deliberately avoids pytest/pytest-asyncio so it can run in any environment
where the project's dependencies are installed.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ALIAS_PREFIX = "192.168.99"
DEFAULT_DATA_ROOT = Path("/tmp/ts-smoke")
HEALTH_TIMEOUT_SECONDS = 25.0
STOP_GRACE_SECONDS = 5.0


# ----------------------------------------------------------------------
# Result accumulator
# ----------------------------------------------------------------------


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    elapsed_ms: float = 0.0


@dataclass
class SmokeReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, fn: Callable[[], None]) -> None:
        start = time.monotonic()
        try:
            fn()
        except SmokeFailure as failure:
            self.checks.append(
                CheckResult(
                    name=name,
                    passed=False,
                    detail=str(failure),
                    elapsed_ms=(time.monotonic() - start) * 1000,
                ),
            )
            return
        self.checks.append(
            CheckResult(
                name=name,
                passed=True,
                elapsed_ms=(time.monotonic() - start) * 1000,
            ),
        )

    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def print(self) -> None:
        width = max((len(c.name) for c in self.checks), default=20)
        print()
        print(f"{'check':<{width}}  {'result':<6}  {'ms':>7}  detail")
        print(f"{'-' * width}  {'-' * 6}  {'-' * 7}  ------")
        for check in self.checks:
            status = "PASS" if check.passed else "FAIL"
            print(
                f"{check.name:<{width}}  {status:<6}  "
                f"{check.elapsed_ms:>7.1f}  {check.detail}",
            )


class SmokeFailure(AssertionError):
    """Raised by a check body to record a non-passing result."""


# ----------------------------------------------------------------------
# Cluster lifecycle
# ----------------------------------------------------------------------


@dataclass
class SmokeCluster:
    alias_prefix: str
    data_root: Path
    config_path: Path
    processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    log_handles: list[Any] = field(default_factory=list)

    @property
    def ips(self) -> tuple[str, ...]:
        return tuple(f"{self.alias_prefix}.{i}" for i in (10, 11, 12, 13))

    @property
    def coordinator(self) -> dict[str, Any]:
        return {"host": self.ips[0], "port": 9100, "id": "coordinator"}

    def nodes(self) -> list[dict[str, Any]]:
        return [
            {"id": "node-1", "host": self.ips[1], "port": 9101},
            {"id": "node-2", "host": self.ips[2], "port": 9102},
            {"id": "node-3", "host": self.ips[3], "port": 9103},
        ]

    def base_inventory(self) -> dict[str, Any]:
        return {
            "ssh": {"user": "", "identity_file": None, "options": []},
            "local_simulation": True,
            "coordinator": {
                "host": self.ips[0],
                "port": 9100,
                "data_dir": str(self.data_root / "coordinator"),
            },
            "nodes": [
                {
                    "id": node["id"],
                    "host": node["host"],
                    "port": node["port"],
                    "data_dir": str(self.data_root / node["id"]),
                }
                for node in self.nodes()
            ],
            "shards": {
                "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]},
                "1": {"primary": "node-2", "replicas": ["node-3", "node-1"]},
                "2": {"primary": "node-3", "replicas": ["node-1", "node-2"]},
            },
            "shard_count": 3,
            "remote_project_root": ".",
            "remote_venv_python": sys.executable,
            "remote_config_path": str(self.config_path),
            "log_dir": str(self.data_root / "logs"),
        }

    def write_cluster_config(self) -> Path:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            json.dumps(
                {
                    "coordinator": {
                        "host": self.ips[0],
                        "port": 9100,
                    },
                    "shard_count": 3,
                    "nodes": [
                        {"id": n["id"], "host": n["host"], "port": n["port"]}
                        for n in self.nodes()
                    ],
                    "shards": {
                        "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]},
                        "1": {"primary": "node-2", "replicas": ["node-3", "node-1"]},
                        "2": {"primary": "node-3", "replicas": ["node-1", "node-2"]},
                    },
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return self.config_path

    def spawn(self) -> None:
        (self.data_root / "logs").mkdir(parents=True, exist_ok=True)
        for node in self.nodes():
            (self.data_root / node["id"]).mkdir(parents=True, exist_ok=True)
        (self.data_root / "coordinator").mkdir(parents=True, exist_ok=True)

        env = {
            **os.environ,
            "PYTHONPATH": str(REPO_ROOT / "src"),
            "COORDINATOR_URL": f"http://{self.ips[0]}:9100",
            "NODE_ADVERTISE_HOST": "",  # filled per node below
            "NODE_ADVERTISE_PORT": "",
        }

        # Data nodes first so the coordinator finds leaders.
        for node in self.nodes():
            log_handle = (self.data_root / "logs" / f"{node['host']}.log").open("wb")
            env_node = {
                **env,
                "NODE_ADVERTISE_HOST": node["host"],
                "NODE_ADVERTISE_PORT": str(node["port"]),
            }
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "run_node.py"),
                    "--role", "node",
                    "--node-id", node["id"],
                    "--host", node["host"],
                    "--port", str(node["port"]),
                    "--config", str(self.config_path),
                    "--data-dir", str(self.data_root / node["id"]),
                ],
                env=env_node,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )
            self.processes.append(proc)
            self.log_handles.append(log_handle)

        coord_log = (self.data_root / "logs" / f"{self.ips[0]}.log").open("wb")
        coord_proc = subprocess.Popen(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "run_node.py"),
                "--role", "coordinator",
                "--host", self.ips[0],
                "--port", "9100",
                "--config", str(self.config_path),
                "--data-dir", str(self.data_root / "coordinator"),
            ],
            env={**env},
            stdout=coord_log,
            stderr=subprocess.STDOUT,
        )
        self.processes.append(coord_proc)
        self.log_handles.append(coord_log)

    def wait_healthy(self, *, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        targets = [
            (self.ips[0], 9100, "coordinator"),
            *[(n["host"], n["port"], n["id"]) for n in self.nodes()],
        ]
        pending = list(targets)
        while pending and time.monotonic() < deadline:
            still_pending: list[tuple[str, int, str]] = []
            for host, port, role in pending:
                try:
                    response = httpx.get(
                        f"http://{host}:{port}/health", timeout=1.0,
                    )
                    if response.status_code == 200:
                        continue
                except httpx.HTTPError:
                    pass
                still_pending.append((host, port, role))
            pending = still_pending
            if pending:
                time.sleep(0.25)
        if pending:
            self.dump_logs()
            raise SmokeFailure(
                f"{len(pending)} node(s) not healthy within {timeout:.0f}s: "
                + ", ".join(role for _, _, role in pending),
            )

    def dump_logs(self) -> None:
        for log_path in sorted((self.data_root / "logs").glob("*.log")):
            sys.stderr.write(f"\n=== {log_path.name} ===\n")
            try:
                sys.stderr.write(log_path.read_text(encoding="utf-8", errors="replace"))
            except OSError as error:
                sys.stderr.write(f"(failed to read: {error})\n")

    def stop(self) -> None:
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
        deadline = time.monotonic() + STOP_GRACE_SECONDS
        for proc in self.processes:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                proc.kill()
        for handle in self.log_handles:
            with contextlib_suppress(OSError):
                handle.close()


# Lazy import shim for contextlib to keep top-level imports tidy.
def contextlib_suppress(exc_type: type[BaseException]) -> Any:
    import contextlib

    return contextlib.suppress(exc_type)


# ----------------------------------------------------------------------
# Probe / setup helpers
# ----------------------------------------------------------------------


def probe_alias_ips(prefix: str) -> bool:
    target = f"{prefix}.10"
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((target, 0))
        return True
    except (OSError, PermissionError):
        return False


def alias_setup_instructions(prefix: str) -> str:
    return (
        f"\nLoopback alias IPs not available (tried to bind {prefix}.10).\n"
        "Configure loopback aliases once per machine:\n"
        f"  for ip in {prefix}.{{10..13}}; do\n"
        "    sudo ip addr add $ip/32 dev lo        # Linux\n"
        "    # sudo ifconfig lo0 alias $ip/32      # macOS\n"
        "  done\n"
        "Then re-run the smoke test.\n"
    )


# ----------------------------------------------------------------------
# Verification matrix
# ----------------------------------------------------------------------


def verify_health(cluster: SmokeCluster, report: SmokeReport) -> None:
    for host, port, role in [
        (cluster.ips[0], 9100, "coordinator"),
        *[(n["host"], n["port"], n["id"]) for n in cluster.nodes()],
    ]:
        def check(role: str = role, host: str = host, port: int = port) -> None:
            response = httpx.get(f"http://{host}:{port}/health", timeout=2.0)
            if response.status_code != 200:
                raise SmokeFailure(f"{role} status {response.status_code}")
            payload = response.json()
            if payload.get("ok") is not True:
                raise SmokeFailure(f"{role} ok=false")
            expected_role = "coordinator" if role == "coordinator" else "node"
            if payload.get("role") != expected_role:
                raise SmokeFailure(f"{role} role={payload.get('role')}")

        report.add(f"health[{role}]", check)


def verify_cluster_endpoint_reports_ips(cluster: SmokeCluster, report: SmokeReport) -> None:
    def check() -> None:
        response = httpx.get(
            f"http://{cluster.ips[0]}:9100/cluster", timeout=2.0,
        )
        response.raise_for_status()
        payload = response.json()
        if payload["coordinator"]["host"] != cluster.ips[0]:
            raise SmokeFailure(f"coordinator host={payload['coordinator']['host']}")
        advertised = {n["host"] for n in payload["nodes"]}
        if advertised != set(cluster.ips):
            raise SmokeFailure(f"node hosts={advertised}")

    report.add("cluster_static_ips", check)


def verify_cross_ip_write(cluster: SmokeCluster, report: SmokeReport) -> dict[str, int]:
    """Write a document via coordinator and return {document_id: shard_id}."""
    placements: dict[str, int] = {}

    def write_one(doc_id: str, title: str, body: str) -> None:
        response = httpx.post(
            f"http://{cluster.ips[0]}:9100/collections/smoke/documents",
            json={"id": doc_id, "title": title, "body": body},
            timeout=10.0,
        )
        if response.status_code != 200:
            raise SmokeFailure(f"POST status {response.status_code}: {response.text}")
        shard_id = response.json()["shard_id"]
        placements[doc_id] = shard_id

    def check() -> None:
        write_one("smoke-1", "Cross-IP write 1", "first document")
        write_one("smoke-2", "Cross-IP write 2", "second document")
        write_one("smoke-3", "Cross-IP write 3", "third document")
        if len(placements) != 3:
            raise SmokeFailure("expected 3 placements")

    report.add("write_3_docs", check)
    return placements


def verify_search(cluster: SmokeCluster, report: SmokeReport) -> None:
    def check() -> None:
        response = httpx.get(
            f"http://{cluster.ips[0]}:9100/collections/smoke/documents/search",
            params={"q": "document", "limit": 10},
            timeout=5.0,
        )
        if response.status_code != 200:
            raise SmokeFailure(f"status {response.status_code}")
        hits = response.json()["hits"]
        if len(hits) < 3:
            raise SmokeFailure(f"expected >=3 hits, got {len(hits)}")
        ids = {hit["id"] for hit in hits}
        if not {"smoke-1", "smoke-2", "smoke-3"}.issubset(ids):
            raise SmokeFailure(f"missing ids in {ids}")

    report.add("search_fanout", check)


def verify_replica_direct_read(
    cluster: SmokeCluster,
    report: SmokeReport,
    placements: dict[str, int],
) -> None:
    """Pick one document and read it directly from a non-primary replica."""
    primary_by_shard = {
        0: cluster.nodes()[0],
        1: cluster.nodes()[1],
        2: cluster.nodes()[2],
    }
    replica_by_shard = {
        0: cluster.nodes()[1],  # node-2
        1: cluster.nodes()[2],  # node-3
        2: cluster.nodes()[0],  # node-1
    }

    def check() -> None:
        if not placements:
            raise SmokeFailure("no placements from earlier write check")
        doc_id, shard_id = next(iter(placements.items()))
        replica = replica_by_shard[shard_id]
        primary = primary_by_shard[shard_id]
        response = httpx.get(
            f"http://{replica['host']}:{replica['port']}/internal/shards/{shard_id}"
            f"/collections/smoke/documents/{doc_id}",
            timeout=5.0,
        )
        if response.status_code != 200:
            raise SmokeFailure(
                f"replica {replica['id']} returned {response.status_code}: "
                f"{response.text}",
            )
        if response.json()["id"] != doc_id:
            raise SmokeFailure("replica returned wrong doc")
        # Ensure we actually hit a different node than the primary.
        if replica["host"] == primary["host"]:
            raise SmokeFailure("replica and primary are the same node")

    report.add("replica_direct_read", check)


def verify_raft_convergence(cluster: SmokeCluster, report: SmokeReport) -> None:
    expected_nodes = {"node-1", "node-2", "node-3"}
    deadline = time.monotonic() + 15.0

    def check() -> None:
        while time.monotonic() < deadline:
            response = httpx.get(
                f"http://{cluster.ips[0]}:9100/cluster/raft", timeout=2.0,
            )
            response.raise_for_status()
            shards = response.json()["shards"]
            leaders = {s["leader"] for s in shards.values()}
            members_per_shard = [set(s["members"].keys()) for s in shards.values()]
            if (
                leaders and
                None not in leaders and
                leaders.issubset(expected_nodes) and
                all(m == expected_nodes for m in members_per_shard)
            ):
                return
            time.sleep(0.5)
        raise SmokeFailure(f"raft did not converge: {shards}")

    report.add("raft_3_voters_converge", check)


def verify_heartbeat_discovery(cluster: SmokeCluster, report: SmokeReport) -> None:
    deadline = time.monotonic() + 15.0
    expected = {"node-1", "node-2", "node-3"}
    expected_advertise = {
        "node-1": cluster.ips[1],
        "node-2": cluster.ips[2],
        "node-3": cluster.ips[3],
    }

    def check() -> None:
        while time.monotonic() < deadline:
            response = httpx.get(
                f"http://{cluster.ips[0]}:9100/cluster/nodes", timeout=2.0,
            )
            response.raise_for_status()
            payload = response.json()
            alive_ids = {entry["node_id"] for entry in payload["alive"]}
            if expected.issubset(alive_ids):
                advertised = {
                    entry["node_id"]: entry["host"]
                    for entry in payload["alive"]
                }
                for node_id, host in expected_advertise.items():
                    if advertised.get(node_id) != host:
                        raise SmokeFailure(
                            f"{node_id} advertised {advertised.get(node_id)} "
                            f"(expected {host})",
                        )
                return
            time.sleep(0.5)
        raise SmokeFailure(
            f"alive set {alive_ids} missing some of {expected}",
        )

    report.add("heartbeat_discovery", check)


def verify_node_stop_drops_from_alive(
    cluster: SmokeCluster,
    report: SmokeReport,
) -> None:
    """Kill one data node, verify the alive set shrinks."""
    target = cluster.processes[0]  # first data node
    target.terminate()
    try:
        target.wait(timeout=5.0)
    except subprocess.TimeoutExpired:
        target.kill()

    deadline = time.monotonic() + 15.0
    expected_remaining = {"node-2", "node-3"}

    def check() -> None:
        while time.monotonic() < deadline:
            response = httpx.get(
                f"http://{cluster.ips[0]}:9100/cluster/nodes", timeout=2.0,
            )
            response.raise_for_status()
            alive_ids = {entry["node_id"] for entry in response.json()["alive"]}
            if "node-1" not in alive_ids and expected_remaining.issubset(alive_ids):
                return
            time.sleep(0.5)
        raise SmokeFailure(f"node-1 did not drop from alive set: {alive_ids}")

    report.add("node_stop_drop_from_alive", check)


# ----------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--alias-prefix",
        default=DEFAULT_ALIAS_PREFIX,
        help=f"First 3 octets of alias IPs (default {DEFAULT_ALIAS_PREFIX})",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help=f"Where to write per-process data dirs and logs (default {DEFAULT_DATA_ROOT})",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Assume the cluster is already running; do not spawn or wait.",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Leave data_root intact after teardown (default: delete it).",
    )
    parser.add_argument(
        "--skip-stop-check",
        action="store_true",
        help="Do not run the 'kill one node and verify alive shrinks' check.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not probe_alias_ips(args.alias_prefix):
        sys.stderr.write(alias_setup_instructions(args.alias_prefix))
        return 2

    cluster = SmokeCluster(
        alias_prefix=args.alias_prefix,
        data_root=args.data_root,
        config_path=args.data_root / "cluster_config.json",
    )

    report = SmokeReport()
    try:
        if not args.skip_setup:
            cluster.write_cluster_config()
            cluster.spawn()
            cluster.wait_healthy(timeout=HEALTH_TIMEOUT_SECONDS)

        verify_health(cluster, report)
        verify_cluster_endpoint_reports_ips(cluster, report)
        placements = verify_cross_ip_write(cluster, report)
        verify_search(cluster, report)
        verify_replica_direct_read(cluster, report, placements)
        verify_raft_convergence(cluster, report)
        verify_heartbeat_discovery(cluster, report)
        if not args.skip_stop_check:
            verify_node_stop_drops_from_alive(cluster, report)
    except Exception as error:
        sys.stderr.write(f"\nsmoke test setup failed: {type(error).__name__}: {error}\n")
        cluster.dump_logs()
        cluster.stop()
        return 1
    finally:
        if not args.skip_setup:
            cluster.stop()
            if not args.keep_data and cluster.data_root.exists():
                shutil.rmtree(cluster.data_root, ignore_errors=True)

    report.print()
    return 0 if report.passed() else 1


if __name__ == "__main__":
    raise SystemExit(main())
