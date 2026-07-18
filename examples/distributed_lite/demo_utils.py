"""Shared helpers for the local Typesense Lite recovery demos."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

import httpx

from typesense_lite.run_node import RunNodeSpec, spawn_process

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = Path(__file__).with_name("cluster_config.json")
DEFAULT_DATA_DIR = ROOT / ".data" / "typesense_lite"
DEFAULT_COORDINATOR = "http://127.0.0.1:9100"


def get_json(url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = httpx.get(url, params=params, timeout=5.0, trust_env=False)
    response.raise_for_status()
    return response.json()


def post_json(url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    response = httpx.post(url, json=payload, timeout=10.0, trust_env=False)
    response.raise_for_status()
    return response.json()


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def choose_member(
    raft: dict[str, Any], *, leader: bool
) -> tuple[int, str]:
    for shard_id, shard in raft.get("shards", {}).items():
        leader_id = shard.get("leader")
        if leader and leader_id:
            return int(shard_id), str(leader_id)
        for node_id, member in shard.get("members", {}).items():
            if node_id != leader_id and member.get("role") == "follower":
                return int(shard_id), node_id
    raise RuntimeError("no matching Raft member found")


def find_pid_by_port(port: int) -> int:
    result = subprocess.run(
        ["lsof", "-ti", f":{port}"],
        text=True,
        capture_output=True,
        check=False,
    )
    if not result.stdout.strip():
        raise RuntimeError(f"no process is listening on port {port}")
    return int(result.stdout.splitlines()[0])


def kill_process(pid: int) -> None:
    os.kill(pid, signal.SIGKILL)


def start_node(
    *, node_id: str, port: int, config_path: Path, data_dir: Path
) -> subprocess.Popen[bytes]:
    spec = RunNodeSpec(
        role="node",
        host="127.0.0.1",
        port=port,
        config=str(config_path),
        data_dir=str(data_dir),
        node_id=node_id,
    )
    src = str(ROOT / "src")
    existing = os.environ.get("PYTHONPATH")
    pythonpath = os.pathsep.join([src, existing]) if existing else src
    return spawn_process(spec, base_env={"PYTHONPATH": pythonpath}, cwd=ROOT)


def wait_until(
    predicate: Callable[[], Any], *, timeout: float, description: str
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (httpx.HTTPError, KeyError, TypeError) as error:
            last_error = error
        time.sleep(0.2)
    suffix = f": {last_error}" if last_error else ""
    raise RuntimeError(f"timed out waiting for {description}{suffix}")


def wait_for_new_leader(
    base_url: str, shard_id: int, old_leader: str, timeout: float
) -> str:
    def probe() -> str | None:
        leader = get_json(f"{base_url}/cluster/raft")["shards"][str(shard_id)][
            "leader"
        ]
        return leader if leader and leader != old_leader else None

    return str(wait_until(probe, timeout=timeout, description="new Raft leader"))


def wait_for_ready(node_url: str, shard_id: int, timeout: float) -> dict[str, Any]:
    def probe() -> dict[str, Any] | None:
        payload = get_json(f"{node_url}/internal/recovery/state")
        shard = payload.get("shards", {}).get(str(shard_id), {})
        return payload if shard.get("ready") is True else None

    return wait_until(probe, timeout=timeout, description="node recovery")


def document_id_for_shard(shard_id: int, shard_count: int, prefix: str) -> str:
    for suffix in range(10000):
        candidate = f"{prefix}-{int(time.time())}-{suffix}"
        digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
        if int(digest, 16) % shard_count == shard_id:
            return candidate
    raise RuntimeError(f"unable to map a document id to shard {shard_id}")
