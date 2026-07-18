"""Restart one failed local data node and wait for Raft catch-up."""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx

from demo_utils import (
    DEFAULT_CONFIG,
    DEFAULT_DATA_DIR,
    get_json,
    load_config,
    start_node,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-id", required=True, help="Node to recover, for example node-1.")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    data_dir = Path(args.data_dir).resolve()
    config = load_config(config_path)
    node = _find_node(config, args.node_id)
    node_url = f"http://{node['host']}:{int(node['port'])}"

    existing = _probe_health(node_url)
    if existing is not None:
        if existing.get("node_id") == args.node_id and existing.get("ok") is True:
            print(f"{args.node_id} is already running at {node_url}")
            return 0
        raise RuntimeError(
            f"{node_url} is occupied by another service; refusing to start {args.node_id}"
        )

    print(f"Starting {args.node_id} at {node_url}")
    print(f"Reusing data directory: {data_dir}")
    process = start_node(
        node_id=args.node_id,
        port=int(node["port"]),
        config_path=config_path,
        data_dir=data_dir,
    )

    try:
        recovery = _wait_for_recovery(
            node_url=node_url,
            node_id=args.node_id,
            process=process,
            timeout=args.timeout,
        )
    except KeyboardInterrupt:
        _stop_process(process)
        return 130
    except Exception as error:
        _stop_process(process)
        print(f"Recovery failed: {error}")
        return 1

    _print_recovery(recovery)
    print(f"{args.node_id} recovered and is serving at {node_url}")
    print("Keep this terminal open. Press Ctrl-C to stop the recovered node.")
    try:
        return process.wait()
    except KeyboardInterrupt:
        _stop_process(process)
        return 0


def _find_node(config: dict[str, Any], node_id: str) -> dict[str, Any]:
    for node in config.get("nodes", []):
        if node.get("id") == node_id:
            return node
    available = ", ".join(str(node.get("id")) for node in config.get("nodes", []))
    raise ValueError(f"unknown node {node_id!r}; available nodes: {available}")


def _probe_health(node_url: str) -> dict[str, Any] | None:
    try:
        return get_json(f"{node_url}/health")
    except httpx.HTTPError:
        return None


def _wait_for_recovery(
    *,
    node_url: str,
    node_id: str,
    process: subprocess.Popen[bytes],
    timeout: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_status = "starting"
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(f"node process exited with code {exit_code}")
        try:
            recovery = get_json(f"{node_url}/internal/recovery/state")
            if recovery.get("node_id") != node_id:
                raise RuntimeError(
                    f"expected {node_id}, got {recovery.get('node_id')!r} from recovery API"
                )
            last_status = str(recovery.get("status", "starting"))
            if last_status == "failed":
                raise RuntimeError(str(recovery.get("error") or "node recovery failed"))
            if recovery.get("ready") is True and last_status == "healthy":
                return recovery
        except httpx.HTTPError as error:
            last_error = error
        time.sleep(0.2)

    detail = f"; last request error: {last_error}" if last_error else ""
    raise RuntimeError(
        f"timed out after {timeout:g}s (last status: {last_status}){detail}"
    )


def _print_recovery(recovery: dict[str, Any]) -> None:
    for shard_id, shard in sorted(
        recovery.get("shards", {}).items(), key=lambda item: int(item[0])
    ):
        print(
            "  shard={shard} status={status} role={role} leader={leader} "
            "last_log={last_log} commit={commit} applied={applied}".format(
                shard=shard_id,
                status=shard.get("status"),
                role=shard.get("role"),
                leader=shard.get("leader_id"),
                last_log=shard.get("local_last_log_index"),
                commit=shard.get("local_commit_index"),
                applied=shard.get("last_applied"),
            )
        )


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
