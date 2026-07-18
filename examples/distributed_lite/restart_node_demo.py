"""Demonstrate follower restart and automatic Raft log catch-up."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from demo_utils import (
    DEFAULT_CONFIG,
    DEFAULT_COORDINATOR,
    DEFAULT_DATA_DIR,
    choose_member,
    document_id_for_shard,
    find_pid_by_port,
    get_json,
    kill_process,
    load_config,
    post_json,
    start_node,
    wait_for_ready,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_COORDINATOR)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--collection", default="books")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    ports = {node["id"]: int(node["port"]) for node in config["nodes"]}
    shard_id, follower_id = choose_member(
        get_json(f"{base_url}/cluster/raft"), leader=False
    )
    pid = find_pid_by_port(ports[follower_id])
    print(f"target shard={shard_id} follower={follower_id} pid={pid}")
    if not args.yes:
        print("dry run; pass --yes to restart the follower")
        return 0

    kill_process(pid)
    document_id = document_id_for_shard(
        shard_id, int(config["shard_count"]), "restart-demo"
    )
    post_json(
        f"{base_url}/collections/{args.collection}/documents",
        {
            "id": document_id,
            "title": "Follower restart",
            "body": "committed while follower was offline",
        },
    )
    process = start_node(
        node_id=follower_id,
        port=ports[follower_id],
        config_path=config_path,
        data_dir=Path(args.data_dir).resolve(),
    )
    node_url = f"http://127.0.0.1:{ports[follower_id]}"
    recovery = wait_for_ready(node_url, shard_id, args.timeout)
    document = get_json(
        f"{node_url}/internal/shards/{shard_id}/collections/"
        f"{args.collection}/documents/{document_id}"
    )
    print(json.dumps(recovery, indent=2, ensure_ascii=False))
    print(f"PASS pid={process.pid} recovered_document={document['id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
