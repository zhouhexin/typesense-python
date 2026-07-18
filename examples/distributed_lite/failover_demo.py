"""Demonstrate a real Raft leader failover on the local demo cluster."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from demo_utils import (
    DEFAULT_CONFIG,
    DEFAULT_COORDINATOR,
    choose_member,
    document_id_for_shard,
    find_pid_by_port,
    get_json,
    kill_process,
    load_config,
    post_json,
    wait_for_new_leader,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_COORDINATOR)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--collection", default="books")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    config = load_config(Path(args.config).resolve())
    ports = {node["id"]: int(node["port"]) for node in config["nodes"]}
    shard_id, leader_id = choose_member(get_json(f"{base_url}/cluster/raft"), leader=True)
    pid = find_pid_by_port(ports[leader_id])
    print(f"target shard={shard_id} leader={leader_id} pid={pid}")
    if not args.yes:
        print("dry run; pass --yes to terminate the leader")
        return 0

    kill_process(pid)
    new_leader = wait_for_new_leader(base_url, shard_id, leader_id, args.timeout)
    document_id = document_id_for_shard(
        shard_id, int(config["shard_count"]), "failover-demo"
    )
    result = post_json(
        f"{base_url}/collections/{args.collection}/documents",
        {
            "id": document_id,
            "title": "Raft failover",
            "body": "write committed after leader failover",
        },
    )
    search = get_json(
        f"{base_url}/collections/{args.collection}/documents/search",
        params={"q": "leader failover"},
    )
    if document_id not in {hit.get("id") for hit in search.get("hits", [])}:
        raise RuntimeError("document written after failover was not searchable")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"PASS old_leader={leader_id} new_leader={new_leader}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
