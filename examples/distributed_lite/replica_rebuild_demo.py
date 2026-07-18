"""Trigger and verify a leader-aware snapshot rebuild for one follower."""

from __future__ import annotations

import argparse
import json

from demo_utils import (
    DEFAULT_COORDINATOR,
    choose_member,
    get_json,
    post_json,
    wait_for_ready,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_COORDINATOR)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--yes", action="store_true")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    raft = get_json(f"{base_url}/cluster/raft")
    shard_id, follower_id = choose_member(raft, leader=False)
    member = raft["shards"][str(shard_id)]["members"][follower_id]
    print(f"target shard={shard_id} follower={follower_id}")
    if not args.yes:
        print("dry run; pass --yes to rebuild this follower")
        return 0

    result = post_json(
        f"{base_url}/cluster/shards/{shard_id}/members/{follower_id}/rebuild"
    )
    cluster = get_json(f"{base_url}/cluster")
    node = next(item for item in cluster["nodes"] if item["id"] == follower_id)
    node_url = f"http://{node['host']}:{node['port']}"
    recovery = wait_for_ready(node_url, shard_id, args.timeout)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(json.dumps(recovery, indent=2, ensure_ascii=False))
    print(f"PASS previous_status={member.get('recovery_status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
