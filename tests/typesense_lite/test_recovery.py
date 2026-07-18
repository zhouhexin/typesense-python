from typesense_lite.recovery import NodeRecoveryState, node_is_ready, shard_is_ready


def test_node_recovery_state_aggregates_shard_readiness() -> None:
    state = NodeRecoveryState("node-1")
    state.set_shard(0, {"status": "healthy", "ready": True})
    state.set_shard(1, {"status": "catching_up", "ready": False})

    payload = state.to_dict()

    assert payload["status"] == "catching_up"
    assert payload["ready"] is False
    assert shard_is_ready(payload["shards"]["0"]) is True
    assert shard_is_ready(payload["shards"]["1"]) is False
    assert node_is_ready(payload) is False


def test_node_recovery_state_reports_failure() -> None:
    state = NodeRecoveryState("node-1")
    state.set_shard(0, {"status": "healthy", "ready": True})

    state.fail_shard(0, "disk read failed")

    payload = state.to_dict()
    assert payload["status"] == "failed"
    assert payload["ready"] is False
    assert payload["error"] == "disk read failed"
