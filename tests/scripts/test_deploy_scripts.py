from __future__ import annotations

import json
import shlex
from pathlib import Path

import pytest

from typesense_lite.deploy_plan import (
    DeployContext,
    ShellCommand,
    build_logs_command,
    build_ship_config_commands,
    build_start_commands,
    build_status_commands,
    build_stop_commands,
    build_sync_code_commands,
)
from typesense_lite.inventory import Inventory, InventorySSH


pytestmark = [pytest.mark.unit, pytest.mark.deploy]


def _inventory(**overrides) -> Inventory:
    base: dict = dict(
        ssh_user="ubuntu",
        ssh_options=("-o", "StrictHostKeyChecking=accept-new"),
    )
    base.update(overrides)
    return Inventory(
        ssh=InventorySSH(
            user=base["ssh_user"],
            options=base["ssh_options"],
            identity_file=base.get("identity_file"),
        ),
        coordinator=__import__("typesense_lite.inventory", fromlist=["InventoryHost"]).InventoryHost(
            host="192.168.1.10", port=9100, data_dir="/home/u/data/coordinator",
        ),
        nodes={
            "node-1": __import__("typesense_lite.inventory", fromlist=["InventoryNode"]).InventoryNode(
                id="node-1", host="192.168.1.11", port=9101, data_dir="/home/u/data/node-1",
            ),
            "node-2": __import__("typesense_lite.inventory", fromlist=["InventoryNode"]).InventoryNode(
                id="node-2", host="192.168.1.12", port=9101, data_dir="/home/u/data/node-2",
            ),
            "node-3": __import__("typesense_lite.inventory", fromlist=["InventoryNode"]).InventoryNode(
                id="node-3", host="192.168.1.13", port=9101, data_dir="/home/u/data/node-3",
            ),
        },
        shards={
            0: __import__("typesense_lite.inventory", fromlist=["InventoryShard"]).InventoryShard(
                primary="node-1", replicas=("node-2", "node-3"),
            ),
            1: __import__("typesense_lite.inventory", fromlist=["InventoryShard"]).InventoryShard(
                primary="node-2", replicas=("node-3", "node-1"),
            ),
            2: __import__("typesense_lite.inventory", fromlist=["InventoryShard"]).InventoryShard(
                primary="node-3", replicas=("node-1", "node-2"),
            ),
        },
        shard_count=3,
        remote_project_root="/home/u/typesense-python",
        remote_venv_python="/home/u/typesense-python/.venv/bin/python",
        remote_config_path="/home/u/data/cluster_config.json",
        log_dir="/home/u/data/logs",
    )


def _ctx(inventory: Inventory, **overrides) -> DeployContext:
    return DeployContext(
        ssh_user=inventory.ssh.user,
        ssh_options=tuple(inventory.ssh.options),
        askpass=overrides.get("askpass", False),
        local_simulation=overrides.get("local_simulation", False),
        pull_config=overrides.get("pull_config", False),
        ssh_identity_file=overrides.get("ssh_identity_file"),
    )


def test_ship_config_uses_scp_to_each_host(tmp_path: Path) -> None:
    inventory = _inventory()
    config_path = tmp_path / "cluster_config.json"
    config_path.write_text("{}", encoding="utf-8")

    commands = build_ship_config_commands(inventory, _ctx(inventory), local_config_path=str(config_path))

    assert len(commands) == 4  # coordinator + 3 nodes (deduped via set)
    argv_lists = [c.argv for c in commands]
    targets = sorted(arg[-1] for arg in argv_lists)
    assert targets == sorted(
        f"ubuntu@{h}:{inventory.remote_config_path}"
        for h in ("192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13")
    )
    for cmd in commands:
        assert cmd.argv[0] == "scp"
        assert cmd.argv[1:3] == ("-o", "StrictHostKeyChecking=accept-new")
        assert str(config_path.resolve()) in cmd.argv


def test_ship_config_skipped_in_local_simulation(tmp_path: Path) -> None:
    inventory = _inventory()
    config_path = tmp_path / "cluster_config.json"
    config_path.write_text("{}", encoding="utf-8")

    commands = build_ship_config_commands(
        inventory, _ctx(inventory, local_simulation=True), local_config_path=str(config_path),
    )
    assert commands == []


def test_sync_code_uses_rsync_by_default() -> None:
    inventory = _inventory()
    commands = build_sync_code_commands(
        inventory, _ctx(inventory), local_project_root="/local/repo",
    )

    assert len(commands) == 4
    for cmd in commands:
        assert cmd.argv[0] == "rsync"
        assert "-a" in cmd.argv
        assert "--delete" in cmd.argv
        assert ".venv" in cmd.argv
        # rsync target ends with user@host:remote_root
        target = cmd.argv[-1]
        assert target.startswith("ubuntu@")
        assert target.endswith(":/home/u/typesense-python")


def test_sync_code_uses_git_when_configured() -> None:
    inventory = Inventory(
        **{**_inventory().__dict__, "sync_mode": "git"},
    ) if False else _inventory_with_sync("git")

    commands = build_sync_code_commands(
        inventory, _ctx(inventory), local_project_root="/local/repo",
    )

    for cmd in commands:
        assert cmd.argv[0] == "ssh"
        assert "git pull --ff-only" in cmd.argv[-1]


def _inventory_with_sync(mode: str) -> Inventory:
    inv = _inventory()
    object.__setattr__(inv, "sync_mode", mode)
    return inv


def test_sync_code_skipped_in_local_simulation() -> None:
    inventory = _inventory()
    commands = build_sync_code_commands(
        inventory, _ctx(inventory, local_simulation=True), local_project_root="/local/repo",
    )
    assert commands == []


def _ip_in_argv(argv: tuple[str, ...]) -> str | None:
    """Find the first argv element that looks like an inventory IP."""
    candidates = {"192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13"}
    for arg in argv:
        if arg in candidates:
            return arg
        if arg.startswith("ubuntu@") and arg.removeprefix("ubuntu@") in candidates:
            return arg.removeprefix("ubuntu@")
    return None


def test_start_commands_remote_uses_ssh_per_role() -> None:
    inventory = _inventory()
    commands = build_start_commands(inventory, _ctx(inventory))

    # 2 commands per role (mkdir + start) and 4 roles = 8.
    assert len(commands) == 8

    ssh_argv_lists = [c.argv for c in commands if c.argv[0] == "ssh"]
    assert len(ssh_argv_lists) == 8

    # 4 unique host targets (2 commands per host: mkdir + start).
    targets = sorted({_ip_in_argv(argv) for argv in ssh_argv_lists})
    assert targets == ["192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13"]

    # The mkdir commands target the per-role data dir (separate from the
# start commands, which also embed a `mkdir -p` for the log directory).
    mkdir_cmds = [
        c for c in commands
        if c.argv[0] == "ssh" and "mkdir" in c.argv[-1] and "nohup" not in c.argv[-1]
    ]
    assert len(mkdir_cmds) == 4

    # Each mkdir command creates the data and log dirs, not server.pid itself.
    for cmd in mkdir_cmds:
        joined = cmd.argv[-1]
        assert "mkdir -p" in joined
        assert "server.pid" not in joined


def test_start_commands_local_simulation_spawns_python_directly() -> None:
    inventory = _inventory()
    commands = build_start_commands(
        inventory, _ctx(inventory, local_simulation=True),
    )

    # 4 roles -> 4 commands (no mkdir prefix in local sim).
    assert len(commands) == 4

    for cmd in commands:
        assert cmd.argv[0] == "/home/u/typesense-python/.venv/bin/python"
        assert "--host" in cmd.argv
        # Local sim binds to the inventory host (loopback alias).
        host_index = cmd.argv.index("--host") + 1
        assert cmd.argv[host_index].startswith("192.168.1.")
        # data dir is the per-node dir.
        assert any("/home/u/data" in arg for arg in cmd.argv)

    # Every data-node command advertises the right host/port via env.
    coordinator_env = next(c.env for c in commands if c.env["ROLE"] == "coordinator")
    assert "NODE_ADVERTISE_HOST" not in coordinator_env

    node_envs = [c.env for c in commands if c.env["ROLE"] == "node"]
    assert len(node_envs) == 3
    advertised_hosts = {env["NODE_ADVERTISE_HOST"] for env in node_envs}
    assert advertised_hosts == {"192.168.1.11", "192.168.1.12", "192.168.1.13"}
    assert {env["COORDINATOR_URL"] for env in node_envs} == {"http://192.168.1.10:9100"}


def test_start_commands_ssh_uses_user_at_host() -> None:
    inventory = _inventory()
    commands = build_start_commands(inventory, _ctx(inventory))

    ssh_main_cmds = [c for c in commands if c.argv[0] == "ssh" and "nohup" in c.argv[-1]]
    for cmd in ssh_main_cmds:
        assert cmd.argv[1:3] == ("-o", "StrictHostKeyChecking=accept-new")
        assert any(
            arg.startswith("ubuntu@192.168.1.")
            for arg in cmd.argv
        )


def test_start_commands_pull_config_nodes_fetch_from_coordinator() -> None:
    inventory = _inventory()
    commands = build_start_commands(inventory, _ctx(inventory, pull_config=True))

    start_shells = [c.argv[-1] for c in commands if c.argv[0] == "ssh" and "nohup" in c.argv[-1]]
    coordinator_shell = next(shell for shell in start_shells if "--role coordinator" in shell)
    node_shells = [shell for shell in start_shells if "--role node" in shell]

    assert "--config /home/u/data/cluster_config.json" in coordinator_shell
    assert len(node_shells) == 3
    for shell in node_shells:
        assert "--config" not in shell
        assert "--coordinator-url http://192.168.1.10:9100" in shell
        assert "--advertise-host" in shell
        assert "--advertise-port 9101" in shell


def test_ship_config_can_target_coordinator_only(tmp_path: Path) -> None:
    inventory = _inventory()
    config_path = tmp_path / "cluster_config.json"
    config_path.write_text("{}", encoding="utf-8")

    commands = build_ship_config_commands(
        inventory,
        _ctx(inventory, pull_config=True),
        local_config_path=str(config_path),
        targets=[inventory.coordinator.host],
    )

    assert len(commands) == 1
    assert commands[0].argv[-1] == f"ubuntu@192.168.1.10:{inventory.remote_config_path}"


def test_ssh_identity_file_is_in_ssh_scp_and_rsync_options(tmp_path: Path) -> None:
    inventory = _inventory(identity_file="/home/u/.ssh/deploy_key")
    ctx = _ctx(inventory, ssh_identity_file="/home/u/.ssh/deploy_key")
    config_path = tmp_path / "cluster_config.json"
    config_path.write_text("{}", encoding="utf-8")

    scp_cmd = build_ship_config_commands(
        inventory, ctx, local_config_path=str(config_path),
    )[0]
    rsync_cmd = build_sync_code_commands(
        inventory, ctx, local_project_root="/local/repo",
    )[0]
    ssh_cmd = build_start_commands(inventory, ctx)[0]

    assert scp_cmd.argv[1:3] == ("-i", "/home/u/.ssh/deploy_key")
    assert "-i /home/u/.ssh/deploy_key" in rsync_cmd.argv[rsync_cmd.argv.index("-e") + 1]
    assert ssh_cmd.argv[1:3] == ("-i", "/home/u/.ssh/deploy_key")


    inventory = _inventory()
    commands = build_stop_commands(inventory, _ctx(inventory))

    assert len(commands) == 4
    for cmd in commands:
        assert cmd.argv[0] == "ssh"
        joined = " ".join(cmd.argv)
        assert "kill" in joined
        assert "server.pid" in joined


def test_stop_commands_local_simulation_use_bash_c() -> None:
    inventory = _inventory()
    commands = build_stop_commands(
        inventory, _ctx(inventory, local_simulation=True),
    )

    for cmd in commands:
        assert cmd.argv[0] == "bash"
        assert cmd.argv[1] == "-c"
        joined = " ".join(cmd.argv)
        assert "kill" in joined
        assert "server.pid" in joined


def test_status_commands_probe_health() -> None:
    inventory = _inventory()
    commands = build_status_commands(inventory, _ctx(inventory))

    assert len(commands) == 4
    for cmd in commands:
        assert cmd.argv[0] == "ssh"
        joined = " ".join(cmd.argv)
        assert "/health" in joined
        assert "server.pid" in joined


def test_status_commands_local_simulation_uses_local_bash() -> None:
    inventory = _inventory()
    commands = build_status_commands(
        inventory, _ctx(inventory, local_simulation=True),
    )

    for cmd in commands:
        assert cmd.argv[0] == "bash"
        joined = " ".join(cmd.argv)
        assert "/health" in joined


def test_logs_command_filters_by_host() -> None:
    inventory = _inventory()
    commands = build_logs_command(inventory, _ctx(inventory), host="192.168.1.11")

    assert len(commands) == 1
    assert commands[0].argv[0] == "ssh"
    joined = " ".join(commands[0].argv)
    assert "192.168.1.11" in joined
    assert "tail" in joined
    assert "-F" in joined


def test_logs_command_all_hosts() -> None:
    inventory = _inventory()
    commands = build_logs_command(inventory, _ctx(inventory))

    assert len(commands) == 4
    hosts = {_ip_in_argv(c.argv) for c in commands}
    assert hosts == {"192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13"}


def test_deploy_context_askpass_adds_ssh_options() -> None:
    inv = _inventory()
    ctx = _ctx(inv, askpass=True)

    opts = ctx.ssh_prefix()
    assert "-o" in opts
    assert "BatchMode=no" in opts
    assert "NumberOfPasswordPrompts=1" in opts


def test_shell_command_render_quotes_arguments() -> None:
    cmd = ShellCommand(
        argv=("echo", "hello world"),
        description="say hi",
    )
    rendered = cmd.render()
    assert rendered.startswith("# say hi")
    assert shlex.quote("hello world") in rendered


def test_shell_command_rejects_empty_argv() -> None:
    with pytest.raises(ValueError):
        ShellCommand(argv=())


def test_inventory_examples_parse() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    example = repo_root / "scripts" / "inventory.example.json"
    local_sim = repo_root / "scripts" / "inventory.local_sim.example.json"

    parsed = Inventory.from_file(example)
    assert parsed.ssh.user == "ubuntu"
    assert parsed.coordinator.host == "192.168.1.10"
    assert parsed.shard_count == 3

    parsed_sim = Inventory.from_file(local_sim)
    assert parsed_sim.local_simulation is True
    assert parsed_sim.ssh.user == ""


def test_examples_match_documented_schema(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    for name in ("inventory.example.json", "inventory.local_sim.example.json"):
        path = repo_root / "scripts" / name
        data = json.loads(path.read_text(encoding="utf-8"))
        # Required keys exist.
        assert {"coordinator", "nodes", "shards", "ssh"}.issubset(data)
        # shard_count matches.
        assert data["shard_count"] == len(data["shards"])
        # All shard node ids exist in nodes.
        node_ids = {n["id"] for n in data["nodes"]}
        for shard in data["shards"].values():
            assert shard["primary"] in node_ids
            for replica in shard["replicas"]:
                assert replica in node_ids