"""Pure command generation for ``scripts/deploy_cluster.py``.

The functions in this module build ``ShellCommand`` objects describing the
shell commands the deploy script wants to run, but do not execute anything.
Tests assert the exact argv list to guarantee SSH / scp / rsync invocations
match expectations across platforms.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from .inventory import Inventory  # noqa: F401


@dataclass(frozen=True)
class ShellCommand:
    """One shell command to be executed by the deploy driver."""

    argv: tuple[str, ...]
    env: Mapping[str, str] = field(default_factory=dict)
    description: str = ""

    def __post_init__(self) -> None:
        if not self.argv:
            raise ValueError("ShellCommand.argv must be non-empty")

    def render(self) -> str:
        rendered = " ".join(shlex.quote(part) for part in self.argv)
        if self.description:
            return f"# {self.description}\n{rendered}"
        return rendered


@dataclass(frozen=True)
class DeployContext:
    """Knobs that influence command generation without changing semantics."""

    ssh_user: str
    ssh_options: tuple[str, ...]
    askpass: bool = False
    local_simulation: bool = False
    pull_config: bool = False
    ssh_identity_file: str | None = None
    local_config_source: str | None = None  # path to the generated config file

    def ssh_prefix(self) -> tuple[str, ...]:
        """Return the argv prefix to use when invoking ssh/scp/rsync."""
        opts: list[str] = []
        if self.askpass:
            opts.extend(["-o", "BatchMode=no", "-o", "NumberOfPasswordPrompts=1"])
        if self.ssh_identity_file:
            opts.extend(["-i", self.ssh_identity_file])
        opts.extend(self.ssh_options)
        return tuple(opts)


def build_ship_config_commands(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    local_config_path: str,
    targets: Sequence[str] | None = None,
) -> list[ShellCommand]:
    """scp the local cluster_config.json to target hosts."""
    if ctx.local_simulation:
        return []
    source = Path(local_config_path).resolve()
    cmds: list[ShellCommand] = []
    target_hosts = list(targets) if targets is not None else _remote_targets(inventory)
    for host in target_hosts:
        remote = f"{_ssh_target_arg(ctx, host)}:{inventory.remote_config_path}"
        ssh_opts = ctx.ssh_prefix()
        argv = ("scp", *ssh_opts, str(source), remote)
        cmds.append(
            ShellCommand(
                argv=argv,
                description=f"ship cluster config to {host}",
            ),
        )
    return cmds


def build_sync_code_commands(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    local_project_root: str,
) -> list[ShellCommand]:
    """rsync (default) or git pull the project tree onto each remote host."""
    if ctx.local_simulation:
        return []
    cmds: list[ShellCommand] = []
    ssh_opts = ctx.ssh_prefix()
    targets = _remote_targets(inventory)
    for host in targets:
        remote_target = f"{_ssh_target_arg(ctx, host)}:{inventory.remote_project_root}"
        if inventory.sync_mode == "rsync":
            argv = (
                "rsync",
                "-a",
                "--delete",
                "--exclude", ".venv",
                "--exclude", ".data",
                "--exclude", "__pycache__",
                "-e", _ssh_transport(ssh_opts),
                f"{local_project_root.rstrip('/')}/",
                remote_target,
            )
        elif inventory.sync_mode == "git":
            argv = (
                "ssh", *ssh_opts, _ssh_target_arg(ctx, host),
                f"cd {shlex.quote(inventory.remote_project_root)} && git pull --ff-only",
            )
        else:
            raise ValueError(f"unknown sync_mode {inventory.sync_mode!r}")
        cmds.append(
            ShellCommand(
                argv=argv,
                description=f"sync project tree to {host} ({inventory.sync_mode})",
            ),
        )
    return cmds


def build_start_commands(
    inventory: Inventory,
    ctx: DeployContext,
) -> list[ShellCommand]:
    """Return the commands that start coordinator + every data node."""
    cmds: list[ShellCommand] = []
    cmds.extend(_start_command_for_host(inventory, ctx, role="coordinator"))
    for node_id in inventory.nodes:
        cmds.extend(_start_command_for_host(inventory, ctx, role="node", node_id=node_id))
    return cmds


def build_stop_commands(
    inventory: Inventory,
    ctx: DeployContext,
) -> list[ShellCommand]:
    """Return commands that stop coordinator + every data node."""
    cmds: list[ShellCommand] = []
    cmds.extend(_stop_command_for_host(inventory, ctx, role="coordinator"))
    for node_id in inventory.nodes:
        cmds.extend(_stop_command_for_host(inventory, ctx, role="node", node_id=node_id))
    return cmds


def build_status_commands(
    inventory: Inventory,
    ctx: DeployContext,
) -> list[ShellCommand]:
    """Return commands that probe /health on every host.

    Each command is a curl over the local PID file plus a /health probe.
    For simplicity we always issue a curl; the executor can interpret the
    output to decide up/down.
    """
    cmds: list[ShellCommand] = []
    cmds.extend(_status_command_for_host(inventory, ctx, role="coordinator"))
    for node_id in inventory.nodes:
        cmds.extend(_status_command_for_host(inventory, ctx, role="node", node_id=node_id))
    return cmds


def build_logs_command(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    host: str | None = None,
) -> list[ShellCommand]:
    """Return commands that tail the log file(s) for the given host(s)."""
    cmds: list[ShellCommand] = []
    ssh_opts = ctx.ssh_prefix()
    targets = [host] if host else _remote_targets(inventory)
    for h in targets:
        log_path = f"{_remote_log_dir(inventory)}/{h}.log"
        if ctx.local_simulation:
            argv = ("tail", "-F", f"{inventory.log_dir}/{h}.log")
        else:
            argv = (
                "ssh", *ssh_opts, _ssh_target_arg(ctx, h),
                "tail", "-F", log_path,
            )
        cmds.append(ShellCommand(argv=argv, description=f"tail logs on {h}"))
    return cmds


# ---------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------


def _remote_targets(inventory: Inventory) -> list[str]:
    hosts = sorted({inventory.coordinator.host, *(n.host for n in inventory.nodes.values())})
    return hosts


def _ssh_user_at(ctx: DeployContext) -> str:
    return ctx.ssh_user or ""



def _ssh_target_arg(ctx: DeployContext, host: str) -> str:
    """Return ``user@host`` or just ``host`` for ssh/scp/rsync targets."""
    return f"{_ssh_user_at(ctx)}@{host}" if _ssh_user_at(ctx) else host


def _ssh_transport(ssh_opts: Sequence[str]) -> str:
    """Compose ``ssh`` invocation used as rsync transport."""
    return "ssh " + " ".join(shlex.quote(opt) for opt in ssh_opts)


def _remote_log_dir(inventory: Inventory) -> str:
    """Co-locate logs next to the data directory on each remote host."""
    return f"{inventory.coordinator.data_dir.rsplit('/', 1)[0]}/logs"


def _start_command_for_host(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    role: str,
    node_id: str | None = None,
) -> list[ShellCommand]:
    if role == "coordinator":
        host = inventory.coordinator.host
        port = inventory.coordinator.port
        data_dir = inventory.coordinator.data_dir
        log_file = f"{inventory.log_dir}/{host}.log"
        env_overrides: dict[str, str] = {"ROLE": "coordinator"}
        include_config = True
    else:
        assert node_id is not None
        node = inventory.nodes[node_id]
        host = node.host
        port = node.port
        data_dir = node.data_dir
        log_file = f"{inventory.log_dir}/{host}.log"
        env_overrides = {
            "ROLE": "node",
            "NODE_ID": node_id,
            "COORDINATOR_URL": inventory.coordinator_url_value(),
        }
        include_config = not ctx.pull_config

    if include_config:
        env_overrides["CLUSTER_CONFIG"] = inventory.remote_config_path
    env_overrides["DATA_DIR"] = data_dir
    env_overrides["HOST"] = "0.0.0.0"
    env_overrides["PORT"] = str(port)
    if role == "node":
        env_overrides["NODE_ADVERTISE_HOST"] = host
        env_overrides["NODE_ADVERTISE_PORT"] = str(port)

    pid_file = f"{data_dir}/server.pid"
    log_dir = Path(log_file).parent
    inner_python = inventory.remote_venv_python
    remote_cmd_parts = [
        f"{inner_python}",
        f"{inventory.remote_project_root}/scripts/run_node.py",
        "--role", env_overrides["ROLE"],
        "--host", "0.0.0.0" if not ctx.local_simulation else host,
        "--port", str(port),
        "--data-dir", data_dir,
    ]
    if include_config:
        remote_cmd_parts.extend(["--config", inventory.remote_config_path])
    if "NODE_ID" in env_overrides:
        remote_cmd_parts.extend(["--node-id", env_overrides["NODE_ID"]])
    if "COORDINATOR_URL" in env_overrides:
        remote_cmd_parts.extend(["--coordinator-url", env_overrides["COORDINATOR_URL"]])
    if "NODE_ADVERTISE_HOST" in env_overrides:
        remote_cmd_parts.extend(["--advertise-host", env_overrides["NODE_ADVERTISE_HOST"]])
    if "NODE_ADVERTISE_PORT" in env_overrides:
        remote_cmd_parts.extend(["--advertise-port", env_overrides["NODE_ADVERTISE_PORT"]])

    remote_cmd = " ".join(shlex.quote(part) for part in remote_cmd_parts)

    if ctx.local_simulation:
        cmd = ShellCommand(
            argv=tuple(remote_cmd_parts),
            env=env_overrides,
            description=f"start {role}{':' + node_id if node_id else ''} locally on {host}",
        )
        return [cmd]

    ssh_opts = ctx.ssh_prefix()
    pid_argv = (
        "ssh", *ssh_opts, _ssh_target_arg(ctx, host),
        f"mkdir -p {shlex.quote(data_dir)} {shlex.quote(str(log_dir))}",
    )
    pid_cmd = ShellCommand(
        argv=pid_argv,
        description=f"ensure pid dir on {host}",
    )

    argv = (
        "ssh", *ssh_opts, _ssh_target_arg(ctx, host),
        f"nohup {remote_cmd} > {shlex.quote(log_file)} 2>&1 & echo $! > {shlex.quote(pid_file)}",
    )
    main_cmd = ShellCommand(
        argv=argv,
        env=env_overrides,
        description=f"start {role}{':' + node_id if node_id else ''} on {host}",
    )
    return [pid_cmd, main_cmd]


def _stop_command_for_host(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    role: str,
    node_id: str | None = None,
) -> list[ShellCommand]:
    if role == "coordinator":
        host = inventory.coordinator.host
        data_dir = inventory.coordinator.data_dir
    else:
        assert node_id is not None
        node = inventory.nodes[node_id]
        host = node.host
        data_dir = node.data_dir
    pid_file = f"{data_dir}/server.pid"

    if ctx.local_simulation:
        argv = ("bash", "-c", f"if [ -f {shlex.quote(pid_file)} ]; then kill $(cat {shlex.quote(pid_file)}) && rm {shlex.quote(pid_file)}; fi")
        return [ShellCommand(argv=argv, description=f"stop {role} on {host}")]

    ssh_opts = ctx.ssh_prefix()
    argv = (
        "ssh", *ssh_opts, _ssh_target_arg(ctx, host),
        f"if [ -f {shlex.quote(pid_file)} ]; then kill $(cat {shlex.quote(pid_file)}) && rm {shlex.quote(pid_file)}; fi",
    )
    return [ShellCommand(argv=argv, description=f"stop {role} on {host}")]


def _status_command_for_host(
    inventory: Inventory,
    ctx: DeployContext,
    *,
    role: str,
    node_id: str | None = None,
) -> list[ShellCommand]:
    if role == "coordinator":
        host = inventory.coordinator.host
        port = inventory.coordinator.port
        data_dir = inventory.coordinator.data_dir
    else:
        assert node_id is not None
        node = inventory.nodes[node_id]
        host = node.host
        port = node.port
        data_dir = node.data_dir
    pid_file = f"{data_dir}/server.pid"
    health_url = f"http://{host}:{port}/health"

    if ctx.local_simulation:
        argv = (
            "bash", "-c",
            f"pid=$(cat {shlex.quote(pid_file)} 2>/dev/null) && "
            f"curl -sSf -o /dev/null -w '%{{http_code}}' {shlex.quote(health_url)} || echo DOWN",
        )
        return [ShellCommand(argv=argv, description=f"status {role} on {host}")]

    ssh_opts = ctx.ssh_prefix()
    argv = (
        "ssh", *ssh_opts, _ssh_target_arg(ctx, host),
        f"pid=$(cat {shlex.quote(pid_file)} 2>/dev/null); "
        f"if [ -z \"$pid\" ]; then echo DOWN; else "
        f"code=$(curl -sS -o /dev/null -w '%{{http_code}}' {shlex.quote(health_url)}); "
        f"echo $pid:$code; fi",
    )
    return [ShellCommand(argv=argv, description=f"status {role} on {host}")]