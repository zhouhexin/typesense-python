"""Multi-machine deployment driver for Typesense Lite.

Subcommands
===========

* ``deploy``  -- generate the cluster_config.json, ship it, sync the project
  tree, and start the coordinator + every data node on its assigned host.
* ``status``  -- show whether each role is running and whether ``/health``
  responds 200.
* ``stop``    -- terminate every role and clean up PID files.
* ``logs``    -- tail the on-disk log file for one or every host.

Common flags
============

* ``--inventory PATH``  path to the JSON inventory (see ``inventory.example.json``).
* ``--dry-run``         print the generated shell commands without executing.
* ``--askpass``         enable interactive password prompts (uses
  ``SSH_ASKPASS`` if installed, otherwise ``BatchMode=no``).
* ``--pull-config``     emit ``COORDINATOR_URL`` instead of shipping the
  config file (data nodes will fetch it from the coordinator at startup).
* ``--local-sim``       skip ssh/scp/rsync entirely; fork the process tree on
  the local machine, bound to the inventory-defined IPs. Used together with
  loopback alias IPs to simulate a multi-machine cluster on one host.

The pure command-generation lives in :mod:`typesense_lite.deploy_plan`;
this script is just the CLI wrapper.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

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
from typesense_lite.inventory import Inventory


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_OUT = ROOT / ".data" / "deploy" / "cluster_config.json"


def _build_context(
    inventory: Inventory,
    *,
    askpass: bool,
    local_sim: bool,
    pull_config: bool = False,
) -> DeployContext:
    return DeployContext(
        ssh_user=inventory.ssh.user,
        ssh_options=tuple(inventory.ssh.options),
        askpass=askpass,
        local_simulation=local_sim,
        pull_config=pull_config,
        ssh_identity_file=(
            str(Path(inventory.ssh.identity_file).expanduser())
            if inventory.ssh.identity_file
            else None
        ),
    )


def _emit(commands: list[ShellCommand], *, dry_run: bool) -> int:
    rc = 0
    for command in commands:
        print(command.render())
        if dry_run:
            continue
        env = os.environ.copy()
        env.update(command.env)
        result = subprocess.run(command.argv, env=env)
        if result.returncode != 0:
            rc = result.returncode
            break
    return rc


def _ensure_local_config(inventory: Inventory, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(inventory.to_cluster_config(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def cmd_deploy(args: argparse.Namespace) -> int:
    inventory = Inventory.from_file(args.inventory)
    ctx = _build_context(
        inventory,
        askpass=args.askpass,
        local_sim=args.local_sim,
        pull_config=args.pull_config,
    )

    config_path = Path(args.config_out)
    _ensure_local_config(inventory, config_path)

    commands: list[ShellCommand] = []

    if args.pull_config:
        commands.extend(
            build_ship_config_commands(
                inventory,
                ctx,
                local_config_path=str(config_path),
                targets=[inventory.coordinator.host],
            ),
        )
    else:
        commands.extend(
            build_ship_config_commands(
                inventory, ctx, local_config_path=str(config_path),
            ),
        )

    if not args.local_sim:
        commands.extend(
            build_sync_code_commands(
                inventory, ctx, local_project_root=str(ROOT),
            ),
        )

    commands.extend(build_start_commands(inventory, ctx))

    rc = _emit(commands, dry_run=args.dry_run)
    if rc != 0 or args.dry_run:
        return rc

    # Best-effort wait for coordinator /health (no-op in dry-run).
    deadline = time.monotonic() + args.timeout
    coordinator_url = inventory.coordinator_url_value()
    while time.monotonic() < deadline:
        try:
            response = subprocess.run(
                ["curl", "-sSf", "-o", "/dev/null", f"{coordinator_url}/health"],
                check=False,
                timeout=2,
            )
        except subprocess.TimeoutExpired:
            response = None
        if response is not None and response.returncode == 0:
            print(f"coordinator healthy at {coordinator_url}")
            return 0
        time.sleep(1)
    print(f"timed out waiting for coordinator at {coordinator_url}", file=sys.stderr)
    return 1


def cmd_status(args: argparse.Namespace) -> int:
    inventory = Inventory.from_file(args.inventory)
    ctx = _build_context(inventory, askpass=args.askpass, local_sim=args.local_sim)
    commands = build_status_commands(inventory, ctx)
    return _emit(commands, dry_run=args.dry_run)


def cmd_stop(args: argparse.Namespace) -> int:
    inventory = Inventory.from_file(args.inventory)
    ctx = _build_context(inventory, askpass=args.askpass, local_sim=args.local_sim)
    return _emit(build_stop_commands(inventory, ctx), dry_run=args.dry_run)


def cmd_logs(args: argparse.Namespace) -> int:
    inventory = Inventory.from_file(args.inventory)
    ctx = _build_context(inventory, askpass=args.askpass, local_sim=args.local_sim)
    commands = build_logs_command(inventory, ctx, host=args.host)
    return _emit(commands, dry_run=args.dry_run)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", required=True, help="Path to inventory JSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--askpass", action="store_true")
    parser.add_argument(
        "--pull-config",
        action="store_true",
        help="Skip scp; nodes will fetch config from coordinator at startup.",
    )
    parser.add_argument(
        "--local-sim",
        action="store_true",
        help="Skip ssh/scp/rsync; fork processes locally bound to inventory IPs.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    deploy = subparsers.add_parser("deploy", help="deploy the cluster")
    deploy.add_argument(
        "--config-out",
        default=str(DEFAULT_CONFIG_OUT),
        help="Where to write the generated cluster_config.json",
    )
    deploy.add_argument(
        "--timeout", type=int, default=30,
        help="Seconds to wait for coordinator /health after deploy",
    )
    deploy.set_defaults(func=cmd_deploy)

    status = subparsers.add_parser("status", help="show process and /health status")
    status.set_defaults(func=cmd_status)

    stop = subparsers.add_parser("stop", help="terminate every role")
    stop.set_defaults(func=cmd_stop)

    logs = subparsers.add_parser("logs", help="tail logs (single host or all)")
    logs.add_argument("--host", default=None, help="If set, tail logs for this host only")
    logs.set_defaults(func=cmd_logs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
