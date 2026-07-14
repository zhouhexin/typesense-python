"""Build and spawn a single Typesense Lite coordinator or data-node process.

This module is the per-machine entrypoint used by the multi-machine deploy
script (``scripts/deploy_cluster.py``). It owns the small bit of logic that
turns a role / host / port / config tuple into a uvicorn command and the
matching environment variables that ``typesense_lite.server`` consumes.

The local single-host demo launcher
(``examples/distributed_lite/start_cluster.py``) reuses this module so that
both code paths share the same env-var contract.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9100
DEFAULT_DATA_DIR = ".data/typesense_lite"


@dataclass(frozen=True)
class RunNodeSpec:
    """Inputs for spawning one Typesense Lite process."""

    role: str
    host: str
    port: int
    config: str | None
    data_dir: str
    node_id: str | None = None
    log_file: str | None = None
    coordinator_url: str | None = None
    advertise_host: str | None = None
    advertise_port: int | None = None
    extra_env: Mapping[str, str] = field(default_factory=dict)


def build_command(spec: RunNodeSpec) -> list[str]:
    """Return the argv used to launch uvicorn for ``spec``."""
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "typesense_lite.server:app",
        "--host",
        spec.host,
        "--port",
        str(spec.port),
    ]


def build_env(
    spec: RunNodeSpec,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return the env dict that ``typesense_lite.server`` expects."""
    env: dict[str, str] = dict(base_env if base_env is not None else os.environ)
    env["ROLE"] = spec.role
    if spec.config:
        env["CLUSTER_CONFIG"] = spec.config
    else:
        env.pop("CLUSTER_CONFIG", None)
    env["DATA_DIR"] = spec.data_dir
    if spec.node_id:
        env["NODE_ID"] = spec.node_id
    if spec.coordinator_url:
        env["COORDINATOR_URL"] = spec.coordinator_url
    if spec.advertise_host:
        env["NODE_ADVERTISE_HOST"] = spec.advertise_host
    if spec.advertise_port is not None:
        env["NODE_ADVERTISE_PORT"] = str(spec.advertise_port)
    for key, value in spec.extra_env.items():
        env[key] = value
    return env


def spawn_process(
    spec: RunNodeSpec,
    *,
    base_env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
) -> subprocess.Popen[bytes]:
    """Spawn one Typesense Lite process and return the Popen handle."""
    return subprocess.Popen(
        build_command(spec),
        env=build_env(spec, base_env),
        cwd=str(cwd) if cwd is not None else None,
    )


def parse_args(argv: Sequence[str] | None = None) -> RunNodeSpec:
    """Parse CLI args (and env fallbacks) into a :class:`RunNodeSpec`."""
    parser = argparse.ArgumentParser(
        description="Run a single Typesense Lite coordinator or data-node process.",
    )
    parser.add_argument(
        "--role",
        choices=("coordinator", "node"),
        default=os.environ.get("ROLE", "coordinator"),
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", DEFAULT_HOST),
        help="Address uvicorn binds to (default: 0.0.0.0 for cross-machine reach).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", str(DEFAULT_PORT))),
    )
    parser.add_argument(
        "--node-id",
        default=os.environ.get("NODE_ID"),
        help="Required when --role=node.",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("CLUSTER_CONFIG"),
        help="Path to cluster_config.json (or CLUSTER_CONFIG env).",
    )
    parser.add_argument(
        "--coordinator-url",
        default=os.environ.get("COORDINATOR_URL"),
        help="Coordinator URL used by data nodes for pull-config and registration.",
    )
    parser.add_argument(
        "--advertise-host",
        default=os.environ.get("NODE_ADVERTISE_HOST"),
        help="Host data nodes advertise to the coordinator.",
    )
    parser.add_argument(
        "--advertise-port",
        type=int,
        default=(
            int(os.environ["NODE_ADVERTISE_PORT"])
            if os.environ.get("NODE_ADVERTISE_PORT")
            else None
        ),
        help="Port data nodes advertise to the coordinator.",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("DATA_DIR", DEFAULT_DATA_DIR),
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="If set, append stdout/stderr to this file in addition to the console.",
    )
    parsed = parser.parse_args(list(argv) if argv is not None else None)

    if parsed.role == "node" and not parsed.node_id:
        parser.error("--node-id is required when --role=node")
    if not parsed.config and not (
        parsed.role == "node" and parsed.coordinator_url
    ):
        parser.error("--config (or CLUSTER_CONFIG env) is required")

    return RunNodeSpec(
        role=parsed.role,
        host=parsed.host,
        port=parsed.port,
        config=parsed.config,
        data_dir=parsed.data_dir,
        node_id=parsed.node_id,
        log_file=parsed.log_file,
        coordinator_url=parsed.coordinator_url,
        advertise_host=parsed.advertise_host,
        advertise_port=parsed.advertise_port,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint: parse args and exec a single process in the foreground."""
    spec = parse_args(argv)
    cmd = build_command(spec)
    env = build_env(spec)

    if spec.log_file:
        log_path = Path(spec.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            with subprocess.Popen(
                cmd,
                env=env,
                stdout=handle,
                stderr=subprocess.STDOUT,
            ) as proc:
                return proc.wait()

    return subprocess.call(cmd, env=env)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())