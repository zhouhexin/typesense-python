"""Start a local Typesense Lite demo cluster."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = Path(__file__).with_name("cluster_config.json")
DEFAULT_DATA_DIR = ROOT / ".data" / "typesense_lite"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    processes: list[subprocess.Popen[bytes]] = []
    try:
        for node in config["nodes"]:
            processes.append(
                _start_process(
                    role="node",
                    port=int(node["port"]),
                    config_path=config_path,
                    data_dir=Path(args.data_dir),
                    node_id=node["id"],
                )
            )

        coordinator = config["coordinator"]
        processes.append(
            _start_process(
                role="coordinator",
                port=int(coordinator["port"]),
                config_path=config_path,
                data_dir=Path(args.data_dir),
            )
        )

        print("Typesense Lite cluster started:")
        print(f"  coordinator: http://{coordinator['host']}:{coordinator['port']}")
        for node in config["nodes"]:
            print(f"  {node['id']}: http://{node['host']}:{node['port']}")
        print("Press Ctrl-C to stop.")

        for process in processes:
            process.wait()
    except KeyboardInterrupt:
        print("\nStopping cluster...")
    finally:
        _stop_processes(processes)

    return 0


def _start_process(
    *,
    role: str,
    port: int,
    config_path: Path,
    data_dir: Path,
    node_id: str | None = None,
) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["ROLE"] = role
    env["CLUSTER_CONFIG"] = str(config_path)
    env["DATA_DIR"] = str(data_dir)
    env["PYTHONPATH"] = _pythonpath()
    if node_id is not None:
        env["NODE_ID"] = node_id

    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "typesense_lite.server:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]
    return subprocess.Popen(command, cwd=ROOT, env=env)


def _pythonpath() -> str:
    src = str(ROOT / "src")
    existing = os.environ.get("PYTHONPATH")
    if existing:
        return os.pathsep.join([src, existing])
    return src


def _stop_processes(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
    for process in processes:
        if process.poll() is None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    raise SystemExit(main())

