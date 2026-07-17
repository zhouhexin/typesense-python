"""Start a local Typesense Lite demo cluster (single host, multiple processes).

This launcher remains a convenience wrapper for the localhost demo. The
shared spawn logic lives in :mod:`typesense_lite.run_node`, which is also
used by the multi-machine deploy script.
"""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
from pathlib import Path

from typesense_lite.run_node import RunNodeSpec, spawn_process


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = Path(__file__).with_name("cluster_config.json")
DEFAULT_DATA_DIR = ROOT / ".data" / "typesense_lite"

# Local demo binds to loopback only; cross-machine deployment uses
# run_node.py with --host 0.0.0.0 directly.
LOCAL_DEMO_HOST = "127.0.0.1"


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
    spec = RunNodeSpec(
        role=role,
        host=LOCAL_DEMO_HOST,
        port=port,
        config=str(config_path),
        data_dir=str(data_dir),
        node_id=node_id,
    )
    env = _local_demo_env()
    return spawn_process(spec, base_env=env, cwd=ROOT)


def _local_demo_env() -> dict[str, str]:
    os = __import__("os")
    src = str(ROOT / "src")
    existing = os.environ.get("PYTHONPATH")
    no_proxy = _merge_no_proxy(os.environ.get("NO_PROXY", ""))
    env = {"NO_PROXY": no_proxy, "no_proxy": no_proxy}
    if existing:
        env["PYTHONPATH"] = os.pathsep.join([src, existing])
    else:
        env["PYTHONPATH"] = src
    return env


def _merge_no_proxy(existing: str) -> str:
    entries = [item.strip() for item in existing.split(",") if item.strip()]
    for required in ("127.0.0.1", "localhost"):
        if required not in entries:
            entries.append(required)
    return ",".join(entries)


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
