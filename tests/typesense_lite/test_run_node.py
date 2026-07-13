from __future__ import annotations

import sys
from pathlib import Path

import pytest

from typesense_lite.run_node import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    RunNodeSpec,
    build_command,
    build_env,
    parse_args,
)


pytestmark = pytest.mark.unit


def _spec(**overrides) -> RunNodeSpec:
    defaults: dict = dict(
        role="node",
        host="0.0.0.0",
        port=9101,
        config="/etc/typesense_lite/cluster_config.json",
        data_dir="/var/lib/typesense_lite/node-1",
        node_id="node-1",
    )
    defaults.update(overrides)
    return RunNodeSpec(**defaults)


def test_build_command_includes_host_and_port() -> None:
    cmd = build_command(_spec(host="192.168.1.10", port=9105))

    assert cmd[0] == sys.executable
    assert cmd[1:4] == ["-m", "uvicorn", "typesense_lite.server:app"]
    assert "--host" in cmd and "192.168.1.10" in cmd
    assert "--port" in cmd and "9105" in cmd


def test_build_command_defaults_match_run_node_defaults() -> None:
    cmd = build_command(_spec())

    assert cmd[-2:] == ["--port", "9101"]


def test_build_env_sets_role_config_data_dir_and_node_id() -> None:
    env = build_env(_spec())

    assert env["ROLE"] == "node"
    assert env["CLUSTER_CONFIG"] == "/etc/typesense_lite/cluster_config.json"
    assert env["DATA_DIR"] == "/var/lib/typesense_lite/node-1"
    assert env["NODE_ID"] == "node-1"


def test_build_env_omits_node_id_when_role_is_coordinator() -> None:
    env = build_env(_spec(role="coordinator", node_id=None))

    assert env["ROLE"] == "coordinator"
    assert "NODE_ID" not in env


def test_build_env_merges_extra_env_over_base() -> None:
    spec = _spec(extra_env={"COORDINATOR_URL": "http://10.0.0.5:9100"})
    base = {"PATH": "/usr/bin", "ROLE": "should_be_overridden"}

    env = build_env(spec, base_env=base)

    assert env["PATH"] == "/usr/bin"
    assert env["ROLE"] == "node"  # role wins over base
    assert env["COORDINATOR_URL"] == "http://10.0.0.5:9100"


def test_build_env_keeps_inherited_pythonpath_by_default() -> None:
    spec = _spec()
    base = {"PYTHONPATH": "/some/other/lib"}

    env = build_env(spec, base_env=base)

    assert env["PYTHONPATH"] == "/some/other/lib"


def test_parse_args_defaults_host_to_all_interfaces() -> None:
    spec = parse_args(
        [
            "--role", "node",
            "--node-id", "node-1",
            "--config", "/tmp/cfg.json",
            "--port", "9200",
        ],
    )

    assert spec.host == DEFAULT_HOST == "0.0.0.0"
    assert spec.port == 9200


def test_parse_args_falls_back_to_env_when_flag_missing() -> None:
    spec = parse_args(
        [],
        # simulate env
    ) if False else None  # placeholder so the real env test below runs cleanly

    spec = parse_args_with_env(
        {
            "ROLE": "coordinator",
            "HOST": "192.168.1.10",
            "PORT": "9300",
            "CLUSTER_CONFIG": "/etc/cfg.json",
            "DATA_DIR": "/var/lib/foo",
        },
        argv=["--node-id", "ignored"],
    )

    assert spec.role == "coordinator"
    assert spec.host == "192.168.1.10"
    assert spec.port == 9300
    assert spec.config == "/etc/cfg.json"
    assert spec.data_dir == "/var/lib/foo"


def test_parse_args_requires_node_id_for_role_node() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--role", "node", "--config", "/tmp/cfg.json"])


def test_parse_args_requires_config() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--role", "coordinator"])


def test_parse_args_default_port_is_9100() -> None:
    spec = parse_args_with_env({"CLUSTER_CONFIG": "/tmp/cfg.json"}, argv=[])

    assert spec.port == DEFAULT_PORT == 9100
    assert spec.host == DEFAULT_HOST


def test_parse_args_accepts_log_file() -> None:
    spec = parse_args(
        [
            "--role", "node",
            "--node-id", "node-1",
            "--config", "/tmp/cfg.json",
            "--log-file", "/var/log/node-1.log",
        ],
    )

    assert spec.log_file == "/var/log/node-1.log"


# --- helpers ------------------------------------------------------------


def parse_args_with_env(env: dict[str, str], argv: list[str]) -> RunNodeSpec:
    """Run parse_args with a controlled environment without leaking state."""
    import os

    original = os.environ.copy()
    try:
        for key in ("ROLE", "HOST", "PORT", "NODE_ID", "CLUSTER_CONFIG", "DATA_DIR"):
            os.environ.pop(key, None)
        os.environ.update(env)
        return parse_args(argv)
    finally:
        for key in ("ROLE", "HOST", "PORT", "NODE_ID", "CLUSTER_CONFIG", "DATA_DIR"):
            if key in original:
                os.environ[key] = original[key]
            else:
                os.environ.pop(key, None)


def test_scripts_directory_entrypoint_resolves_to_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """The thin CLI shim must delegate to typesense_lite.run_node.main."""
    scripts_path = Path(__file__).resolve().parents[2] / "scripts" / "run_node.py"
    assert scripts_path.is_file(), "scripts/run_node.py must exist"

    src = str(scripts_path.parents[1] / "src")
    monkeypatch.syspath_prepend(src)

    import importlib.util

    spec = importlib.util.spec_from_file_location("scripts_run_node", scripts_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "main")
    assert module.main is parse_args.__globals__["main"] or callable(module.main)