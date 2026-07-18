from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


pytestmark = pytest.mark.unit
EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples" / "distributed_lite"


def _load_script(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.syspath_prepend(str(EXAMPLES_DIR))
    spec = importlib.util.spec_from_file_location(
        "recover_node_script", EXAMPLES_DIR / "recover_node.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeProcess:
    def __init__(self) -> None:
        self.terminated = False

    def poll(self) -> None:
        return None

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True


def test_main_recovers_requested_node(monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load_script(monkeypatch)
    process = FakeProcess()
    recovery = {
        "node_id": "node-2",
        "status": "healthy",
        "ready": True,
        "shards": {},
    }
    monkeypatch.setattr(
        script,
        "load_config",
        lambda path: {"nodes": [{"id": "node-2", "host": "127.0.0.1", "port": 9102}]},
    )
    monkeypatch.setattr(script, "_probe_health", lambda url: None)
    monkeypatch.setattr(script, "start_node", lambda **kwargs: process)
    monkeypatch.setattr(script, "_wait_for_recovery", lambda **kwargs: recovery)
    monkeypatch.setattr(
        sys,
        "argv",
        ["recover_node.py", "--node-id", "node-2", "--data-dir", "/tmp/demo"],
    )

    assert script.main() == 0


def test_main_does_not_duplicate_running_node(monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load_script(monkeypatch)
    monkeypatch.setattr(
        script,
        "load_config",
        lambda path: {"nodes": [{"id": "node-1", "host": "127.0.0.1", "port": 9101}]},
    )
    monkeypatch.setattr(
        script,
        "_probe_health",
        lambda url: {"ok": True, "node_id": "node-1"},
    )
    monkeypatch.setattr(
        script,
        "start_node",
        lambda **kwargs: pytest.fail("an online node must not be started again"),
    )
    monkeypatch.setattr(sys, "argv", ["recover_node.py", "--node-id", "node-1"])

    assert script.main() == 0


def test_main_stops_child_when_recovery_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    script = _load_script(monkeypatch)
    process = FakeProcess()
    monkeypatch.setattr(
        script,
        "load_config",
        lambda path: {"nodes": [{"id": "node-3", "host": "127.0.0.1", "port": 9103}]},
    )
    monkeypatch.setattr(script, "_probe_health", lambda url: None)
    monkeypatch.setattr(script, "start_node", lambda **kwargs: process)

    def fail(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("catch-up failed")

    monkeypatch.setattr(script, "_wait_for_recovery", fail)
    monkeypatch.setattr(sys, "argv", ["recover_node.py", "--node-id", "node-3"])

    assert script.main() == 1
    assert process.terminated is True
