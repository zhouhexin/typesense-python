# Typesense Lite Raft Election Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Typesense Lite 实现 per-shard Raft leader 选举、基于 leader 的写入复制、coordinator leader discovery，以及集群控制台中的真实 Raft 状态展示。

**Architecture:** 每个 shard 是一个独立 Raft group，成员来自静态 shard primary + replicas。data node 持有 Raft runtime、持久化 term/vote/log，并通过 RequestVote / AppendEntries 选 leader 和复制 log；coordinator 查询每个 shard leader 并把写入路由给 leader。

**Tech Stack:** Python 3.9+、FastAPI、httpx、pytest、pytest-asyncio、JSON 文件持久化、现有 `typesense_lite` package。

---

## 阶段划分

1. **阶段 1：Raft 数据模型和持久化**
2. **阶段 2：RequestVote 和 AppendEntries 协议核心**
3. **阶段 3：Data node Raft runtime 和内部 API**
4. **阶段 4：Leader election background loop**
5. **阶段 5：Raft log command 应用到 document store**
6. **阶段 6：Coordinator leader discovery 和写入改造**
7. **阶段 7：公共 Raft 状态接口和集群控制台**
8. **阶段 8：故障转移、恢复和端到端验收**

每个阶段都必须先写失败测试，再实现最小代码，最后提交。

## 文件职责

### 新增文件

- `src/typesense_lite/raft_types.py`  
  定义 Raft role、request/response、log entry、command 等 TypedDict/dataclass。

- `src/typesense_lite/raft_storage.py`  
  管理每个 node/shard 的 `state.json` 和 `log.jsonl` 读写。

- `src/typesense_lite/raft_core.py`  
  纯内存 Raft 状态机：term、vote、append entries、commit index、log conflict resolution。

- `src/typesense_lite/raft_runtime.py`  
  data node 上的 per-shard runtime：HTTP peer 调用、选举 loop、heartbeat loop、command replication、apply committed entries。

- `src/typesense_lite/leader_directory.py`  
  coordinator 侧 leader discovery、leader cache、stale leader 刷新。

- `tests/typesense_lite/test_raft_storage.py`
- `tests/typesense_lite/test_raft_core.py`
- `tests/typesense_lite/test_raft_runtime.py`
- `tests/typesense_lite/test_leader_directory.py`

### 修改文件

- `src/typesense_lite/node.py`  
  增加 Raft command apply 所需的明确方法，复用现有 add/update/delete document 能力。

- `src/typesense_lite/server.py`  
  data node 增加内部 Raft API；coordinator 增加 `/cluster/raft`；生命周期中启动/停止 data node Raft runtime。

- `src/typesense_lite/coordinator.py`  
  写入、更新、删除改为通过 leader directory 路由到 Raft leader。

- `src/typesense_lite/cluster.py`  
  增加获取 shard voters 的 helper。

- `src/typesense_lite/web/cluster.html`  
  从静态选举状态升级为真实 Raft 状态展示。

- `tests/typesense_lite/test_server.py`  
  覆盖新增 Raft API、public `/cluster/raft`、cluster console 页面内容。

---

# 阶段 1：Raft 数据模型和持久化

## 任务 1.1：定义 Raft 类型

**文件：**
- 新增：`src/typesense_lite/raft_types.py`
- 测试：`tests/typesense_lite/test_raft_core.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_core.py` 新增：

```python
from typesense_lite.raft_types import RaftLogEntry, RaftRole


def test_raft_role_values() -> None:
    assert RaftRole.FOLLOWER.value == "follower"
    assert RaftRole.CANDIDATE.value == "candidate"
    assert RaftRole.LEADER.value == "leader"


def test_raft_log_entry_shape() -> None:
    entry = RaftLogEntry(
        index=1,
        term=2,
        command={
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        },
    )

    assert entry.index == 1
    assert entry.term == 2
    assert entry.command["type"] == "add_document"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：导入 `typesense_lite.raft_types` 失败。

- [ ] **Step 3：实现类型定义**

创建 `src/typesense_lite/raft_types.py`：

```python
"""Shared Raft data types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, TypedDict


class RaftRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftCommand(TypedDict, total=False):
    type: Literal["add_document", "update_document", "delete_document"]
    collection: str
    document: dict[str, Any]
    document_id: str
    changes: dict[str, Any]


@dataclass(frozen=True)
class RaftLogEntry:
    index: int
    term: int
    command: RaftCommand


@dataclass
class RaftPersistentState:
    current_term: int = 0
    voted_for: str | None = None
    commit_index: int = 0
    last_applied: int = 0
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_types.py tests/typesense_lite/test_raft_core.py
git commit -m "feat: add raft data types"
```

## 任务 1.2：实现 Raft 持久化

**文件：**
- 新增：`src/typesense_lite/raft_storage.py`
- 测试：`tests/typesense_lite/test_raft_storage.py`

- [ ] **Step 1：写失败测试**

创建 `tests/typesense_lite/test_raft_storage.py`：

```python
from typesense_lite.raft_storage import RaftStorage
from typesense_lite.raft_types import RaftLogEntry, RaftPersistentState


def test_storage_loads_default_state(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)

    state = storage.load_state()

    assert state == RaftPersistentState()


def test_storage_saves_and_loads_state(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)

    storage.save_state(
        RaftPersistentState(
            current_term=3,
            voted_for="node-2",
            commit_index=7,
            last_applied=6,
        )
    )

    assert storage.load_state() == RaftPersistentState(
        current_term=3,
        voted_for="node-2",
        commit_index=7,
        last_applied=6,
    )


def test_storage_appends_and_loads_log_entries(tmp_path) -> None:
    storage = RaftStorage(tmp_path, node_id="node-1", shard_id=0)
    entry = RaftLogEntry(
        index=1,
        term=1,
        command={"type": "add_document", "collection": "books", "document": {"id": "book-1"}},
    )

    storage.append_entries([entry])

    assert storage.load_log() == [entry]
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_storage.py -q
```

预期：导入 `RaftStorage` 失败。

- [ ] **Step 3：实现 `RaftStorage`**

创建 `src/typesense_lite/raft_storage.py`：

```python
"""Durable local storage for per-shard Raft state."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .raft_types import RaftLogEntry, RaftPersistentState


class RaftStorage:
    def __init__(self, data_dir: str | Path, *, node_id: str, shard_id: int) -> None:
        self.root = Path(data_dir) / "raft" / node_id / f"shard-{shard_id}"
        self.state_path = self.root / "state.json"
        self.log_path = self.root / "log.jsonl"

    def load_state(self) -> RaftPersistentState:
        if not self.state_path.exists():
            return RaftPersistentState()
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        return RaftPersistentState(
            current_term=int(payload.get("current_term", 0)),
            voted_for=payload.get("voted_for"),
            commit_index=int(payload.get("commit_index", 0)),
            last_applied=int(payload.get("last_applied", 0)),
        )

    def save_state(self, state: RaftPersistentState) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(asdict(state), ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )

    def load_log(self) -> list[RaftLogEntry]:
        if not self.log_path.exists():
            return []
        entries: list[RaftLogEntry] = []
        for line in self.log_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            entries.append(
                RaftLogEntry(
                    index=int(payload["index"]),
                    term=int(payload["term"]),
                    command=payload["command"],
                )
            )
        return entries

    def replace_log(self, entries: list[RaftLogEntry]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        lines = [
            json.dumps(asdict(entry), ensure_ascii=False, sort_keys=True)
            for entry in entries
        ]
        self.log_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    def append_entries(self, entries: list[RaftLogEntry]) -> None:
        if not entries:
            return
        current = self.load_log()
        current.extend(entries)
        self.replace_log(current)
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_storage.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_storage.py tests/typesense_lite/test_raft_storage.py
git commit -m "feat: persist raft state and log"
```

---

# 阶段 2：Raft 协议核心

## 任务 2.1：实现 RequestVote

**文件：**
- 新增/修改：`src/typesense_lite/raft_core.py`
- 测试：`tests/typesense_lite/test_raft_core.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_core.py` 追加：

```python
from typesense_lite.raft_core import RaftCore


def test_request_vote_grants_vote_for_newer_term() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2"])

    result = core.handle_request_vote(
        term=1,
        candidate_id="node-2",
        last_log_index=0,
        last_log_term=0,
    )

    assert result == {"term": 1, "vote_granted": True}
    assert core.state.current_term == 1
    assert core.state.voted_for == "node-2"


def test_request_vote_rejects_second_candidate_in_same_term() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2", "node-3"])
    core.handle_request_vote(term=1, candidate_id="node-2", last_log_index=0, last_log_term=0)

    result = core.handle_request_vote(
        term=1,
        candidate_id="node-3",
        last_log_index=0,
        last_log_term=0,
    )

    assert result == {"term": 1, "vote_granted": False}
    assert core.state.voted_for == "node-2"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：导入 `RaftCore` 失败。

- [ ] **Step 3：实现 RequestVote 最小核心**

创建 `src/typesense_lite/raft_core.py`：

```python
"""Pure Raft state machine logic."""

from __future__ import annotations

from .raft_types import RaftLogEntry, RaftPersistentState, RaftRole


class RaftCore:
    def __init__(
        self,
        *,
        node_id: str,
        members: list[str],
        state: RaftPersistentState | None = None,
        log: list[RaftLogEntry] | None = None,
    ) -> None:
        self.node_id = node_id
        self.members = list(members)
        self.state = state or RaftPersistentState()
        self.log = list(log or [])
        self.role = RaftRole.FOLLOWER
        self.leader_id: str | None = None

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def handle_request_vote(
        self,
        *,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> dict[str, int | bool]:
        if term < self.state.current_term:
            return {"term": self.state.current_term, "vote_granted": False}

        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self.role = RaftRole.FOLLOWER
            self.leader_id = None

        log_is_up_to_date = (
            last_log_term > self.last_log_term
            or (last_log_term == self.last_log_term and last_log_index >= self.last_log_index)
        )
        can_vote = self.state.voted_for in {None, candidate_id}
        vote_granted = can_vote and log_is_up_to_date and candidate_id in self.members

        if vote_granted:
            self.state.voted_for = candidate_id

        return {"term": self.state.current_term, "vote_granted": vote_granted}
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_core.py tests/typesense_lite/test_raft_core.py
git commit -m "feat: implement raft request vote"
```

## 任务 2.2：实现 AppendEntries 和日志冲突处理

**文件：**
- 修改：`src/typesense_lite/raft_core.py`
- 测试：`tests/typesense_lite/test_raft_core.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_core.py` 追加：

```python
def test_append_entries_accepts_heartbeat_from_new_leader() -> None:
    core = RaftCore(node_id="node-1", members=["node-1", "node-2"])

    result = core.handle_append_entries(
        term=1,
        leader_id="node-2",
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )

    assert result == {"term": 1, "success": True, "match_index": 0}
    assert core.leader_id == "node-2"
    assert core.role == RaftRole.FOLLOWER


def test_append_entries_rejects_when_previous_log_does_not_match() -> None:
    core = RaftCore(
        node_id="node-1",
        members=["node-1", "node-2"],
        log=[RaftLogEntry(index=1, term=1, command={"type": "add_document", "collection": "books", "document": {"id": "a"}})],
    )

    result = core.handle_append_entries(
        term=2,
        leader_id="node-2",
        prev_log_index=1,
        prev_log_term=2,
        entries=[],
        leader_commit=0,
    )

    assert result == {"term": 2, "success": False, "match_index": 1}


def test_append_entries_replaces_conflicting_entries() -> None:
    core = RaftCore(
        node_id="node-1",
        members=["node-1", "node-2"],
        log=[
            RaftLogEntry(index=1, term=1, command={"type": "add_document", "collection": "books", "document": {"id": "a"}}),
            RaftLogEntry(index=2, term=1, command={"type": "add_document", "collection": "books", "document": {"id": "b"}}),
        ],
    )
    new_entry = RaftLogEntry(
        index=2,
        term=2,
        command={"type": "add_document", "collection": "books", "document": {"id": "c"}},
    )

    result = core.handle_append_entries(
        term=2,
        leader_id="node-2",
        prev_log_index=1,
        prev_log_term=1,
        entries=[new_entry],
        leader_commit=2,
    )

    assert result == {"term": 2, "success": True, "match_index": 2}
    assert core.log == [
        RaftLogEntry(index=1, term=1, command={"type": "add_document", "collection": "books", "document": {"id": "a"}}),
        new_entry,
    ]
    assert core.state.commit_index == 2
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：`handle_append_entries` 不存在。

- [ ] **Step 3：实现 AppendEntries**

在 `src/typesense_lite/raft_core.py` 的 `RaftCore` 中增加：

```python
    def handle_append_entries(
        self,
        *,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: list[RaftLogEntry],
        leader_commit: int,
    ) -> dict[str, int | bool]:
        if term < self.state.current_term:
            return {"term": self.state.current_term, "success": False, "match_index": self.last_log_index}

        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None

        self.role = RaftRole.FOLLOWER
        self.leader_id = leader_id

        if prev_log_index > 0:
            previous = self._entry_at(prev_log_index)
            if previous is None or previous.term != prev_log_term:
                return {"term": self.state.current_term, "success": False, "match_index": self.last_log_index}

        for entry in entries:
            existing = self._entry_at(entry.index)
            if existing is not None and existing.term != entry.term:
                self.log = [item for item in self.log if item.index < entry.index]
            if self._entry_at(entry.index) is None:
                self.log.append(entry)

        if leader_commit > self.state.commit_index:
            self.state.commit_index = min(leader_commit, self.last_log_index)

        return {"term": self.state.current_term, "success": True, "match_index": self.last_log_index}

    def _entry_at(self, index: int) -> RaftLogEntry | None:
        for entry in self.log:
            if entry.index == index:
                return entry
        return None
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_core.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_core.py tests/typesense_lite/test_raft_core.py
git commit -m "feat: implement raft append entries"
```

---

# 阶段 3：Data node Raft runtime 和内部 API

## 任务 3.1：为 ClusterMap 增加 voters helper

**文件：**
- 修改：`src/typesense_lite/cluster.py`
- 测试：`tests/typesense_lite/test_cluster.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_cluster.py` 追加：

```python
def test_get_shard_voters_returns_primary_then_replicas() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    voters = cluster.get_shard_voters(1)

    assert [node.id for node in voters] == ["node-2", "node-1"]
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_cluster.py::test_get_shard_voters_returns_primary_then_replicas -q
```

预期：`get_shard_voters` 不存在。

- [ ] **Step 3：实现 helper**

在 `src/typesense_lite/cluster.py` 中增加：

```python
    def get_shard_voters(self, shard_id: int) -> list[NodeInfo]:
        placement = self.shards[shard_id]
        return [self.nodes[placement.primary], *[self.nodes[node_id] for node_id in placement.replicas]]
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_cluster.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/cluster.py tests/typesense_lite/test_cluster.py
git commit -m "feat: expose shard raft voters"
```

## 任务 3.2：实现 RaftRuntime 的同步处理接口

**文件：**
- 新增：`src/typesense_lite/raft_runtime.py`
- 测试：`tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

创建 `tests/typesense_lite/test_raft_runtime.py`：

```python
import httpx
import pytest

from typesense_lite.raft_runtime import RaftRuntime


@pytest.mark.asyncio
async def test_runtime_exposes_request_vote(tmp_path) -> None:
    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))),
        apply_command=lambda command: {"ok": True},
    )

    result = await runtime.handle_request_vote(
        {
            "term": 1,
            "candidate_id": "node-2",
            "last_log_index": 0,
            "last_log_term": 0,
        }
    )

    assert result == {"term": 1, "vote_granted": True}


@pytest.mark.asyncio
async def test_runtime_exposes_append_entries(tmp_path) -> None:
    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))),
        apply_command=lambda command: {"ok": True},
    )

    result = await runtime.handle_append_entries(
        {
            "term": 1,
            "leader_id": "node-2",
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit": 0,
        }
    )

    assert result == {"term": 1, "success": True, "match_index": 0}
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py -q
```

预期：导入 `RaftRuntime` 失败。

- [ ] **Step 3：实现 runtime 最小同步接口**

创建 `src/typesense_lite/raft_runtime.py`：

```python
"""Runtime wrapper for one node in one shard Raft group."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from .raft_core import RaftCore
from .raft_storage import RaftStorage
from .raft_types import RaftLogEntry


class RaftRuntime:
    def __init__(
        self,
        *,
        node_id: str,
        shard_id: int,
        members: list[str],
        data_dir: str | Path,
        peer_urls: dict[str, str],
        client: httpx.AsyncClient,
        apply_command: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self.node_id = node_id
        self.shard_id = shard_id
        self.members = members
        self.peer_urls = peer_urls
        self.client = client
        self.apply_command = apply_command
        self.storage = RaftStorage(data_dir, node_id=node_id, shard_id=shard_id)
        self.core = RaftCore(
            node_id=node_id,
            members=members,
            state=self.storage.load_state(),
            log=self.storage.load_log(),
        )

    async def handle_request_vote(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.core.handle_request_vote(
            term=int(payload["term"]),
            candidate_id=str(payload["candidate_id"]),
            last_log_index=int(payload["last_log_index"]),
            last_log_term=int(payload["last_log_term"]),
        )
        self.storage.save_state(self.core.state)
        return result

    async def handle_append_entries(self, payload: dict[str, Any]) -> dict[str, Any]:
        entries = [
            RaftLogEntry(
                index=int(entry["index"]),
                term=int(entry["term"]),
                command=entry["command"],
            )
            for entry in payload.get("entries", [])
        ]
        result = self.core.handle_append_entries(
            term=int(payload["term"]),
            leader_id=str(payload["leader_id"]),
            prev_log_index=int(payload["prev_log_index"]),
            prev_log_term=int(payload["prev_log_term"]),
            entries=entries,
            leader_commit=int(payload["leader_commit"]),
        )
        self.storage.save_state(self.core.state)
        self.storage.replace_log(self.core.log)
        return result
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: add raft runtime request handlers"
```

## 任务 3.3：data node 暴露内部 Raft API

**文件：**
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_server.py` 追加：

```python
def test_data_node_exposes_raft_state(tmp_path) -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1", data_dir=tmp_path)
    client = TestClient(app)

    response = client.get("/internal/raft/0/state")

    assert response.status_code == 200
    assert response.json()["node"] == "node-1"
    assert response.json()["shard_id"] == 0
    assert response.json()["role"] == "follower"


def test_data_node_exposes_request_vote(tmp_path) -> None:
    app = create_app(role="node", cluster_config=CONFIG, node_id="node-1", data_dir=tmp_path)
    client = TestClient(app)

    response = client.post(
        "/internal/raft/0/request_vote",
        json={"term": 1, "candidate_id": "node-1", "last_log_index": 0, "last_log_term": 0},
    )

    assert response.status_code == 200
    assert response.json()["vote_granted"] is True
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q -k "raft_state or request_vote"
```

预期：404。

- [ ] **Step 3：在 node app 初始化 runtimes**

在 `src/typesense_lite/server.py` 的 `role == "node"` 分支创建 runtimes：

```python
        raft_client = httpx.AsyncClient(timeout=2.0)
        raft_runtimes = {
            shard_id: RaftRuntime(
                node_id=node_id,
                shard_id=shard_id,
                members=[member.id for member in cluster.get_shard_voters(shard_id)],
                data_dir=data_dir or ".data/typesense_lite",
                peer_urls={
                    member.id: member.url
                    for member in cluster.get_shard_voters(shard_id)
                    if member.id != node_id
                },
                client=raft_client,
                apply_command=lambda command: node.apply_raft_command(command),
            )
            for shard_id, placement in cluster.shards.items()
            if node_id == placement.primary or node_id in placement.replicas
        }
```

同时给 `SearchNode` 增加 `apply_raft_command()`，见阶段 5 会完善。此阶段可先实现 add/update/delete dispatch。

- [ ] **Step 4：增加内部 API**

在 data node route 区域增加：

```python
        @app.get("/internal/raft/{shard_id}/state")
        def get_raft_state(shard_id: int) -> dict[str, Any]:
            runtime = raft_runtimes[shard_id]
            return runtime.state()

        @app.post("/internal/raft/{shard_id}/request_vote")
        async def request_vote(shard_id: int, payload: dict[str, Any]) -> dict[str, Any]:
            return await raft_runtimes[shard_id].handle_request_vote(payload)

        @app.post("/internal/raft/{shard_id}/append_entries")
        async def append_entries(shard_id: int, payload: dict[str, Any]) -> dict[str, Any]:
            return await raft_runtimes[shard_id].handle_append_entries(payload)
```

- [ ] **Step 5：为 runtime 增加 `state()`**

在 `RaftRuntime` 增加：

```python
    def state(self) -> dict[str, Any]:
        return {
            "node": self.node_id,
            "shard_id": self.shard_id,
            "role": self.core.role.value,
            "current_term": self.core.state.current_term,
            "voted_for": self.core.state.voted_for,
            "leader_id": self.core.leader_id,
            "commit_index": self.core.state.commit_index,
            "last_applied": self.core.state.last_applied,
            "last_log_index": self.core.last_log_index,
            "last_log_term": self.core.last_log_term,
            "members": self.members,
        }
```

- [ ] **Step 6：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q -k "raft_state or request_vote"
```

预期：通过。

- [ ] **Step 7：提交**

```bash
git add src/typesense_lite/server.py src/typesense_lite/node.py src/typesense_lite/raft_runtime.py tests/typesense_lite/test_server.py
git commit -m "feat: expose raft data node APIs"
```

---

# 阶段 4：Leader election background loop

## 任务 4.1：实现 candidate 发起选举

**文件：**
- 修改：`src/typesense_lite/raft_runtime.py`
- 测试：`tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_runtime.py` 追加：

```python
@pytest.mark.asyncio
async def test_runtime_starts_election_and_becomes_leader_with_majority(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/internal/raft/0/request_vote"
        return httpx.Response(200, json={"term": 1, "vote_granted": True})

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )

    await runtime.start_election()

    assert runtime.state()["role"] == "leader"
    assert runtime.state()["current_term"] == 1
    assert runtime.state()["leader_id"] == "node-1"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_runtime_starts_election_and_becomes_leader_with_majority -q
```

预期：`start_election` 不存在。

- [ ] **Step 3：实现 `start_election()`**

在 `RaftRuntime` 增加：

```python
    async def start_election(self) -> None:
        self.core.role = RaftRole.CANDIDATE
        self.core.state.current_term += 1
        self.core.state.voted_for = self.node_id
        self.core.leader_id = None
        self.storage.save_state(self.core.state)

        votes = 1
        request = {
            "term": self.core.state.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self.core.last_log_index,
            "last_log_term": self.core.last_log_term,
        }
        for peer_id, peer_url in self.peer_urls.items():
            try:
                response = await self.client.post(
                    f"{peer_url}/internal/raft/{self.shard_id}/request_vote",
                    json=request,
                )
                response.raise_for_status()
            except httpx.HTTPError:
                continue
            payload = response.json()
            if int(payload["term"]) > self.core.state.current_term:
                self.core.state.current_term = int(payload["term"])
                self.core.state.voted_for = None
                self.core.role = RaftRole.FOLLOWER
                self.storage.save_state(self.core.state)
                return
            if payload.get("vote_granted") is True:
                votes += 1

        if votes >= self._majority():
            self.core.role = RaftRole.LEADER
            self.core.leader_id = self.node_id

    def _majority(self) -> int:
        return len(self.members) // 2 + 1
```

并在文件顶部导入 `RaftRole`。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_runtime_starts_election_and_becomes_leader_with_majority -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: add raft leader election"
```

## 任务 4.2：实现 heartbeat loop 启停

**文件：**
- 修改：`src/typesense_lite/raft_runtime.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_runtime.py` 追加：

```python
@pytest.mark.asyncio
async def test_leader_sends_append_entries_heartbeat(tmp_path) -> None:
    seen_paths = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"term": 1, "success": True, "match_index": 0})

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )
    await runtime.start_election()

    await runtime.send_heartbeat()

    assert seen_paths == ["/internal/raft/0/append_entries"]
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_sends_append_entries_heartbeat -q
```

预期：`send_heartbeat` 不存在。

- [ ] **Step 3：实现 heartbeat**

在 `RaftRuntime` 增加：

```python
    async def send_heartbeat(self) -> None:
        if self.core.role is not RaftRole.LEADER:
            return
        payload = {
            "term": self.core.state.current_term,
            "leader_id": self.node_id,
            "prev_log_index": self.core.last_log_index,
            "prev_log_term": self.core.last_log_term,
            "entries": [],
            "leader_commit": self.core.state.commit_index,
        }
        for peer_url in self.peer_urls.values():
            try:
                await self.client.post(
                    f"{peer_url}/internal/raft/{self.shard_id}/append_entries",
                    json=payload,
                )
            except httpx.HTTPError:
                continue
```

- [ ] **Step 4：在 FastAPI lifespan 中启动 loop**

在 `server.py` 的 data node 分支中添加 startup/shutdown 管理。第一版可以先启动简单后台任务，测试阶段主要通过 runtime 单元测试覆盖，集成阶段再验证。

- [ ] **Step 5：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py -q
```

预期：通过。

- [ ] **Step 6：提交**

```bash
git add src/typesense_lite/raft_runtime.py src/typesense_lite/server.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: add raft heartbeat loop"
```

---

# 阶段 5：Raft log command 应用到 document store

## 任务 5.1：SearchNode 支持 apply Raft command

**文件：**
- 修改：`src/typesense_lite/node.py`
- 测试：`tests/typesense_lite/test_node.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_node.py` 追加：

```python
def test_search_node_applies_add_document_raft_command(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)

    result = node.apply_raft_command(
        0,
        {"type": "add_document", "collection": "books", "document": {"id": "book-1", "title": "Raft"}},
    )

    assert result["id"] == "book-1"
    assert node.get_document(0, "books", "book-1")["title"] == "Raft"


def test_search_node_applies_delete_document_raft_command(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "book-1", "title": "Raft"})

    result = node.apply_raft_command(
        0,
        {"type": "delete_document", "collection": "books", "document_id": "book-1"},
    )

    assert result["id"] == "book-1"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_node.py -q -k "raft_command"
```

预期：`apply_raft_command` 不存在。

- [ ] **Step 3：实现 command apply**

在 `src/typesense_lite/node.py` 的 `SearchNode` 增加：

```python
    def apply_raft_command(self, shard_id: int, command: dict[str, Any]) -> Document:
        command_type = command.get("type")
        collection = str(command.get("collection") or "")
        if command_type == "add_document":
            document = command["document"]
            return self.add_document(shard_id, collection, document)
        if command_type == "update_document":
            return self.update_document(
                shard_id,
                collection,
                str(command["document_id"]),
                command["changes"],
            )
        if command_type == "delete_document":
            return self.delete_document(shard_id, collection, str(command["document_id"]))
        raise ValueError(f"unknown raft command type {command_type!r}")
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_node.py -q -k "raft_command"
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/node.py tests/typesense_lite/test_node.py
git commit -m "feat: apply raft commands to search node"
```

## 任务 5.2：Leader command replication

**文件：**
- 修改：`src/typesense_lite/raft_runtime.py`
- 测试：`tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_runtime.py` 追加：

```python
@pytest.mark.asyncio
async def test_leader_commits_command_after_majority_replication(tmp_path) -> None:
    applied = []

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"term": 1, "success": True, "match_index": 1})

    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: applied.append(command) or {"id": command["document"]["id"]},
    )
    await runtime.start_election()

    result = await runtime.submit_command(
        {"type": "add_document", "collection": "books", "document": {"id": "book-1"}}
    )

    assert result["ok"] is True
    assert result["commit_index"] == 1
    assert applied == [{"type": "add_document", "collection": "books", "document": {"id": "book-1"}}]
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_commits_command_after_majority_replication -q
```

预期：`submit_command` 不存在。

- [ ] **Step 3：实现 `submit_command()`**

在 `RaftRuntime` 增加 leader command submit。核心行为：

- 非 leader 返回 `ok=false` 和 `leader_id`。
- leader 创建下一条 log entry。
- 向 peers 发送 AppendEntries。
- 成功响应数量达到 majority 后更新 commit index。
- 调用 `apply_committed()`。

- [ ] **Step 4：运行 runtime 测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: replicate raft document commands"
```

---

# 阶段 6：Coordinator leader discovery 和写入改造

## 任务 6.1：实现 LeaderDirectory

**文件：**
- 新增：`src/typesense_lite/leader_directory.py`
- 测试：`tests/typesense_lite/test_leader_directory.py`

- [ ] **Step 1：写失败测试**

创建 `tests/typesense_lite/test_leader_directory.py`：

```python
import httpx
import pytest

from typesense_lite.cluster import ClusterMap
from typesense_lite.leader_directory import LeaderDirectory


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 1,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {"0": {"primary": "node-1", "replicas": ["node-2"]}},
}


@pytest.mark.asyncio
async def test_discovers_shard_leader() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.port == 9102:
            return httpx.Response(200, json={"node": "node-2", "role": "leader", "current_term": 3})
        return httpx.Response(200, json={"node": "node-1", "role": "follower", "current_term": 3, "leader_id": "node-2"})

    cluster = ClusterMap.from_dict(CONFIG)
    directory = LeaderDirectory(cluster, httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    leader = await directory.get_leader(0)

    assert leader.id == "node-2"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_leader_directory.py -q
```

预期：导入失败。

- [ ] **Step 3：实现 LeaderDirectory**

创建 `src/typesense_lite/leader_directory.py`，实现：

- `get_leader(shard_id) -> NodeInfo`
- `refresh(shard_id) -> NodeInfo | None`
- `invalidate(shard_id)`

leader 查询路径为 `/internal/raft/{shard_id}/state`。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_leader_directory.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/leader_directory.py tests/typesense_lite/test_leader_directory.py
git commit -m "feat: discover raft shard leaders"
```

## 任务 6.2：Coordinator 写入走 Raft leader

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 测试：`tests/typesense_lite/test_coordinator.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_coordinator.py` 追加：

```python
@pytest.mark.asyncio
async def test_add_document_routes_to_raft_leader() -> None:
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.port, request.url.path))
        if request.url.path == "/internal/raft/0/state":
            role = "leader" if request.url.port == 9102 else "follower"
            return httpx.Response(200, json={"node": f"node-{request.url.port - 9100}", "role": role, "current_term": 1})
        if request.url.path == "/internal/raft/0/commands":
            return httpx.Response(200, json={"ok": True, "leader": "node-2", "term": 1, "commit_index": 1, "result": {"id": "book-1"}})
        return httpx.Response(404)

    cluster = ClusterMap.from_dict(CONFIG_WITH_REPLICA)
    coordinator = Coordinator(cluster, client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    result = await coordinator.add_document("books", {"id": "book-1", "title": "Raft"})

    assert result["ok"] is True
    assert result["leader"] == "node-2"
    assert ("POST", 9102, "/internal/raft/0/commands") in requests
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py -q -k "raft_leader"
```

预期：仍调用旧 document write path。

- [ ] **Step 3：改造 Coordinator**

在 `Coordinator.__init__` 中创建 `LeaderDirectory`。将 `add_document/update_document/delete_document` 改为发送 Raft command 到 leader：

```python
response = await self._client.post(
    f"{leader.url}/internal/raft/{shard_id}/commands",
    json=command,
)
```

遇到 `409` 时 invalidate leader cache 并重试一次。

- [ ] **Step 4：运行相关测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py tests/typesense_lite/test_server.py -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/coordinator.py tests/typesense_lite/test_coordinator.py
git commit -m "feat: route writes through raft leaders"
```

---

# 阶段 7：公共 Raft 状态接口和集群控制台

## 任务 7.1：Coordinator 暴露 `/cluster/raft`

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_server.py` 追加：

```python
def test_coordinator_cluster_raft_endpoint(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "node": "node-1",
                "shard_id": 0,
                "role": "leader",
                "current_term": 2,
                "commit_index": 4,
                "last_applied": 4,
            },
        )

    app = create_app(
        role="coordinator",
        cluster_config=CONFIG,
        data_dir=tmp_path,
        coordinator_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client = TestClient(app)

    response = client.get("/cluster/raft")

    assert response.status_code == 200
    assert response.json()["shards"]["0"]["members"]["node-1"]["role"] == "leader"
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q -k "cluster_raft_endpoint"
```

预期：404。

- [ ] **Step 3：实现聚合接口**

在 `Coordinator` 增加 `raft_status()`，遍历 shard voters 调用 `/internal/raft/{shard_id}/state`。在 `server.py` coordinator 分支增加：

```python
        @app.get("/cluster/raft")
        async def cluster_raft() -> dict[str, Any]:
            return await coordinator.raft_status()
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q -k "cluster_raft_endpoint"
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/coordinator.py src/typesense_lite/server.py tests/typesense_lite/test_server.py
git commit -m "feat: expose raft cluster status"
```

## 任务 7.2：Cluster console 展示真实 Raft 状态

**文件：**
- 修改：`src/typesense_lite/web/cluster.html`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_server.py` 的 cluster console 测试中增加断言：

```python
assert "fetch('/cluster/raft')" in response.text
assert "Raft Status" in response.text
assert "commit_index" in response.text
assert "last_applied" in response.text
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py::test_coordinator_serves_cluster_console_page -q
```

预期：缺少 `/cluster/raft`。

- [ ] **Step 3：更新页面**

在 `cluster.html` 中：

- 将 `Election Status` 改为 `Raft Status`。
- 页面加载和 Refresh 时调用 `/cluster/raft`。
- 每个 shard 展示 leader、term。
- 每个 member 展示 role、term、commit_index、last_applied。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py::test_coordinator_serves_cluster_console_page -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/web/cluster.html tests/typesense_lite/test_server.py
git commit -m "feat: show raft state in cluster console"
```

---

# 阶段 8：故障转移、恢复和端到端验收

## 任务 8.1：本地三节点单 shard leader 故障转移测试

**文件：**
- 新增：`tests/typesense_lite/test_raft_integration.py`

- [x] **Step 1：写集成测试**

创建 `tests/typesense_lite/test_raft_integration.py`，用 `httpx.MockTransport` 或 in-process runtime 模拟三节点：

```python
@pytest.mark.asyncio
async def test_new_leader_elected_after_leader_stops(tmp_path) -> None:
    # 创建 node-1/node-2/node-3 三个 RaftRuntime。
    # 让 node-1 成为 leader。
    # 停止 node-1 heartbeat。
    # 触发 node-2 election。
    # 断言 node-2 role=leader 且 term 增加。
```

该测试必须实际断言：

- 新 leader 不是旧 leader。
- term 增加。
- majority 成立。

- [x] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_integration.py -q
```

预期：根据当前 runtime 能力缺失失败。

- [x] **Step 3：补齐 timeout / tick 控制接口**

在 `RaftRuntime` 增加测试友好的 `tick()` 或显式 `start_election()` 控制，避免测试依赖真实睡眠。

- [x] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_integration.py -q
```

预期：通过。

- [x] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_integration.py
git commit -m "test: cover raft leader failover"
```

## 任务 8.2：完整验证和文档更新

**文件：**
- 修改：`README.md` 或现有启动文档
- 修改：`docs/superpowers/plans/2026-06-20-typesense-lite-raft-election.md`

- [x] **Step 1：运行完整测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
```

预期：全部通过。

- [x] **Step 2：运行 lint**

```bash
.venv/bin/python -m ruff check src/typesense_lite tests/typesense_lite examples/distributed_lite
```

预期：`All checks passed!`

- [x] **Step 3：手工启动集群**

```bash
.venv/bin/python examples/distributed_lite/start_cluster.py
```

预期：

```text
coordinator: http://127.0.0.1:9100
node-1: http://127.0.0.1:9101
node-2: http://127.0.0.1:9102
node-3: http://127.0.0.1:9103
```

- [x] **Step 4：手工验证 Raft 状态接口**

```bash
curl http://127.0.0.1:9100/cluster/raft
```

预期：每个 shard 都有 leader 和 member role。

- [x] **Step 5：手工验证页面**

打开：

```text
http://127.0.0.1:9100/cluster-console
```

预期：

- 能看到 `Raft Status`。
- 每个 shard 有 leader。
- 每个 member 有 role、term、commit index。

- [x] **Step 6：提交文档**

```bash
git add README.md docs/superpowers/plans/2026-06-20-typesense-lite-raft-election.md
git commit -m "docs: document raft election workflow"
```

---

## 总体验收标准

- [x] data node 暴露 RequestVote、AppendEntries、Raft state API。
- [x] 每个 shard 独立选出 leader。
- [x] leader 停止后 follower 能成为新 leader。
- [x] coordinator 写入通过 shard leader。
- [x] Raft log 达到多数派后才提交。
- [x] 已提交 command 应用到 document store 和 index。
- [x] `/cluster/raft` 能展示每个 shard 的 leader、term、member role。
- [x] `/cluster-console` 展示真实 Raft 状态，不再显示静态选举占位。
- [x] 完整测试通过。
- [x] ruff 检查通过。

## 实际验收记录

- `tests/typesense_lite/test_raft_integration.py` 覆盖三 voter Raft group 中旧 leader 停止后，新 leader 以更高 term 获得多数派选票。
- `tests/typesense_lite/test_raft_runtime.py` 覆盖后台选举/心跳、leader 写入多数派提交、follower 应用已提交日志、heartbeat 推进 commit 后补应用日志。
- 端到端手工验证使用临时 data dir 启动 `examples/distributed_lite/start_cluster.py`，确认 `/cluster/raft` 返回每个 shard 的 leader/member role。
- 写入 `raft-test-1` 后，搜索命中正常，并直接访问 shard 0 follower `node-2` 验证文档已复制并应用；`/cluster/raft` 中 shard 0 leader 和 follower 的 `commit_index`、`last_applied` 都为 `1`。
- 默认 `examples/distributed_lite/cluster_config.json` 每个 shard 仍是 2 voter，只能验证复制和多数派提交；真正容忍 1 个 voter 故障需要每个 shard 配置至少 3 voter。

## 风险和约束

- 当前默认 cluster config 每个 shard 只有 2 个 voters。2 节点 Raft group 容错能力有限：任意一个节点不可用时无法形成多数派提交。建议后续把默认集群调整为每个 shard 3 voters。
- 第一版不做 snapshot/log compaction，长时间大量写入后 `log.jsonl` 会增长。
- 第一版不做 membership change。
- 第一版只有单 coordinator；coordinator 本身不是 Raft 复制对象。
