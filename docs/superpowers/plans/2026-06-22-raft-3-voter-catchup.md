# Raft 三 Voter 与自动追日志实现计划

> **给执行开发的 agent：** 必须按任务逐步执行。推荐使用 `superpowers:subagent-driven-development`，或使用 `superpowers:executing-plans`。步骤使用 checkbox（`- [ ]`）跟踪状态。

**目标：** 将默认集群改为每个 shard 由 3 个 voter 组成，并补齐 Raft leader 对落后 follower 的日志追赶能力，使单节点故障恢复后能自动 catch up。

**架构：** 默认仍保持单机多进程、多端口模型，但每个 shard 的 Raft group 成员改为 `primary + 2 replicas`，即 3 voter。Raft leader 维护每个 peer 的 `next_index` 和 `match_index`，写入和 heartbeat 都通过同一个复制函数向 follower 推送从 `next_index` 开始的日志后缀；如果 follower 日志不匹配，leader 回退 `next_index` 后重试，直到 follower 追上或 peer 不可用。

**技术栈：** Python 3.9+、FastAPI、httpx、pytest、pytest-asyncio、JSON 文件持久化、现有 `typesense_lite` package。

---

## 当前问题

现在默认配置是 3 个 node、3 个 shard，但每个 shard 只有 2 个 voter：

```json
"0": {"primary": "node-1", "replicas": ["node-2"]}
```

这意味着：

- 可以验证 leader election、heartbeat、Raft log 多数派提交。
- 但 2 voter group 的多数派是 2，只要任意 1 个 voter 宕机，就不能提交新写入。
- 因此无法做到“真实单节点故障转移后继续写入”。

当前 `RaftRuntime.submit_command()` 只把最新 entry 发给 peer：

```python
"prev_log_index": entry.index - 1,
"entries": [entry],
```

这意味着：

- 如果 follower 离线错过多条日志，恢复后不会自动拿到完整日志后缀。
- 如果 follower 日志和 leader 冲突，leader 也没有 `next_index` 回退机制来修复冲突。
- `send_heartbeat()` 只发空 entries，不能用于恢复落后 follower。

## 目标行为

1. 默认 `examples/distributed_lite/cluster_config.json` 每个 shard 都有 3 voter。
2. 任意 1 个 data node 停止后，每个 shard 仍保留 2/3 多数派。
3. 旧 leader 停止后，对应 shard 能选出新 leader。
4. 一个 follower 停止期间错过多条写入，恢复后 leader 会通过 heartbeat 自动补齐日志。
5. follower 日志存在冲突时，leader 能回退 `next_index` 并覆盖冲突日志。
6. `/cluster/raft` 能展示 3 个 member 的 role、term、commit_index、last_applied。

## 非目标

本阶段不做：

- Raft snapshot / log compaction。
- 动态 membership change。
- 多 coordinator 高可用。
- 线性一致读。
- 自动进程拉起或容器编排。

---

## 文件职责

### 修改文件

- `examples/distributed_lite/cluster_config.json`  
  默认每个 shard 使用 3 voter：一个 primary、两个 replicas。

- `src/typesense_lite/raft_runtime.py`  
  增加 leader 侧 peer progress：`next_index`、`match_index`。统一写入复制和 heartbeat 复制逻辑。实现落后 follower catch-up 和 conflict backoff。

- `src/typesense_lite/raft_core.py`  
  保持大部分逻辑不变。必要时增强 AppendEntries mismatch 响应，让 leader 更容易回退。

- `src/typesense_lite/coordinator.py`  
  搜索、list、get 当前仍优先 primary。默认配置变成 3 voter 后无需先改路由；但测试要确认 3 voter 不破坏现有 coordinator 行为。

- `README.md`  
  更新默认集群说明：每 shard 3 voter，支持单节点故障下多数派继续工作。

- `docs/superpowers/plans/2026-06-22-raft-3-voter-catchup.md`  
  开发过程中更新验收记录。

### 修改测试

- `tests/typesense_lite/test_cluster.py`  
  覆盖默认 cluster config 每个 shard 有 3 voter。

- `tests/typesense_lite/test_raft_runtime.py`  
  覆盖 leader peer progress、落后 follower catch-up、冲突日志修复。

- `tests/typesense_lite/test_raft_integration.py`  
  覆盖三节点 Raft group 中一个 follower 离线、leader 继续提交、follower 恢复后自动追上。

- `tests/typesense_lite/test_server.py`  
  覆盖 `/cluster/raft` 在 3 voter shard 下展示 3 个 members。

---

# 阶段 1：默认集群改为每 shard 3 voter

## 任务 1.1：更新默认 cluster config

**Files:**
- Modify: `examples/distributed_lite/cluster_config.json`
- Modify: `tests/typesense_lite/test_cluster.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_cluster.py` 新增：

```python
from pathlib import Path


def test_default_cluster_config_uses_three_voters_per_shard() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / "distributed_lite"
        / "cluster_config.json"
    )
    cluster = ClusterMap.from_file(config_path)

    assert cluster.shard_count == 3
    for shard_id in range(cluster.shard_count):
        voters = cluster.get_shard_voters(shard_id)
        assert len(voters) == 3
        assert {node.id for node in voters} == {"node-1", "node-2", "node-3"}
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_cluster.py::test_default_cluster_config_uses_three_voters_per_shard -q
```

预期：失败，当前每个 shard 只有 2 个 voter。

- [ ] **Step 3：修改默认配置**

将 `examples/distributed_lite/cluster_config.json` 改为：

```json
{
  "coordinator": {
    "host": "127.0.0.1",
    "port": 9100
  },
  "shard_count": 3,
  "nodes": [
    {
      "id": "node-1",
      "host": "127.0.0.1",
      "port": 9101
    },
    {
      "id": "node-2",
      "host": "127.0.0.1",
      "port": 9102
    },
    {
      "id": "node-3",
      "host": "127.0.0.1",
      "port": 9103
    }
  ],
  "shards": {
    "0": {
      "primary": "node-1",
      "replicas": ["node-2", "node-3"]
    },
    "1": {
      "primary": "node-2",
      "replicas": ["node-3", "node-1"]
    },
    "2": {
      "primary": "node-3",
      "replicas": ["node-1", "node-2"]
    }
  }
}
```

说明：

- 每个 shard 的 voters 都是 3 个节点。
- primary 仍轮转，方便初始选举 timeout 继续倾向原 primary。
- replicas 顺序用于展示和搜索候选顺序，不影响 Raft 多数派计算。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_cluster.py -q
```

预期：全部通过。

- [ ] **Step 5：提交**

```bash
git add examples/distributed_lite/cluster_config.json tests/typesense_lite/test_cluster.py
git commit -m "feat: use three raft voters per shard"
```

---

# 阶段 2：Leader peer progress 模型

## 任务 2.1：Leader 维护 next_index 和 match_index

**Files:**
- Modify: `src/typesense_lite/raft_runtime.py`
- Modify: `tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_runtime.py` 新增：

```python
@pytest.mark.asyncio
async def test_leader_initializes_peer_progress_after_election(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
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
    assert runtime.peer_progress["node-2"].next_index == 1
    assert runtime.peer_progress["node-2"].match_index == 0
    assert runtime.peer_progress["node-3"].next_index == 1
    assert runtime.peer_progress["node-3"].match_index == 0
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_initializes_peer_progress_after_election -q
```

预期：失败，`RaftRuntime` 当前没有 `peer_progress`。

- [ ] **Step 3：实现 peer progress**

在 `src/typesense_lite/raft_runtime.py` 增加：

```python
from dataclasses import dataclass


@dataclass
class PeerProgress:
    next_index: int
    match_index: int = 0
```

在 `RaftRuntime.__init__()` 增加：

```python
self.peer_progress: dict[str, PeerProgress] = {}
```

新增 helper：

```python
def _become_leader(self) -> None:
    self.core.role = RaftRole.LEADER
    self.core.leader_id = self.node_id
    next_index = self.core.last_log_index + 1
    self.peer_progress = {
        peer_id: PeerProgress(next_index=next_index)
        for peer_id in self.peer_urls
    }
```

将 `start_election()` 中：

```python
self.core.role = RaftRole.LEADER
self.core.leader_id = self.node_id
```

替换为：

```python
self._become_leader()
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_initializes_peer_progress_after_election -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: track raft peer progress"
```

---

# 阶段 3：统一复制函数和落后 follower 追日志

## 任务 3.1：实现 `_replicate_to_peer()`

**Files:**
- Modify: `src/typesense_lite/raft_runtime.py`
- Modify: `tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试：follower 从空日志追上 leader**

在 `tests/typesense_lite/test_raft_runtime.py` 新增：

```python
@pytest.mark.asyncio
async def test_heartbeat_catches_up_follower_with_missing_entries(tmp_path) -> None:
    follower_applied = []
    follower = RaftRuntime(
        node_id="node-2",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path / "follower",
        peer_urls={"node-1": "http://node-1", "node-3": "http://node-3"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: follower_applied.append(command) or {"ok": True},
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        if request.url.host == "node-2":
            payload = request.read()
            import json

            return httpx.Response(
                200,
                json=await follower.handle_append_entries(
                    json.loads(payload.decode("utf-8"))
                ),
            )
        return httpx.Response(503)

    leader = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path / "leader",
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )
    await leader.start_election()
    leader.core.log = [
        RaftLogEntry(
            index=1,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-1"},
            },
        ),
        RaftLogEntry(
            index=2,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-2"},
            },
        ),
    ]
    leader.core.state.commit_index = 2
    leader.core.state.last_applied = 2
    leader.peer_progress["node-2"].next_index = 1

    await leader.send_heartbeat()

    assert follower.state()["last_log_index"] == 2
    assert follower.state()["commit_index"] == 2
    assert follower.state()["last_applied"] == 2
    assert [command["document"]["id"] for command in follower_applied] == [
        "book-1",
        "book-2",
    ]
```

- [ ] **Step 2：运行测试确认失败**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_heartbeat_catches_up_follower_with_missing_entries -q
```

预期：失败，当前 heartbeat 只发空 entries。

- [ ] **Step 3：实现复制 helper**

在 `RaftRuntime` 增加：

```python
def _entries_from(self, start_index: int) -> list[RaftLogEntry]:
    return [entry for entry in self.core.log if entry.index >= start_index]
```

新增 `_replicate_to_peer()`：

```python
async def _replicate_to_peer(self, peer_id: str, peer_url: str) -> bool:
    progress = self.peer_progress.setdefault(
        peer_id,
        PeerProgress(next_index=self.core.last_log_index + 1),
    )

    while progress.next_index >= 1:
        prev_log_index = progress.next_index - 1
        payload = {
            "term": self.core.state.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": self._log_term(prev_log_index),
            "entries": [
                {
                    "index": entry.index,
                    "term": entry.term,
                    "command": entry.command,
                }
                for entry in self._entries_from(progress.next_index)
            ],
            "leader_commit": self.core.state.commit_index,
        }

        try:
            response = await self.client.post(
                f"{peer_url}/internal/raft/{self.shard_id}/append_entries",
                json=payload,
            )
            response.raise_for_status()
        except httpx.HTTPError:
            return False

        result = response.json()
        peer_term = int(result["term"])
        if peer_term > self.core.state.current_term:
            self.core.state.current_term = peer_term
            self.core.state.voted_for = None
            self.core.role = RaftRole.FOLLOWER
            self.core.leader_id = None
            self.storage.save_state(self.core.state)
            return False

        if result.get("success") is True:
            match_index = int(result["match_index"])
            progress.match_index = match_index
            progress.next_index = match_index + 1
            return True

        progress.next_index = max(1, progress.next_index - 1)

    return False
```

修改 `send_heartbeat()`：

```python
async def send_heartbeat(self) -> None:
    if self.core.role is not RaftRole.LEADER:
        return

    for peer_id, peer_url in self.peer_urls.items():
        await self._replicate_to_peer(peer_id, peer_url)
```

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_heartbeat_catches_up_follower_with_missing_entries -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: catch up followers from raft heartbeat"
```

---

# 阶段 4：写入复制使用 peer progress

## 任务 4.1：`submit_command()` 使用 `_replicate_to_peer()`

**Files:**
- Modify: `src/typesense_lite/raft_runtime.py`
- Modify: `tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试：一个 follower 离线仍可提交**

在 `tests/typesense_lite/test_raft_runtime.py` 新增：

```python
@pytest.mark.asyncio
async def test_three_voter_leader_commits_when_one_follower_is_down(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        if request.url.host == "node-2":
            return httpx.Response(
                200,
                json={"term": 1, "success": True, "match_index": 1},
            )
        return httpx.Response(503)

    applied = []
    runtime = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path,
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: applied.append(command) or {"ok": True},
    )
    await runtime.start_election()

    result = await runtime.submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    )

    assert result["ok"] is True
    assert result["commit_index"] == 1
    assert runtime.peer_progress["node-2"].match_index == 1
    assert runtime.peer_progress["node-3"].match_index == 0
    assert applied[0]["document"]["id"] == "book-1"
```

- [ ] **Step 2：运行测试确认当前行为**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_three_voter_leader_commits_when_one_follower_is_down -q
```

预期：如果当前旧实现已能提交，该测试可能通过；但改造后仍必须保持通过，并验证 `peer_progress`。

- [ ] **Step 3：重写 `submit_command()` 复制部分**

将 `submit_command()` 中手写 peer 循环替换为：

```python
self.core.log.append(entry)
self.storage.replace_log(self.core.log)

replicated = 1
for peer_id, peer_url in self.peer_urls.items():
    if await self._replicate_to_peer(peer_id, peer_url):
        if self.peer_progress[peer_id].match_index >= entry.index:
            replicated += 1

if replicated < self._majority():
    return {
        "ok": False,
        "error": "no raft majority available",
        "leader": self.node_id,
        "term": self.core.state.current_term,
    }

self.core.state.commit_index = entry.index
self._apply_committed_entries()
self.storage.save_state(self.core.state)
```

注意：leader 自己也应该通过 `_apply_committed_entries()` 应用命令，避免 leader 和 follower 应用逻辑分叉。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_three_voter_leader_commits_when_one_follower_is_down -q
```

预期：通过。

- [ ] **Step 5：运行现有 runtime 测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py -q
```

预期：全部通过。

- [ ] **Step 6：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: replicate writes with raft peer progress"
```

---

# 阶段 5：冲突日志回退和覆盖

## 任务 5.1：leader 遇到 AppendEntries mismatch 时回退 next_index

**Files:**
- Modify: `src/typesense_lite/raft_runtime.py`
- Modify: `tests/typesense_lite/test_raft_runtime.py`

- [ ] **Step 1：写失败测试**

在 `tests/typesense_lite/test_raft_runtime.py` 新增：

```python
@pytest.mark.asyncio
async def test_leader_repairs_conflicting_follower_log(tmp_path) -> None:
    follower_applied = []
    follower = RaftRuntime(
        node_id="node-2",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path / "follower",
        peer_urls={"node-1": "http://node-1", "node-3": "http://node-3"},
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(500))
        ),
        apply_command=lambda command: follower_applied.append(command) or {"ok": True},
    )
    follower.core.log = [
        RaftLogEntry(
            index=1,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-1"},
            },
        ),
        RaftLogEntry(
            index=2,
            term=99,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "wrong"},
            },
        ),
    ]
    follower.storage.replace_log(follower.core.log)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json={"term": 1, "vote_granted": True})
        if request.url.host == "node-2":
            import json

            return httpx.Response(
                200,
                json=await follower.handle_append_entries(
                    json.loads(request.content.decode("utf-8"))
                ),
            )
        return httpx.Response(503)

    leader = RaftRuntime(
        node_id="node-1",
        shard_id=0,
        members=["node-1", "node-2", "node-3"],
        data_dir=tmp_path / "leader",
        peer_urls={"node-2": "http://node-2", "node-3": "http://node-3"},
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        apply_command=lambda command: {"ok": True},
    )
    await leader.start_election()
    leader.core.log = [
        RaftLogEntry(
            index=1,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-1"},
            },
        ),
        RaftLogEntry(
            index=2,
            term=1,
            command={
                "type": "add_document",
                "collection": "books",
                "document": {"id": "book-2"},
            },
        ),
    ]
    leader.core.state.commit_index = 2
    leader.core.state.last_applied = 2
    leader.peer_progress["node-2"].next_index = 3

    await leader.send_heartbeat()

    assert [entry.term for entry in follower.core.log] == [1, 1]
    assert follower.core.log[1].command["document"]["id"] == "book-2"
    assert follower.state()["last_applied"] == 2
```

- [ ] **Step 2：运行测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_repairs_conflicting_follower_log -q
```

预期：如果阶段 3 的 `_replicate_to_peer()` 已包含 while 回退，该测试应通过；否则失败。

- [ ] **Step 3：补齐回退逻辑**

如果失败，确认 `_replicate_to_peer()` 包含：

```python
if result.get("success") is not True:
    progress.next_index = max(1, progress.next_index - 1)
    continue
```

该循环会先用 `prev_log_index=2` 失败，再回退到 `prev_log_index=1`，随后发送从 index 2 开始的 leader 日志，触发 follower 删除冲突 entry 并追加正确 entry。

- [ ] **Step 4：运行测试确认通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_runtime.py::test_leader_repairs_conflicting_follower_log -q
```

预期：通过。

- [ ] **Step 5：提交**

```bash
git add src/typesense_lite/raft_runtime.py tests/typesense_lite/test_raft_runtime.py
git commit -m "feat: repair conflicting raft follower logs"
```

---

# 阶段 6：节点恢复后的自动 catch-up 集成测试

## 任务 6.1：三节点中一个 follower 离线、恢复后自动追上

**Files:**
- Modify: `tests/typesense_lite/test_raft_integration.py`

- [ ] **Step 1：写集成测试**

在 `tests/typesense_lite/test_raft_integration.py` 新增：

```python
@pytest.mark.asyncio
async def test_recovered_follower_catches_up_after_missing_committed_writes(
    tmp_path,
) -> None:
    runtimes: dict[str, RaftRuntime] = {}
    applied: dict[str, list[dict]] = {
        "node-1": [],
        "node-2": [],
        "node-3": [],
    }
    stopped_nodes: set[str] = set()

    async def handler(request: httpx.Request) -> httpx.Response:
        target_node = request.url.host or ""
        if target_node in stopped_nodes:
            return httpx.Response(503)

        payload = json.loads(request.content.decode("utf-8"))
        runtime = runtimes[target_node]
        if request.url.path.endswith("/request_vote"):
            return httpx.Response(200, json=await runtime.handle_request_vote(payload))
        if request.url.path.endswith("/append_entries"):
            return httpx.Response(200, json=await runtime.handle_append_entries(payload))
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    members = ["node-1", "node-2", "node-3"]
    for node_id in members:
        runtimes[node_id] = RaftRuntime(
            node_id=node_id,
            shard_id=0,
            members=members,
            data_dir=tmp_path / node_id,
            peer_urls={
                peer_id: f"http://{peer_id}"
                for peer_id in members
                if peer_id != node_id
            },
            client=client,
            apply_command=lambda command, node_id=node_id: applied[node_id].append(command)
            or {"ok": True},
        )

    await runtimes["node-1"].start_election()
    stopped_nodes.add("node-3")

    first = await runtimes["node-1"].submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-1"},
        }
    )
    second = await runtimes["node-1"].submit_command(
        {
            "type": "add_document",
            "collection": "books",
            "document": {"id": "book-2"},
        }
    )

    assert first["ok"] is True
    assert second["ok"] is True
    assert runtimes["node-3"].state()["last_log_index"] == 0

    stopped_nodes.remove("node-3")
    await runtimes["node-1"].send_heartbeat()

    assert runtimes["node-3"].state()["last_log_index"] == 2
    assert runtimes["node-3"].state()["commit_index"] == 2
    assert runtimes["node-3"].state()["last_applied"] == 2
    assert [command["document"]["id"] for command in applied["node-3"]] == [
        "book-1",
        "book-2",
    ]
```

- [ ] **Step 2：运行测试确认失败或通过**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_integration.py::test_recovered_follower_catches_up_after_missing_committed_writes -q
```

预期：在阶段 3-4 完成后应通过；如果失败，说明 runtime catch-up 仍有缺口，回到 `_replicate_to_peer()` 修正。

- [ ] **Step 3：运行集成测试文件**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_raft_integration.py -q
```

预期：全部通过。

- [ ] **Step 4：提交**

```bash
git add tests/typesense_lite/test_raft_integration.py
git commit -m "test: cover raft follower catch up after recovery"
```

---

# 阶段 7：Cluster console 和 public 状态验证

## 任务 7.1：`/cluster/raft` 覆盖 3 voter 展示

**Files:**
- Modify: `tests/typesense_lite/test_server.py`

- [ ] **Step 1：写测试**

在 `tests/typesense_lite/test_server.py` 新增或扩展 cluster raft 测试，使用 1 shard 3 node 配置：

```python
THREE_VOTER_CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 1,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
        {"id": "node-3", "host": "127.0.0.1", "port": 9103},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]},
    },
}


def test_cluster_raft_endpoint_reports_three_voters(tmp_path) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        node_id = {
            9101: "node-1",
            9102: "node-2",
            9103: "node-3",
        }[request.url.port]
        role = "leader" if node_id == "node-1" else "follower"
        return httpx.Response(
            200,
            json={
                "node": node_id,
                "shard_id": 0,
                "role": role,
                "current_term": 3,
                "leader_id": "node-1",
                "commit_index": 7,
                "last_applied": 7,
            },
        )

    app = create_app(
        role="coordinator",
        cluster_config=THREE_VOTER_CONFIG,
        data_dir=tmp_path,
        coordinator_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    client = TestClient(app)

    response = client.get("/cluster/raft")

    assert response.status_code == 200
    shard = response.json()["shards"]["0"]
    assert shard["leader"] == "node-1"
    assert set(shard["members"]) == {"node-1", "node-2", "node-3"}
    assert shard["members"]["node-3"]["role"] == "follower"
```

- [ ] **Step 2：运行测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py::test_cluster_raft_endpoint_reports_three_voters -q
```

预期：通过。如果失败，检查 `ClusterMap.get_shard_voters()` 是否正确返回 `primary + replicas`。

- [ ] **Step 3：提交**

```bash
git add tests/typesense_lite/test_server.py
git commit -m "test: report three raft voters in cluster status"
```

---

# 阶段 8：文档和端到端验收

## 任务 8.1：更新 README

**Files:**
- Modify: `README.md`

- [ ] **Step 1：更新默认 Raft 说明**

将 README 中当前这段说明：

```text
默认配置中每个 shard 是 2 个 voter...
```

替换为：

```text
默认配置中每个 shard 是 3 个 voter，可以容忍任意 1 个 voter 暂时不可用。
当 follower 离线期间错过写入，恢复后 leader 会通过 heartbeat 自动推送缺失日志，
直到 follower 的 last_log_index、commit_index 和 last_applied 追上 leader。
```

- [ ] **Step 2：提交**

```bash
git add README.md
git commit -m "docs: document three voter raft cluster"
```

## 任务 8.2：完整自动化验证

- [ ] **Step 1：运行全量测试**

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
```

预期：全部通过。

- [ ] **Step 2：运行 ruff**

```bash
.venv/bin/python -m ruff check src/typesense_lite tests/typesense_lite examples/distributed_lite
```

预期：`All checks passed!`

## 任务 8.3：本地多进程手工验收

- [ ] **Step 1：启动干净集群**

```bash
DATA_DIR=$(mktemp -d /tmp/typesense-lite-raft-3voter.XXXXXX)
.venv/bin/python examples/distributed_lite/start_cluster.py --data-dir "$DATA_DIR"
```

预期输出包含：

```text
coordinator: http://127.0.0.1:9100
node-1: http://127.0.0.1:9101
node-2: http://127.0.0.1:9102
node-3: http://127.0.0.1:9103
```

- [ ] **Step 2：检查每 shard 都有 3 voter**

```bash
curl -s http://127.0.0.1:9100/cluster/raft
```

预期：

- `shards.0.members` 有 `node-1/node-2/node-3`。
- `shards.1.members` 有 `node-1/node-2/node-3`。
- `shards.2.members` 有 `node-1/node-2/node-3`。
- 每个 shard 有一个 `leader`。

- [ ] **Step 3：写入和搜索**

```bash
curl -s -X POST http://127.0.0.1:9100/collections/books/documents \
  -H 'Content-Type: application/json' \
  -d '{"id":"raft-3voter-1","title":"Raft three voter","body":"catch up test"}'

curl -s 'http://127.0.0.1:9100/collections/books/documents/search?q=raft&limit=10'
```

预期：

- 写入返回 `ok=true`。
- 搜索 `found >= 1`。

- [ ] **Step 4：模拟单节点停止**

在另一个 shell 找出并停止任意一个 data node，例如停止 `9102` 对应进程。然后继续写入：

```bash
curl -s -X POST http://127.0.0.1:9100/collections/books/documents \
  -H 'Content-Type: application/json' \
  -d '{"id":"raft-3voter-2","title":"Node down write","body":"majority still works"}'
```

预期：

- 写入仍返回 `ok=true`，因为每个 shard 还有 2/3 多数派。
- `/cluster/raft` 中对应节点显示 unavailable，其他节点继续有 leader。

- [ ] **Step 5：恢复节点并检查 catch-up**

重新启动刚才停止的 data node。等待 1-2 秒后：

```bash
curl -s http://127.0.0.1:9100/cluster/raft
```

预期：

- 恢复节点重新出现在 members 中。
- 对应 shard 的恢复节点 `last_log_index`、`commit_index`、`last_applied` 追上 leader。

当前 `start_cluster.py` 没有单独重启一个 node 的子命令。如果手工验收时不方便单独重启，可以先用集成测试覆盖恢复语义；后续再补一个 `examples/distributed_lite/start_node.py` 或给 `start_cluster.py` 增加 `--only-node` 参数。

- [ ] **Step 6：更新本计划验收记录并提交**

在本文件追加实际命令输出摘要：

```text
pytest: N passed
ruff: All checks passed
/cluster/raft: each shard has 3 members
manual write while one node down: ok=true
recovered follower: last_applied caught up
```

提交：

```bash
git add docs/superpowers/plans/2026-06-22-raft-3-voter-catchup.md
git commit -m "docs: record raft catch up validation"
```

---

## 实际验收记录

验收时间：2026-06-22

自动化验证：

```text
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
123 passed, 84 warnings in 0.84s

.venv/bin/python -m ruff check src/typesense_lite tests/typesense_lite examples/distributed_lite
All checks passed!
```

本地多进程验证：

- 使用临时数据目录 `/tmp/typesense-lite-raft-verify.mCNfds` 启动 `coordinator + node-1 + node-2 + node-3`。
- 初始 `/cluster/raft` 显示 shard 0/1/2 都有 3 个 member，初始 leader 分别是 `node-1/node-2/node-3`。
- 基线写入 `raft-base-1` 返回 `ok=true`，提交到 shard 0，`commit_index=1`。
- 停止 `node-2` 后，shard 1 从原 leader `node-2` 切换到 `node-3`，`node-1` 保持 follower，`node-2` 显示 unavailable。
- 故障期间写入 `raft-failover-1` 返回 `ok=true`，提交到 shard 1 新 leader `node-3`，`commit_index=1`。
- 搜索 `q=failover` 返回 `found=2`，结果包含 `raft-failover-1`。
- 使用同一数据目录重启 `node-2` 后，`/cluster/raft` 显示 shard 1 的 `node-2/node-3/node-1` 均达到 `commit_index=1`、`last_applied=1`，恢复节点已自动 catch up。

## 最终验收标准

- [x] 默认 `cluster_config.json` 每个 shard 有 3 个 voter。
- [x] 停止任意 1 个 data node 后，其余 2 个 voter 仍可形成多数派。
- [x] 旧 leader 停止后，剩余节点可选出新 leader。
- [x] leader 维护每个 follower 的 `next_index` 和 `match_index`。
- [x] heartbeat 不再只是空心跳；当 follower 落后时会推送缺失日志。
- [x] follower 离线期间错过多条 committed writes，恢复后能自动追上。
- [x] follower 有冲突日志时，leader 能回退 `next_index` 并覆盖冲突日志。
- [x] leader 只在多数派复制后推进 commit。
- [x] leader 和 follower 都通过统一的 `_apply_committed_entries()` 应用已提交日志。
- [x] `/cluster/raft` 能展示每个 shard 的 3 个 member。
- [x] `tests/typesense_lite` 全量通过。
- [x] ruff 检查通过。

## 风险和注意点

- 3 个节点、3 个 shard、每 shard 全员参与，会让每个 data node 持有全部 shard 的副本。这牺牲容量扩展性，但适合当前“单机多进程演示真实容错”的目标。
- 如果未来要体现数据容量扩展，需要增加更多节点，例如 5 个 node、每 shard 3 voter，让不同 shard 分布在不同三元组上。
- 当前 catch-up 采用简单递减 `next_index`，日志很长时效率较低；后续可通过 conflict term / conflict index 优化。
- 本阶段不做 snapshot，所以 follower 长时间离线后仍通过日志后缀追赶。
- 默认配置改为每 shard 全副本后，搜索任意节点都可能命中数据；但 coordinator 当前仍按 primary-first 搜索，这不影响正确性。
