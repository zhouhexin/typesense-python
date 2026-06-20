# Typesense Lite Raft 选举设计文档

## 目标

在当前单机多进程 Typesense Lite 分布式搜索 demo 上实现真正的 Raft 风格 leader 选举和基于 leader 的写入复制，让集群状态页面展示真实的选举状态，而不是静态 primary 配置。

本设计采用 **per-shard Raft group**：每个 shard 独立组成一个 Raft 复制组，每个复制组在该 shard 的 primary 和 replicas 之间选出 leader。coordinator 负责发现 shard leader 并把写入路由到 leader，但不再直接决定 primary。

## 当前架构现状

当前系统由一个 coordinator 和多个 data node 组成：

- coordinator 是唯一公开入口。
- data node 只提供内部 API。
- shard placement 来自静态 `cluster_config.json`。
- `ClusterMap.get_primary(shard_id)` 返回静态 primary。
- 写入先发到静态 primary，再复制到 replicas。
- 搜索优先静态 primary，失败后 fallback 到 replicas。
- `/cluster/health` 只做请求级健康检查。
- `/cluster-console` 当前展示静态 primary mode，不具备真实选举能力。

这个模型能演示分片、复制、健康状态和手动 repair，但不能在 primary 故障后自动产生新的主节点。

## 核心架构决策

### 1. 每个 shard 是一个独立 Raft group

每个 shard 的成员来自现有 shard placement：

```json
{
  "0": {"primary": "node-1", "replicas": ["node-2"]},
  "1": {"primary": "node-2", "replicas": ["node-3"]},
  "2": {"primary": "node-3", "replicas": ["node-1"]}
}
```

对于 shard `0`，Raft voters 是 `["node-1", "node-2"]`。原始 `primary` 只作为 bootstrap 的初始优先节点，不再永久决定 leader。

### 2. 不实现 Raft membership change

第一版不支持动态加减节点，不支持 joint consensus。Raft group 成员仍由静态 cluster config 决定。

原因：

- 当前项目没有动态扩容能力。
- membership change 会显著增加协议复杂度。
- 当前目标是让 leader 选举和复制语义真实可运行，而不是做完整生产级 Raft。

### 3. 写入必须经过 shard leader

coordinator 收到写入请求后：

1. 根据 document id 计算 shard id。
2. 查询或使用缓存的 shard leader。
3. 将写入转发给 leader 的内部 Raft 写接口。
4. leader 追加 Raft log。
5. leader 通过 AppendEntries 复制给 followers。
6. 达到多数派后提交。
7. leader 和 followers 将已提交 log 应用到本地 document store 和 index。

如果 coordinator 找不到 leader，公共写接口返回 `503`，并给出清晰错误。

### 4. 搜索先读 leader，允许降级读 follower

搜索不进入 Raft log。搜索路径：

1. coordinator 优先查询每个 shard 当前 leader。
2. 如果 leader 不可用，允许查询健康 follower。
3. 返回 warning，说明本次读可能来自 follower，存在短暂落后风险。

这样保持 demo 可用性，同时明确一致性语义。

### 5. Raft 状态存储在 data node 本地

每个 data node 对每个参与的 shard 持久化：

```text
.data/typesense_lite/
  raft/
    node-1/
      shard-0/
        state.json
        log.jsonl
      shard-2/
        state.json
        log.jsonl
```

`state.json` 保存：

- `current_term`
- `voted_for`
- `commit_index`
- `last_applied`

`log.jsonl` 每行保存一条 Raft log entry：

- `index`
- `term`
- `command`

角色 `follower/candidate/leader`、选举超时时间、heartbeat 时间是运行态内存状态，重启后重新以 follower 启动。

## Raft 状态模型

### Node role

每个 data node 在每个 shard group 内有一个角色：

- `follower`
- `candidate`
- `leader`

### Persistent state

```python
class RaftPersistentState(TypedDict):
    current_term: int
    voted_for: str | None
    commit_index: int
    last_applied: int
```

### Log entry

```python
class RaftLogEntry(TypedDict):
    index: int
    term: int
    command: dict[str, Any]
```

### Command 类型

第一版只复制 document 写路径：

```json
{
  "type": "add_document",
  "collection": "books",
  "document": {"id": "book-1", "title": "Distributed search"}
}
```

```json
{
  "type": "update_document",
  "collection": "books",
  "document_id": "book-1",
  "changes": {"title": "New title"}
}
```

```json
{
  "type": "delete_document",
  "collection": "books",
  "document_id": "book-1"
}
```

## 内部 Raft API

这些接口只暴露在 data node 上。

### 获取 Raft 状态

```http
GET /internal/raft/{shard_id}/state
```

响应：

```json
{
  "node": "node-1",
  "shard_id": 0,
  "role": "leader",
  "current_term": 3,
  "voted_for": "node-1",
  "leader_id": "node-1",
  "commit_index": 12,
  "last_applied": 12,
  "last_log_index": 12,
  "last_log_term": 3,
  "members": ["node-1", "node-2"],
  "last_heartbeat_at": "2026-06-20T10:15:30Z"
}
```

### RequestVote

```http
POST /internal/raft/{shard_id}/request_vote
```

请求：

```json
{
  "term": 4,
  "candidate_id": "node-2",
  "last_log_index": 12,
  "last_log_term": 3
}
```

响应：

```json
{
  "term": 4,
  "vote_granted": true
}
```

### AppendEntries

用于 heartbeat 和 log replication。

```http
POST /internal/raft/{shard_id}/append_entries
```

请求：

```json
{
  "term": 4,
  "leader_id": "node-2",
  "prev_log_index": 12,
  "prev_log_term": 3,
  "entries": [
    {
      "index": 13,
      "term": 4,
      "command": {
        "type": "add_document",
        "collection": "books",
        "document": {"id": "book-1", "title": "Distributed search"}
      }
    }
  ],
  "leader_commit": 12
}
```

响应：

```json
{
  "term": 4,
  "success": true,
  "match_index": 13
}
```

### Leader 写入入口

```http
POST /internal/raft/{shard_id}/commands
```

请求：

```json
{
  "type": "add_document",
  "collection": "books",
  "document": {"id": "book-1", "title": "Distributed search"}
}
```

leader 响应：

```json
{
  "ok": true,
  "shard_id": 0,
  "leader": "node-2",
  "term": 4,
  "commit_index": 13,
  "result": {"id": "book-1"}
}
```

非 leader 响应：

```json
{
  "ok": false,
  "error": "not leader",
  "leader_id": "node-2",
  "term": 4
}
```

HTTP 状态码使用 `409 Conflict`，coordinator 收到后刷新 leader 缓存并重试一次。

## Coordinator 行为变更

### Leader discovery

coordinator 新增 `LeaderDirectory`：

- 查询每个 candidate 的 `/internal/raft/{shard_id}/state`。
- 优先使用返回 `role=leader` 的节点。
- 如果没有明确 leader，但某个 follower 返回 `leader_id`，再查询该 node。
- 缓存每个 shard 的 leader，缓存带 term。
- 写入遇到 `409 not leader` 或连接失败时刷新缓存。

### 写入路径

`Coordinator.add_document()` 改为：

1. 验证 document id。
2. 计算 shard id。
3. 调用 leader directory 获取 leader。
4. 调用 leader 的 `/internal/raft/{shard_id}/commands`。
5. 返回 leader、term、commit index。

`update_document()` 和 `delete_document()` 同样走 Raft command。

### Import 和 upload

批量导入仍逐条调用 public document write。每条文档独立提交 Raft log。第一版不实现批量 command。

### Read/search

搜索使用 leader directory 排序 candidate：

1. leader
2. followers

如果查询 follower 成功，响应中加入 warning：

```json
"warnings": ["searched follower node-2 because leader node-1 failed"]
```

## Data node 行为变更

### Raft runtime

data node 启动时：

1. 读取 cluster config。
2. 判断自己参与哪些 shard。
3. 为每个参与 shard 创建一个 Raft group runtime。
4. 从本地文件恢复 persistent state 和 log。
5. 启动 background election loop。

### Election loop

每个 shard runtime 独立运行：

- follower 超过 election timeout 没收到 heartbeat，变 candidate。
- candidate 增加 term，投自己一票。
- candidate 向其他 voters 发送 RequestVote。
- 获得多数票后变 leader。
- leader 周期性发送 AppendEntries heartbeat。
- 任意节点收到更高 term 请求时降级 follower。

第一版使用 deterministic timeout 参数，但每个 node 加入少量 jitter，避免永久平票。

### Apply command

已提交 log entry 应用到现有 `SearchNode`：

- `add_document` -> `node.add_document()`
- `update_document` -> `node.update_document()`
- `delete_document` -> `node.delete_document()`

应用必须按 log index 顺序执行。

## 公共 API 变更

### 新增 Raft 状态接口

coordinator 暴露：

```http
GET /cluster/raft
```

响应聚合所有 shard/member 的 Raft 状态：

```json
{
  "shards": {
    "0": {
      "leader": "node-2",
      "term": 4,
      "members": {
        "node-1": {"role": "follower", "term": 4, "commit_index": 13},
        "node-2": {"role": "leader", "term": 4, "commit_index": 13}
      }
    }
  }
}
```

### `/cluster-console`

页面从静态 primary mode 升级为真实 Raft 状态：

- 每个 shard 展示 leader。
- 展示每个 member 的 role、term、commit index、last applied。
- 展示 `healthy/degraded/unavailable/no leader`。
- 展示最近 heartbeat。
- 保留 consistency check 和 repair。

## 故障场景

### Leader 停止

1. follower 超时后发起新一轮 election。
2. 获得多数票的 follower 成为新 leader。
3. coordinator 下一次写入刷新 leader cache。
4. cluster console 展示 leader 变化和 term 增加。

### 少数派不可用

如果 leader 仍能获得多数派确认，写入继续成功。不可用 follower 恢复后通过 AppendEntries 补日志。

### 多数派不可用

无法提交写入。公共写接口返回 `503`：

```json
{
  "ok": false,
  "error": "no raft majority available",
  "shard_id": 0,
  "leader": "node-1",
  "term": 4
}
```

### Log 冲突

follower 收到 AppendEntries 时：

- 如果 `prev_log_index` 不存在，返回 `success=false`。
- 如果 `prev_log_term` 不匹配，删除本地冲突 entry 及其后续 entries。
- leader 降低该 follower 的 `next_index` 并重试。

## 测试策略

### 单元测试

- Raft state 持久化。
- RequestVote term 和 log freshness 判断。
- AppendEntries heartbeat。
- AppendEntries log conflict resolution。
- majority commit。
- apply committed command。

### 集成测试

- 三节点单 shard 选出 leader。
- leader 停止后 follower 选出新 leader。
- 写入通过 leader 复制到多数派。
- coordinator 能发现 leader 并写入。
- coordinator 遇到 stale leader cache 后刷新。
- cluster console 页面包含真实 Raft 状态入口。

### 手工验证

启动本地集群：

```bash
.venv/bin/python examples/distributed_lite/start_cluster.py
```

访问：

```text
http://127.0.0.1:9100/cluster-console
```

停掉某个 leader node 后观察：

- `/cluster/raft` 中 term 增加。
- shard leader 变更。
- 新写入能路由到新 leader。

## 非目标

第一版不做：

- Raft membership change。
- Snapshot / log compaction。
- Linearizable read index。
- 跨 shard 事务。
- 多 coordinator 高可用。
- 网络分区下的生产级运维工具。

这些可以作为后续阶段。

