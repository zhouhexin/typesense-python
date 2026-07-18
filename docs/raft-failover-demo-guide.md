# Typesense Lite 节点 Raft 故障转移演示指南

## 1. 演示目标

本指南用于演示 Typesense Lite 在单机多进程环境中的真实 Raft Leader
故障转移过程。默认集群包含：

- 1 个 Coordinator，监听 `127.0.0.1:9100`；
- 3 个 Data Node，分别监听 `127.0.0.1:9101`、`9102`、`9103`；
- 每个 shard 有 3 个 voter，可以容忍其中 1 个 voter 宕机；
- Leader 宕机后，剩余两个 voter 形成多数派并选出新 Leader；
- Coordinator 重新发现 Leader 后，写入和搜索可以继续执行。

演示将验证以下完整流程：

1. 查看宕机前的集群健康和 Raft Leader。
2. 强制终止一个真实 Leader 进程。
3. 观察旧 Leader 下线和新 Leader 选举。
4. 通过新 Leader 写入测试文档。
5. 搜索该文档，确认服务在单节点故障后仍可用。

## 2. 演示前准备

### 2.1 进入项目目录

打开终端并执行：

```bash
cd /Users/zhouhexin/PycharmProjects/typesense-python
```

### 2.2 确认 Python 环境和依赖

```bash
.venv/bin/python --version
.venv/bin/python -c "import fastapi, httpx, uvicorn; print('dependencies OK')"
```

预期输出包含：

```text
dependencies OK
```

如果 `.venv` 不存在，先创建环境并安装依赖：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 2.3 检查演示端口

```bash
lsof -nP -iTCP:9100-9103 -sTCP:LISTEN
```

首次启动前，该命令应当没有输出。如果已有旧进程占用端口，应先正常停止
旧集群，避免演示脚本定位到无关进程。

### 2.4 使用独立演示数据目录

本指南统一使用以下目录：

```text
/tmp/typesense-lite-class-demo
```

这样可以避免已有开发数据影响课堂演示。需要完全重新演示时，应先停止整个
集群，再删除该临时目录：

```bash
rm -rf /tmp/typesense-lite-class-demo
```

不要在集群仍然运行时删除数据目录。

## 3. 启动默认三节点集群

打开“终端 1”，执行：

```bash
cd /Users/zhouhexin/PycharmProjects/typesense-python

PYTHONPATH=src python \
  examples/distributed_lite/start_cluster.py \
  --data-dir /tmp/typesense-lite-class-demo
```

该命令会启动 4 个进程：

| 角色 | 地址 | 用途 |
| --- | --- | --- |
| Coordinator | `http://127.0.0.1:9100` | 接收公共 API 和前端请求 |
| Node 1 | `http://127.0.0.1:9101` | Data Node 和 Raft voter |
| Node 2 | `http://127.0.0.1:9102` | Data Node 和 Raft voter |
| Node 3 | `http://127.0.0.1:9103` | Data Node 和 Raft voter |

保持终端 1 运行，不要关闭或按 `Ctrl+C`。

## 4. 确认集群已经就绪

打开“终端 2”，执行以下检查。

### 4.1 检查三个 Data Node

```bash
curl -s http://127.0.0.1:9101/health | python3 -m json.tool
curl -s http://127.0.0.1:9102/health | python3 -m json.tool
curl -s http://127.0.0.1:9103/health | python3 -m json.tool
```

三个响应都应包含：

```json
{
  "ok": true
}
```

响应还可能包含 `status`、`ready` 和 recovery 信息。

### 4.2 检查集群健康状态

```bash
curl -s http://127.0.0.1:9100/cluster/health | python3 -m json.tool
```

正常情况下，顶层应显示：

```json
{
  "status": "healthy",
  "ok": true
}
```

每个 shard 的状态应为 `healthy`，三个 voter 应处于 ready 状态。

### 4.3 查看当前 Raft Leader

```bash
curl -s http://127.0.0.1:9100/cluster/raft | python3 -m json.tool
```

记录任意一个 shard 的以下字段：

- `leader`：当前 Leader 节点 ID；
- `term`：当前 Raft term；
- `members`：三个 voter 的角色和日志进度；
- `commit_index` 和 `last_applied`：已提交和已应用位置。

不同运行中选出的 Leader 可能不同，这是正常现象。

## 5. 打开可视化控制台

在浏览器中打开：

```text
http://127.0.0.1:9100/cluster-console
```

重点观察以下区域：

- Cluster Health；
- Election Status；
- Raft Status；
- 每个 shard 的 Leader、term、member role；
- `last_log_index`、`commit_index`、`last_applied`；
- recovery status 和 ready 状态。

故障发生后，可以刷新页面或使用页面中的刷新操作查看最新状态。

## 6. 执行 Leader 宕机演示

项目提供了专用脚本：

```text
examples/distributed_lite/failover_demo.py
```

脚本会自动完成以下操作：

1. 调用 `/cluster/raft` 查找一个真实 Leader；
2. 根据节点端口定位对应进程 PID；
3. 使用 `SIGKILL` 模拟进程突然宕机；
4. 等待同一 shard 选出不同的新 Leader；
5. 通过 Coordinator 写入一条测试文档；
6. 搜索测试文档并验证结果。

### 6.1 先执行安全预览

在终端 2 中执行：

```bash
cd /Users/zhouhexin/PycharmProjects/typesense-python

PYTHONPATH=src python \
  examples/distributed_lite/failover_demo.py
```

此时脚本不会终止进程，只会输出目标。例如：

```text
target shard=0 leader=node-2 pid=12345
dry run; pass --yes to terminate the leader
```

确认输出中的 PID 对应 `9101-9103` 中的一个节点端口后，再执行真实故障。

### 6.2 强制终止 Leader

```bash
PYTHONPATH=src python \
  examples/distributed_lite/failover_demo.py \
  --yes
```

`--yes` 表示确认允许脚本发送 `SIGKILL`。不要在同一端口运行其他重要服务。

### 6.3 预期结果

脚本成功时会输出写入响应，并以类似内容结束：

```text
PASS old_leader=node-2 new_leader=node-1
```

具体节点 ID 由当时的选举结果决定，可能与示例不同。

该输出证明：

- 原 Leader 进程已经停止；
- 剩余两个 voter 形成了多数派；
- 集群选出了不同的新 Leader；
- Coordinator 已发现新 Leader；
- 新 Leader 成功提交写入；
- 新写入的文档可以通过分布式搜索查询到。

## 7. 观察故障转移结果

### 7.1 查看 Raft 状态

```bash
curl -s http://127.0.0.1:9100/cluster/raft | python3 -m json.tool
```

与宕机前对比：

- `leader` 应从旧节点变为新节点；
- `term` 通常会增加；
- 旧 Leader 对应 member 应显示不可用或请求错误；
- 新 Leader 的 `role` 应为 `leader`；
- 仍在线的 follower 应继续同步日志。

### 7.2 查看集群健康状态

```bash
curl -s http://127.0.0.1:9100/cluster/health | python3 -m json.tool
```

单节点宕机后，集群通常会显示 `degraded`，但 3 voter 中仍有 2 个 ready voter，
因此仍具备多数派，写入可以继续。

### 7.3 验证搜索服务

脚本已经自动搜索写入的测试文档。也可以在浏览器打开：

```text
http://127.0.0.1:9100/search
```

使用以下关键词搜索：

```text
leader failover
```

结果中应包含脚本写入的 `Raft failover` 文档。

## 8. 课堂讲解建议

演示时可以按照以下顺序说明：

1. 当前系统不是简单地把固定 primary 写在配置里，而是每个 shard 运行独立
   的 Raft group。
2. 每个 shard 有 3 个 voter，多数派数量为 2。
3. Leader 宕机后，剩余两个 voter 仍能形成多数派并进行新一轮选举。
4. Coordinator 不缓存失效 Leader；写入失败后会使缓存失效并重新发现 Leader。
5. 新 Leader 产生后，写入通过 Raft 日志复制并在多数派确认后提交。
6. 搜索请求仍由 Coordinator 按 shard 聚合，因此调用方不需要知道 Leader
   已经变化。
7. 如果再终止第二个 voter，集群将失去多数派。读取可能按可用副本降级，
   但新的 Raft 写入不能安全提交。

## 9. 演示结束后恢复集群

`failover_demo.py` 不会自动重启被终止的 Leader。可以只恢复宕机节点，也可以
停止剩余进程后重新启动整个演示集群。

### 9.1 只恢复宕机节点

假设被终止的是 `node-1`，并且集群启动时使用的数据目录是
`/tmp/typesense-lite-class-demo`，在新终端中执行：

```bash
PYTHONPATH=src python \
  examples/distributed_lite/recover_node.py \
  --node-id node-1 \
  --data-dir /tmp/typesense-lite-class-demo
```

脚本会执行以下检查：

1. 从集群配置读取 `node-1` 对应的地址和端口；
2. 拒绝在端口已被其他服务占用时重复启动；
3. 使用原数据目录启动节点，保留原有文档、Raft term 和日志；
4. 轮询 `/internal/recovery/state`，等待所有 shard 状态变为 `healthy`；
5. 输出每个 shard 的角色、Leader、日志索引、提交索引和应用索引。

看到 `node-1 recovered and is serving` 表示恢复完成。该脚本以前台方式保持
节点运行，因此演示期间需要保持此终端打开。按 `Ctrl+C` 可以停止恢复节点。

如果宕机的是其他节点，将 `--node-id` 改为 `node-2` 或 `node-3`。如果追赶
时间超过默认的 60 秒，可以增加 `--timeout 120`。

### 9.2 Rebuild 副本重建功能说明

Cluster Console 中某个 Raft member 后面的 `Rebuild` 表示“重建该节点上的
这个 shard 副本”。它不是重启节点进程，也不会重建整个集群。

#### 9.2.1 Rebuild 的执行流程

点击 `Rebuild` 后，Coordinator 会执行以下步骤：

1. 确认目标 member 是当前 shard 的 follower voter；
2. 重新发现该 shard 当前真正的 Raft Leader；
3. 从 Leader 导出该 shard 的完整数据快照和已提交日志；
4. 校验快照中的 shard ID、来源节点、Leader 角色、term、snapshot index
   和 SHA-256 checksum；
5. 将目标 follower 标记为 `rebuilding`，暂时不让它承接正常查询；
6. 原子替换 follower 上该 shard 的本地集合和文档数据；
7. 将快照之后产生的增量写入继续通过 Raft 日志同步到 follower；
8. 完成校验和日志追赶后，将副本恢复为 `healthy` 和 `ready`。

系统禁止对当前 Leader 执行 Rebuild，因为 Leader 是快照来源，也是该 shard
当前已提交状态的权威副本。如果页面中的角色刚刚发生变化，应先刷新页面，
确认目标仍然是 follower 后再操作。

#### 9.2.2 什么时候需要 Rebuild

以下情况适合使用 Rebuild：

- follower 的本地 shard 数据目录丢失或损坏；
- follower 与 Leader 的文档 checksum 不一致；
- follower 长时间停留在 `catching_up`，日志索引不再前进；
- Leader 已经缺少 follower 所需的旧日志，仅靠 AppendEntries 无法补齐；
- 手动检查确认某个 follower 副本内容不完整，需要从 Leader 整体重建。

节点只是短暂离线时，不要立即执行 Rebuild。使用恢复脚本重新启动节点后，
Raft Leader 会优先自动推送缺失日志。只有自动 catch-up 超时、失败或数据校验
不一致时，才需要执行 Rebuild。

#### 9.2.3 Rebuild、Repair 和恢复脚本的区别

| 操作 | 作用范围 | 主要用途 | 能否启动宕机进程 |
|---|---|---|---|
| `recover_node.py` | 一个 Data Node 进程 | 使用原数据目录重新启动宕机节点，并等待自动 catch-up | 可以 |
| `Repair` | 某个 collection 中缺失的文档 | 根据文档 ID 补齐普通副本差异 | 不可以 |
| `Rebuild` | 某个节点上的整个 shard 副本 | 从当前 Leader 安装完整快照，再追赶增量日志 | 不可以 |

如果目标节点的进程已经宕机，Coordinator 无法调用其内部重建接口，此时直接
点击 Rebuild 会失败。正确顺序是：

1. 使用 `recover_node.py --node-id <节点>` 启动宕机节点；
2. 在 Cluster Console 中等待节点从 `recovering` 或 `catching_up` 变为
   `healthy`；
3. 如果长时间无法恢复，确认目标 member 是 follower；
4. 点击该 follower 对应的 `Rebuild`；
5. 刷新页面，观察状态依次经过 `rebuilding`、`validating`，最后变为
   `healthy / ready`；
6. 对照 Leader 和 follower 的 `last log`、`commit`、`last applied`，确认
   三个索引已经追平。

也可以不通过页面，直接调用 Coordinator 接口。例如重建 shard 0 上的
`node-3` follower：

```bash
curl -X POST \
  http://127.0.0.1:9100/cluster/shards/0/members/node-3/rebuild
```

执行前必须通过 `/cluster/raft` 确认 `node-3` 不是 shard 0 当前的 Leader。

### 9.3 停止剩余进程

回到终端 1，按：

```text
Ctrl+C
```

启动器会停止仍在运行的 Coordinator 和 Data Node。

### 9.4 确认端口已经释放

```bash
lsof -nP -iTCP:9100-9103 -sTCP:LISTEN
```

该命令应没有输出。

### 9.5 重新启动完整集群

```bash
PYTHONPATH=src .venv/bin/python \
  examples/distributed_lite/start_cluster.py \
  --data-dir /tmp/typesense-lite-class-demo
```

使用同一个数据目录可以继续演示节点恢复和日志 catch-up。需要从全新状态开始
时，先停止集群，再删除数据目录并重新启动。

## 10. 常见问题排查

### 10.1 `no process is listening on port`

原因：默认三节点集群没有启动完成，或者配置端口与运行端口不一致。

处理：

```bash
lsof -nP -iTCP:9100-9103 -sTCP:LISTEN
curl -s http://127.0.0.1:9100/cluster/raft | python3 -m json.tool
```

确认 `9100-9103` 都有对应进程后重试。

### 10.2 `timed out waiting for new Raft leader`

可能原因：

- 除目标 Leader 外，另一个 voter 也不可用；
- 集群尚未完成初始选举；
- 端口被其他进程占用；
- 默认配置被修改，不再是每 shard 3 voter。

处理：停止整个集群，确认端口释放，然后使用默认配置重新启动。

也可以延长等待时间：

```bash
PYTHONPATH=src python \
  examples/distributed_lite/failover_demo.py \
  --timeout 30 \
  --yes
```

### 10.3 `no raft leader for shard`

初始选举可能尚未完成。等待数秒后检查：

```bash
curl -s http://127.0.0.1:9100/cluster/raft | python3 -m json.tool
```

确认目标 shard 的 `leader` 不为空后再运行演示脚本。

### 10.4 端口已被占用

```bash
lsof -nP -iTCP:9100-9103 -sTCP:LISTEN
```

先确认占用进程是否属于旧的 Typesense Lite 集群。不要直接终止无法确认归属的
进程；优先回到旧集群的启动终端使用 `Ctrl+C` 正常停止。

### 10.5 页面没有立即显示新 Leader

先点击 Cluster Console 的刷新操作，再直接调用状态接口确认：

```bash
curl -s http://127.0.0.1:9100/cluster/raft | python3 -m json.tool
```

接口已经显示新 Leader 而页面仍是旧数据时，刷新浏览器页面。

### 10.6 所有节点都显示 `502 Bad Gateway`

如果 Cluster Console 中三个 Data Node 同时显示 `502 Bad Gateway`，并且终端
持续输出对 `127.0.0.1:9101-9103` 的请求失败，通常是启动终端设置了
`HTTP_PROXY`、`HTTPS_PROXY` 或 `ALL_PROXY`，导致本地集群请求被发送给代理。

当前实现的内部集群客户端默认忽略环境代理，本地启动器也会设置 `NO_PROXY`。
应用修改后必须停止并重新启动整个集群，已有进程不会自动更新 HTTP 客户端。

仍需手动排查时执行：

```bash
env | grep -i proxy
curl --noproxy '*' -s http://127.0.0.1:9101/health | python3 -m json.tool
```

临时启动前也可以设置：

```bash
export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"
```

## 11. 安全注意事项

- `failover_demo.py --yes` 会真实发送 `SIGKILL`，只应在本地演示集群中运行；
- 演示前必须先执行不带 `--yes` 的 dry run；
- 一次只终止一个 voter；
- 不要在集群运行期间删除数据目录；
- 不要把该脚本用于生产服务器；
- 演示结束后检查 `9100-9103`，确保没有遗留进程。

## 12. 最终验收清单

正式展示前逐项确认：

- [ ] Coordinator 和三个 Data Node 均已启动；
- [ ] 三个 Data Node 的 `/health` 返回 `ok: true`；
- [ ] `/cluster/health` 初始状态为 `healthy`；
- [ ] `/cluster/raft` 中每个 shard 都有 Leader；
- [ ] Cluster Console 可以正常打开；
- [ ] dry run 输出的 PID 属于 `9101-9103`；
- [ ] `--yes` 执行后出现不同的新 Leader；
- [ ] 故障后仍可写入文档；
- [ ] 故障后可以搜索到新写入文档；
- [ ] 演示结束后已停止所有进程。
