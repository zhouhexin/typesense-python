# gaojios 功能选择性合并实施方案

> 日期：2026-07-17  
> 基准项目：`/Users/zhouhexin/PycharmProjects/typesense-python`  
> 功能来源：`/Users/zhouhexin/PycharmProjects/gaojios`  
> 执行原则：以基准项目的架构、接口和已有功能为准，只选择性吸收 `gaojios` 的新增能力，任何阶段都不得造成现有功能回退。

## 1. 背景与目标

当前基准项目已经具备一套可运行的分布式搜索引擎实验系统，包括：

- 单机多进程、多端口集群模拟；
- coordinator 与 data node 的职责拆分；
- collection 和 document 的创建、查询、更新、删除；
- TXT、DOCX 文件上传和批量导入；
- 中文关键词切分与基础倒排索引搜索；
- 每个 shard 三 voter 的 Raft 选举、日志复制、单节点故障转移和恢复后自动 catch-up；
- shard health、leader、选举、复制进度、一致性检查和手动修复页面；
- 真实多机器 IP、节点注册、心跳发现、配置拉取、跨机器 HTTP 重试和部署脚本；
- Search、Admin、Cluster Console 三个独立前端页面。

`gaojios` 在同一代码基础上增加了高级查询、节点恢复状态、快照导出、副本完整性校验和 leader-aware 副本重建。此次工作要将这些能力合并到基准项目，同时完整保留基准项目已有的单机、多机、Raft、文档管理和前端能力。

最终目标不是将两个目录机械覆盖，而是形成一套统一实现：

1. 搜索接口兼容原有基础查询，并支持字段权重、前缀、容错、过滤、排序、分页、聚合和高亮。
2. Raft 节点可以报告真实恢复阶段，区分进程存活、节点可服务和副本已追平。
3. follower 无法只靠日志追赶恢复时，可以从当前 leader 获取并安装 shard 快照。
4. Cluster Console 可以观察恢复状态、完整性结果并触发受控重建。
5. 单机模拟和真实多机部署都使用同一套能力，不引入仅在 loopback 环境有效的实现。

## 2. 基线与零回退约束

### 2.1 合并前基线

基准提交：

```text
2f4e305 repair 支持真实多机器部署（支持多机器 IP、配置分发、节点启动、节点发现、跨机器通信和部署脚本）
```

2026-07-17 实测结果：

```text
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
178 passed, 7 skipped
```

静态检查基线存在 5 个历史问题，均为未使用导入或变量：

- `examples/distributed_lite/start_cluster.py`：未使用 `sys`；
- `scripts/deploy_cluster.py`：未使用 `shlex`；
- `scripts/smoke_test.py`：未使用 `signal`；
- `src/typesense_lite/inventory.py`：未使用 `field`；
- `tests/typesense_lite/test_multi_machine.py`：未使用局部变量 `response`。

这些问题不是本次合并产生，但最终验收前应清理，使静态检查达到零错误。

### 2.2 不允许回退的能力

以下能力属于强制兼容清单。任何一项失败，都视为当前阶段未完成：

| 类别 | 必须保留的行为 |
| --- | --- |
| 基础搜索 | 原有 `q`、`limit` 调用仍能返回相同结构的命中结果；中文内容仍可检索 |
| 文档管理 | collection/document 的创建、列表、详情、更新、删除保持可用 |
| 文件导入 | TXT、DOCX 上传和现有批量导入接口保持可用 |
| Raft | 每 shard 三 voter、真实选举、多数派提交、leader 故障转移、follower catch-up 保持可用 |
| 集群管理 | health、Raft 状态、一致性检查和现有手动 repair 接口保持可用 |
| 单机运行 | `examples/distributed_lite/start_cluster.py` 的单机多进程启动方式保持可用 |
| 多机运行 | inventory、部署脚本、外部 IP、节点注册、心跳、配置拉取和跨机重试保持可用 |
| 前端 | `/`、`/admin`、`/cluster-console` 均保持可访问，现有按钮和工作流不得丢失 |
| API 兼容 | 已存在的 URL、HTTP method、必填参数和主要响应字段不得删除或改名 |

### 2.3 每阶段强制质量门槛

每个阶段必须依次通过：

1. 本阶段新增的单元测试和接口测试。
2. `tests/typesense_lite` 全量测试，结果不得少于基线已有测试，且不得新增非预期 skip。
3. 受影响的前端页面路由和关键 DOM 测试。
4. Ruff 静态检查；历史问题可以在阶段 1 清理，之后保持零错误。
5. 对涉及 Raft、恢复或重建的阶段，执行三节点集成测试，验证单节点离线和恢复。
6. 对涉及 server/coordinator 的阶段，检查多机模块仍被调用，防止被 `gaojios` 的旧版实现覆盖。

如果一个阶段未通过以上门槛，应只修复该阶段，不进入后续阶段，也不通过删除测试、放宽断言或隐藏异常来制造通过结果。

## 3. 差异分析与合并边界

### 3.1 基准项目独有且必须保留

以下模块由基准项目负责真实多机部署和节点发现，`gaojios` 中不存在，禁止删除：

- `src/typesense_lite/deploy_plan.py`
- `src/typesense_lite/http_client.py`
- `src/typesense_lite/inventory.py`
- `src/typesense_lite/node_directory.py`
- `src/typesense_lite/node_registrar.py`
- `src/typesense_lite/run_node.py`
- `scripts/deploy_cluster.py`
- `scripts/smoke_test.py`

以下基准实现也必须保留并与新增功能组合：

- `server.create_app()` 的 `bind_host`、`bind_port`、`coordinator_url`、`coordinator_instance` 参数；
- data node 启动时向 coordinator 注册和发送 heartbeat；
- data node 通过 `COORDINATOR_URL` 拉取集群配置；
- `coordinator.py` 中的 `NodeDirectory`、跨机器 client 和 HTTP 重试包装；
- `/cluster/nodes`、`/internal/cluster/nodes/register`、`/internal/cluster/nodes/{node_id}/heartbeat`；
- 当前 `start_cluster.py` 基于 `RunNodeSpec` 的启动逻辑。

### 3.2 从 gaojios 选择性吸收

| 能力 | 主要来源文件 | 合并方式 |
| --- | --- | --- |
| 高级查询和 BM25 评分 | `index.py` | 移植查询模型和算法，保留旧 `search()` 入口 |
| 查询参数传递与结果聚合 | `node.py`、`coordinator.py`、`server.py` | 在现有多机调用链上扩展参数，不覆盖网络层 |
| 高级搜索前端 | `web/search.html`、`web/admin.html` | 合并控件和展示，保留现有页面功能 |
| 恢复状态模型 | `recovery.py`、`raft_runtime.py` | 新增模型并嵌入现有三 voter runtime |
| 快照和校验 | `rebuild.py`、`storage.py`、`node.py` | 新增快照协议和原子替换能力 |
| leader-aware 重建 | `server.py`、`coordinator.py` | 在当前 leader 发现和跨机 client 上实现 |
| 重建演示 | `examples/distributed_lite/*.py` | 新增演示文件，不替换当前启动器 |

### 3.3 明确不直接复制的内容

- 不整体替换 `server.py`，因为会丢失多机配置拉取、节点注册和 heartbeat。
- 不整体替换 `coordinator.py`，因为会丢失 `NodeDirectory` 和跨机器重试。
- 不替换 `start_cluster.py`，因为 `gaojios` 版本缺少当前真实多机抽象。
- 不整体替换 README，只增量补充高级搜索、恢复和重建说明。
- 不把 `/health` 改成单纯的 readiness 接口。部署脚本依赖它判断进程存活；新增 `ready`、`status`、`error` 字段或独立恢复接口表达可服务状态。
- 不改变静态 Raft membership，不在本次工作中实现 joint consensus、动态扩缩容或 coordinator 高可用。

## 4. 合并后的总体架构

### 4.1 搜索请求链路

```text
Web Search / API Client
        |
        v
Coordinator public search API
        |
        +-- 解析并校验高级查询参数
        +-- 按 shard 选择可用副本
        +-- 通过当前跨机 HTTP client 并发 fan-out
        |
        v
Data Node InvertedIndex.search_query()
        |
        +-- 分词、前缀和 typo 候选
        +-- 字段权重与 BM25 评分
        +-- filter / sort / facet / highlight
        |
        v
Coordinator 合并 shard 结果并做全局排序、分页、facet 汇总
```

兼容要求：旧客户端只发送 `q` 和 `limit` 时，仍走同一接口并返回 `found`、`hits` 等既有字段。新增参数均有默认值，不得迫使旧客户端修改。

### 4.2 恢复与重建链路

```text
Follower 启动
   |
   +-- 读取本地 Raft state/log
   +-- 进入 RECOVERING，禁止被当成 ready 副本
   +-- 通过 AppendEntries 自动追日志
   |
   +-- 日志可追平 ----------> READY
   |
   +-- 日志缺口无法追平
          |
          +-- 发现当前 leader
          +-- 获取 manifest 和 snapshot
          +-- 校验 shard/member/checksum
          +-- 原子替换本地 shard 数据
          +-- 安装 snapshot metadata
          +-- 继续追增量日志
          +-- 完整性校验通过 ----> READY
```

重建期间节点进程仍然存活，因此 `/health` 继续返回存活信息；路由层依据 recovery state 判断该副本是否可以承接搜索或修复任务。

## 5. 阶段 1：高级搜索与前端同步

### 5.1 目标

在不改变原有基础关键词搜索用法的前提下，增加可选高级参数，并将这些能力同步到独立 Search 页面和 Admin 页面。

### 5.2 后端任务

1. 扩展 `src/typesense_lite/index.py`：
   - 保留当前中英文分词行为；
   - 增加 `search_query()` 结构化入口；
   - 增加 BM25 风格评分，同时确保简单查询结果稳定；
   - 支持 `query_by` 和 `query_by_weights`；
   - 支持 `prefix` 和 `num_typos`；
   - 支持 `filter_by` 的字段比较和逻辑组合；
   - 支持 `sort_by`；
   - 支持 `facet_by` 和 facet count；
   - 支持 `page`、`per_page`；
   - 支持 `highlight_fields`；
   - 旧 `search(query, limit)` 委托给新实现，保持返回兼容。
2. 扩展 `src/typesense_lite/node.py`：
   - 新增结构化查询方法；
   - 保留原有方法签名；
   - 确保 CRUD 后索引更新行为不变。
3. 扩展 `src/typesense_lite/coordinator.py`：
   - 将高级参数传给各 shard；
   - 保留 `NodeDirectory` 和跨机重试；
   - 全局合并分数、排序、分页和 facet；
   - 某个副本失败时继续使用现有 fallback 语义。
4. 扩展 `src/typesense_lite/server.py`：
   - public search API 接受新增可选参数；
   - data node internal search API 接受相同参数；
   - 参数错误返回清晰的 4xx，不将用户输入错误伪装成节点故障。

### 5.3 前端任务

1. `web/search.html` 增加：
   - collection、关键词、查询字段、字段权重；
   - prefix、typo 容错；
   - filter、sort、facet；
   - page 和 per-page；
   - 高亮结果和 facet 展示；
   - 基础模式默认保持简洁，高级参数不填写时不改变原搜索行为。
2. `web/admin.html` 的文档列表和测试搜索保留现有功能，并适配新的结果结构。
3. 不将搜索重新塞回 Admin 主流程；Search 仍是独立页面，Cluster 和文档管理仍属于后台管理。

### 5.4 测试与验收

- 中文关键词和英文关键词基础搜索回归通过；
- 每个新增查询参数有独立测试；
- 多 shard 全局排序、分页和 facet 聚合测试通过；
- 原 `limit` 行为兼容；
- Search 页面可构造高级查询并正确显示结果；
- Admin 的 collection/document/upload/import 工作流不受影响；
- 全量测试和 Ruff 通过。

## 6. 阶段 2：节点恢复状态与可观测性

### 6.1 目标

让系统明确区分“进程在线”“Raft 正在恢复”“副本已追平并可服务”，避免恢复中的节点被错误地用于查询、repair 或重建源。

### 6.2 后端任务

1. 新增 `src/typesense_lite/recovery.py`：
   - 定义 `RecoveryStatus`；
   - 定义 `NodeRecoveryState`；
   - 提供 `shard_is_ready()`、`node_is_ready()`；
   - 状态至少覆盖 `starting`、`recovering`、`ready`、`rebuilding`、`failed`。
2. 扩展 `raft_runtime.py`：
   - 汇报本地 last log、commit index、leader last log、leader commit index；
   - 在 heartbeat/append/rebuild 后刷新恢复状态；
   - 增加恢复事件时间、错误原因和进度；
   - 保留当前并发 heartbeat、投票 timer reset 和 peer progress 实现。
3. 扩展 `server.py`：
   - 新增 `GET /internal/recovery/state`；
   - `/health` 保持 liveness 兼容，增加 readiness 字段；
   - `/cluster/raft` 聚合 recovery 数据；
   - coordinator 路由排除未 ready 的副本，但在所有副本未 ready 时返回明确诊断。

### 6.3 前端任务

Cluster Console 增加每个 shard/member 的：

- recovery status；
- current term、role、leader；
- local last log / commit index；
- leader last log / commit index；
- lag；
- last recovery event；
- error；
- ready/not ready 状态。

现有选举、health、一致性和 repair 模块必须保留。

### 6.4 测试与验收

- 新节点启动时状态转换正确；
- follower 落后时显示 recovering 和准确 lag；
- catch-up 完成后自动变为 ready；
- 恢复中副本不会承接正常查询；
- `/health` 仍可被部署脚本用于存活探测；
- Cluster Console 能展示真实状态且无字段缺失；
- 单节点故障转移测试保持通过。

## 7. 阶段 3：快照、完整性校验和副本重建

### 7.1 目标

补齐日志追赶无法解决的数据恢复场景。重建必须从当前 shard leader 获取一致快照，校验后原子安装，并在安装完成后继续追增量日志。

### 7.2 后端任务

1. 新增 `src/typesense_lite/rebuild.py`：
   - 定义 `ReplicaIntegrityResult`；
   - 生成稳定快照 checksum；
   - 构建和校验 snapshot payload；
   - 校验 shard id、source member、文档数量、checksum 和元数据。
2. 扩展 `storage.py`：
   - `iter_shard_collections()`；
   - `shard_path()`；
   - `shard_has_documents()`；
   - `replace_shard_documents()`；
   - 临时目录写入、校验、替换，避免失败时破坏原数据。
3. 扩展 `node.py`：
   - 导出 shard collection 快照；
   - 安装 shard 快照并重建内存索引；
   - 检查副本完整性；
   - 安装失败时保留原数据并上报错误。
4. 扩展 `raft_runtime.py`：
   - 提供 committed command/log 查询；
   - 支持 recovery override；
   - 安装 snapshot metadata；
   - 使用 rebuild lock 防止同一 shard 并发重建。
5. 扩展 `server.py`：
   - `GET /internal/shards/{shard_id}/snapshot/manifest`；
   - `GET /internal/shards/{shard_id}/snapshot/export`；
   - `POST /internal/shards/{shard_id}/rebuild`；
   - 自动发现当前 leader；
   - 只允许从 ready leader 拉取；
   - 重建完成后执行完整性校验和增量日志 catch-up。
6. 扩展 coordinator 管理接口：
   - 提供目标 shard/member 的手动重建入口；
   - 校验目标不是当前 leader；
   - 将已有 repair 与 snapshot rebuild 的用途明确区分。

### 7.3 前端任务

Cluster Console 增加：

- shard 快照 manifest 查看；
- 副本 checksum 和一致性结果；
- 对 follower 的“重建副本”按钮；
- 二次确认和运行中禁用状态；
- 重建进度、来源 leader、完成结果和错误详情；
- leader、ready follower 不显示不适用操作。

### 7.4 安全约束

- 不允许从 follower 或未知 leader 构建权威快照；
- 不允许重建当前 leader；
- 不允许跨 shard 安装快照；
- checksum 不一致时拒绝安装；
- 重建失败不能清空原数据；
- 同一 shard 同时只允许一个重建任务；
- 多机环境所有下载均使用现有带 timeout/retry 的 HTTP client；
- 重建接口作为内部管理能力，继续遵守项目当前可信私网安全边界。

### 7.5 测试与验收

- 空 shard、有数据 shard、多 collection shard 快照测试；
- checksum 篡改、shard 不匹配、来源错误均被拒绝；
- follower 数据缺失后可从 leader 完整重建；
- 重建期间写入的新日志最终能够 catch-up；
- leader 故障时不会从过期来源继续安装；
- 安装失败时原副本数据保持可读；
- 手动 repair 既有测试保持通过；
- Cluster Console 操作和错误展示正确。

## 8. 阶段 4：演示、文档与最终回归

### 8.1 演示脚本

选择性合并：

- `failover_demo.py`：演示 leader 故障和重新选举；
- `restart_node_demo.py`：演示 follower 重启和日志 catch-up；
- `replica_rebuild_demo.py`：演示数据损坏后的快照重建。

这些脚本必须复用当前 `RunNodeSpec` 和现有启动配置，不得复制 `gaojios` 的旧启动器。

### 8.2 文档更新

README 增量补充：

- 高级搜索参数示例；
- recovery state 字段说明；
- repair 与 rebuild 的区别；
- 手动重建步骤；
- 自动恢复流程；
- 单机演示命令；
- 真实多机部署下的网络、超时和安全注意事项。

### 8.3 最终验收矩阵

| 场景 | 验收结果 |
| --- | --- |
| 单节点基础中英文搜索 | 必须通过 |
| 高级字段权重、过滤、排序、facet、分页、高亮 | 必须通过 |
| document CRUD、TXT/DOCX 上传、批量导入 | 必须通过 |
| 三 voter 正常选举和多数派写入 | 必须通过 |
| leader 停止后单节点故障转移 | 必须通过 |
| follower 恢复后日志 catch-up | 必须通过 |
| follower 数据缺失后的 snapshot rebuild | 必须通过 |
| rebuild 后继续接收增量日志 | 必须通过 |
| Search/Admin/Cluster Console 三页面 | 必须通过 |
| loopback 多端口部署 | 必须通过 |
| 多机器配置、节点发现和跨机 URL | 必须通过测试覆盖 |
| `pytest tests/typesense_lite` | 零失败 |
| Ruff | 零错误 |

## 9. 执行顺序与提交策略

严格按以下顺序执行，不跨阶段混合提交：

1. 文档：本实施方案和基线记录。
2. 阶段 1A：高级索引和查询 API。
3. 阶段 1B：Search/Admin 前端同步。
4. 阶段 2：恢复状态和 Cluster Console 展示。
5. 阶段 3A：快照、校验和本地安装。
6. 阶段 3B：leader-aware 重建 API 和 Cluster Console 操作。
7. 阶段 4：演示、README、全量回归和静态检查清理。

每个提交只包含对应阶段的代码、测试和必要文档更新。阶段完成的定义是代码、前端、测试和文档共同完成，而不是仅后端方法存在。

## 10. 变更控制规则

后续开发严格遵守本文件：

1. 如果实际代码结构与方案存在偏差，优先保持现有功能和公开接口，再在本文档记录调整原因。
2. 不通过删除、重命名或弱化现有接口来简化合并。
3. 不直接用 `gaojios` 文件覆盖同名文件；逐段移植并审查差异。
4. 每次修改 `server.py`、`coordinator.py`、`raft_runtime.py` 后，必须专门检查多机能力和 Raft 回归。
5. 每项后端管理能力都必须同步到对应前端页面；搜索能力进入 Search，文档管理进入 Admin，集群/选举/恢复/修复进入 Cluster Console。
6. 新增响应字段可以扩展现有结构，但不得删除旧字段。
7. 出现无法同时满足新功能与兼容性的情况时，暂停该项并先更新设计，不擅自牺牲现有功能。

## 11. 当前执行状态

- [x] 完成本地两个仓库的文件级差异分析。
- [x] 记录基准提交和合并前测试结果。
- [x] 明确基准项目独有能力和禁止覆盖范围。
- [x] 编写合并设计、阶段计划和零回退验收门槛。
- [x] 阶段 1：高级搜索与前端同步。
- [x] 阶段 2：节点恢复状态与可观测性。
- [x] 阶段 3：快照、完整性校验和副本重建。
- [x] 阶段 4：演示、文档与最终回归。
- [x] gaojios 全量功能等价兼容审计通过。

## 12. 最终执行记录

2026-07-17 按本方案完成四个阶段，最终验证结果：

```text
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
195 passed, 7 skipped

.venv/bin/python -m ruff check \
  src/typesense_lite tests/typesense_lite scripts examples/distributed_lite
All checks passed!

PYTHONPYCACHEPREFIX=/private/tmp/typesense-lite-pyc-final \
  PYTHONPATH=src .venv/bin/python -m compileall -q \
  src/typesense_lite examples/distributed_lite scripts
成功

git diff --check
成功
```

相对于合并前 `178 passed, 7 skipped`，新增 17 项通过测试，没有减少原有
测试，也没有新增 skip。基线中的 5 个 Ruff 历史问题已清理。

已尝试启动默认三节点真实进程进行手工端到端检查，但当前受控执行环境
禁止监听本地 TCP 端口，uvicorn 在绑定 `127.0.0.1:9100-9103` 时返回
`operation not permitted`。因此本次无法在该环境保留可访问的开发服务；
HTTP 路由、Raft、恢复和重建行为已通过 FastAPI TestClient、MockTransport
和三节点 runtime 集成测试验证。该限制属于执行沙箱，不是应用启动错误。

## 13. gaojios 全量兼容审计

2026-07-17 在四阶段开发完成后，追加执行了源仓库功能清单、HTTP 路由、
测试用例和实现差异审计。首次审计发现 5 类待补齐能力，随后在不改变当前
产品边界的前提下完成修复和功能等价验收。

### 13.1 已确认覆盖

- `gaojios` 的 35 个 Typesense Lite HTTP 路由在当前项目中全部存在；
- 当前项目额外保留了节点注册、心跳、配置分发、节点列表和公共副本重建路由；
- `gaojios` 没有当前项目缺少的源码、示例或测试文件；
- 高级搜索、恢复状态、快照校验、leader-aware 重建和恢复期间读隔离的主体行为已覆盖；
- 当前项目自身 207 项 Typesense Lite 测试结果为 `200 passed, 7 skipped`。

### 13.2 首次审计发现和处理结果

将 `gaojios` 的 159 项 Typesense Lite 测试指向当前项目实现后，结果为：

```text
147 passed, 12 failed
```

其中发现的后端缺口均已补齐：

1. [x] coordinator 在缓存 leader 写入返回 5xx、409、连接失败或返回
   `ok: false` 后，使缓存失效、重新发现 leader 并执行有上限的多轮重试。
2. [x] `/cluster/health` 聚合 recovery state，保留原字段并增加顶层
   `status`、`ok` 以及节点 `status`、`ready`、`recovery`；每个 shard 按
   自身 ready voter 多数派计算健康状态。
3. [x] `LeaderDirectory` 只接受当前 shard voter 的 leader 提示，并在缓存前
   再次验证目标节点可访问且 `role == leader`。
4. [x] Raft heartbeat 并发复制以 `heartbeat_interval` 为等待上限，超时后
   取消慢 peer 任务，同时保留单节点 Raft group 的空 peer 兼容行为。

前端差异为：

1. [x] Search 页面已增加 Prefix、Fuzzy、Filter + Facet、Pagination 查询预设。
2. Admin 页面不包含重复的高级搜索模块。这是此前“搜索独立页面、Admin 只做
   后台管理”的明确产品要求，搜索能力本身未丢失，不应直接恢复旧布局。
3. Cluster Console 不再显示错误的 `static primary mode` 文案，改为展示真实
   Raft 选举状态；这是正确性修复，不属于需要恢复的功能。

### 13.3 最终兼容验收

2026-07-17 补齐缺口后的验证结果：

```text
当前项目全量测试：200 passed, 7 skipped
gaojios 原始兼容测试：156 passed, 3 failed
产品边界映射后兼容测试：156 passed, 3 deselected
Ruff：All checks passed
compileall：成功
git diff --check：成功
```

原始兼容测试剩余 3 项只检查 Admin 中存在重复搜索模块和 Cluster Console
包含 `static primary mode` 文案，分别与已确认的三页面职责和真实 Raft 展示
冲突，因此在功能等价映射中排除。所有后端兼容测试和 Search 查询预设测试
均已通过。

由于 `gaojios` 测试文件使用 Python 3.10 的 `int | None` 语法，而两个项目
均声明兼容 Python 3.9，兼容运行只在临时测试副本中加入 postponed annotation，
没有修改测试逻辑或源仓库文件。

### 13.4 本地代理导致的 502 修复

课堂演示中发现 Coordinator 对 `127.0.0.1:9101-9103` 的内部请求同时返回
`502 Bad Gateway`。Data Node 自身不会为 `/health` 和 Raft state 返回 502，
该响应来自启动环境中的 HTTP 代理。`httpx.AsyncClient` 默认读取
`HTTP_PROXY`、`HTTPS_PROXY` 和 `ALL_PROXY`，当 `NO_PROXY` 未正确包含
loopback 地址时，集群内部请求会被错误发送给代理。

修复内容：

- `make_cross_machine_client()` 默认设置 `trust_env=False`，集群内部通信直连；
- 调用方仍可显式传入 `trust_env=True`，保留特殊部署覆盖能力；
- 本地启动器把 `127.0.0.1` 和 `localhost` 合并到大小写两种 `NO_PROXY`；
- 故障转移和恢复演示脚本的同步 HTTP 请求同样禁用环境代理；
- 故障转移指南和 README 增加 502 排查说明。

修复后全量测试为 `200 passed, 7 skipped`，Ruff、compileall 和
`git diff --check` 均通过。已有集群进程必须完整重启才能使用新客户端配置。
