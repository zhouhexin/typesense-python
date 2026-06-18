# Typesense Lite 剩余工作详细实现计划

> **给 agentic workers 的要求：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans`，按任务逐项执行本计划。步骤使用 checkbox（`- [ ]`）格式，便于跟踪进度。

**目标：** 把当前 Typesense Lite 从“基础分片关键词搜索 demo”扩展为一个更完整的本地分布式搜索系统，补齐 collection/document 管理、批量导入、集群健康可视化、副本一致性检查、手动修复和清晰的故障语义。

**架构：** 保持当前的静态集群、单机多进程模型。继续使用 coordinator 作为唯一公共入口，data node 只暴露内部 API。所有增强都围绕现有分片、primary/replica、JSONL 持久化和倒排索引实现，不引入 Raft、动态扩容或生产级存储。

**技术栈：** Python 3.9+、FastAPI、httpx、Uvicorn、pytest、原生 HTML/CSS/JS。

---

## 总体原则

1. **先补数据管理，再补分布式增强。**
   没有 document 删除、更新、列表能力时，副本修复和一致性检查会缺少基础 API。

2. **每个阶段必须能独立运行、独立验证、独立提交。**
   不把阶段 1、2、3 混在一个大提交里。

3. **保留 demo 定位。**
   做清楚“primary 挂了写失败”“replica 落后需要手动修复”，不伪装成生产级高可用系统。

4. **兼容现有数据。**
   已经写入的旧 JSONL 行是普通 document JSON，后续改成操作日志 envelope 后，读取时仍要能识别旧格式。

5. **前端只做轻量控制台。**
   不引入 React/Vue，不增加构建流程。继续使用 `src/typesense_lite/web/search.html` 和 `src/typesense_lite/web/admin.html`。

## 当前基线

当前已经具备：

- `POST /collections/{collection}/documents`
- `GET /collections/{collection}/documents/search`
- `GET /cluster`
- `GET /health`
- data-node 内部写入和搜索 API
- 3 shard + 1 replica 的静态集群配置
- JSONL 持久化和重启重建索引
- `/search` 搜索页
- `/admin` 后台页，包含 Cluster 和 Add Document

当前仍缺：

- collection 列表和显式管理
- document retrieve/update/delete/list
- 文件上传和批量导入
- node/shard 健康状态汇总
- replica 一致性检查
- replica 手动修复
- primary 写失败时的结构化错误
- 搜索 fallback 使用 replica 时的 warning
- `query_by`、字段权重、分页

## 范围边界

包含：

- 显式 collection 管理。
- document 查询、更新、删除、列表 API。
- JSON/JSONL 批量导入。
- `.txt`、`.md`、`.json`、`.jsonl` 文件上传。
- 后台页面展示 collections、导入入口和集群状态。
- coordinator 汇总 data node 健康状态。
- 副本一致性检查和手动修复。
- primary 不可用时给出清晰的写入失败语义。
- 搜索使用 replica fallback 时给出 warning。
- 简单搜索增强：`query_by`、字段权重、分页。

不包含：

- Raft/Paxos。
- 自动 primary promotion。
- 动态节点加入/移除。
- 自动 shard rebalance。
- 生产级 compaction。
- BM25。
- 中文分词。
- 认证和权限控制。

## 文件结构

- 修改 `src/typesense_lite/index.py`：document 删除、查询、列表、字段选择、字段权重。
- 修改 `src/typesense_lite/storage.py`：collection 发现、JSONL 操作日志、delete tombstone、向后兼容旧 JSONL。
- 修改 `src/typesense_lite/node.py`：collection/document 操作、document id 列表。
- 修改 `src/typesense_lite/coordinator.py`：公共 API 路由逻辑、批量导入、健康检查、repair、分页。
- 修改 `src/typesense_lite/server.py`：新增 public/internal FastAPI routes。
- 修改 `src/typesense_lite/web/admin.html`：collection、导入、健康、repair 面板。
- 修改 `src/typesense_lite/web/search.html`：collection selector、`query_by`、分页控件。
- 修改 `src/typesense_lite/schemas.py`：必要时增加 response dataclass 或 type alias。
- 新增 `src/typesense_lite/importer.py`：文件内容解析。
- 新增 `src/typesense_lite/health.py`：健康状态聚合。
- 新增 `src/typesense_lite/repair.py`：一致性检查和手动修复。
- 新增或修改 `tests/typesense_lite/*.py`：每个能力必须有测试。

---

# 阶段 1：Collection 和 Document 管理

## 阶段目标

把当前“只支持写入和搜索”的隐式 collection 模型，扩展成可管理的 collection/document 模型。

完成后应该支持：

- 查看所有 collections。
- 查看某个 collection 的 documents。
- 按 id 查询 document。
- 更新 document。
- 删除 document。
- 删除后搜索结果不再出现。
- 重启 data node 后删除状态仍然生效。

## API 设计

Coordinator 公共 API：

```http
GET    /collections
GET    /collections/{collection}/documents
GET    /collections/{collection}/documents/{document_id}
PATCH  /collections/{collection}/documents/{document_id}
DELETE /collections/{collection}/documents/{document_id}
```

Data node 内部 API：

```http
GET    /internal/shards/{shard_id}/collections/{collection}/documents
GET    /internal/shards/{shard_id}/collections/{collection}/documents/{document_id}
PATCH  /internal/shards/{shard_id}/collections/{collection}/documents/{document_id}
DELETE /internal/shards/{shard_id}/collections/{collection}/documents/{document_id}
```

示例响应：

```json
{
  "documents": [
    {"id": "book-1", "title": "Distributed Search", "body": "Shard routing"}
  ]
}
```

删除响应：

```json
{
  "ok": true,
  "id": "book-1",
  "shard_id": 0,
  "primary": "node-1",
  "replicas": [{"node": "node-2", "ok": true}],
  "warnings": []
}
```

## 数据行为

- `PATCH` 是浅合并：保留旧 document 字段，覆盖请求里出现的字段。
- `PATCH` 不允许修改 `id`。如果 body 里包含不同的 `id`，返回 400。
- `DELETE` 写入 tombstone，而不是直接改写 JSONL 文件。
- 重启时按 JSONL 顺序回放操作：
  - 普通 document JSON：当作旧格式 upsert。
  - `{"op": "upsert", "document": {...}}`：写入/覆盖。
  - `{"op": "delete", "id": "book-1"}`：删除内存索引和 document。

## 任务 1.1：索引层能力

**文件：**
- 修改：`src/typesense_lite/index.py`
- 测试：`tests/typesense_lite/test_index.py`

- [ ] 增加测试：删除 document 后，`search()` 不再返回该 document。
- [ ] 增加测试：删除 document 后，旧 term posting list 被清理。
- [ ] 增加测试：`get_document("doc-1")` 返回完整 document。
- [ ] 增加测试：`get_document("missing")` 抛出 `KeyError`。
- [ ] 增加测试：`list_documents()` 按 id 升序返回。
- [ ] 实现 `get_document(document_id: str) -> Document`。
- [ ] 实现 `list_documents() -> list[Document]`。
- [ ] 实现 `delete_document(document_id: str) -> Document`，返回被删除的 document。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_index.py -q`。

## 任务 1.2：Storage 操作日志

**文件：**
- 修改：`src/typesense_lite/storage.py`
- 测试：`tests/typesense_lite/test_node.py`

- [ ] 增加测试：新写入的 JSONL 行是 `{"op": "upsert", "document": {...}}`。
- [ ] 增加测试：删除操作写入 `{"op": "delete", "id": "doc-1"}`。
- [ ] 增加测试：旧格式普通 document JSONL 行仍能被加载。
- [ ] 增加测试：upsert、delete、upsert 同一个 id 后，最终状态是最后一次 upsert。
- [ ] 增加测试：`iter_collection_paths()` 能发现多个 shard 和多个 collection。
- [ ] 实现 `append_upsert(shard_id, collection, document)`。
- [ ] 实现 `append_delete(shard_id, collection, document_id)`。
- [ ] 实现读取时识别 envelope 和旧格式 document。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_node.py -q`。

## 任务 1.3：SearchNode 管理能力

**文件：**
- 修改：`src/typesense_lite/node.py`
- 测试：`tests/typesense_lite/test_node.py`

- [ ] 增加测试：`SearchNode.get_document(shard_id, collection, id)` 返回 document。
- [ ] 增加测试：`SearchNode.list_documents(shard_id, collection)` 返回当前 documents。
- [ ] 增加测试：`SearchNode.update_document(...)` 浅合并字段并重建索引。
- [ ] 增加测试：`SearchNode.delete_document(...)` 删除后搜索不到。
- [ ] 增加测试：重启 node 后 delete 和 update 仍然生效。
- [ ] 增加测试：`SearchNode.list_collections()` 返回已存在 collection 名称。
- [ ] 实现对应方法。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_node.py -q`。

## 任务 1.4：Data Node 内部 API

**文件：**
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：内部 document list endpoint 返回 200 和 documents。
- [ ] 增加测试：内部 retrieve endpoint 返回 200；missing 返回 404。
- [ ] 增加测试：内部 patch endpoint 修改 document 后搜索结果更新。
- [ ] 增加测试：内部 delete endpoint 删除 document 后搜索结果为空。
- [ ] 实现内部 routes。
- [ ] missing document 统一返回 404。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 任务 1.5：Coordinator 公共 API

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_coordinator.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`GET /collections` 汇总所有 data nodes 的 collection 名称并去重。
- [ ] 增加测试：`GET /collections/books/documents` fan-out 所有 shards 并合并 documents。
- [ ] 增加测试：`GET /collections/books/documents/book-1` 根据 id 路由到 shard。
- [ ] 增加测试：`PATCH` 写 primary 成功后复制到 replicas。
- [ ] 增加测试：`DELETE` 写 primary 成功后复制 tombstone 到 replicas。
- [ ] 实现 public routes。
- [ ] 对 list 操作使用 shard fan-out。
- [ ] 对单 document 操作使用 `cluster.get_shard_id(document_id)`。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py tests/typesense_lite/test_server.py -q`。

## 阶段 1 验收标准

- [ ] 可以通过 API 列出 collections。
- [ ] 可以通过 API 列出 documents。
- [ ] 可以查询、更新、删除单个 document。
- [ ] 删除后搜索结果不再包含该 document。
- [ ] 重启 data node 后删除和更新仍然生效。
- [ ] 所有 `tests/typesense_lite` 测试通过。

---

# 阶段 2：批量导入和文件上传

## 阶段目标

让用户能用真实文件测试搜索系统，不再只能在后台页面手工添加单条 document。

完成后应该支持：

- 粘贴 JSON array 批量导入。
- 上传 `.txt` 和 `.md` 文件。
- 上传 `.json` 文件，支持 object 或 object list。
- 上传 `.jsonl` 文件，每行一个 document。
- 后台页面展示每条导入结果。

## API 设计

```http
POST /collections/{collection}/documents/import
POST /collections/{collection}/documents/upload
```

`/import` 请求示例：

```json
[
  {"id": "doc-1", "title": "A", "body": "hello search"},
  {"id": "doc-2", "title": "B", "body": "distributed shard"}
]
```

`/import` 响应示例：

```json
{
  "ok": true,
  "total": 2,
  "succeeded": 2,
  "failed": 0,
  "results": [
    {"id": "doc-1", "ok": true, "shard_id": 0, "warnings": []},
    {"id": "doc-2", "ok": true, "shard_id": 1, "warnings": []}
  ]
}
```

## 文件解析规则

- `.txt` / `.md`
  - `id`: 文件名 slug，例如 `readme-md`。
  - `title`: 原始文件名。
  - `body`: 文件全文。
  - `source`: 原始文件名。

- `.json`
  - 如果是 object：导入单个 document。
  - 如果是 array：每个元素导入为 document。
  - 每个 document 必须有字符串 `id`；没有 id 时使用文件名加序号生成。

- `.jsonl`
  - 每个非空行必须是 JSON object。
  - 每行导入一个 document。
  - 没有 id 时使用文件名加行号生成。

## 任务 2.1：Importer

**文件：**
- 新增：`src/typesense_lite/importer.py`
- 测试：`tests/typesense_lite/test_importer.py`

- [ ] 增加测试：`.txt` 解析为一个 document，包含 `id/title/body/source`。
- [ ] 增加测试：`.md` 解析为一个 document。
- [ ] 增加测试：`.json` object 解析为一个 document。
- [ ] 增加测试：`.json` array 解析为多个 documents。
- [ ] 增加测试：`.jsonl` 多行解析为多个 documents。
- [ ] 增加测试：空行会被跳过。
- [ ] 增加测试：不支持的后缀抛出 `ValueError`。
- [ ] 增加测试：非法 JSON 抛出 `ValueError`，错误信息包含文件名或行号。
- [ ] 实现 `parse_upload(filename: str, content: bytes) -> list[Document]`。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_importer.py -q`。

## 任务 2.2：批量导入 API

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`POST /collections/books/documents/import` 导入多个 documents。
- [ ] 增加测试：部分 document 缺少合法 id 时，返回单条失败，不影响其他合法 document。
- [ ] 增加测试：`POST /collections/books/documents/upload` 上传 `.txt` 成功。
- [ ] 增加测试：上传不支持后缀返回 400。
- [ ] 实现 `Coordinator.import_documents(collection, documents)`。
- [ ] 实现 multipart upload route，使用 `UploadFile`。
- [ ] 为缺失 id 的导入文档生成稳定 id。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 任务 2.3：后台导入面板

**文件：**
- 修改：`src/typesense_lite/web/admin.html`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`/admin` 页面包含 `Import Documents`。
- [ ] 增加测试：`/admin` 页面包含 file input。
- [ ] 增加 JSON 批量粘贴 textarea。
- [ ] 增加上传文件 input。
- [ ] 增加 Import 按钮。
- [ ] 导入后渲染结果表格，列为 `id`、`ok`、`shard`、`warnings`。
- [ ] 导入失败时显示错误 message，不清空用户输入。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 阶段 2 验收标准

- [ ] 可以导入 JSON array。
- [ ] 可以上传 `.txt/.md/.json/.jsonl`。
- [ ] 上传后的文档可以被 `/search` 搜到。
- [ ] 后台页面能展示导入结果。
- [ ] 不支持格式有清晰错误。

---

# 阶段 3：Cluster Health 和 Shard 可视化

## 阶段目标

让用户在后台页面看到每个 node 和 shard 的健康状态，而不是只能靠终端日志判断。

完成后应该支持：

- coordinator 检查所有 data node 的 `/health`。
- 返回 node 可用状态。
- 返回每个 shard 的 primary/replica 可用状态。
- 后台页面展示 healthy/degraded/unavailable。

## API 设计

```http
GET /cluster/health
```

响应示例：

```json
{
  "nodes": {
    "node-1": {"ok": true, "url": "http://127.0.0.1:9101", "error": null},
    "node-2": {"ok": false, "url": "http://127.0.0.1:9102", "error": "connect failed"}
  },
  "shards": {
    "0": {
      "status": "healthy",
      "primary": {"node": "node-1", "ok": true},
      "replicas": [{"node": "node-2", "ok": false}]
    }
  }
}
```

Shard 状态规则：

- `healthy`：primary 可用。
- `degraded`：primary 不可用，但至少一个 replica 可用。
- `unavailable`：primary 和所有 replicas 都不可用。

## 任务 3.1：Health 聚合模块

**文件：**
- 新增：`src/typesense_lite/health.py`
- 修改：`src/typesense_lite/coordinator.py`
- 测试：`tests/typesense_lite/test_coordinator.py`

- [ ] 增加测试：所有节点健康时，shard status 是 `healthy`。
- [ ] 增加测试：primary 失败、replica 正常时，shard status 是 `degraded`。
- [ ] 增加测试：primary 和 replica 都失败时，shard status 是 `unavailable`。
- [ ] 增加测试：节点错误信息会写入 `nodes[node_id].error`。
- [ ] 实现 `collect_cluster_health(cluster, client)`。
- [ ] `Coordinator.cluster_health()` 调用 health 模块。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py -q`。

## 任务 3.2：Health API

**文件：**
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`GET /cluster/health` 返回 nodes 和 shards。
- [ ] 增加测试：data node 不暴露 `/cluster/health`。
- [ ] 实现 route。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 任务 3.3：后台健康状态面板

**文件：**
- 修改：`src/typesense_lite/web/admin.html`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`/admin` 包含 `Cluster Health`。
- [ ] 增加 Refresh Health 按钮。
- [ ] 页面加载时自动调用 `/cluster/health`。
- [ ] 展示 node 状态列表。
- [ ] 展示 shard 状态列表。
- [ ] `healthy/degraded/unavailable` 使用不同文字和颜色。
- [ ] 请求失败时显示错误 message。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 阶段 3 验收标准

- [ ] `/cluster/health` 能看到 node 和 shard 状态。
- [ ] 后台页面能刷新健康状态。
- [ ] 停掉某个 data node 后，后台能显示 degraded 或 unavailable。
- [ ] 搜索 fallback 行为仍然正常。

---

# 阶段 4：Replica 一致性检查和手动修复

## 阶段目标

让用户知道 replica 是否落后，并能手动触发修复。

这个阶段不做自动修复，不做 automatic promotion，只做检查和手动修复。

## API 设计

```http
GET  /collections/{collection}/consistency
POST /collections/{collection}/repair
```

一致性响应示例：

```json
{
  "collection": "books",
  "shards": {
    "0": {
      "primary": "node-1",
      "replicas": {
        "node-2": {
          "missing_on_replica": ["book-1"],
          "extra_on_replica": []
        }
      }
    }
  }
}
```

修复响应示例：

```json
{
  "collection": "books",
  "repaired": 1,
  "shards": {
    "0": {
      "node-2": {"repaired": ["book-1"], "failed": []}
    }
  }
}
```

## 任务 4.1：Data Node Document ID 列表

**文件：**
- 修改：`src/typesense_lite/node.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`SearchNode.list_document_ids(shard_id, collection)` 返回排序后的 ids。
- [ ] 增加内部 endpoint：`GET /internal/shards/{shard_id}/collections/{collection}/document_ids`。
- [ ] 增加测试：endpoint 返回 `{"ids": ["book-1", "book-2"]}`。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_server.py -q`。

## 任务 4.2：一致性检查

**文件：**
- 新增：`src/typesense_lite/repair.py`
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_coordinator.py`

- [ ] 增加测试：primary 有 id、replica 缺 id 时，返回 `missing_on_replica`。
- [ ] 增加测试：replica 有额外 id 时，返回 `extra_on_replica`。
- [ ] 增加测试：replica 不可用时，返回该 replica 的 error。
- [ ] 实现 `check_consistency(collection)`。
- [ ] 增加 `GET /collections/{collection}/consistency`。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py -q`。

## 任务 4.3：手动修复

**文件：**
- 修改：`src/typesense_lite/repair.py`
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 修改：`src/typesense_lite/web/admin.html`
- 测试：`tests/typesense_lite/test_coordinator.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：repair 会从 primary 读取缺失 document 并写入 replica。
- [ ] 增加测试：primary 缺失 document 时，该 id 记入 failed。
- [ ] 增加测试：replica 写入失败时，该 id 记入 failed。
- [ ] 增加 `POST /collections/{collection}/repair`。
- [ ] 后台页面增加 Consistency Check 按钮。
- [ ] 后台页面增加 Repair 按钮。
- [ ] 后台页面展示 missing/extra/repaired/failed。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py tests/typesense_lite/test_server.py -q`。

## 阶段 4 验收标准

- [ ] 可以看到 replica 缺失哪些 document。
- [ ] 可以手动修复缺失 document。
- [ ] 修复后再次检查 consistency，missing 列表为空。
- [ ] replica 不可用时有清晰错误，不导致整个 coordinator 崩溃。

---

# 阶段 5：清晰的故障语义

## 阶段目标

把当前 demo 的故障边界说清楚，并通过 API 结构体现出来。

当前策略保持不变：

- 写入必须 primary 成功。
- replica 写失败不阻塞主流程，但要返回 warning。
- 搜索优先 primary，primary 不可用时尝试 replica。
- 使用 replica 搜索时要返回 warning。

## 写入错误响应

primary 写失败时返回 503：

```json
{
  "ok": false,
  "error": "primary node node-1 unavailable",
  "shard_id": 0,
  "primary": "node-1",
  "replicas": [{"node": "node-2", "ok": false}],
  "warnings": []
}
```

replica 写失败时返回 200：

```json
{
  "ok": true,
  "id": "book-1",
  "shard_id": 0,
  "primary": "node-1",
  "replicas": [{"node": "node-2", "ok": false}],
  "warnings": ["replica node-2 write failed"]
}
```

## 任务 5.1：Primary 写失败结构

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_coordinator.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：primary 写失败时 coordinator 抛出结构化错误对象或返回错误结果。
- [ ] 增加测试：FastAPI route 把 primary 写失败转换成 503。
- [ ] 增加测试：错误响应包含 `ok/error/shard_id/primary/replicas/warnings`。
- [ ] 实现结构化错误。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py tests/typesense_lite/test_server.py -q`。

## 任务 5.2：Replica 写失败 warning

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 测试：`tests/typesense_lite/test_coordinator.py`

- [ ] 增加测试：replica 写失败时，整体结果 `ok=true`。
- [ ] 增加测试：replica 结果里对应 node `ok=false`。
- [ ] 增加测试：warnings 包含失败 replica node id。
- [ ] 确保 create/update/delete/import 都使用相同 warning 语义。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py -q`。

## 任务 5.3：搜索 fallback warning

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_coordinator.py`

- [ ] 增加测试：primary 搜索失败、replica 成功时，结果包含 hits。
- [ ] 增加测试：warnings 包含 `searched replica node-2 because primary node-1 failed`。
- [ ] 增加测试：所有候选节点失败时，warnings 包含 shard unavailable。
- [ ] 保持当前 search response 的 `found/hits/warnings` 结构。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py -q`。

## 阶段 5 验收标准

- [ ] primary 写失败是 503。
- [ ] replica 写失败是 200 + warning。
- [ ] 搜索使用 replica fallback 时有 warning。
- [ ] 所有失败响应对前端可读。

---

# 阶段 6：搜索质量增强

## 阶段目标

在不引入复杂搜索算法的前提下，让搜索更接近真实使用。

完成后支持：

- `query_by` 指定搜索字段。
- `title` 默认权重高于 `body`。
- 分页。
- 搜索页可以配置字段和翻页。

## API 设计

```http
GET /collections/{collection}/documents/search?q=search&query_by=title,body&page=1&per_page=10
```

响应示例：

```json
{
  "found": 24,
  "page": 1,
  "per_page": 10,
  "hits": []
}
```

向后兼容：

- 旧的 `limit=10` 仍然可用。
- 如果传了 `per_page`，优先使用 `per_page`。
- 如果没传 `query_by`，默认索引所有顶层字符串字段。

## 任务 6.1：字段选择

**文件：**
- 修改：`src/typesense_lite/index.py`
- 修改：`src/typesense_lite/node.py`
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 测试：`tests/typesense_lite/test_index.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`query_by=title` 时，只匹配 title。
- [ ] 增加测试：`query_by=body` 时，只匹配 body。
- [ ] 增加测试：不传 `query_by` 时保持当前行为，搜索所有顶层字符串字段。
- [ ] 实现 `query_by` 参数贯穿 server -> coordinator -> node -> index。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_index.py tests/typesense_lite/test_server.py -q`。

## 任务 6.2：字段权重

**文件：**
- 修改：`src/typesense_lite/index.py`
- 测试：`tests/typesense_lite/test_index.py`

- [ ] 增加测试：title 命中一次的 score 大于 body 命中一次。
- [ ] 增加测试：默认权重为 `title=2.0`，其他字段 `1.0`。
- [ ] 调整索引结构，保留 term 在不同字段中的频次。
- [ ] 计算 score 时乘以字段权重。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_index.py -q`。

## 任务 6.3：分页

**文件：**
- 修改：`src/typesense_lite/coordinator.py`
- 修改：`src/typesense_lite/server.py`
- 修改：`src/typesense_lite/web/search.html`
- 测试：`tests/typesense_lite/test_coordinator.py`
- 测试：`tests/typesense_lite/test_server.py`

- [ ] 增加测试：`page=1&per_page=2` 返回前 2 条。
- [ ] 增加测试：`page=2&per_page=2` 返回第 3-4 条。
- [ ] 增加测试：`limit=10` 仍然可用。
- [ ] response 增加 `page` 和 `per_page`。
- [ ] 搜索页增加 `query_by` 输入框，默认 `title,body`。
- [ ] 搜索页增加上一页/下一页按钮。
- [ ] 运行 `PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite/test_coordinator.py tests/typesense_lite/test_server.py -q`。

## 阶段 6 验收标准

- [ ] 可以按字段搜索。
- [ ] title 命中排序更靠前。
- [ ] 搜索页可以翻页。
- [ ] 旧的 `limit` 参数仍然不破坏。

---

## 推荐执行顺序

推荐分两轮做：

第一轮：

1. 阶段 1：Collection 和 document 管理。
2. 阶段 2：批量导入和文件上传。
3. 阶段 3：Cluster health 和 shard 可视化。

第二轮：

4. 阶段 4：Replica 一致性检查和手动修复。
5. 阶段 5：清晰的故障语义。
6. 阶段 6：搜索质量增强。

原因：

- 阶段 1-3 直接提升可用性，适合先验收。
- 阶段 4-5 更偏分布式一致性和故障语义，需要在管理 API 稳定后做。
- 阶段 6 涉及索引结构变化，最好放在功能稳定后再改。

## 每阶段验证命令

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/typesense_lite -q
.venv/bin/ruff check src/typesense_lite tests/typesense_lite examples/distributed_lite
PYTHONPATH=src .venv/bin/python -m compileall -q src/typesense_lite tests/typesense_lite
```

## 最终手动验收

启动集群：

```bash
.venv/bin/python examples/distributed_lite/start_cluster.py
```

运行 demo：

```bash
.venv/bin/python examples/distributed_lite/demo.py
```

浏览器打开：

```text
http://127.0.0.1:9100/search
http://127.0.0.1:9100/admin
```

手动验证：

- [ ] 后台可以添加 document。
- [ ] 后台可以批量导入文件。
- [ ] 搜索页能搜索导入内容。
- [ ] 停掉一个 data node 后，health 面板显示 degraded。
- [ ] replica 修复前后 consistency 结果发生变化。
- [ ] 删除 document 后搜索不到。
- [ ] 重启集群后数据状态仍然正确。

## 确认门

在用户确认前，不实现本计划。

需要用户确认：

- 是实现全部阶段，还是只实现其中一部分。
- 第一轮文件上传是否只支持 `.txt/.md/.json/.jsonl`。
- 是否继续排除自动 primary promotion。

