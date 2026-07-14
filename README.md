# Typesense Python Client

Python client for the Typesense API: https://github.com/typesense/typesense

## Installation

```
$ pip install typesense
```

You can also add `typesense` to your project's `requirements.txt`.

## Usage

You can find some examples [here](https://github.com/typesense/typesense-python/blob/master/examples/collection_operations.py).

See detailed [API documentation](https://typesense.org/api).

## Typesense Lite 本地分布式搜索 Demo

本仓库现在包含一个轻量的本地分布式搜索服务，代码位于
`src/typesense_lite`，默认用单机多进程、多端口模拟一个 coordinator
和多个 data node。

启动默认集群：

```bash
.venv/bin/python examples/distributed_lite/start_cluster.py
```

默认端口：

- coordinator: `http://127.0.0.1:9100`
- data node: `http://127.0.0.1:9101`、`9102`、`9103`

前端入口：

- 搜索页：`http://127.0.0.1:9100/`
- 管理后台：`http://127.0.0.1:9100/admin`
- 集群控制台：`http://127.0.0.1:9100/cluster-console`

常用接口：

```bash
curl http://127.0.0.1:9100/cluster/health
curl http://127.0.0.1:9100/cluster/raft
curl -X POST http://127.0.0.1:9100/collections/books/documents \
  -H 'Content-Type: application/json' \
  -d '{"id":"book-1","title":"Raft Test","body":"hello raft"}'
curl 'http://127.0.0.1:9100/collections/books/documents/search?q=raft'
```

当前 Raft 实现是 per-shard Raft group：每个 shard 独立选 leader，
coordinator 写入时先发现 shard leader，再把写入提交到
`/internal/raft/{shard_id}/commands`。默认配置中每个 shard 是 3 个
voter，可以容忍任意 1 个 voter 暂时不可用。当 follower 离线期间错过
写入，恢复后 leader 会通过 heartbeat 自动推送缺失日志，直到 follower
的 `last_log_index`、`commit_index` 和 `last_applied` 追上 leader。

## Typesense Lite 多机部署

单机 demo 之外，`typesense_lite` 也可以部署到多台真实机器上，靠
`scripts/deploy_cluster.py` 编排：scp `cluster_config.json`、同步
代码、启动 coordinator + 每个 data node，配套 status / stop / logs
子命令与心跳式节点发现。

### 1. 准备 inventory

复制 `scripts/inventory.example.json` 到你自己的 `inventory.json`，
按需修改：

```jsonc
{
  "ssh": {"user": "ubuntu", "identity_file": "~/.ssh/id_rsa"},
  "coordinator": {"host": "192.168.1.10", "port": 9100,
                  "data_dir": "/home/ubuntu/typesense-lite-data/coordinator"},
  "nodes": [
    {"id": "node-1", "host": "192.168.1.11", "port": 9101,
     "data_dir": "/home/ubuntu/typesense-lite-data/node-1"},
    {"id": "node-2", "host": "192.168.1.12", "port": 9101, "data_dir": "..."},
    {"id": "node-3", "host": "192.168.1.13", "port": 9101, "data_dir": "..."}
  ],
  "shards": {
    "0": {"primary": "node-1", "replicas": ["node-2", "node-3"]},
    "1": {"primary": "node-2", "replicas": ["node-3", "node-1"]},
    "2": {"primary": "node-3", "replicas": ["node-1", "node-2"]}
  },
  "shard_count": 3,
  "remote_project_root": "/home/ubuntu/typesense-python",
  "remote_venv_python": "/home/ubuntu/typesense-python/.venv/bin/python",
  "remote_config_path": "/home/ubuntu/typesense-lite-data/cluster_config.json",
  "log_dir": "/home/ubuntu/typesense-lite-data/logs",
  "sync_mode": "rsync"
}
```

> 目标机器假设：Python ≥ 3.9、仓库代码已在每台机器上（脚本会做
> `rsync` 或 `git pull`）、coordinator 与所有 data node 网络互通、
> 对外监听端口（默认 9100/9101）未被防火墙拦截。
>
> 安全边界：当前多机部署是 demo/实验模式，不实现 TLS、mTLS、API key
> 认证或 ACL。只应部署在可信私有网络内，并通过防火墙或安全组限制
> coordinator/data node 端口的访问范围。

### 2. 部署

```bash
# 默认文件下发模式
.venv/bin/python scripts/deploy_cluster.py deploy \
    --inventory inventory.json

# 先演练，确认 ssh/scp 命令正确
.venv/bin/python scripts/deploy_cluster.py deploy \
    --inventory inventory.json --dry-run

# 不预置 cluster_config.json，节点启动时向 coordinator 拉取
.venv/bin/python scripts/deploy_cluster.py deploy \
    --inventory inventory.json --pull-config

# 需要交互输入 ssh 密码（要求本机已装 sshpass）
.venv/bin/python scripts/deploy_cluster.py deploy \
    --inventory inventory.json --askpass
```

部署脚本会：
1. 把 inventory 转成 `cluster_config.json` 写到 `.data/deploy/` 下；
2. 把该 JSON scp 到每台机器的 `remote_config_path`；
3. 把项目代码 `rsync` 到 `remote_project_root`（或 `git pull`）；
4. 在每台机器上 `nohup` 启动 `scripts/run_node.py`，把 stdout/stderr
   重定向到 `log_dir/<host>.log`，把 PID 写到 `data_dir/server.pid`；
5. 等待 coordinator `/health` 返回 200。

### 3. 验证

```bash
# 进程 + /health 探测
.venv/bin/python scripts/deploy_cluster.py status --inventory inventory.json

# 集群健康（在 coordinator 机器上）
curl http://192.168.1.10:9100/cluster/health
curl http://192.168.1.10:9100/cluster/nodes    # 包含 static + alive
curl http://192.168.1.10:9100/cluster/raft

# 写一条文档，确认跨机路由
curl -X POST http://192.168.1.10:9100/collections/books/documents \
    -H 'Content-Type: application/json' \
    -d '{"id":"book-1","title":"Multi-machine","body":"hello"}'

# 从 coordinator 查
curl 'http://192.168.1.10:9100/collections/books/documents/search?q=multi'
```

### 4. 查看日志 / 停止

```bash
.venv/bin/python scripts/deploy_cluster.py logs   --inventory inventory.json
.venv/bin/python scripts/deploy_cluster.py logs   --inventory inventory.json --host 192.168.1.11
.venv/bin/python scripts/deploy_cluster.py stop   --inventory inventory.json
```

### 5. 单机模拟多机（CI / 本地手测）

如果没有多台物理机器，可以在 loopback 上加别名 IP，用
`--local-sim` 跳过 ssh/scp/rsync，直接在本机 fork 进程，仍然走真实
网络栈：

```bash
# 一次性配置（需要 sudo）
for ip in 192.168.99.{10..13}; do
    sudo ip addr add $ip/32 dev lo          # Linux
    # sudo ifconfig lo0 alias $ip/32        # macOS
done

# 模拟部署
.venv/bin/python scripts/deploy_cluster.py deploy \
    --inventory scripts/inventory.local_sim.example.json --local-sim

# 验证（用真实 IP 而不是 127.0.0.1）
curl http://192.168.99.10:9100/health
```

集成测试 `tests/typesense_lite/test_multi_machine.py` 就是用这套机
制跑的：如果探测到 alias IP 就跑完整流程，否则自动 skip。

### 6. 节点发现协议

data node 启动后会向 coordinator 发：
- `POST /internal/cluster/nodes/register` — 注册自己；
- 之后每 5s 一次 `PUT /internal/cluster/nodes/{id}/heartbeat` — 续期。

coordinator 在内存里维护 `alive_nodes`；30s 内没收到心跳就视作离
线。`GET /cluster/nodes` 返回 `{static, alive, alive_timeout_seconds}`，
运维和测试都能直接看到哪些节点在线。

### 7. 故障排查

| 现象 | 排查 |
|---|---|
| deploy 卡在等待 `/health` | `deploy_cluster.py logs` 看 coordinator 日志；多半是 9100/9101 被防火墙挡 |
| `alive` 集合少了节点 | 检查 `COORDINATOR_URL` 是否能从该节点机器解析到 coordinator；`logs` 看 `register` 报错 |
| Raft 选举慢 | 调 `RaftRuntime(election_timeout=...)`；inventory 里给 coordinator_url 加 LB |
| ssh 报错 | `ssh -o StrictHostKeyChecking=accept-new user@host echo ok` 先自测 |
| 想清空重启 | `deploy_cluster.py stop` 然后删 `data_dir/*.jsonl` 与 `data_dir/raft/` |

## 运行测试

测试分两层：

### 单元测试（pytest，CI 默认跑）

按 marker 分组，定义在 `pytest.ini`：

| Marker | 文件 | 覆盖 |
|---|---|---|
| `unit` | `test_run_node.py` | `RunNodeSpec` / `build_command` / `build_env` / `parse_args` |
| `inventory` | `test_inventory.py` | inventory schema + 与 `cluster_config.json` 双向转换 |
| `http` | `test_http_client.py` | `cross_machine_timeout` / `get_with_retry` / 结构化日志 |
| `discovery` | `test_node_discovery.py` | `NodeDirectory` + `NodeRegistrar` + `/cluster/nodes` 端点 |
| `deploy` | `tests/scripts/test_deploy_scripts.py` | `deploy_plan` 纯函数生成 ssh/scp/rsync 命令 |
| `multi_machine` | `test_multi_machine.py` | 端到端跨 IP 集成（需 alias IP，否则 skip） |

常用命令：

```bash
# 跑全部 unit 测试（不依赖网络）
pytest -m unit

# 只跑 deploy 相关
pytest -m deploy

# 跑 discovery + http（FastAPI TestClient，无需进程）
pytest -m "discovery or http"

# 跑全部包含 multi_machine 在内的测试（alias IP 没配时会 skip）
pytest -m "multi_machine or unit"
```

### 端到端冒烟（独立脚本，无需 pytest）

```bash
# 配好 alias IP 后：
python scripts/smoke_test.py

# 可选参数：
python scripts/smoke_test.py --alias-prefix 10.0.99   # 改用 10.0.99.10..13
python scripts/smoke_test.py --data-root /tmp/ts-smoke
python scripts/smoke_test.py --keep-data               # 测完不清理，方便看日志
python scripts/smoke_test.py --skip-setup              # 假定集群已在跑
python scripts/smoke_test.py --skip-stop-check         # 跳过"kill 一节点看 alive 收缩"

# shell 包装等价：
scripts/run_smoke_tests.sh
```

`smoke_test.py` 会：
1. 探测 loopback alias IP（不可用时打印配置指令并退出 2）；
2. 生成 `cluster_config.json` 并 fork 1 个 coordinator + 3 个 data node 进程；
3. 等所有 `/health` 返回 200；
4. 跑 9 项验证（health / cluster endpoint / 跨 IP 写入 / 搜索 fan-out / 副本直接读 / Raft 收敛 / heartbeat 发现 / kill 一节点看 alive 收缩）；
5. 输出每项 `PASS/FAIL` 与耗时，失败时 dump 日志；
6. 优雅停进程、清理数据；
7. 全部 PASS 退出 0，否则 1。

适合：CI 在配好 alias IP 的 runner 上跑、PR 前本地手测、新机器环境验证。

## Async usage

Use `AsyncClient` when working in an async runtime:

```python
import asyncio
import typesense


async def main() -> None:
    client = typesense.AsyncClient({
        "api_key": "abcd",
        "nodes": [{"host": "localhost", "port": "8108", "protocol": "http"}],
        "connection_timeout_seconds": 2,
    })

    print(await client.collections.retrieve())
    await client.api_call.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

See `examples/async_collection_operations.py` for a fuller async walkthrough.

## Compatibility

| Typesense Server | typesense-python |
|------------------|------------------|
| \>= v30.0        | \>= v2.0.0       |
| \>= v28.0        | \>= v1.0.0       |
| \>= v26.0        | \>= v0.20.0      |
| \>= v0.25.0      | \>= v0.16.0      |
| \>= v0.23.0      | \>= v0.14.0      |
| \>= v0.21.0      | \>= v0.13.0      |
| \>= v0.20.0      | \>= v0.11.0      |
| \>= v0.19.0      | \>= v0.10.0      |
| \>= v0.17.0      | \>= v0.9.0       |
| \>= v0.16.0      | \>= v0.8.0       |
| \>= v0.15.0      | \>= v0.7.0       |

## Contributing

> [!NOTE]
> Development happens in async-only code; sync code is generated automatically via `utils/run-unasync.py`.

Bug reports and pull requests are welcome on GitHub at [https://github.com/typesense/typesense-python].
If you change any part of the client's source code, run `uv run utils/run-unasync.py` before opening a PR to keep the generated sync files in sync.

## License

`typesense-python` is distributed under the Apache 2 license.
