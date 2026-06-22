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
