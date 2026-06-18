# Typesense Lite Distributed Search Design

## Overview

This document defines a demo-grade distributed search engine built alongside the
existing `typesense-python` SDK. The existing SDK remains unchanged. The new code
adds a small service layer named `typesense_lite` that can run one coordinator
process and multiple data-node processes on localhost, each listening on a
different port.

The first version focuses on making the distributed mechanics visible and easy to
understand: sharding, primary/replica placement, document routing, keyword
indexing, query fan-out, and global result merging.

## Goals

- Run a local cluster with one coordinator and multiple data nodes.
- Store documents by collection.
- Route writes by `hash(document_id) % shard_count`.
- Maintain a primary and optional replicas for each shard.
- Build a basic in-memory inverted index on each data node.
- Search with simple keyword matching.
- Fan out search requests across shards.
- Merge shard-local hits into global top results.
- Persist documents to local JSONL files and rebuild indexes on startup.
- Provide scripts for starting the demo cluster and running an end-to-end demo.

## Non-Goals

- Production-grade consensus such as Raft or Paxos.
- Dynamic membership, automatic failover leadership, or shard rebalancing.
- BM25, vector search, semantic search, Chinese tokenization, or advanced ranking.
- Complex collection schemas, field weights, filters, sorting, or faceting.
- Production-grade storage, compaction, snapshots, or recovery semantics.
- Replacing the existing Typesense Python SDK.

## Package Layout

Add a new package and examples:

```text
src/typesense_lite/
  __init__.py
  schemas.py
  tokenizer.py
  index.py
  storage.py
  node.py
  cluster.py
  coordinator.py
  server.py

examples/distributed_lite/
  cluster_config.json
  start_cluster.py
  demo.py

tests/typesense_lite/
  test_tokenizer.py
  test_index.py
  test_cluster.py
  test_node.py
  test_coordinator.py
```

## Process Model

The demo cluster runs as separate local processes:

```text
coordinator  http://127.0.0.1:9100
node-1       http://127.0.0.1:9101
node-2       http://127.0.0.1:9102
node-3       http://127.0.0.1:9103
```

The coordinator is the only public entry point for document writes and searches.
Data-node APIs are considered internal and are called by the coordinator.

## Cluster Configuration

Use a static JSON config for the first version:

```json
{
  "coordinator": {"host": "127.0.0.1", "port": 9100},
  "shard_count": 3,
  "nodes": [
    {"id": "node-1", "host": "127.0.0.1", "port": 9101},
    {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    {"id": "node-3", "host": "127.0.0.1", "port": 9103}
  ],
  "shards": {
    "0": {"primary": "node-1", "replicas": ["node-2"]},
    "1": {"primary": "node-2", "replicas": ["node-3"]},
    "2": {"primary": "node-3", "replicas": ["node-1"]}
  }
}
```

`ClusterMap` loads this config and exposes:

- `get_shard_id(document_id: str) -> int`
- `get_primary(shard_id: int) -> NodeInfo`
- `get_replicas(shard_id: int) -> list[NodeInfo]`
- `get_search_candidates(shard_id: int) -> list[NodeInfo]`
- `node_url(node_id: str) -> str`

## Data Model

Documents are JSON objects. Every document must have a non-empty string `id`.
All other fields are accepted without schema validation.

Only string fields participate in indexing. Nested values are ignored in the
first version unless they are already string values at the top level.

Internal storage:

```text
documents: dict[str, dict]
inverted_index: dict[str, dict[str, int]]
```

The inverted index maps:

```text
term -> document_id -> term_frequency
```

## Tokenization

`tokenizer.py` implements a deterministic basic tokenizer:

- lowercase input
- split on non-alphanumeric characters
- drop empty tokens

Example:

```text
"Distributed search, Search!" -> ["distributed", "search", "search"]
```

## Indexing

`InvertedIndex.add_document(document)`:

1. Validate that `document["id"]` exists and is a string.
2. Extract all top-level string fields except `id`.
3. Concatenate the strings into a single text body.
4. Tokenize the body.
5. Count token frequency per document.
6. Update `term -> doc_id -> frequency`.

`InvertedIndex.search(query, limit)`:

1. Tokenize the query.
2. For each token, read its posting list.
3. Accumulate `score += term_frequency` per document.
4. Return hits sorted by score descending, then id ascending for stable ordering.

The first version uses simple term-frequency scoring:

```text
score = sum(matched_term_frequency)
```

## Storage

Each data node writes documents to JSONL:

```text
.data/typesense_lite/{node_id}/shard-{shard_id}/collections/{collection}/documents.jsonl
```

Each line is one full document. On startup, a data node scans its data directory,
loads each document, and rebuilds the in-memory indexes.

For the first version, updates are append-only. If the same document id appears
multiple times in the JSONL file, the latest record wins when rebuilding.

## Coordinator API

Public endpoints:

```http
POST /collections/{collection}/documents
GET  /collections/{collection}/documents/search?q={query}&limit={limit}
GET  /cluster
GET  /health
```

Write response:

```json
{
  "ok": true,
  "id": "doc-1",
  "shard_id": 2,
  "primary": "node-3",
  "replicas": [
    {"node": "node-1", "ok": true}
  ],
  "warnings": []
}
```

Search response:

```json
{
  "found": 1,
  "hits": [
    {
      "id": "doc-1",
      "score": 3.0,
      "document": {
        "id": "doc-1",
        "title": "distributed search engine"
      },
      "shard_id": 2,
      "node_id": "node-3"
    }
  ]
}
```

## Data Node API

Internal endpoints:

```http
POST /internal/shards/{shard_id}/collections/{collection}/documents
GET  /internal/shards/{shard_id}/collections/{collection}/search?q={query}&limit={limit}
GET  /health
```

Data nodes do not decide shard placement. They accept the shard id supplied by
the coordinator and maintain local storage and indexes for that shard.

## Write Flow

```text
client -> coordinator
coordinator validates document id
coordinator computes shard_id = hash(id) % shard_count
coordinator sends write to primary node for the shard
primary persists document and updates its local index
coordinator sends the same write to replica nodes
coordinator returns success if primary write succeeds
replica failures are returned as warnings
```

The first version uses synchronous replication with relaxed success semantics:

- primary success means the write succeeds
- replica failures do not fail the request
- replica failures are visible in `warnings`

This keeps the demo understandable while still showing how replication affects
availability and consistency.

## Search Flow

```text
client -> coordinator
coordinator iterates every shard
coordinator selects primary first, then replicas if primary fails
coordinator concurrently queries one available node per shard
each data node returns shard-local top hits
coordinator merges all hits
coordinator sorts by score descending, then id ascending
coordinator returns global top results
```

If all candidates for a shard fail, the coordinator skips that shard and adds a
warning to the response.

## Health and Failure Behavior

Each process exposes `GET /health`.

The coordinator uses request failures as health signals during a single search or
write operation. The first version does not need a background health checker.
For a shard search, the coordinator tries candidates in this order:

1. primary
2. replicas in config order

For a write, the coordinator requires the primary. If the primary is unavailable,
the write fails. Automatic promotion is out of scope for the first version.

## Startup Script

`examples/distributed_lite/start_cluster.py` starts all processes from the static
config using `subprocess.Popen`.

Example command:

```bash
python examples/distributed_lite/start_cluster.py
```

The script should:

- load `cluster_config.json`
- start each data node with `ROLE=node`, `NODE_ID`, and `CLUSTER_CONFIG`
- start the coordinator with `ROLE=coordinator` and `CLUSTER_CONFIG`
- stream process output to the terminal
- terminate child processes on Ctrl-C

## Demo Script

`examples/distributed_lite/demo.py` should:

1. write a small set of documents through the coordinator
2. search for one or more keywords
3. print shard placement and search results
4. explain how to stop one node manually and rerun the search

## Dependencies

Add runtime dependencies:

```text
fastapi
uvicorn
```

Use existing `httpx` for coordinator-to-node HTTP calls.

## Test Plan

Unit tests:

- tokenizer lowercases and splits as expected
- index adds documents and returns term-frequency ranked hits
- cluster map computes stable shard ids and returns placements
- data node writes, persists, reloads, and searches documents
- coordinator routes writes to the expected primary and replicas
- coordinator merges shard-local results into globally sorted hits

Integration test:

- start app instances in-process or with test clients where practical
- write documents through the coordinator
- verify search returns hits across multiple shards
- simulate a failed primary during search and verify replica fallback

## Acceptance Criteria

The feature is complete when:

- `python examples/distributed_lite/start_cluster.py` starts one coordinator and
  three data nodes on localhost.
- `python examples/distributed_lite/demo.py` writes sample documents and performs
  a keyword search through the coordinator.
- Documents are distributed across three shards by document id.
- Each shard has one configured replica.
- Search results are merged across shards and sorted by score.
- Stopping one data node still allows searches for shards with available
  replicas.
- Unit tests for tokenizer, index, cluster map, node, and coordinator pass.

