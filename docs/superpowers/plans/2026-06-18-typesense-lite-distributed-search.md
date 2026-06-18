# Typesense Lite Distributed Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a localhost multi-process distributed keyword-search demo with one coordinator and multiple data nodes.

**Architecture:** Add a separate `typesense_lite` package beside the existing SDK. Data nodes own local document storage and inverted indexes; the coordinator owns shard routing, replication writes, query fan-out, and result merging.

**Tech Stack:** Python 3.9+, FastAPI, Uvicorn, httpx, pytest.

---

## File Structure

- Create `src/typesense_lite/__init__.py`: package marker.
- Create `src/typesense_lite/schemas.py`: dataclasses and typed helpers shared by node and coordinator.
- Create `src/typesense_lite/tokenizer.py`: basic lowercasing regex tokenizer.
- Create `src/typesense_lite/index.py`: in-memory document store and inverted index.
- Create `src/typesense_lite/storage.py`: JSONL document persistence and reload.
- Create `src/typesense_lite/cluster.py`: static cluster config loading and shard placement lookup.
- Create `src/typesense_lite/node.py`: data-node operations for write, reload, and shard-local search.
- Create `src/typesense_lite/coordinator.py`: write routing, replica writes, search fan-out, result merge.
- Create `src/typesense_lite/server.py`: FastAPI app factory selected by environment.
- Create `examples/distributed_lite/cluster_config.json`: default local cluster.
- Create `examples/distributed_lite/start_cluster.py`: starts coordinator and data-node processes.
- Create `examples/distributed_lite/demo.py`: writes sample documents and searches.
- Modify `pyproject.toml`: add `fastapi` and `uvicorn` runtime dependencies.
- Create tests under `tests/typesense_lite/`.

## Task 1: Tokenizer and Inverted Index

**Files:**
- Create: `src/typesense_lite/__init__.py`
- Create: `src/typesense_lite/tokenizer.py`
- Create: `src/typesense_lite/index.py`
- Test: `tests/typesense_lite/test_tokenizer.py`
- Test: `tests/typesense_lite/test_index.py`

- [ ] **Step 1: Write failing tokenizer tests**

```python
from typesense_lite.tokenizer import tokenize


def test_tokenize_lowercases_and_splits_on_non_alphanumeric() -> None:
    assert tokenize("Distributed search, Search!") == [
        "distributed",
        "search",
        "search",
    ]


def test_tokenize_drops_empty_tokens() -> None:
    assert tokenize("  ---  ") == []
```

- [ ] **Step 2: Run tokenizer tests**

Run: `pytest tests/typesense_lite/test_tokenizer.py -v`
Expected: FAIL because `typesense_lite.tokenizer` does not exist.

- [ ] **Step 3: Implement tokenizer**

```python
import re

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")


def tokenize(text: str) -> list[str]:
    return [match.group(0).lower() for match in TOKEN_PATTERN.finditer(text)]
```

- [ ] **Step 4: Write failing index tests**

```python
from typesense_lite.index import InvertedIndex


def test_index_searches_string_fields_by_term_frequency() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "title": "Search search engine"})
    index.add_document({"id": "doc-2", "title": "Distributed engine"})

    hits = index.search("search engine", limit=10)

    assert [hit["id"] for hit in hits] == ["doc-1", "doc-2"]
    assert hits[0]["score"] == 3.0
    assert hits[1]["score"] == 1.0


def test_index_replaces_existing_document() -> None:
    index = InvertedIndex()
    index.add_document({"id": "doc-1", "title": "old term"})
    index.add_document({"id": "doc-1", "title": "new term"})

    assert index.search("old", limit=10) == []
    assert index.search("new", limit=10)[0]["id"] == "doc-1"
```

- [ ] **Step 5: Run index tests**

Run: `pytest tests/typesense_lite/test_index.py -v`
Expected: FAIL because `InvertedIndex` does not exist.

- [ ] **Step 6: Implement index**

Define `InvertedIndex` with `documents`, `_document_terms`, `add_document`, `_remove_document_terms`, and `search`. Validate that `id` is a non-empty string and index only top-level string fields except `id`.

- [ ] **Step 7: Verify task**

Run: `pytest tests/typesense_lite/test_tokenizer.py tests/typesense_lite/test_index.py -v`
Expected: PASS.

## Task 2: Storage and Data Node

**Files:**
- Create: `src/typesense_lite/storage.py`
- Create: `src/typesense_lite/node.py`
- Test: `tests/typesense_lite/test_node.py`

- [ ] **Step 1: Write failing node persistence test**

```python
from typesense_lite.node import SearchNode


def test_node_persists_reloads_and_searches_documents(tmp_path) -> None:
    node = SearchNode(node_id="node-1", data_dir=tmp_path)
    node.add_document(0, "books", {"id": "doc-1", "title": "Distributed search"})

    reloaded = SearchNode(node_id="node-1", data_dir=tmp_path)
    hits = reloaded.search(0, "books", "distributed", limit=10)

    assert hits[0]["id"] == "doc-1"
    assert hits[0]["document"]["title"] == "Distributed search"
```

- [ ] **Step 2: Run node test**

Run: `pytest tests/typesense_lite/test_node.py -v`
Expected: FAIL because `SearchNode` does not exist.

- [ ] **Step 3: Implement storage**

Implement `DocumentStorage` with `append_document`, `iter_documents`, and deterministic paths under `{data_dir}/{node_id}/shard-{shard_id}/collections/{collection}/documents.jsonl`.

- [ ] **Step 4: Implement data node**

Implement `SearchNode` that owns per `(shard_id, collection)` `InvertedIndex` instances, reloads documents in `__init__`, appends on write, and returns shard-local search hits enriched with `shard_id` and `node_id`.

- [ ] **Step 5: Verify task**

Run: `pytest tests/typesense_lite/test_node.py -v`
Expected: PASS.

## Task 3: Cluster Map

**Files:**
- Create: `src/typesense_lite/schemas.py`
- Create: `src/typesense_lite/cluster.py`
- Test: `tests/typesense_lite/test_cluster.py`

- [ ] **Step 1: Write failing cluster tests**

```python
from typesense_lite.cluster import ClusterMap


CONFIG = {
    "coordinator": {"host": "127.0.0.1", "port": 9100},
    "shard_count": 3,
    "nodes": [
        {"id": "node-1", "host": "127.0.0.1", "port": 9101},
        {"id": "node-2", "host": "127.0.0.1", "port": 9102},
    ],
    "shards": {
        "0": {"primary": "node-1", "replicas": ["node-2"]},
        "1": {"primary": "node-2", "replicas": ["node-1"]},
        "2": {"primary": "node-1", "replicas": ["node-2"]},
    },
}


def test_cluster_map_returns_placements() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    assert cluster.get_primary(1).id == "node-2"
    assert [node.id for node in cluster.get_replicas(1)] == ["node-1"]
    assert cluster.node_url("node-2") == "http://127.0.0.1:9102"


def test_cluster_map_shard_id_is_stable() -> None:
    cluster = ClusterMap.from_dict(CONFIG)

    assert cluster.get_shard_id("doc-1") == cluster.get_shard_id("doc-1")
    assert 0 <= cluster.get_shard_id("doc-1") < 3
```

- [ ] **Step 2: Run cluster tests**

Run: `pytest tests/typesense_lite/test_cluster.py -v`
Expected: FAIL because `ClusterMap` does not exist.

- [ ] **Step 3: Implement schemas and cluster map**

Use dataclasses `NodeInfo`, `ShardPlacement`, and `ClusterMap`. Use SHA-256 for stable shard hashing instead of Python's process-randomized `hash()`.

- [ ] **Step 4: Verify task**

Run: `pytest tests/typesense_lite/test_cluster.py -v`
Expected: PASS.

## Task 4: Coordinator

**Files:**
- Create: `src/typesense_lite/coordinator.py`
- Test: `tests/typesense_lite/test_coordinator.py`

- [ ] **Step 1: Write failing coordinator tests with fake transport**

Use `httpx.MockTransport` to assert writes go to primary and replica, and searches merge hits from each shard sorted by score.

- [ ] **Step 2: Run coordinator tests**

Run: `pytest tests/typesense_lite/test_coordinator.py -v`
Expected: FAIL because `Coordinator` does not exist.

- [ ] **Step 3: Implement coordinator**

Implement async `add_document(collection, document)` and `search(collection, query, limit)`. Primary write failure raises an exception. Replica write failures become warnings. Search tries primary then replicas per shard.

- [ ] **Step 4: Verify task**

Run: `pytest tests/typesense_lite/test_coordinator.py -v`
Expected: PASS.

## Task 5: FastAPI Server

**Files:**
- Create: `src/typesense_lite/server.py`
- Test: `tests/typesense_lite/test_server.py`

- [ ] **Step 1: Write failing server smoke tests**

Use FastAPI `TestClient` to verify `GET /health` and data-node internal write/search endpoints.

- [ ] **Step 2: Add dependencies**

Add `fastapi>=0.115.0` and `uvicorn>=0.30.0` to `pyproject.toml`.

- [ ] **Step 3: Implement server app factory**

Expose `create_app(role, cluster_config_path, node_id=None, data_dir=None)`. The module-level `app` reads `ROLE`, `CLUSTER_CONFIG`, `NODE_ID`, and `DATA_DIR` from environment for Uvicorn.

- [ ] **Step 4: Verify task**

Run: `pytest tests/typesense_lite/test_server.py -v`
Expected: PASS.

## Task 6: Examples and End-to-End Verification

**Files:**
- Create: `examples/distributed_lite/cluster_config.json`
- Create: `examples/distributed_lite/start_cluster.py`
- Create: `examples/distributed_lite/demo.py`

- [ ] **Step 1: Add cluster config**

Use one coordinator on port `9100`, three data nodes on `9101`-`9103`, three shards, and one replica per shard.

- [ ] **Step 2: Add start script**

Start Uvicorn subprocesses with `ROLE`, `NODE_ID`, `CLUSTER_CONFIG`, and `DATA_DIR`. Terminate children on Ctrl-C.

- [ ] **Step 3: Add demo script**

Write sample documents through `http://127.0.0.1:9100`, run searches, and print JSON responses.

- [ ] **Step 4: Verify all tests**

Run: `pytest tests/typesense_lite -v`
Expected: PASS.

- [ ] **Step 5: Verify formatting**

Run: `ruff check src/typesense_lite tests/typesense_lite examples/distributed_lite`
Expected: PASS.

