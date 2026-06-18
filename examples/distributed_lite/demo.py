"""Write documents and run keyword searches against a Typesense Lite cluster."""

from __future__ import annotations

import json

import httpx


COORDINATOR_URL = "http://127.0.0.1:9100"

DOCUMENTS = [
    {
        "id": "book-1",
        "title": "Distributed search basics",
        "body": "Search engines split documents across shards.",
    },
    {
        "id": "book-2",
        "title": "Replication for availability",
        "body": "Replicas keep search available when a node is stopped.",
    },
    {
        "id": "book-3",
        "title": "Keyword indexing",
        "body": "An inverted index maps search terms to document ids.",
    },
    {
        "id": "book-4",
        "title": "Coordinator query fanout",
        "body": "The coordinator merges search hits from every shard.",
    },
]


def main() -> int:
    with httpx.Client(timeout=5.0) as client:
        print("Cluster:")
        _print_json(client.get(f"{COORDINATOR_URL}/cluster").json())

        print("\nWriting documents:")
        for document in DOCUMENTS:
            response = client.post(
                f"{COORDINATOR_URL}/collections/books/documents",
                json=document,
            )
            response.raise_for_status()
            _print_json(response.json())

        print("\nSearch: search")
        _print_json(_search(client, "search"))

        print("\nSearch: coordinator")
        _print_json(_search(client, "coordinator"))

    print(
        "\nStop one data-node process and rerun this demo to observe replica fallback."
    )
    return 0


def _search(client: httpx.Client, query: str) -> dict:
    response = client.get(
        f"{COORDINATOR_URL}/collections/books/documents/search",
        params={"q": query, "limit": 10},
    )
    response.raise_for_status()
    return response.json()


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
