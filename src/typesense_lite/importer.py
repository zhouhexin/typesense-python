"""Upload parsers for Typesense Lite document imports."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

Document = dict[str, Any]


def parse_upload(filename: str, content: bytes) -> list[Document]:
    """Parse an uploaded file into one or more documents."""

    suffix = Path(filename).suffix.lower()
    text = content.decode("utf-8")
    slug = _slugify(filename)

    if suffix in {".txt", ".md"}:
        return [
            {
                "id": slug,
                "title": filename,
                "body": text,
                "source": filename,
            }
        ]

    if suffix == ".json":
        return _parse_json(filename, text, slug)

    if suffix == ".jsonl":
        return _parse_jsonl(filename, text, slug)

    raise ValueError(f"unsupported upload type for {filename}")


def _parse_json(filename: str, text: str, slug: str) -> list[Document]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f"invalid JSON in {filename}: {error.msg}") from error

    if isinstance(payload, dict):
        return [_with_generated_id(payload, f"{slug}-1")]

    if isinstance(payload, list):
        documents: list[Document] = []
        for index, item in enumerate(payload, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"{filename} item {index} must be a JSON object")
            documents.append(_with_generated_id(item, f"{slug}-{index}"))
        return documents

    raise ValueError(f"{filename} must contain a JSON object or array")


def _parse_jsonl(filename: str, text: str, slug: str) -> list[Document]:
    documents: list[Document] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"invalid JSON in {filename} line {line_number}: {error.msg}"
            ) from error
        if not isinstance(payload, dict):
            raise ValueError(f"{filename} line {line_number} must be a JSON object")
        documents.append(_with_generated_id(payload, f"{slug}-{line_number}"))
    return documents


def _with_generated_id(document: Document, generated_id: str) -> Document:
    if isinstance(document.get("id"), str) and document["id"]:
        return dict(document)
    return {**document, "id": generated_id}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "document"
