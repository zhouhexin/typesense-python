"""Upload parsers for Typesense Lite document imports."""

from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any

from docx import Document as DocxDocument
from pypdf import PdfReader

Document = dict[str, Any]


def parse_upload(filename: str, content: bytes) -> list[Document]:
    """Parse an uploaded file into one or more documents."""

    suffix = Path(filename).suffix.lower()
    slug = _slugify(filename)

    if suffix in {".txt", ".md"}:
        text = content.decode("utf-8")
        return [
            {
                "id": slug,
                "title": filename,
                "body": text,
                "source": filename,
            }
        ]

    if suffix == ".json":
        text = content.decode("utf-8")
        return _parse_json(filename, text, slug)

    if suffix == ".jsonl":
        text = content.decode("utf-8")
        return _parse_jsonl(filename, text, slug)

    if suffix == ".docx":
        return [_document_from_text(filename, slug, _extract_docx(filename, content))]

    if suffix == ".pdf":
        return [_document_from_text(filename, slug, _extract_pdf(filename, content))]

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


def _extract_docx(filename: str, content: bytes) -> str:
    try:
        document = DocxDocument(BytesIO(content))
        text = "\n".join(paragraph.text for paragraph in document.paragraphs)
    except Exception as error:
        raise ValueError(f"could not parse {filename}: {error}") from error
    return _require_text(filename, text)


def _extract_pdf(filename: str, content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as error:
        raise ValueError(f"could not parse {filename}: {error}") from error
    return _require_text(filename, text)


def _document_from_text(filename: str, document_id: str, body: str) -> Document:
    return {
        "id": document_id,
        "title": filename,
        "body": body,
        "source": filename,
    }


def _require_text(filename: str, text: str) -> str:
    body = text.strip()
    if not body:
        raise ValueError(f"no extractable text found in {filename}")
    return body


def _with_generated_id(document: Document, generated_id: str) -> Document:
    if isinstance(document.get("id"), str) and document["id"]:
        return dict(document)
    return {**document, "id": generated_id}


def _slugify(value: str) -> str:
    # Python's Unicode-aware ``\w`` preserves CJK filenames instead of
    # collapsing every Chinese DOCX/PDF upload to the same extension-only ID.
    slug = re.sub(r"[\W_]+", "-", value.casefold()).strip("-")
    return slug or "document"
