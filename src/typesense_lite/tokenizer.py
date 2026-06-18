"""Basic tokenizer for the Typesense Lite demo search engine."""

from __future__ import annotations

import re

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")


def tokenize(text: str) -> list[str]:
    """Return lowercase alphanumeric tokens from text."""
    return [match.group(0).lower() for match in TOKEN_PATTERN.finditer(text)]

