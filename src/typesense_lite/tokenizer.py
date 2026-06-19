"""Basic tokenizer for the Typesense Lite demo search engine."""

from __future__ import annotations

import re

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+")


def tokenize(text: str) -> list[str]:
    """Return searchable tokens from English/number words and Chinese text."""
    tokens: list[str] = []
    for match in TOKEN_PATTERN.finditer(text):
        value = match.group(0)
        if _is_chinese(value):
            tokens.extend(_chinese_tokens(value))
        else:
            tokens.append(value.lower())
    return tokens


def _chinese_tokens(value: str) -> list[str]:
    if len(value) == 1:
        return [value]
    return [value[index : index + 2] for index in range(len(value) - 1)]


def _is_chinese(value: str) -> bool:
    return all("\u4e00" <= char <= "\u9fff" for char in value)
