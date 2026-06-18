"""In-memory document store and inverted index."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .tokenizer import tokenize

Document = dict[str, Any]
SearchHit = dict[str, Any]


class InvertedIndex:
    """Small in-memory inverted index with term-frequency scoring."""

    def __init__(self) -> None:
        self.documents: dict[str, Document] = {}
        self._index: dict[str, dict[str, int]] = defaultdict(dict)
        self._document_terms: dict[str, Counter[str]] = {}

    def add_document(self, document: Document) -> None:
        document_id = document.get("id")
        if not isinstance(document_id, str) or not document_id:
            raise ValueError("document id must be a non-empty string")

        if document_id in self.documents:
            self._remove_document_terms(document_id)

        terms = Counter(tokenize(self._indexable_text(document)))
        self.documents[document_id] = dict(document)
        self._document_terms[document_id] = terms

        for term, frequency in terms.items():
            self._index[term][document_id] = frequency

    def get_document(self, document_id: str) -> Document:
        return self.documents[document_id]

    def list_documents(self) -> list[Document]:
        return [self.documents[document_id] for document_id in sorted(self.documents)]

    def delete_document(self, document_id: str) -> Document:
        document = self.documents.pop(document_id)
        self._remove_document_terms(document_id)
        return document

    def search(self, query: str, limit: int = 10) -> list[SearchHit]:
        scores: dict[str, float] = defaultdict(float)

        for term in tokenize(query):
            for document_id, frequency in self._index.get(term, {}).items():
                scores[document_id] += float(frequency)

        hits = [
            {
                "id": document_id,
                "score": score,
                "document": self.documents[document_id],
            }
            for document_id, score in scores.items()
        ]
        hits.sort(key=lambda hit: (-hit["score"], hit["id"]))
        return hits[:limit]

    def _remove_document_terms(self, document_id: str) -> None:
        for term in self._document_terms.get(document_id, {}):
            postings = self._index.get(term)
            if postings is None:
                continue
            postings.pop(document_id, None)
            if not postings:
                self._index.pop(term, None)
        self._document_terms.pop(document_id, None)

    @staticmethod
    def _indexable_text(document: Document) -> str:
        return " ".join(
            value
            for key, value in document.items()
            if key != "id" and isinstance(value, str)
        )
