"""In-memory document store and search engine for the Typesense Lite demo."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from functools import cmp_to_key
import html
import math
import re
from typing import Any

from .tokenizer import tokenize

Document = dict[str, Any]
SearchHit = dict[str, Any]

_FILTER_SPLIT_RE = re.compile(r"\s*(?:&&|\band\b)\s*", re.IGNORECASE)
_FILTER_RE = re.compile(r"^\s*([^:]+?)\s*:\s*(>=|<=|>|<|=)?\s*(.+?)\s*$")


@dataclass(frozen=True)
class _SortSpec:
    field: str
    direction: str


class InvertedIndex:
    """Small in-memory search index with a compatible basic search API."""

    def __init__(self) -> None:
        self.documents: dict[str, Document] = {}
        self._index: dict[str, dict[str, int]] = defaultdict(dict)
        self._document_terms: dict[str, Counter[str]] = {}
        self._document_fields: dict[str, dict[str, Counter[str]]] = {}

    def add_document(self, document: Document) -> None:
        document_id = document.get("id")
        if not isinstance(document_id, str) or not document_id:
            raise ValueError("document id must be a non-empty string")

        if document_id in self.documents:
            self._remove_document_terms(document_id)

        terms = Counter(tokenize(self._indexable_text(document)))
        field_terms = {
            field: Counter(tokenize(value))
            for field, value in document.items()
            if field != "id" and isinstance(value, str)
        }
        self.documents[document_id] = dict(document)
        self._document_terms[document_id] = terms
        self._document_fields[document_id] = field_terms

        for term, frequency in terms.items():
            self._index[term][document_id] = frequency

    def get_document(self, document_id: str) -> Document:
        return self.documents[document_id]

    def list_documents(self) -> list[Document]:
        return [self.documents[document_id] for document_id in sorted(self.documents)]

    def list_document_ids(self) -> list[str]:
        """Return sorted list of document IDs."""
        return sorted(self.documents.keys())

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

    def search_query(
        self,
        query: str,
        *,
        query_by: str | None = None,
        query_by_weights: str | None = None,
        prefix: bool = False,
        num_typos: int = 0,
        filter_by: str | None = None,
        sort_by: str | None = None,
        facet_by: str | None = None,
        page: int = 1,
        per_page: int | None = 10,
        highlight_fields: str | None = None,
    ) -> dict[str, Any]:
        query_terms = tokenize(query)
        searchable_fields = self._resolve_search_fields(query_by)
        field_weights = self._resolve_field_weights(searchable_fields, query_by_weights)
        facet_fields = _split_csv(facet_by)
        requested_highlight_fields = (
            _split_csv(highlight_fields) if highlight_fields is not None else None
        )
        field_stats = self._build_field_stats(searchable_fields)

        hits: list[SearchHit] = []
        for document_id, document in self.documents.items():
            if not _matches_filter(document, filter_by):
                continue

            score, matched_terms, matched_fields = self._score_document(
                document_id=document_id,
                query_terms=query_terms,
                searchable_fields=searchable_fields,
                field_weights=field_weights,
                field_stats=field_stats,
                total_docs=len(self.documents),
                prefix=prefix,
                num_typos=num_typos,
            )
            if query_terms and score <= 0:
                continue

            hits.append(
                {
                    "id": document_id,
                    "score": score,
                    "text_match": score,
                    "document": document,
                    "_matched_terms": sorted(matched_terms),
                    "_matched_fields": matched_fields,
                }
            )

        _sort_hits(hits, _parse_sort_by(sort_by))
        total = len(hits)
        facet_counts = _facet_counts_from_hits(hits, facet_fields)
        paginated_hits = _paginate_hits(hits, page=page, per_page=per_page)

        for hit in paginated_hits:
            matched_fields = hit.pop("_matched_fields", {})
            matched_terms = hit.pop("_matched_terms", [])
            fields = (
                requested_highlight_fields
                if requested_highlight_fields is not None
                else list(matched_fields)
            )
            highlights = self._build_highlights(
                hit["document"], fields, matched_fields, matched_terms
            )
            if highlights:
                hit["highlights"] = highlights
                hit["highlight"] = highlights[0]["snippet"]
            hit["text_match_info"] = {
                "score": hit["score"],
                "matched_terms": matched_terms,
            }

        return {
            "found": total,
            "out_of": total,
            "page": page,
            "per_page": per_page if per_page is not None else total,
            "hits": paginated_hits,
            "facet_counts": facet_counts,
        }

    def _resolve_search_fields(self, query_by: str | None) -> list[str]:
        available_fields = self._available_string_fields()
        requested_fields = _split_csv(query_by)
        if not requested_fields:
            return available_fields
        return [field for field in requested_fields if field in available_fields]

    @staticmethod
    def _resolve_field_weights(
        fields: list[str], query_by_weights: str | None
    ) -> dict[str, float]:
        weights = [1.0] * len(fields)
        for index, value in enumerate(_split_csv(query_by_weights)[: len(fields)]):
            try:
                weights[index] = float(value)
            except ValueError:
                weights[index] = 1.0
        return dict(zip(fields, weights))

    def _available_string_fields(self) -> list[str]:
        fields: set[str] = set()
        for field_terms in self._document_fields.values():
            fields.update(field_terms)
        return sorted(fields)

    def _build_field_stats(self, fields: list[str]) -> dict[str, dict[str, Any]]:
        stats: dict[str, dict[str, Any]] = {}
        for field in fields:
            doc_freq: Counter[str] = Counter()
            total_length = 0
            doc_count = 0
            for document_fields in self._document_fields.values():
                terms = document_fields.get(field)
                if not terms:
                    continue
                doc_count += 1
                total_length += sum(terms.values())
                doc_freq.update(terms.keys())
            stats[field] = {
                "doc_freq": doc_freq,
                "avg_len": (total_length / doc_count) if doc_count else 0.0,
            }
        return stats

    def _score_document(
        self,
        *,
        document_id: str,
        query_terms: list[str],
        searchable_fields: list[str],
        field_weights: dict[str, float],
        field_stats: dict[str, dict[str, Any]],
        total_docs: int,
        prefix: bool,
        num_typos: int,
    ) -> tuple[float, set[str], dict[str, set[str]]]:
        if not query_terms:
            return 0.0, set(), {}

        document_fields = self._document_fields.get(document_id, {})
        score = 0.0
        matched_terms: set[str] = set()
        matched_fields: dict[str, set[str]] = defaultdict(set)
        for field in searchable_fields:
            terms = document_fields.get(field)
            if not terms:
                continue
            stats = field_stats.get(field, {})
            doc_len = sum(terms.values())
            for query_term in query_terms:
                match = _best_match(
                    query_term, terms, prefix=prefix, num_typos=num_typos
                )
                if match is None:
                    continue
                matched_token, match_strength = match
                score += (
                    field_weights.get(field, 1.0)
                    * match_strength
                    * _bm25(
                        tf=terms[matched_token],
                        df=int(stats.get("doc_freq", {}).get(matched_token, 0)),
                        doc_len=doc_len,
                        avg_len=float(stats.get("avg_len") or 0.0),
                        total_docs=total_docs,
                    )
                )
                matched_terms.add(matched_token)
                matched_fields[field].add(matched_token)
        return score, matched_terms, matched_fields

    @staticmethod
    def _build_highlights(
        document: Document,
        fields: list[str],
        matched_fields: dict[str, set[str]],
        matched_terms: list[str],
    ) -> list[dict[str, str]]:
        highlights: list[dict[str, str]] = []
        for field in fields:
            value = document.get(field)
            if not isinstance(value, str):
                continue
            terms = matched_fields.get(field, set()) or set(matched_terms)
            snippet = _highlight_text(value, terms)
            if snippet is not None:
                highlights.append({"field": field, "snippet": snippet})
        return highlights

    def _remove_document_terms(self, document_id: str) -> None:
        for term in self._document_terms.get(document_id, {}):
            postings = self._index.get(term)
            if postings is None:
                continue
            postings.pop(document_id, None)
            if not postings:
                self._index.pop(term, None)
        self._document_terms.pop(document_id, None)
        self._document_fields.pop(document_id, None)

    @staticmethod
    def _indexable_text(document: Document) -> str:
        return " ".join(
            value
            for key, value in document.items()
            if key != "id" and isinstance(value, str)
        )


def _split_csv(value: str | None) -> list[str]:
    if value is None:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _bm25(*, tf: int, df: int, doc_len: int, avg_len: float, total_docs: int) -> float:
    if tf <= 0 or total_docs <= 0:
        return 0.0
    k1 = 1.5
    b = 0.75
    idf = math.log1p((total_docs - df + 0.5) / (df + 0.5))
    norm = 1.0 if avg_len <= 0 else 1.0 - b + b * (doc_len / avg_len)
    return idf * ((tf * (k1 + 1.0)) / (tf + k1 * norm))


def _best_match(
    query_term: str,
    terms: Counter[str],
    *,
    prefix: bool,
    num_typos: int,
) -> tuple[str, float] | None:
    if query_term in terms:
        return query_term, 1.0
    if prefix:
        matches = [term for term in terms if term.startswith(query_term)]
        if matches:
            return min(matches, key=lambda term: (len(term), -terms[term])), 0.85
    if num_typos <= 0:
        return None

    candidates = []
    for term in terms:
        distance = _edit_distance(query_term, term, num_typos)
        if distance is not None:
            candidates.append((distance, -terms[term], term))
    if not candidates:
        return None
    distance, _, term = min(candidates)
    return term, 0.7 if distance == 1 else 0.55


def _matches_filter(document: Document, filter_by: str | None) -> bool:
    if filter_by is None or not filter_by.strip():
        return True
    clauses = [clause for clause in _FILTER_SPLIT_RE.split(filter_by) if clause]
    return all(_matches_clause(document, clause) for clause in clauses)


def _matches_clause(document: Document, clause: str) -> bool:
    match = _FILTER_RE.match(clause)
    if match is None:
        return True
    field, operator, raw_value = match.groups()
    values = document.get(field.strip())
    values = values if isinstance(values, list) else [values]
    operator = operator or "="
    if operator == "=":
        expected = _coerce_scalar(raw_value)
        return any(_compare_equal(item, expected) for item in values)

    expected = _coerce_number(raw_value)
    if expected is None:
        return False
    for item in values:
        actual = _coerce_number(item)
        if actual is None:
            continue
        if operator == ">" and actual > expected:
            return True
        if operator == ">=" and actual >= expected:
            return True
        if operator == "<" and actual < expected:
            return True
        if operator == "<=" and actual <= expected:
            return True
    return False


def _compare_equal(left: Any, right: Any) -> bool:
    if isinstance(left, str) and isinstance(right, str):
        return left.casefold() == right.casefold()
    return left == right


def _coerce_scalar(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.strip().strip('"').strip("'")
    number = _coerce_number(stripped)
    return stripped if number is None else number


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_sort_by(sort_by: str | None) -> list[_SortSpec]:
    specs = []
    for clause in _split_csv(sort_by):
        field, separator, direction = clause.partition(":")
        direction = direction.strip().lower() if separator else "asc"
        if direction not in {"asc", "desc"}:
            direction = "asc"
        specs.append(_SortSpec(field=field.strip(), direction=direction))
    return specs


def _sort_hits(hits: list[SearchHit], sort_specs: list[_SortSpec]) -> None:
    hits.sort(key=cmp_to_key(lambda left, right: _compare_hits(left, right, sort_specs)))


def _compare_hits(left: SearchHit, right: SearchHit, specs: list[_SortSpec]) -> int:
    for spec in specs:
        comparison = _compare_values(
            _hit_sort_value(left, spec.field), _hit_sort_value(right, spec.field)
        )
        if comparison:
            return comparison if spec.direction == "asc" else -comparison
    score_comparison = _compare_values(right.get("score", 0), left.get("score", 0))
    if score_comparison:
        return score_comparison
    return _compare_values(left["id"], right["id"])


def _hit_sort_value(hit: SearchHit, field: str) -> Any:
    if field in {"_text_match", "text_match", "score"}:
        return hit.get("score", 0.0)
    return hit.get("document", {}).get(field)


def _compare_values(left: Any, right: Any) -> int:
    left_key = _normalize_sort_value(left)
    right_key = _normalize_sort_value(right)
    return (left_key > right_key) - (left_key < right_key)


def _normalize_sort_value(value: Any) -> tuple[int, Any]:
    if value is None:
        return (2, "")
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float)):
        return (0, float(value))
    if isinstance(value, list):
        return _normalize_sort_value(value[0]) if value else (2, "")
    return (1, str(value).casefold())


def _paginate_hits(
    hits: list[SearchHit], *, page: int, per_page: int | None
) -> list[SearchHit]:
    if per_page is None:
        return list(hits)
    page = max(page, 1)
    per_page = max(per_page, 1)
    start = (page - 1) * per_page
    return hits[start : start + per_page]


def _facet_counts_from_hits(
    hits: list[SearchHit], facet_fields: list[str]
) -> list[dict[str, Any]]:
    result = []
    for field in facet_fields:
        counts: Counter[Any] = Counter()
        for hit in hits:
            value = hit.get("document", {}).get(field)
            counts.update(value if isinstance(value, list) else [value] if value is not None else [])
        result.append(
            {
                "field_name": field,
                "counts": [
                    {"value": value, "count": count}
                    for value, count in sorted(
                        counts.items(), key=lambda item: (-item[1], str(item[0]).casefold())
                    )
                ],
            }
        )
    return result


def _highlight_text(text: str, terms: set[str]) -> str | None:
    matches = []
    lowered = text.casefold()
    for term in sorted(terms, key=len, reverse=True):
        start = 0
        while term and (index := lowered.find(term.casefold(), start)) >= 0:
            matches.append((index, index + len(term)))
            start = index + len(term)
    if not matches:
        return None

    snippet_start = max(min(start for start, _ in matches) - 24, 0)
    snippet_end = min(max(end for _, end in matches) + 24, len(text))
    pieces = []
    cursor = snippet_start
    for start, end in sorted(matches):
        if end <= snippet_start or start >= snippet_end or start < cursor:
            continue
        pieces.extend((html.escape(text[cursor:start]), "<mark>", html.escape(text[start:end]), "</mark>"))
        cursor = end
    pieces.append(html.escape(text[cursor:snippet_end]))
    return "".join(pieces)


def _edit_distance(left: str, right: str, max_distance: int) -> int | None:
    if abs(len(left) - len(right)) > max_distance:
        return None
    previous = list(range(len(right) + 1))
    for row, left_char in enumerate(left, start=1):
        current = [row]
        row_min = row
        for column, right_char in enumerate(right, start=1):
            value = min(
                current[column - 1] + 1,
                previous[column] + 1,
                previous[column - 1] + (left_char != right_char),
            )
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return None
        previous = current
    return previous[-1] if previous[-1] <= max_distance else None
