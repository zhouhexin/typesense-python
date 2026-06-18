from typesense_lite.tokenizer import tokenize


def test_tokenize_lowercases_and_splits_on_non_alphanumeric() -> None:
    assert tokenize("Distributed search, Search!") == [
        "distributed",
        "search",
        "search",
    ]


def test_tokenize_drops_empty_tokens() -> None:
    assert tokenize("  ---  ") == []
