from typesense_lite.tokenizer import tokenize


def test_tokenize_lowercases_and_splits_on_non_alphanumeric() -> None:
    assert tokenize("Distributed search, Search!") == [
        "distributed",
        "search",
        "search",
    ]


def test_tokenize_drops_empty_tokens() -> None:
    assert tokenize("  ---  ") == []


def test_tokenize_chinese_text_as_bigrams_with_single_character_fallback() -> None:
    assert tokenize("分布式搜索") == ["分布", "布式", "式搜", "搜索"]
    assert tokenize("搜") == ["搜"]


def test_tokenize_mixed_english_and_chinese_text() -> None:
    assert tokenize("Typesense 支持中文搜索 v2") == [
        "typesense",
        "支持",
        "持中",
        "中文",
        "文搜",
        "搜索",
        "v2",
    ]
