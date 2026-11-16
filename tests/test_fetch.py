"""Tests for URL validation and article extraction."""

from __future__ import annotations

import pytest

from app.fetch import FetchProblem, TooManyRequests, _extract_article, normalize_url


@pytest.mark.parametrize(
    "raw, origin",
    [
        ("http://demo-a:8001/article/alpha", "http://demo-a:8001"),
        ("HTTP://Demo-A:8001/x", "http://demo-a:8001"),
        ("https://example.com/a?b=c", "https://example.com:443"),
        ("http://example.com:8080/a#frag", "http://example.com:8080"),
        ("http://[::1]:9000/a", "http://[::1]:9000"),
    ],
)
def test_normalize_url_origin(raw: str, origin: str) -> None:
    url, actual = normalize_url(raw)
    assert actual == origin
    assert url == raw.strip()


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "ftp://example.com/a",
        "example.com/a",
        "http://",
        "http://user:pass@example.com/a",
        "http://example.com:99999/a",
        "http://example.com/" + "a" * 3000,
    ],
)
def test_normalize_url_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalize_url(raw)


def test_extract_article_prefers_h1_and_paragraphs() -> None:
    html = """
    <html><head><title>站点标题</title></head><body>
      <nav>导航</nav>
      <article><h1>真正标题</h1><p>第一段</p><p>第二段</p></article>
    </body></html>
    """
    title, body = _extract_article(html)
    assert title == "真正标题"
    assert body == "第一段\n\n第二段"


def test_extract_article_without_article_element() -> None:
    with pytest.raises(FetchProblem) as excinfo:
        _extract_article("<html><body><div>没有正文</div></body></html>")
    assert excinfo.value.code == "no_article"


def test_extract_article_empty_article() -> None:
    with pytest.raises(FetchProblem) as excinfo:
        _extract_article("<html><body><article>   </article></body></html>")
    assert excinfo.value.code == "empty_article"


def test_extract_article_ignores_script_and_style() -> None:
    html = (
        "<html><body><article><h1>T</h1>"
        "<script>var x = '<p>假段落</p>';</script>"
        "<p>真段落</p></article></body></html>"
    )
    title, body = _extract_article(html)
    assert title == "T"
    assert body == "真段落"


@pytest.mark.parametrize(
    "header, expected",
    [
        ("5", 5),
        (" 0 ", 0),
        ("120", 120),
        (None, None),
        ("", None),
        ("1.5", None),
        ("-3", None),
        ("Wed, 21 Oct 2015 07:28:00 GMT", None),
        ("5s", None),
    ],
)
def test_retry_after_accepts_only_integer_seconds(header, expected) -> None:
    exc = TooManyRequests(header, 429)
    assert exc.retry_after_seconds() == expected


def test_429_carries_http_status_and_message() -> None:
    exc = TooManyRequests("7", 429)
    assert exc.http_status == 429
    assert exc.code == "rate_limited"
    assert "7" in exc.reason
