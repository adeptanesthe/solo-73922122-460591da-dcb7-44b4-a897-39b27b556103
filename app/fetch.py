"""Fetching one article page: no redirects, strict decoding, article extraction."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup

USER_AGENT = "CorpusCollector/1.0 (+local demonstration; article extraction)"
MAX_BODY_BYTES = 4 * 1024 * 1024
TIMEOUT = httpx.Timeout(20.0, connect=10.0)


class FetchProblem(Exception):
    def __init__(self, code: str, reason: str, http_status: int | None = None) -> None:
        super().__init__(reason)
        self.code = code
        self.reason = reason
        self.http_status = http_status


class TooManyRequests(FetchProblem):
    def __init__(self, retry_after: str | None, http_status: int) -> None:
        self.retry_after_raw = retry_after
        note = f"Retry-After: {retry_after}" if retry_after else "无 Retry-After"
        super().__init__("rate_limited", f"源站限流（HTTP 429，{note}）", http_status)

    def retry_after_seconds(self) -> int | None:
        """Only plain integer seconds are honoured; HTTP-date or junk is not."""
        if self.retry_after_raw is None:
            return None
        value = self.retry_after_raw.strip()
        if not value.isdigit():
            return None
        return int(value)


@dataclass
class FetchOutcome:
    http_status: int
    title: str
    body: str


def normalize_url(raw: str) -> tuple[str, str]:
    """Validate an article URL and return ``(url, origin)``.

    Only absolute http(s) URLs are accepted.  The URL is used exactly as
    submitted, since the collector must request the given URL and nothing
    else.
    """
    candidate = (raw or "").strip()
    if not candidate:
        raise ValueError("URL 不能为空")
    if len(candidate) > 2000:
        raise ValueError("URL 过长（最多 2000 字符）")
    parts = urlsplit(candidate)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError("只接受 http/https 文章 URL")
    if not parts.hostname:
        raise ValueError("URL 缺少主机名")
    if parts.username or parts.password:
        raise ValueError("URL 不得包含认证信息")
    try:
        port = parts.port
    except ValueError:
        raise ValueError("URL 端口无效") from None
    host = parts.hostname.lower()
    default_port = 80 if scheme == "http" else 443
    port = default_port if port is None else port
    host_part = f"[{host}]" if ":" in host else host
    return candidate, f"{scheme}://{host_part}:{port}"


def _reject_unsupported_host(host: str, port: int) -> None:
    """Fail fast on unresolvable names and multicast/unspecified addresses."""
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise FetchProblem("dns", f"域名解析失败：{exc}") from exc
    except OSError as exc:
        raise FetchProblem("dns", f"域名解析失败：{exc}") from exc
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if ip.is_multicast or ip.is_unspecified:
            raise FetchProblem("blocked_host", f"不支持的地址：{ip}")


def _extract_article(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template", "svg"]):
        tag.decompose()

    title = ""
    title_node = soup.find("title")
    if title_node and title_node.get_text(strip=True):
        title = title_node.get_text(" ", strip=True)
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        title = str(og_title["content"]).strip() or title

    article = soup.find("article")
    if article is None:
        raise FetchProblem("no_article", "页面中没有 <article> 元素")
    heading = article.find("h1")
    if heading:
        text = heading.get_text(" ", strip=True)
        if text:
            title = text

    paragraphs = [p.get_text(" ", strip=True) for p in article.find_all("p")]
    body = "\n\n".join(p for p in paragraphs if p).strip()
    if not body:
        body = article.get_text("\n", strip=True)
    if not body:
        raise FetchProblem("empty_article", "article 元素中没有正文内容")
    if not title:
        title = "（无标题）"
    return title, body


def _decode(resp: httpx.Response, content: bytes) -> str:
    encoding = resp.charset_encoding or "utf-8"
    try:
        return content.decode(encoding, errors="replace")
    except LookupError:
        return content.decode("utf-8", errors="replace")


async def fetch_article(client: httpx.AsyncClient, url: str) -> FetchOutcome:
    """Perform exactly one GET for ``url``.

    Redirects are never followed: a 3xx is a hard failure.  A 429 is raised as
    :class:`TooManyRequests` so the caller can apply the origin cooldown;
    everything else that is unusable raises :class:`FetchProblem`.
    """
    parts = urlsplit(url)
    await asyncio.to_thread(
        _reject_unsupported_host,
        parts.hostname or "",
        parts.port or (443 if parts.scheme == "https" else 80),
    )
    try:
        async with client.stream(
            "GET",
            url,
            follow_redirects=False,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*;q=0.8"},
        ) as resp:
            status = resp.status_code
            if 300 <= status < 400:
                location = resp.headers.get("location", "")
                raise FetchProblem(
                    "redirect",
                    f"源站返回重定向 HTTP {status}，本系统不跟随重定向"
                    + (f"（Location: {location}）" if location else ""),
                    status,
                )
            if status == 429:
                raise TooManyRequests(resp.headers.get("retry-after"), status)
            if status >= 400:
                raise FetchProblem("http_error", f"源站返回 HTTP {status}", status)
            content_type = resp.headers.get("content-type", "")
            if content_type and "html" not in content_type.lower() and "xml" not in content_type.lower():
                raise FetchProblem("not_html", f"内容类型不是 HTML（{content_type}）", status)
            chunks: list[bytes] = []
            size = 0
            async for chunk in resp.aiter_bytes():
                size += len(chunk)
                if size > MAX_BODY_BYTES:
                    raise FetchProblem(
                        "too_large",
                        f"页面超过 {MAX_BODY_BYTES // (1024 * 1024)} MiB 上限",
                        status,
                    )
                chunks.append(chunk)
            html = _decode(resp, b"".join(chunks))
    except httpx.TimeoutException as exc:
        raise FetchProblem("timeout", f"请求超时（{exc.__class__.__name__}）") from exc
    except httpx.HTTPError as exc:
        raise FetchProblem("network", f"网络错误：{exc.__class__.__name__}: {exc}") from exc

    title, body = _extract_article(html)
    return FetchOutcome(http_status=status, title=title, body=body)
