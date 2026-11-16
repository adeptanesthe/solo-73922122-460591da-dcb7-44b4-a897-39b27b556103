"""API tests using FastAPI's TestClient (collector loop disabled)."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from app.api import build_app
from app.config import Settings


@pytest.fixture()
def client(tmp_path):
    settings = Settings(db_path=str(tmp_path / "corpus.db"), enable_collector=False)
    app = build_app(settings)
    with TestClient(app) as c:
        yield c


def test_submit_and_read_batch(client):
    urls = [
        "http://demo-a:8001/article/alpha",
        "http://demo-b:8001/article/beta",
    ]
    resp = client.post("/api/batches", json={"urls": urls})
    assert resp.status_code == 201, resp.text
    data = resp.json()
    batch_id = data["batch_id"]
    assert data["total"] == 2
    assert resp.headers["location"].endswith(f"/api/batches/{batch_id}")

    detail = client.get(f"/api/batches/{batch_id}").json()
    assert detail["status"] == "queued"
    assert [p["url"] for p in detail["pages"]] == urls
    assert [p["status"] for p in detail["pages"]] == ["queued", "queued"]
    assert detail["counts"]["queued"] == 2


def test_submit_accepts_textarea_text(client):
    resp = client.post(
        "/api/batches",
        json={"text": "http://a:80/x\n\nhttp://b:80/y\n"},
    )
    assert resp.status_code == 201
    assert resp.json()["total"] == 2


def test_submit_rejects_more_than_twenty(client):
    urls = [f"http://a:80/{i}" for i in range(21)]
    resp = client.post("/api/batches", json={"urls": urls})
    assert resp.status_code == 400
    assert "20" in json.dumps(resp.json(), ensure_ascii=False)


def test_submit_accepts_twenty(client):
    urls = [f"http://a:80/{i}" for i in range(20)]
    resp = client.post("/api/batches", json={"urls": urls})
    assert resp.status_code == 201
    assert resp.json()["total"] == 20


def test_submit_validation_errors(client):
    resp = client.post("/api/batches", json={"urls": ["ftp://a/x", "http://b:80/ok"]})
    assert resp.status_code == 400
    body = resp.json()["detail"]
    assert body["errors"]

    resp = client.post("/api/batches", json={"urls": []})
    assert resp.status_code == 400


def test_duplicate_urls_are_collapsed(client):
    resp = client.post(
        "/api/batches",
        json={"urls": ["http://a:80/x", "http://a:80/x", "http://b:80/y"]},
    )
    assert resp.status_code == 201
    assert resp.json()["total"] == 2


def test_cancel_batch(client):
    batch_id = client.post(
        "/api/batches", json={"urls": ["http://a:80/x", "http://b:80/y"]}
    ).json()["batch_id"]
    cancelled = client.post(f"/api/batches/{batch_id}/cancel").json()
    assert cancelled["status"] == "cancelled"
    assert all(p["status"] == "cancelled" for p in cancelled["pages"])
    assert all(p["reason"] for p in cancelled["pages"])
    # Second cancel is a no-op.
    again = client.post(f"/api/batches/{batch_id}/cancel").json()
    assert again["status"] == "cancelled"


def test_unknown_batch_is_404(client):
    assert client.get("/api/batches/nope").status_code == 404
    assert client.post("/api/batches/nope/cancel").status_code == 404
    assert client.get("/api/batches/nope/corpus.jsonl").status_code == 404


def test_jsonl_download_only_contains_successful_pages(client, tmp_path):
    repo = client.app.state.repo
    batch_id = repo.create_batch(
        [("http://a:80/one", "http://a:80"), ("http://b:80/two", "http://b:80")]
    )
    pages = client.get(f"/api/batches/{batch_id}").json()["pages"]
    first = repo.claim_page(pages[0]["id"], "test")
    assert first
    repo.start_request(first["id"])
    assert repo.save_result(first["id"], "标题一", "正文一", 200) == "ok"
    second = repo.claim_page(pages[1]["id"], "test")
    assert second
    repo.start_request(second["id"])
    repo.save_failure(second["id"], "http_error", "源站返回 HTTP 500", 500)

    resp = client.get(f"/api/batches/{batch_id}/corpus.jsonl")
    assert resp.status_code == 200
    assert "attachment" in resp.headers["content-disposition"]
    lines = [json.loads(line) for line in resp.text.splitlines() if line]
    assert lines == [{"title": "标题一", "body": "正文一", "url": "http://a:80/one"}]

    detail = client.get(f"/api/batches/{batch_id}").json()
    assert detail["pages"][1]["reason"] == "源站返回 HTTP 500"
    assert detail["pages"][1]["status"] == "failed"


def test_cancelled_batch_jsonl_is_empty(client):
    repo = client.app.state.repo
    batch_id = repo.create_batch([("http://a:80/one", "http://a:80")])
    page = client.get(f"/api/batches/{batch_id}").json()["pages"][0]
    repo.claim_page(page["id"], "test")
    repo.start_request(page["id"])
    client.post(f"/api/batches/{batch_id}/cancel")
    assert repo.save_result(page["id"], "迟到标题", "迟到正文", 200) == "cancelled"
    resp = client.get(f"/api/batches/{batch_id}/corpus.jsonl")
    assert resp.text == ""


def test_batch_detail_exposes_source_cooldown(client):
    batch_id = client.post(
        "/api/batches",
        json={"urls": ["http://a:80/x", "http://b:80/y"]},
    ).json()["batch_id"]
    gov = client.app.state.governor
    gov.start_cooldown("http://a:80", 30)
    detail = client.get(f"/api/batches/{batch_id}").json()
    sources = {s["origin"]: s for s in detail["sources"]}
    assert set(sources) == {"http://a:80", "http://b:80"}
    assert sources["http://a:80"]["cooldown_remaining"] > 25
    assert sources["http://b:80"]["cooldown_remaining"] == 0


def test_index_page_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "文章语料采集器" in resp.text
