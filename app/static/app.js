/* Minimal front-end: submit, poll status, cancel, download. */
(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const form = $("submit-form");
  const urlsInput = $("urls");
  const submitBtn = $("submit-btn");
  const formError = $("form-error");
  const batchesEl = $("batches");
  const detailPanel = $("detail-panel");
  const detailId = $("detail-id");
  const detailMeta = $("detail-meta");
  const pagesBody = $("pages-body");
  const eventsEl = $("events");
  const cancelBtn = $("cancel-btn");
  const downloadLink = $("download-link");
  const refreshBtn = $("refresh-btn");

  let currentBatch = null;
  let pollTimer = null;
  let maxUrls = 20;

  const STATUS_LABEL = {
    queued: "排队中",
    backoff: "等待重试",
    running: "采集中",
    ok: "成功",
    failed: "失败",
    cancelled: "已取消",
  };
  const BATCH_STATUS_LABEL = {
    queued: "排队中",
    running: "进行中",
    completed: "已完成",
    cancelled: "已取消",
  };

  function statusChip(status, label) {
    const span = document.createElement("span");
    span.className = `status ${status}`;
    span.textContent = label || status;
    return span;
  }

  function fmtTime(ts) {
    if (!ts) return "—";
    return new Date(ts * 1000).toLocaleString();
  }

  function showError(message) {
    formError.hidden = !message;
    formError.textContent = message || "";
  }

  async function api(path, options) {
    const resp = await fetch(path, options);
    let body = null;
    try {
      body = await resp.json();
    } catch (_) {
      body = null;
    }
    if (!resp.ok) {
      let detail = `HTTP ${resp.status}`;
      if (body && body.detail) {
        if (typeof body.detail === "string") detail = body.detail;
        else if (Array.isArray(body.detail.errors)) detail = body.detail.errors.join("\n");
        else if (body.detail.message) detail = body.detail.message;
        else detail = JSON.stringify(body.detail);
      }
      throw new Error(detail);
    }
    return body;
  }

  async function loadConfig() {
    try {
      const cfg = await api("/api/config");
      maxUrls = cfg.max_urls || 20;
      $("max-urls").textContent = String(maxUrls);
      urlsInput.placeholder = `http://demo-a:8001/article/alpha\nhttp://demo-b:8001/article/beta`;
      const sources = cfg.demo_sources || [];
      sources.forEach((source, index) => {
        const id = index === 0 ? "demo-a-base" : "demo-b-base";
        if ($(id)) $(id).textContent = source.base || "未配置";
      });
      const samples = $("demo-samples");
      samples.textContent = "";
      const a = (sources[0] && sources[0].base) || "";
      const b = (sources[1] && sources[1].base) || "";
      const list = [
        `${a}/article/alpha`,
        `${a}/article/beta?delay=1.5`,
        `${b}/article/gamma?limit=1&retry_after=3`,
        `${a}/redirect/delta`,
        `${b}/no-article/epsilon`,
        `${a}/article/zeta?fail=500`,
      ];
      list.forEach((url) => {
        if (!url.startsWith("http")) return;
        const li = document.createElement("li");
        const link = document.createElement("a");
        link.href = "#";
        link.textContent = url;
        link.addEventListener("click", (event) => {
          event.preventDefault();
          urlsInput.value = urlsInput.value
            ? `${urlsInput.value.replace(/\s+$/, "")}\n${url}`
            : url;
        });
        li.appendChild(link);
        samples.appendChild(li);
      });
    } catch (err) {
      /* config is optional */
    }
  }

  async function submit(event) {
    event.preventDefault();
    showError("");
    const text = urlsInput.value;
    const urls = text
      .split(/\r?\n/)
      .map((u) => u.trim())
      .filter(Boolean);
    if (urls.length === 0) {
      showError("请至少输入 1 个文章 URL。");
      return;
    }
    if (urls.length > maxUrls) {
      showError(`单批最多 ${maxUrls} 个 URL，当前有 ${urls.length} 个。`);
      return;
    }
    submitBtn.disabled = true;
    try {
      const created = await api("/api/batches", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls }),
      });
      urlsInput.value = "";
      await refreshBatches();
      await openBatch(created.batch_id);
    } catch (err) {
      showError(`提交失败：${err.message}`);
    } finally {
      submitBtn.disabled = false;
    }
  }

  async function refreshBatches() {
    try {
      const data = await api("/api/batches");
      renderBatches(data.batches || []);
    } catch (err) {
      batchesEl.textContent = `加载批次失败：${err.message}`;
    }
  }

  function renderBatches(batches) {
    batchesEl.textContent = "";
    if (!batches.length) {
      const p = document.createElement("p");
      p.className = "empty";
      p.textContent = "还没有批次。";
      batchesEl.appendChild(p);
      return;
    }
    for (const batch of batches) {
      const card = document.createElement("div");
      card.className = "batch-card" + (currentBatch === batch.id ? " active" : "");
      card.addEventListener("click", () => openBatch(batch.id));

      const left = document.createElement("div");
      const id = document.createElement("div");
      id.className = "id";
      id.textContent = batch.id;
      const when = document.createElement("div");
      when.className = "when";
      const counts = batch.counts || {};
      when.textContent =
        `${fmtTime(batch.created_at)} · 共 ${batch.total} 页 · ` +
        `成功 ${counts.ok || 0} / 失败 ${counts.failed || 0} / ` +
        `取消 ${counts.cancelled || 0} / 待处理 ${counts.queued || 0}`;
      left.append(id, when);

      card.append(left, statusChip(batch.status, BATCH_STATUS_LABEL[batch.status] || batch.status));
      batchesEl.appendChild(card);
    }
  }

  async function openBatch(batchId) {
    currentBatch = batchId;
    try {
      const batch = await api(`/api/batches/${encodeURIComponent(batchId)}`);
      renderDetail(batch);
      schedulePoll(batch);
    } catch (err) {
      showError(`读取批次失败：${err.message}`);
    }
    refreshBatches();
  }

  function renderDetail(batch) {
    detailPanel.hidden = false;
    detailId.textContent = batch.id;
    const counts = batch.counts || {};
    detailMeta.textContent = "";
    const bits = [
      ["状态", BATCH_STATUS_LABEL[batch.status] || batch.status],
      ["创建时间", fmtTime(batch.created_at)],
      ["页数", String(batch.total)],
      ["成功", String(counts.ok || 0)],
      ["失败", String(counts.failed || 0)],
      ["已取消", String(counts.cancelled || 0)],
      ["待处理", `${(counts.queued || 0) + (counts.backoff || 0) + (counts.running || 0)}`],
    ];
    bits.forEach(([label, value], index) => {
      if (index) detailMeta.append(" · ");
      const strong = document.createElement("strong");
      strong.textContent = value;
      detailMeta.append(`${label}：`, strong);
    });

    downloadLink.href = `/api/batches/${encodeURIComponent(batch.id)}/corpus.jsonl`;
    const hasCorpus = (counts.ok || 0) > 0;
    downloadLink.classList.toggle("ghost", !hasCorpus);
    downloadLink.setAttribute("aria-disabled", hasCorpus ? "false" : "true");
    cancelBtn.disabled = batch.status === "cancelled" || batch.status === "completed";

    const sourceBits = (batch.sources || [])
      .filter((s) => (s.cooldown_remaining || 0) > 0.05 || (s.next_request_allowed_in || 0) > 0.05)
      .map((s) => {
        if ((s.cooldown_remaining || 0) > 0.05) {
          return `${s.origin} 冷却中（${Math.ceil(s.cooldown_remaining)}s 后可重试）`;
        }
        return `${s.origin} 下次请求 ${Math.ceil(s.next_request_allowed_in)}s 后`;
      });
    if (sourceBits.length) {
      const note = document.createElement("div");
      note.className = "source-note";
      note.textContent = sourceBits.join(" · ");
      detailMeta.appendChild(note);
    }

    pagesBody.textContent = "";
    for (const page of batch.pages || []) {
      const tr = document.createElement("tr");

      const pos = document.createElement("td");
      pos.textContent = String(page.position + 1);

      const url = document.createElement("td");
      url.className = "url";
      url.textContent = page.url;

      const status = document.createElement("td");
      status.appendChild(statusChip(page.status, STATUS_LABEL[page.status] || page.status));

      const title = document.createElement("td");
      title.textContent = page.title || "—";

      const length = document.createElement("td");
      length.textContent = page.status === "ok" ? String(page.body_length || 0) : "—";

      const attempts = document.createElement("td");
      attempts.textContent =
        String(page.attempts || 0) + (page.retry_count ? `（重试 ${page.retry_count}）` : "");

      const reason = document.createElement("td");
      reason.className = "reason";
      reason.textContent = page.reason || "";

      tr.append(pos, url, status, title, length, attempts, reason);
      pagesBody.appendChild(tr);
    }

    eventsEl.textContent = "";
    for (const event of batch.events || []) {
      const li = document.createElement("li");
      li.textContent = `${new Date(event.created_at * 1000).toLocaleTimeString()} · ${event.message || event.kind}`;
      eventsEl.appendChild(li);
    }
  }

  function schedulePoll(batch) {
    if (pollTimer) clearTimeout(pollTimer);
    const active = batch.status === "queued" || batch.status === "running";
    if (!active || !currentBatch) return;
    pollTimer = setTimeout(async () => {
      if (!currentBatch) return;
      try {
        const fresh = await api(`/api/batches/${encodeURIComponent(currentBatch)}`);
        renderDetail(fresh);
        refreshBatches();
        schedulePoll(fresh);
      } catch (err) {
        schedulePoll(batch);
      }
    }, 800);
  }

  async function cancelCurrent() {
    if (!currentBatch) return;
    cancelBtn.disabled = true;
    try {
      const batch = await api(
        `/api/batches/${encodeURIComponent(currentBatch)}/cancel`,
        { method: "POST" }
      );
      renderDetail(batch);
      refreshBatches();
    } catch (err) {
      showError(`取消失败：${err.message}`);
      cancelBtn.disabled = false;
    }
  }

  form.addEventListener("submit", submit);
  refreshBtn.addEventListener("click", () => {
    refreshBatches();
    if (currentBatch) openBatch(currentBatch);
  });
  cancelBtn.addEventListener("click", cancelCurrent);

  loadConfig();
  refreshBatches();
  setInterval(() => {
    if (!currentBatch) refreshBatches();
  }, 4000);
})();
