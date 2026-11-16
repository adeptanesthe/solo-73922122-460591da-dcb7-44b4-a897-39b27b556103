# 文章语料采集器（Article Corpus Collector）

FastAPI + SQLite 的采集网页：一次提交至多 20 个文章 URL，逐页查看状态，下载含
`title`、`body`、`url` 的 JSONL 语料。应用以**两个采集进程**运行，二者通过
SQLite 共享同一套按源限速规则。

## 快速开始

```bash
docker compose up --build
```

* 采集网页：<http://localhost:8000/>
* 演示源 A：<http://localhost:8001/>（请求日志：`/requests.json`）
* 演示源 B：<http://localhost:8002/>（请求日志：`/requests.json`）

演示源提供：

| 参数 | 作用 |
| --- | --- |
| `delay=1.5` | 应答前等待 1.5 秒（模拟慢源） |
| `fail=500` | 固定返回该 HTTP 状态 |
| `limit=2&retry_after=3` | 该 URL 前 2 次返回 429，`Retry-After: 3` |
| `/redirect/<slug>` | 返回 302 跳转（用于验证不跟随重定向） |
| `/no-article/<slug>` | 没有 `<article>` 的页面（解析失败演示） |

示例（可直接粘贴进网页文本框——这些 URL 由采集进程访问，容器内用服务名）：

```
http://demo-a:8001/article/alpha
http://demo-a:8001/article/beta?delay=1.5
http://demo-b:8001/article/gamma?limit=1&retry_after=3
http://demo-a:8001/redirect/delta
http://demo-b:8001/no-article/epsilon
http://demo-a:8001/article/zeta?fail=500
```

> 采集器在容器内通过 `demo-a:8001` / `demo-b:8001` 访问演示源（见
> `DEMO_A_URL`/`DEMO_B_URL`）。从宿主机人工检查演示源页面时，改用
> `localhost:8001` / `localhost:8002`（同一服务，映射到宿主端口）。

## 行为约定

**只采集给定 URL，不跟随重定向。** 每个 URL 恰好发起一次 `GET`（429 时最多再
重试一次）；3xx 视为失败页并显示原因（含 `Location`），不写入语料。没有
`<article>`、正文为空、非 HTML、超时、网络错误等也都作为失败页展示原因。

**同源限速（协议 + 主机 + 端口相同即同源）。** 两个采集进程共享 SQLite 中的
`origin_gate` 表：

* 同源请求**不重叠**：取得带租约的请求槽位后才发请求，读完响应（或失败）才释放；
* 同源两次请求的**开始时间至少间隔 1 秒**（默认 1.05 秒留出调度抖动余量），
  并以上一次请求**实际发出**的时刻为锚点；
* 租约有过期时间（默认 90 秒），进程崩溃不会永久锁死某个源。

**429 处理。** 仅当 `Retry-After` 为整数秒时按该值暂停；否则按
`RETRY_AFTER_FALLBACK`（默认 2 秒）。暂停写入 `origin_gate.cooldown_until`，
对该源的**所有批次、所有进程**立即生效，冷却结束后该页重试一次；其他源不受影响
（各源独立等待，不阻塞事件循环）。重试仍 429 则记为失败页。

**取消。** 取消后不再发送新请求：排队中的页面立即标记 `cancelled`，正在运行的
页面也在取消事务里标记；若响应在取消之后才到达，正文会被丢弃（页面显示
"迟到的正文已丢弃"），绝不进入 JSONL。已触发的源站冷却保存在 `origin_gate`
中，不会因取消而失效。失败页同样不写入语料，但会展示失败原因。

## JSONL 格式

每行一个已成功采集的页面，按提交顺序：

```json
{"title": "标题", "body": "正文段落，以空行分隔", "url": "提交的原始 URL"}
```

`GET /api/batches/{id}/corpus.jsonl` 下载（`Content-Disposition: attachment`）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/batches` | `{"urls": [...]}` 或 `{"text": "每行一个"}`，最多 20 个，返回 `batch_id` |
| `GET` | `/api/batches` | 批次列表与各状态计数 |
| `GET` | `/api/batches/{id}` | 批次详情：每页状态、标题、正文长度、尝试次数、原因 |
| `POST` | `/api/batches/{id}/cancel` | 取消批次（幂等） |
| `GET` | `/api/batches/{id}/corpus.jsonl` | 下载语料 |
| `GET` | `/api/batches/{id}/corpus` | JSON 形式的语料预览 |
| `GET` | `/api/demo/{a,b}/requests` | 代理演示源请求日志 |
| `GET` | `/healthz` | 健康检查 |

页面状态：`queued`（排队）→ `running`（采集中）→ `ok` / `failed`；
429 时为 `backoff`（等待冷却后重试一次）；取消为 `cancelled`。

## 结构

```
app/
  api.py         FastAPI 路由（提交/状态/取消/下载），并在每个 Web 进程内运行采集循环
  worker.py     采集循环：按源挑选页面、申请槽位、请求、存结果
  governor.py   跨进程同源限速（SQLite 事务 + 租约 + 冷却）
  db.py         SQLite 表结构与事务（批次、页面、事件）
  fetch.py      URL 校验、单次 GET（不跟随重定向）、正文抽取
  config.py     环境变量配置
  static/       浏览器界面
demo/source.py  本地演示源（延迟、失败、429、请求时间日志）
tests/          单元与集成测试
```

## 本地开发与测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
```

测试不需要 Docker：`tests/test_worker.py` 会在临时端口启动
`demo.source` 并对真实 HTTP 请求计时，验证间隔 ≥1 秒、无重叠、429 冷却、
取消语义与双采集器共享限速。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `CORPUS_DB` | `/data/corpus.db`（容器内） | SQLite 路径 |
| `WEB_WORKERS` | `2` | Web/采集进程数 |
| `ENABLE_COLLECTOR` | `true` | 是否在该进程内运行采集循环 |
| `MIN_REQUEST_INTERVAL` | `1.05` | 同源最小开始间隔（秒） |
| `ORIGIN_LEASE_SECONDS` | `90` | 单次请求槽位租约 |
| `RETRY_AFTER_FALLBACK` | `2` | 429 缺少整数 Retry-After 时的暂停秒数 |
| `MAX_RETRY_AFTER` | 不限制 | 可选，给整数 Retry-After 设上限 |
| `MAX_URLS_PER_BATCH` | `20` | 单批上限 |
| `DEMO_A_URL` / `DEMO_B_URL` | `http://demo-a:8001` 等 | 演示源地址 |
