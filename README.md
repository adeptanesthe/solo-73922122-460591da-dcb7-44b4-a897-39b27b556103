# 文章语料采集器（Article Corpus Collector）

FastAPI + SQLite 的采集网页：一次提交至多 20 个文章 URL，逐页查看状态，下载含
`title`、`body`、`url` 的 JSONL 语料。应用以**两个独立采集进程**运行（两个容器，
各一个进程），二者通过共享的 SQLite 协同，对同一源表现得像一个礼貌的客户端。

## 快速开始

```bash
docker compose up --build
```

* 采集网页（进程 A）：<http://localhost:8000/>
* 采集网页（进程 B）：<http://localhost:8003/>（同一应用，两个进程各自 serving）
* 演示源 A：<http://localhost:8001/>（请求日志：`/requests.json`）
* 演示源 B：<http://localhost:8002/>（请求日志：`/requests.json`）

演示源提供：

| 参数 | 作用 |
| --- | --- |
| `delay=4` | 应答前等待 4 秒（模拟慢源） |
| `fail=500` | 固定返回该 HTTP 状态 |
| `limit=2&retry_after=3` | 该 URL 前 2 次返回 429，`Retry-After: 3` |
| `/redirect/<slug>` | 返回 302 跳转（用于验证不跟随重定向） |
| `/no-article/<slug>` | 没有 `<article>` 的页面（解析失败演示） |

示例（可直接粘贴进网页文本框——这些 URL 由采集进程访问，容器内用服务名）：

```
http://demo-a:8001/article/alpha
http://demo-a:8001/article/beta?delay=4
http://demo-a:8001/article/gamma?delay=4
http://demo-b:8001/article/delta?limit=1&retry_after=3
http://demo-a:8001/redirect/epsilon
http://demo-b:8001/no-article/zeta
http://demo-a:8001/article/zeta?fail=500
```

> 采集器在容器内通过 `demo-a:8001` / `demo-b:8001` 访问演示源（见
> `DEMO_A_URL`/`DEMO_B_URL`）。从宿主机人工检查演示源页面时，改用
> `localhost:8001` / `localhost:8002`（同一服务，映射到宿主端口）。

## 慢源验证（租约 1.5 秒 + 响应 4 秒）

compose 故意把源站租约设为 **1.5 秒**（`ORIGIN_LEASE_SECONDS`），而演示源可用
`delay=4` 给出 4 秒的慢响应——响应时间远超租约。提交两个同源 `delay=4` 页面后
打开演示源 A 的请求日志（网页里有链接）：两条请求的 `[received_at, finished_at]`
时间窗**不重叠**，开始间隔 ≥1 秒。采集进程在请求在途期间持续**续租**，所以慢但
正常的响应不会被另一个进程抢走；不靠加长租约，也不靠全局串行。

## 进程退出与接手验证

1. 提交一个慢页面，例如 `http://demo-a:8001/article/alpha?delay=8`；
2. 在批次详情表格的“进程”列查看当前采集者（如 `collector-b-xxxxxxxx`）；
3. 杀掉该进程：`docker compose kill collector-b`（compose 对采集进程不设自动
   重启，进程保持退出）；
4. 观察网页：“采集进程”区里 `collector-b` 变为**已退出**；遗留的 `running`
   页面在数秒内被 `collector-a` 自动重新排队并完成（事件流里有“接手”记录），
   **无需重启任何进程**；
5. 被取消的页面不会被接手逻辑复活：取消一个批次后即使杀掉采集进程，页面始终
   保持 `cancelled`。

接手是安全的：每次页面认领都带有唯一令牌，退出进程的迟到写入（保存正文、标记
失败、重新排队）都会被拒绝，无法改写新认领者的页面；源站槽位的释放也按令牌
校验，旧进程无法释放新占用。

## 行为约定

**只采集给定 URL，不跟随重定向。** 每个 URL 恰好发起一次 `GET`（429 时最多再
重试一次）；3xx 视为失败页并显示原因（含 `Location`），不写入语料。没有
`<article>`、正文为空、非 HTML、超时、网络错误等也都作为失败页展示原因。

**同源限速（协议 + 主机 + 端口相同即同源）。** 两个采集进程共享 SQLite 中的
`origin_gate` 表：

* 同源请求**不重叠**：取得带租约的请求槽位后才发请求，读完响应（或失败）才释放；
* 请求在途期间持有者按租约的 1/3 间隔**续租**：慢响应再久也不会被抢走；
* 进程退出后租约自然到期（默认 1.5 秒，见 `ORIGIN_LEASE_SECONDS`），其他进程
  才能获得槽位——崩溃的进程不会永久锁死某个源；
* 同源两次请求的**开始时间至少间隔 1 秒**（默认 1.05 秒留出调度抖动余量），
  并以上一次请求**实际发出**的时刻为锚点。

**进程存活与接手。** 每个采集进程每 0.5 秒在 `workers` 表写入心跳；失联超过
`WORKER_DEAD_AFTER`（默认为 `max(2s, 2×租约)`，租约 1.5 秒时即 3 秒）即被判定
退出。存活进程每秒扫描一次，把退出进程遗留的 `running` 页面重新排队（已取消的
批次除外），随后按正常流程认领采集。页面认领携带唯一令牌，退出进程迟到的一切
写入与释放都会被拒绝。

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
| `GET` | `/api/batches/{id}` | 批次详情：每页状态、标题、正文长度、尝试次数、所属进程、原因 |
| `POST` | `/api/batches/{id}/cancel` | 取消批次（幂等） |
| `GET` | `/api/batches/{id}/corpus.jsonl` | 下载语料 |
| `GET` | `/api/batches/{id}/corpus` | JSON 形式的语料预览 |
| `GET` | `/api/workers` | 各采集进程心跳与存活状态 |
| `GET` | `/api/demo/{a,b}/requests` | 代理演示源请求日志 |
| `GET` | `/healthz` | 健康检查 |

页面状态：`queued`（排队）→ `running`（采集中）→ `ok` / `failed`；
429 时为 `backoff`（等待冷却后重试一次）；取消为 `cancelled`。

## 结构

```
app/
  api.py         FastAPI 路由（提交/状态/取消/下载/进程心跳），并在每个 Web 进程内运行采集循环
  worker.py     采集循环：心跳与接手扫描、按源挑选页面、申请槽位、续租、请求、存结果
  governor.py   跨进程同源限速（SQLite 事务 + 租约与续租 + 冷却）
  db.py         SQLite 表结构与事务（批次、页面、事件、进程心跳、认领令牌）
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

测试不需要 Docker：`tests/test_worker.py` 与 `tests/test_takeover.py` 会在临时
端口启动 `demo.source` 并对真实 HTTP 请求计时，验证间隔 ≥1 秒、短租约下慢响应
不重叠、429 冷却、取消语义、双采集器共享限速，以及**进程退出后的自动接手**
（用真实子进程猝死模拟）与**迟到写入被拒绝**。

## 配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `CORPUS_DB` | `/data/corpus.db`（容器内） | SQLite 路径 |
| `WEB_WORKERS` | `2` | 单容器内 Web/采集进程数（compose 中每容器为 1，两容器共两进程） |
| `WORKER_NAME` | `worker` | 进程名前缀（页面与心跳中显示的采集者） |
| `ENABLE_COLLECTOR` | `true` | 是否在该进程内运行采集循环 |
| `MIN_REQUEST_INTERVAL` | `1.05` | 同源最小开始间隔（秒） |
| `ORIGIN_LEASE_SECONDS` | `90`（compose 中为 `1.5`） | 单次请求槽位租约；在途请求靠续租保持 |
| `WORKER_DEAD_AFTER` | `max(2s, 2×租约)` | 心跳失联多久后判定进程退出并接手其页面 |
| `RETRY_AFTER_FALLBACK` | `2` | 429 缺少整数 Retry-After 时的暂停秒数 |
| `MAX_RETRY_AFTER` | 不限制 | 可选，给整数 Retry-After 设上限 |
| `MAX_URLS_PER_BATCH` | `20` | 单批上限 |
| `DEMO_A_URL` / `DEMO_B_URL` | `http://demo-a:8001` 等 | 演示源地址 |
