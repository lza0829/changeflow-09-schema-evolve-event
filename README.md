# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Orders CDC · Schema Evolve（跨版本）**：v1 旧字段（`order_status` / `total_amount`）→ seq 4 演化点（`op=evolve`, v1→v2）→ v2 新字段（`status` / `amount`）；含一条刻意缺 `amount` 的事件用于验证映射缺失隔离。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 流内 Schema 演化

「Schema 演化」页维护演化点与旧→新字段映射，并查看每次 Apply 的演化对账。

**演化点识别/登记**

- 专用 evolve 事件：`op` 为 `evolve`，`schema_version`（或 `payload.to_version`）给出目标版本，`from_version` 缺省为当前版本。
- 普通事件显式声明更高的 `schema_version` 时，该 seq 视为隐式演化点。
- 也可在页面上手工登记演化点（`source=manual`，优先于夹具检测点），刷新后仍在（SQLite `schema_evolutions`）。
- 夹具检测到的演化点在 Apply 时落库，便于刷新后查看。

**入账与 Apply 规则**

- 演化点**之前**的事件按旧形状原样应用，Sink 行 `schema_version` 标注基础版本（兼容策略 `apply_legacy_shape`）。
- 演化点**之后**的事件先按 `(from_version → to_version)` 的映射规则规范化，再进账本/Apply；已是新字段形状的事件直接通过。
- 映射缺失关键字段（`required` 字段源/目标都不存在）或规则未配置时，**不写 Sink**：按流级 `mapping_failure_policy` 隔离（`quarantine`，默认）或跳过（`skip`），并在账本 `mapping_json` 与隔离清单标明 `missing_fields`。
- 演化控制事件只登记与推进游标，不写 Sink。
- `delete` 按 entity_key 删除，不校验 payload 字段。

**报告对账**

一次含演化的 Apply 结束后，`report.evolution` 与 run 列提供：演化前条数、演化后条数、演化点条数、映射成功数、映射失败数、最终 applied、Sink 版本分布，以及 `consistent` / `inconsistencies`。这些计数全部从本次 run 的账本行派生，可与演化配置、隔离列表、Sink 字段形态逐项解释：

- `pre + post + marker = 账本行数`
- `post = mapped + mapping_failed + post_duplicate`
- `applied = pre + mapped`
- `quarantined = 缺口隔离 + 映射隔离`

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链，以及本题的「流内 Schema 演化应用」。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等其余能力仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。

