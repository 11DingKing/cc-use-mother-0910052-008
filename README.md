# 行情缺口修复与数据血缘业务服务

这是一个使用 Python、FastAPI 与 SQLite 实现的纯后端业务服务，包含领域模型、数据访问、业务编排、接口和异常路径测试。项目可在单个 Linux 应用容器内离线运行，使用本地 SQLite 或内存替身，不依赖外部运行服务。

## 安装

```bash
python3 -m pip install -r requirements.txt
```

## 测试

```bash
python3 -m pytest -q
```

## 构建检查

```bash
python3 -m compileall -q app
```

## API 导入冒烟

```bash
python3 -c "from app.main import app; print(len(app.routes))"
```

## 启动

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 行情缺口、补数与数据血缘

针对行情服务商漏发分钟线、旧数据迟到、多来源取值不一致等问题，系统为每次
K 线写入建立**批次**与**版本**，并围绕批次登记缺口、编排补数、追溯来源血缘。

### 数据模型

| 表 | 作用 |
| --- | --- |
| `ingestion_batches` | 每次写入（常规导入 import / 补数 backfill / 手工修正 manual）的批次，含来源、区间、写入计量与 running/completed/partial/interrupted 状态 |
| `candle_versions` | 每根 K 线的全部取值版本（current / superseded / candidate），是历史快照复现的依据 |
| `stock_candles` | 当前值，新增 `source`、`source_batch_id`、`current_version_id` 血缘列 |
| `data_gaps` | 缺口区间登记（open / partial / filled / ignored），记录发现批次与填平批次 |
| `backfill_batches` | 补数任务：策略、来源优先级清单、已处理来源（断点续跑依据） |
| `source_conflicts` | 同一根 K 线在不同来源/批次间的取值冲突，未裁决前只挂起、不覆盖现值 |
| `analysis_results` | 分析版本化：每次计算产生新版本（`version`/`is_current`/`is_published`），记录消费的数据批次与窗口缺口 |
| `backtest_results` | 回测血缘：消费的批次集合、分析版本、缺口窗口；重跑产生新记录，旧记录以 `superseded_by_id` 串联 |

### 关键处理策略

- **重复导入**：同一根 K 线取值完全一致计为 duplicate，不改写来源与版本。
- **时间乱序（旧数据迟到）**：正常入库并计入批次 `late_arrival_count`，同时自动
  刷新与其区间相交的开放缺口（若正好填平则置 filled）。
- **来源冲突**：默认 `suspend`——挂起冲突、保留候选版本、现值不动；也可按
  `source_priority` 让高优先级来源直接生效，或在手工修正时用 `trusted` 直接生效。
  冲突经 `/conflicts/{id}/resolve` 显式裁决（keep_existing / use_incoming）。
- **补数中断**：批次先以独立事务落 running 记录，处理失败时数据事务整体回滚、
  批次标记 interrupted；`/backfills/{id}/resume` 按已处理来源清单断点续跑。
- **补数后重算**：`recompute=manual`（默认，仅返回受影响分析/回测清单）、
  `analysis`（自动产生分析新版本）、`analysis_and_backtest`（分析、回测均重跑）。
- **已发布报告**：分析版本经 `publish` 发布后冻结，后续重算只产生新版本，
  历史报告与历史快照始终可复现，不被覆盖或删除。
- **历史快照**：`/snapshot?as_of_batch_id=` 或 `as_of_time=` 复现任一历史时点的
  K 线取值，不影响正在使用的当前数据。

### 主要接口（前缀 `/api/lineage`）

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /{code}/{period}/ingest` | 手工导入/修正（走批次、版本、冲突通道） |
| `GET  /{code}/{period}/gaps` | 缺口列表（可按状态过滤） |
| `POST /{code}/{period}/gaps/scan` | 按交易时段网格扫描登记缺口 |
| `POST /gaps/{gap_id}/ignore` | 忽略缺口（如法定休市误报） |
| `POST /{code}/backfills` | 对登记缺口发起补数（strategy: fill/merge/trusted；recompute 策略） |
| `POST /backfills/{id}/resume` | 中断补数断点续跑 |
| `GET  /{code}/backfills` | 补数批次列表 |
| `GET  /conflicts` | 来源冲突列表 |
| `POST /conflicts/{id}/resolve` | 裁决冲突 |
| `GET  /{code}/{period}/bars/provenance` | 单根 K 线的来源与版本血缘 |
| `GET  /{code}/{period}/snapshot` | 按批次/时刻复现历史快照 |
| `GET  /{code}/{period}/affected` | 查询某区间补数影响了哪些分析与回测 |
| `GET  /batches` | 写入批次列表 |

分析与回测的版本化接口：

- `GET  /api/analysis/{code}?version=N`：读取指定分析版本（缺省为当前版本，返回 `is_stale` 陈旧标记）
- `GET  /api/analysis/{code}/versions`：分析版本列表
- `POST /api/analysis/{code}/publish?version=N`：发布报告（版本冻结）
- `POST /api/backtest/{id}/rerun`：补数后重跑回测，旧记录保留并指向新记录

`init_database()` 会自动建新表，并为已存在的 SQLite 库补齐新增列与索引（轻量
ALTER TABLE 迁移），旧分析记录会自动补排版本号。
