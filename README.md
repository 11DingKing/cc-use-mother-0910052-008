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

## 行情缺口修复与数据血缘

针对行情服务商漏发分钟线、迟到旧数据与多来源数值冲突，系统提供以下能力。

### 核心概念

- **导入批次（import_batches）**：每次写入（定时抓取 `scheduled`、缺口补数 `backfill`、
  手工录入 `manual`、冲突仲裁 `conflict_resolution`）都先以 `running` 状态落库再写数据；
  进程异常退出后，下次启动会把残留批次标记为 `interrupted`，重跑产生**新批次**，旧记录保留。
- **行级版本（candle_data_versions）**：同一根 K 线每次修订追加一个版本，旧版本置为
  `superseded` 但永不删除；当前值表 `stock_candles` 持有 `current_version_id/current_source/last_batch_id` 指针。
- **序列版本（data_versions）**：`(股票,周期)` 的数据每发生实质变更递增一个版本号；
  分析与回测结果记录自己基于哪个版本、哪次批次。
- **数据快照（data_snapshots）**：发布报告时按结果所依据的批次时点，把当时每根 K 线
  绑定的行版本固化为只读快照；补数后已发布报告仍可精确复现，历史回测可指定快照重放。
- **缺口登记（data_gaps）**：导入时按交易周期网格（日线工作日、分钟线两个 A 股交易时段）
  自动检测，也可手工登记；状态在 `open / partial / filled / verified / ignored` 间流转。
- **来源冲突（source_conflicts）**：同根 K 线多来源数值不一致时，按策略仲裁并全程留痕。

### 冲突与重算策略

冲突策略（导入或补数时指定 `conflict_policy`）：

- `trust_priority`（默认）：按来源可信度仲裁（manual > akshare > yahoo），同源迟到修订信任新值；
- `prefer_new` / `prefer_existing`：总是采用新数据 / 保留现值；
- `manual`：只登记为 `pending`，等待人工仲裁，仲裁本身生成可追溯批次。

补数后分析重算策略（`POST /api/analysis/{code}/recompute?policy=`）：

- `always`：数据版本领先于结果依据版本即重算；
- `on_gap_filled`（默认）：仅当原结果标注的缺口已补齐才重算；
- `on_open_gap`：区间仍有未结缺口时重算；
- `manual`：只报告“是否需要重算”，不自动执行。

已发布（`published`）的分析/回测版本永不被覆盖；重算只追加新版本，旧版本与快照保留。

### 常用接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/lineage/ingest` | 带批次的导入（自动去重、识别乱序/迟到、冲突仲裁、缺口检测） |
| GET | `/api/lineage/gaps` | 缺口列表（可按状态过滤） |
| POST | `/api/lineage/gaps` | 手工登记缺口 |
| POST | `/api/lineage/gaps/{id}/ignore` | 忽略误报缺口（留痕） |
| POST | `/api/lineage/gaps/{id}/backfill` | 用提供的数据补数（独立批次、可中断重跑） |
| POST | `/api/lineage/gaps/{id}/backfill-from-provider` | 直接向行情商重新抓取缺口区间 |
| POST | `/api/lineage/gaps/{id}/verify` | 补数后核验，通过置为 `verified` |
| GET | `/api/lineage/batches` | 批次查询（按类型/状态/缺口过滤） |
| GET | `/api/lineage/batches/{id}/lineage` | 批次写入/修订了哪些 K 线 |
| GET | `/api/lineage/candles/lineage` | 追溯单根 K 线的全部版本与来源批次 |
| GET | `/api/lineage/conflicts` | 来源冲突列表 |
| POST | `/api/lineage/conflicts/{id}/resolve` | 人工仲裁（accept/reject/manual） |
| GET/POST | `/api/lineage/snapshots` | 快照列表 / 创建只读快照 |
| GET | `/api/analysis/{code}/versions` | 分析结果版本列表（含受影响缺口ID、数据版本） |
| GET | `/api/analysis/{code}/versions/{v}` | 按版本读取历史结果 |
| POST | `/api/analysis/{code}/publish` | 发布报告（固化快照、锁定不可变） |
| POST | `/api/analysis/{code}/recompute` | 按策略补数后重算 |
| POST | `/api/backtest/run` | 支持 `snapshot_name` 按历史快照回测 |
| POST | `/api/backtest/{id}/publish` | 发布回测报告并固化快照 |
| GET | `/api/backtest/affected-by-gap/{gap_id}` | 查询受某缺口影响的全部回测版本 |

### 版本演进与兼容

- 对既有 SQLite 数据库，启动时自动 `ADD COLUMN` 补全新列，并为历史 K 线补建
  `source=legacy` 的 v1 行版本，迁移可重复执行（幂等）。
- 任何写入行情的路径都经过 `DataLineageService.ingest`，不再有绕过血缘的静默覆盖。
