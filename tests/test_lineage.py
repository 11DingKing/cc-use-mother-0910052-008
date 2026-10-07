"""缺口检测、补数批次、来源血缘、版本化分析/回测与快照的测试。"""

import pytest
from datetime import datetime, timedelta

from sqlalchemy import create_engine

import app.config as config
from app.config import init_database
from app.mappers.lineage_mapper import LineageMapper
from app.data import grid
from app.chan.models import RawCandle
from app.services.lineage_service import DataLineageService
from app.services.analysis_service import AnalysisService
from app.services.stock_service import StockService
from app.services.backtest_service import BacktestService


CODE = "sz000001"
PERIOD = "daily"


# ---------------------------------------------------------------------------
# 独立临时数据库，避免污染文件库
# ---------------------------------------------------------------------------

@pytest.fixture
def db_engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/lineage_test.db")
    config._engine = engine
    config._SessionLocal = None
    init_database()
    yield engine
    config._engine = None
    config._SessionLocal = None


@pytest.fixture
def lineage(db_engine):
    return DataLineageService()


def _candle(day: datetime, close: float = 10.0, src_seed: float = 0.0) -> RawCandle:
    return RawCandle(
        timestamp=day,
        open=close - 0.1,
        high=close + 0.2,
        low=close - 0.2,
        close=close,
        volume=1000.0 + src_seed,
    )


def _trading_days(start: datetime, n: int) -> list:
    days = []
    cursor = start
    while len(days) < n:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# 周期网格
# ---------------------------------------------------------------------------

class TestGrid:
    def test_daily_weekday_gap(self):
        # 周二 -> 下周二：中间有 周三、周四、周五、周一 = 4 根
        t1 = datetime(2024, 1, 2)   # 周二
        t2 = datetime(2024, 1, 9)   # 周二
        assert grid.expected_slot_count("daily", t1, t2) == 4
        slots = grid.expected_slots("daily", t1, t2)
        assert slots == [
            datetime(2024, 1, 3), datetime(2024, 1, 4),
            datetime(2024, 1, 5), datetime(2024, 1, 8),
        ]

    def test_daily_adjacent_no_gap(self):
        t1 = datetime(2024, 1, 2)
        t2 = datetime(2024, 1, 3)
        assert grid.expected_slot_count("daily", t1, t2) == 0

    def test_60min_slots_per_day(self):
        # 当日 11:30 -> 次日 10:30：严格在中间的是当日 14:00、15:00 两根
        t1 = datetime(2024, 1, 2, 11, 30)
        t2 = datetime(2024, 1, 3, 10, 30)
        assert grid.expected_slot_count("60min", t1, t2) == 2

    def test_60min_missing_afternoon(self):
        # 当日 11:30 -> 15:00（右端点存在）：严格在两者之间只有 14:00 一根
        t1 = datetime(2024, 1, 2, 11, 30)
        t2 = datetime(2024, 1, 2, 15, 0)
        assert grid.expected_slot_count("60min", t1, t2) == 1

    def test_30min_slots_count(self):
        t1 = datetime(2024, 1, 2, 9, 30)
        t2 = datetime(2024, 1, 2, 15, 0)
        # 全天8根，开区间排除首尾 = 10:00,10:30,11:00,11:30,13:30,14:00,14:30 共7根
        assert grid.expected_slot_count("30min", t1, t2) == 7


# ---------------------------------------------------------------------------
# 批次、去重、乱序
# ---------------------------------------------------------------------------

class TestIngestBasics:
    def test_first_ingest_creates_batch_and_versions(self, lineage):
        days = _trading_days(datetime(2024, 1, 2), 5)
        summary = lineage.ingest(
            [_candle(d, 10.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="akshare",
        )
        assert summary["counts"]["inserted"] == 5
        assert summary["data_version_no"] == 1

        batch = lineage.get_batch(summary["batch_id"])
        assert batch["status"] == "completed"
        assert batch["batch_type"] == "scheduled"
        assert batch["source"] == "akshare"

        lin = lineage.get_candle_lineage(CODE, PERIOD, days[0])
        assert len(lin["versions"]) == 1
        assert lin["versions"][0]["source"] == "akshare"
        assert lin["versions"][0]["import_batch_id"] == summary["batch_id"]

    def test_duplicate_import_is_idempotent(self, lineage):
        days = _trading_days(datetime(2024, 1, 2), 5)
        candles = [_candle(d, 10.0 + i) for i, d in enumerate(days)]
        first = lineage.ingest(candles, CODE, PERIOD, source="akshare")
        second = lineage.ingest(candles, CODE, PERIOD, source="akshare")

        assert second["counts"]["inserted"] == 0
        assert second["counts"]["duplicate"] == 5
        assert second["data_changed"] is False
        assert second["data_version_no"] is None
        assert first["data_version_no"] == 1

        # 同根K线仍只有一个版本
        lin = lineage.get_candle_lineage(CODE, PERIOD, days[0])
        assert len(lin["versions"]) == 1

    def test_duplicate_within_same_batch(self, lineage):
        day = datetime(2024, 1, 2)
        candles = [_candle(day, 10.0), _candle(day, 10.0)]
        summary = lineage.ingest(candles, CODE, PERIOD, source="akshare")
        assert summary["counts"]["inserted"] == 1
        assert summary["counts"]["duplicate"] == 1

    def test_out_of_order_and_late_arrival(self, lineage):
        days = _trading_days(datetime(2024, 1, 2), 5)
        lineage.ingest(
            [_candle(d, 10.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="akshare",
        )
        # 迟到的旧时间戳数据（按到达顺序乱序）
        late = [
            _candle(days[4], 20.0),
            _candle(days[1], 11.1),
        ]
        summary = lineage.ingest(late, CODE, PERIOD, source="akshare")
        assert summary["counts"]["out_of_order"] == 1
        assert summary["counts"]["late_arrival"] == 2


# ---------------------------------------------------------------------------
# 来源冲突与仲裁
# ---------------------------------------------------------------------------

class TestSourceConflict:
    def test_lower_priority_source_rejected(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")
        # yahoo 优先级低于 akshare，默认 trust_policy 下应拒绝
        summary = lineage.ingest(
            [_candle(day, 99.0)], CODE, PERIOD, source="yahoo"
        )
        assert summary["counts"]["conflict_rejected"] == 1
        assert summary["counts"]["updated"] == 0

        lin = lineage.get_candle_lineage(CODE, PERIOD, day)
        assert len(lin["versions"]) == 1
        conflicts = lineage.list_conflicts(status="rejected")
        assert len(conflicts) == 1
        assert conflicts[0]["existing_values"]["close"] == 10.0
        assert conflicts[0]["incoming_values"]["close"] == 99.0

    def test_higher_priority_source_accepted(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="yahoo")
        summary = lineage.ingest(
            [_candle(day, 11.0)], CODE, PERIOD, source="akshare"
        )
        assert summary["counts"]["conflict_accepted"] == 1
        assert summary["counts"]["updated"] == 1
        lin = lineage.get_candle_lineage(CODE, PERIOD, day)
        assert len(lin["versions"]) == 2
        assert lin["versions"][-1]["status"] == "active"
        assert lin["versions"][0]["status"] == "superseded"
        assert lin["versions"][-1]["change_reason"] == "conflict_win"

    def test_prefer_existing_policy(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="yahoo")
        summary = lineage.ingest(
            [_candle(day, 11.0)], CODE, PERIOD, source="akshare",
            conflict_policy="prefer_existing",
        )
        assert summary["counts"]["conflict_rejected"] == 1

    def test_manual_policy_pending_then_resolve(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")
        summary = lineage.ingest(
            [_candle(day, 12.0)], CODE, PERIOD, source="yahoo",
            conflict_policy="manual",
        )
        assert summary["counts"]["conflict_pending"] == 1
        conflicts = lineage.list_conflicts(status="pending")
        assert len(conflicts) == 1

        # 人工仲裁采用新值：仲裁本身产生一个 conflict_resolution 批次
        resolved = lineage.resolve_conflict(conflicts[0]["id"], "accept")
        assert resolved["status"] == "accepted"
        batches = lineage.list_batches(batch_type="conflict_resolution")
        assert len(batches) == 1

        lin = lineage.get_candle_lineage(CODE, PERIOD, day)
        assert len(lin["versions"]) == 2
        assert lin["versions"][-1]["source"] == "manual"

    def test_manual_resolve_with_explicit_values(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")
        cid = lineage.ingest(
            [_candle(day, 12.0)], CODE, PERIOD, source="yahoo",
            conflict_policy="manual",
        )
        conflict = lineage.list_conflicts(status="pending")[0]
        lineage.resolve_conflict(conflict["id"], "manual", values={
            "open": 10.5, "high": 10.8, "low": 10.4,
            "close": 10.6, "volume": 500.0,
        })
        lin = lineage.get_candle_lineage(CODE, PERIOD, day)
        assert lin["versions"][-1]["close"] == 10.6
        assert lin["versions"][-1]["volume"] == 500.0


# ---------------------------------------------------------------------------
# 缺口登记与补数
# ---------------------------------------------------------------------------

class TestGapAndBackfill:
    def test_gap_detected_on_ingest(self, lineage):
        # 1月2日（周二）与1月8日（周一），中间严格缺 3/4/5 三个工作日
        summary = lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        assert len(summary["gaps_detected"]) == 1
        gap = summary["gaps_detected"][0]
        assert gap["expected_count"] == 3
        assert gap["status"] == "open"

        gaps = lineage.list_gaps(CODE, PERIOD)
        assert len(gaps) == 1
        assert gaps[0]["first_seen_batch_id"] == summary["batch_id"]

    def test_backfill_fills_gap_and_bumps_version(self, lineage):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]

        filled = lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.0 + d * 0.1) for d in (3, 4, 5)],
            source="akshare",
        )
        assert filled["gap"]["status"] == "filled"
        assert filled["gap"]["filled_count"] == 3
        assert filled["batch_type"] == "backfill"
        assert filled["data_version_no"] == 2

        # 缺口能追溯到补齐批次，批次能追溯到缺口
        batches = lineage.list_batches(batch_type="backfill")
        assert batches[0]["trigger_gap_id"] == gap["id"]
        detail = lineage.get_batch_lineage(filled["batch_id"])
        assert len(detail["candles"]) == 3

    def test_partial_backfill(self, lineage):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        result = lineage.backfill_gap(
            gap["id"], [_candle(datetime(2024, 1, 4), 10.1)], source="akshare"
        )
        assert result["gap"]["status"] == "partial"
        assert result["gap"]["filled_count"] == 1
        # 原缺口内部出现新相邻对，不得重复登记第二个缺口
        gaps = lineage.list_gaps(CODE, PERIOD)
        assert len(gaps) == 1
        assert gaps[0]["id"] == gap["id"]

        # 补齐剩余两天后原缺口转 filled
        rest = lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, 3), 10.2), _candle(datetime(2024, 1, 5), 10.3)],
            source="akshare",
        )
        assert rest["gap"]["status"] == "filled"
        assert lineage.list_gaps(CODE, PERIOD)[0]["id"] == gap["id"]

    def test_backfill_interrupted_then_rerun(self, lineage, monkeypatch):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]

        # 让批次在写入途中失败：数据回滚（原子性），批次留下 interrupted
        def boom(*args, **kwargs):
            raise RuntimeError("simulated crash")
        monkeypatch.setattr(LineageMapper, "bump_data_version", boom)

        with pytest.raises(RuntimeError):
            lineage.backfill_gap(
                gap["id"],
                [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
                source="akshare",
            )

        interrupted = lineage.list_batches(status="interrupted")
        assert len(interrupted) == 1
        assert interrupted[0]["batch_type"] == "backfill"

        # 缺口仍为 open，补进去的数据随事务回滚
        monkeypatch.undo()
        assert lineage.list_gaps(CODE, PERIOD)[0]["status"] == "open"

        # 重新补数：产生新批次并成功
        rerun = lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
            source="akshare",
        )
        assert rerun["gap"]["status"] == "filled"
        backfills = lineage.list_batches(batch_type="backfill")
        assert {b["status"] for b in backfills} == {"interrupted", "completed"}

    def test_reconcile_interrupted_on_restart(self, lineage, db_engine):
        # 模拟残留 running 批次（进程被杀）
        with config.db_session_scope() as session:
            LineageMapper(session).create_batch(
                batch_type="scheduled", source="akshare",
                stock_code=CODE, period=PERIOD,
            )
        ids = lineage.reconcile_interrupted()
        assert len(ids) == 1
        assert lineage.get_batch(ids[0])["status"] == "interrupted"

    def test_manual_gap_register_and_ignore(self, lineage):
        gap = lineage.register_gap_manual(
            CODE, PERIOD,
            datetime(2024, 1, 2), datetime(2024, 1, 8),
        )
        assert gap["origin"] == "manual"
        assert gap["expected_count"] == 3
        ignored = lineage.ignore_gap(gap["id"], "服务商确认停牌")
        assert ignored["status"] == "ignored"
        # 忽略的缺口不再影响分析区间判断
        affecting = lineage.get_gaps_affecting_range(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 1, 31)
        )
        assert affecting == []

    def test_verify_gap(self, lineage):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
            source="akshare",
        )
        verified = lineage.verify_gap(gap["id"])
        assert verified["status"] == "verified"


# ---------------------------------------------------------------------------
# 分析结果版本化、发布与重算策略
# ---------------------------------------------------------------------------

def _ingest_series_with_gap(lineage, close_map=None):
    """构造 1月2日~1月31日工作日序列，1月3/4/5 缺失，后续补齐用。"""
    days = _trading_days(datetime(2024, 1, 2), 22)
    missing = {datetime(2024, 1, 3), datetime(2024, 1, 4), datetime(2024, 1, 5)}
    days = [d for d in days if d not in missing]
    candles = []
    for i, d in enumerate(days):
        close = 10.0 + (i % 7) * 0.3
        if close_map and d in close_map:
            close = close_map[d]
        candles.append(_candle(d, close))
    lineage.ingest(candles, CODE, PERIOD, source="akshare")
    return days


class TestAnalysisVersioning:
    def test_repeated_analysis_appends_versions(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        r1 = svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1))
        r2 = svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1))
        versions = svc.list_result_versions(CODE, PERIOD)
        assert [v["result_version"] for v in versions] == [2, 1]
        assert versions[1]["is_current"] is False
        assert versions[0]["is_current"] is True
        # 旧版本仍可按版本号读取
        v1 = svc.get_result_version(CODE, PERIOD, 1)
        assert v1["id"] != svc.get_result(CODE, PERIOD)["id"]
        assert r1 is not None and r2 is not None

    def test_analysis_records_data_lineage_and_gaps(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD,
            datetime(2024, 1, 1), datetime(2024, 2, 1),
        )
        latest = svc.get_result(CODE, PERIOD)
        assert latest["data_version_no"] == 1
        assert latest["based_on_batch_id"] is not None
        assert len(latest["affected_gap_ids"]) == 1

    def test_publish_freezes_report_with_snapshot(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1))
        published = svc.publish_report(CODE, PERIOD, result_version=1)
        assert published["status"] == "published"
        snapshot_name = published["snapshot_name"]
        assert snapshot_name

        # 补数改变当前数据后，已发布版本仍在且快照数值不变
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 50.0) for d in (3, 4, 5)],
            source="manual",
        )
        v1 = svc.get_result_version(CODE, PERIOD, 1)
        assert v1["status"] == "published"
        assert v1["snapshot_name"] == snapshot_name

        snap = lineage.get_snapshot_candles(snapshot_name)
        # 快照里没有后补的那三天（发布时它们尚不存在）
        snap_times = {c.timestamp for c in snap}
        assert datetime(2024, 1, 3) not in snap_times

    def test_recompute_manual_policy_only_reports(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
            source="akshare",
        )
        result = svc.recompute_after_backfill(CODE, PERIOD, policy="manual")
        assert result["recomputed"] is False
        assert result["should_recompute"] is True
        # 仍然只有一个结果版本
        assert len(svc.list_result_versions(CODE, PERIOD)) == 1

    def test_recompute_on_gap_filled(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]

        # 缺口未补齐前：不重算
        before = svc.recompute_after_backfill(CODE, PERIOD, policy="on_gap_filled")
        assert before["recomputed"] is False

        lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
            source="akshare",
        )
        after = svc.recompute_after_backfill(CODE, PERIOD, policy="on_gap_filled")
        assert after["recomputed"] is True
        assert after["reason"] == "affected_gap_filled"
        versions = svc.list_result_versions(CODE, PERIOD)
        assert len(versions) == 2
        # 新版本基于数据 v2
        assert versions[0]["data_version_no"] == 2

    def test_recompute_always(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 3, 1))
        # 追加一天的新数据（无冲突），数据版本推进
        extra_days = _trading_days(datetime(2024, 2, 1), 3)
        lineage.ingest(
            [_candle(d, 12.0) for d in extra_days],
            CODE, PERIOD, source="akshare",
        )
        result = svc.recompute_after_backfill(CODE, PERIOD, policy="always")
        assert result["recomputed"] is True

    def test_recompute_on_open_gap(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 3, 1)
        )
        # 新交易日数据到达（版本推进），但原缺口仍未结
        extra = _trading_days(datetime(2024, 2, 1), 2)
        lineage.ingest(
            [_candle(d, 12.0) for d in extra],
            CODE, PERIOD, source="akshare",
        )
        result = svc.recompute_after_backfill(CODE, PERIOD, policy="on_open_gap")
        assert result["recomputed"] is True
        assert result["reason"] == "open_gap_in_range"

    def test_find_results_affected_by_gap(self, lineage):
        _ingest_series_with_gap(lineage)
        svc = AnalysisService()
        svc.run_analysis(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        affected = svc.find_results_affected_by_gap(gap["id"])
        assert len(affected) == 1
        assert affected[0]["result_version"] == 1


# ---------------------------------------------------------------------------
# 快照不破坏历史；回测版本化
# ---------------------------------------------------------------------------

class TestSnapshotAndBacktest:
    def test_snapshot_is_immutable_after_revision(self, lineage):
        days = _trading_days(datetime(2024, 1, 2), 5)
        lineage.ingest(
            [_candle(d, 10.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="akshare",
        )
        lineage.create_snapshot(CODE, PERIOD, "snap-v1")

        # 高优先级来源修订全部K线
        lineage.ingest(
            [_candle(d, 20.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="manual",
        )
        snap_candles = lineage.get_snapshot_candles("snap-v1")
        assert {c.close for c in snap_candles} == {10.0, 11.0, 12.0, 13.0, 14.0}

        # 当前值已是修订后的
        stock = StockService()
        current = stock.get_candles(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        assert {c.close for c in current} == {20.0, 21.0, 22.0, 23.0, 24.0}

    def test_backtest_versions_and_gap_impact(self, lineage):
        _ingest_series_with_gap(lineage)
        bt = BacktestService()
        bt.run_backtest(
            CODE, PERIOD,
            datetime(2024, 1, 1), datetime(2024, 2, 1),
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]
        affected = bt.find_results_affected_by_gap(gap["id"])
        assert len(affected) == 1
        assert affected[0]["data_version_no"] == 1

        # 补数后再跑：追加新版本
        lineage.backfill_gap(
            gap["id"],
            [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
            source="akshare",
        )
        bt.run_backtest(
            CODE, PERIOD,
            datetime(2024, 1, 1), datetime(2024, 2, 1),
        )
        results = bt.list_results(CODE)
        assert len(results) == 2
        assert results[0]["result_version"] == 2
        assert results[0]["is_current"] is True

    def test_published_backtest_keeps_snapshot_name(self, lineage):
        _ingest_series_with_gap(lineage)
        bt = BacktestService()
        rid = bt.run_backtest(
            CODE, PERIOD, datetime(2024, 1, 1), datetime(2024, 2, 1)
        )["id"]
        pub = bt.publish_report(rid)
        assert pub["report_status"] == "published"
        assert pub["snapshot_name"].startswith("rpt-backtest-")

    def test_backtest_from_snapshot_reproduces_old_data(self, lineage):
        days = _trading_days(datetime(2024, 1, 2), 10)
        lineage.ingest(
            [_candle(d, 10.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="akshare",
        )
        lineage.create_snapshot(CODE, PERIOD, "old-snap")
        # 修订当前数据
        lineage.ingest(
            [_candle(d, 30.0 + i) for i, d in enumerate(days)],
            CODE, PERIOD, source="manual",
        )
        # 按旧快照回测：引擎收到的K线是旧值
        bt = BacktestService()
        report = bt.run_backtest(
            CODE, PERIOD,
            datetime(2024, 1, 1), datetime(2024, 2, 1),
            snapshot_name="old-snap",
        )
        assert report["id"] > 0
        stored = bt.get_result(report["id"])
        assert stored["summary"]["stock_code"] == CODE


# ---------------------------------------------------------------------------
# 批次血缘查询
# ---------------------------------------------------------------------------

class TestProviderBackfill:
    def test_backfill_gap_from_provider(self, lineage):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]

        stock = StockService()

        class FakeFetcher:
            source_name = "akshare"

            def fetch_candles(self, code, period, start, end):
                from app.data.fetcher import FetchResult
                candles = [_candle(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)]
                return FetchResult(
                    candles, code, period, start, end, "akshare", True
                )

        stock.fetchers["akshare"] = FakeFetcher()
        result = stock.backfill_gap_from_provider(gap["id"], source="akshare")
        assert result["gap"]["status"] == "filled"
        assert result["source"] == "akshare"

    def test_backfill_gap_from_provider_still_missing(self, lineage):
        lineage.ingest(
            [_candle(datetime(2024, 1, 2), 10.0),
             _candle(datetime(2024, 1, 8), 11.0)],
            CODE, PERIOD, source="akshare",
        )
        gap = lineage.list_gaps(CODE, PERIOD)[0]

        stock = StockService()

        class EmptyFetcher:
            source_name = "akshare"

            def fetch_candles(self, code, period, start, end):
                from app.data.fetcher import FetchResult
                return FetchResult(
                    [], code, period, start, end, "akshare", False, "still missing"
                )

        stock.fetchers = {"akshare": EmptyFetcher()}
        result = stock.backfill_gap_from_provider(gap["id"], source="akshare")
        assert result["fetched"] is False
        # 缺口保持 open，没有产生 backfill 批次
        assert lineage.list_gaps(CODE, PERIOD)[0]["status"] == "open"
        assert lineage.list_batches(batch_type="backfill") == []


class TestBatchLineage:
    def test_batch_lineage_lists_changed_candles(self, lineage):
        day = datetime(2024, 1, 2)
        s1 = lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")
        s2 = lineage.ingest([_candle(day, 11.0)], CODE, PERIOD, source="manual")

        detail1 = lineage.get_batch_lineage(s1["batch_id"])
        assert detail1["batch"]["id"] == s1["batch_id"]
        assert len(detail1["candles"]) == 1

        detail2 = lineage.get_batch_lineage(s2["batch_id"])
        assert detail2["candles"][0]["change_reason"] == "conflict_win"

    def test_data_versions_chain(self, lineage):
        day = datetime(2024, 1, 2)
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")
        lineage.ingest([_candle(day, 10.0)], CODE, PERIOD, source="akshare")  # 无变化
        lineage.ingest([_candle(day, 11.0)], CODE, PERIOD, source="manual")
        versions = lineage.get_data_versions(CODE, PERIOD)
        assert [v["version_no"] for v in versions] == [1, 2]
        assert versions[0]["status"] == "superseded"
        assert versions[1]["status"] == "active"
