"""缺口登记、补数批次、来源血缘、版本化分析/回测的测试。"""

import pytest
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from app.chan.models import RawCandle
from app.data import gap_detector
from app.services.lineage_service import LineageService
from app.services.analysis_service import AnalysisService
from app.services.backtest_service import BacktestService


# ----------------------------------------------------------------------
# 隔离数据库
# ----------------------------------------------------------------------

@pytest.fixture
def isolated_db(tmp_path):
    import app.config as config
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(
        f"sqlite:///{tmp_path / 'lineage_test.db'}",
        connect_args={"check_same_thread": False},
    )
    session_local = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    old_engine, old_session = config._engine, config._SessionLocal
    config._engine = engine
    config._SessionLocal = session_local
    config.init_database()
    yield session_local
    config._engine, config._SessionLocal = old_engine, old_session
    engine.dispose()


# ----------------------------------------------------------------------
# 辅助
# ----------------------------------------------------------------------

def _candle(ts, close=10.0, source=None):
    return RawCandle(
        timestamp=ts,
        open=close * 0.99,
        high=close * 1.02,
        low=close * 0.98,
        close=close,
        volume=1000.0,
    )


def _daily_series(start: datetime, days: int, gap_on=()):
    out = []
    day = start
    n = 0
    while n < days:
        if day.weekday() < 5:
            if day.date() not in gap_on:
                out.append(_candle(datetime(day.year, day.month, day.day),
                                   close=10.0 + n * 0.1))
            n += 1
        day += timedelta(days=1)
    return out, day


# ----------------------------------------------------------------------
# 缺口探测
# ----------------------------------------------------------------------

class TestGapDetector:
    def test_daily_gap_over_weekend_is_not_gap(self):
        # 周五与下周一相邻，不应报缺口
        fri = datetime(2024, 3, 1)   # 周五
        mon = datetime(2024, 3, 4)
        groups = gap_detector.detect_missing_between_bars(
            "daily", [fri, mon]
        )
        assert groups == []

    def test_daily_missing_weekday_is_gap(self):
        mon = datetime(2024, 3, 4)
        thu = datetime(2024, 3, 7)
        groups = gap_detector.detect_missing_between_bars(
            "daily", [mon, thu]
        )
        assert len(groups) == 1
        start, end, count = groups[0]
        assert start == datetime(2024, 3, 5)
        assert end == datetime(2024, 3, 6)
        assert count == 2

    def test_intraday_missing_slot(self):
        d = datetime(2024, 3, 4)
        groups = gap_detector.detect_missing_between_bars(
            "60min",
            [d.replace(hour=10, minute=30), d.replace(hour=14, minute=0)],
        )
        assert len(groups) == 1
        assert groups[0][2] == 1
        assert groups[0][0] == d.replace(hour=11, minute=30)

    def test_explicit_scan_range(self):
        start = datetime(2024, 3, 4)
        end = datetime(2024, 3, 8)
        present = [datetime(2024, 3, 4), datetime(2024, 3, 5)]
        groups = gap_detector.detect_missing_groups(
            "daily", present, start, end
        )
        # 缺 3/6、3/7、3/8
        assert sum(g[2] for g in groups) == 3


# ----------------------------------------------------------------------
# 导入：去重 / 乱序 / 批次血缘
# ----------------------------------------------------------------------

class TestIngestion:
    def test_insert_and_batch_lineage(self, isolated_db):
        svc = LineageService()
        candles, _ = _daily_series(datetime(2024, 3, 4), 5)
        summary = svc.ingest_candles("000001", "daily", candles, source="akshare")
        assert summary["status"] == "completed"
        assert summary["inserted"] == 5
        assert summary["duplicate"] == 0

        # 每根 K 线都能追溯到批次
        prov = svc.bar_provenance(
            "000001", "daily", datetime(2024, 3, 4)
        )
        assert prov["bar"]["source"] == "akshare"
        assert prov["bar"]["source_batch_id"] == summary["batch_id"]
        assert len(prov["versions"]) == 1

    def test_duplicate_import_is_idempotent(self, isolated_db):
        svc = LineageService()
        candles, _ = _daily_series(datetime(2024, 3, 4), 5)
        first = svc.ingest_candles("000001", "daily", candles, source="akshare")
        again = svc.ingest_candles("000001", "daily", candles, source="yahoo")
        assert again["inserted"] == 0
        assert again["duplicate"] == 5
        assert again["conflicts"] == 0
        # 来源仍是首次导入的来源，不被重复导入改写
        prov = svc.bar_provenance("000001", "daily", datetime(2024, 3, 4))
        assert prov["bar"]["source"] == "akshare"

    def test_late_arrival_is_counted(self, isolated_db):
        svc = LineageService()
        later, _ = _daily_series(datetime(2024, 3, 11), 3)
        svc.ingest_candles("000001", "daily", later, source="akshare")
        old, _ = _daily_series(datetime(2024, 3, 4), 3)
        summary = svc.ingest_candles("000001", "daily", old, source="akshare")
        assert summary["late_arrivals"] == 3

    def test_import_auto_registers_internal_gap(self, isolated_db):
        svc = LineageService()
        # 周一之后直接跳到周四，中间缺两根
        candles = [
            _candle(datetime(2024, 3, 4)),
            _candle(datetime(2024, 3, 7)),
        ]
        svc.ingest_candles("000001", "daily", candles, source="akshare")
        gaps = svc.list_gaps("000001", "daily")
        assert len(gaps) == 1
        assert gaps[0]["missing_count"] == 2


# ----------------------------------------------------------------------
# 来源冲突
# ----------------------------------------------------------------------

class TestSourceConflict:
    def test_conflicting_value_is_suspended_and_kept(self, isolated_db):
        svc = LineageService()
        ts = datetime(2024, 3, 4)
        svc.ingest_candles("000001", "daily", [_candle(ts, close=10.0)], source="akshare")
        summary = svc.ingest_candles(
            "000001", "daily", [_candle(ts, close=12.0)], source="yahoo"
        )
        assert summary["conflicts"] == 1
        assert summary["status"] == "partial"

        # 现值不被静默覆盖
        prov = svc.bar_provenance("000001", "daily", ts)
        assert prov["bar"]["close"] == 10.0
        statuses = [v["status"] for v in prov["versions"]]
        assert "candidate" in statuses

        conflicts = svc.list_conflicts("000001", "daily")
        assert len(conflicts) == 1
        assert conflicts[0]["incoming_values"]["close"] == 12.0

    def test_resolve_use_incoming_creates_new_version(self, isolated_db):
        svc = LineageService()
        ts = datetime(2024, 3, 4)
        svc.ingest_candles("000001", "daily", [_candle(ts, close=10.0)], source="akshare")
        summary = svc.ingest_candles(
            "000001", "daily", [_candle(ts, close=12.0)], source="yahoo"
        )
        result = svc.resolve_conflict(
            summary["conflict_ids"][0], "use_incoming", resolved_by="analyst"
        )
        assert result["status"] == "resolved"

        prov = svc.bar_provenance("000001", "daily", ts)
        assert prov["bar"]["close"] == 12.0
        assert prov["bar"]["source"] == "yahoo"
        current = [v for v in prov["versions"] if v["status"] == "current"]
        assert len(current) == 1
        assert current[0]["values"]["close"] == 12.0
        # 旧值仍可追溯
        assert any(v["values"]["close"] == 10.0 for v in prov["versions"])

    def test_resolve_keep_existing(self, isolated_db):
        svc = LineageService()
        ts = datetime(2024, 3, 4)
        svc.ingest_candles("000001", "daily", [_candle(ts, close=10.0)], source="akshare")
        summary = svc.ingest_candles(
            "000001", "daily", [_candle(ts, close=12.0)], source="yahoo"
        )
        svc.resolve_conflict(summary["conflict_ids"][0], "keep_existing")
        prov = svc.bar_provenance("000001", "daily", ts)
        assert prov["bar"]["close"] == 10.0
        assert svc.list_conflicts("000001", "daily") == []

    def test_source_priority_auto_adopts(self, isolated_db):
        svc = LineageService()
        ts = datetime(2024, 3, 4)
        svc.ingest_candles("000001", "daily", [_candle(ts, close=10.0)], source="yahoo")
        summary = svc.ingest_candles(
            "000001", "daily", [_candle(ts, close=12.0)], source="akshare",
            on_conflict="source_priority", source_priority=["akshare", "yahoo"],
        )
        assert summary["changed"] == 1
        prov = svc.bar_provenance("000001", "daily", ts)
        assert prov["bar"]["close"] == 12.0


# ----------------------------------------------------------------------
# 历史快照
# ----------------------------------------------------------------------

class TestSnapshot:
    def test_snapshot_as_of_batch(self, isolated_db):
        svc = LineageService()
        candles, _ = _daily_series(datetime(2024, 3, 4), 3)
        first = svc.ingest_candles("000001", "daily", candles, source="akshare")

        ts = datetime(2024, 3, 4)
        svc.ingest_candles(
            "000001", "daily", [_candle(ts, close=99.0)], source="akshare",
            kind="manual", on_conflict="trusted",
        )

        old = svc.snapshot("000001", "daily", as_of_batch_id=first["batch_id"])
        assert old[0]["values"]["close"] != 99.0

        current = svc.snapshot("000001", "daily")
        assert current[0]["values"]["close"] == 99.0


# ----------------------------------------------------------------------
# 补数批次
# ----------------------------------------------------------------------

class TestBackfill:
    def _seed_with_intraday_gap(self, svc):
        d = datetime(2024, 3, 4)
        svc.ingest_candles(
            "000001", "60min",
            [
                _candle(d.replace(hour=10, minute=30)),
                _candle(d.replace(hour=14, minute=0)),
            ],
            source="akshare",
        )
        gaps = svc.list_gaps("000001", "60min")
        assert len(gaps) == 1
        return gaps[0]

    def test_backfill_fills_gap_with_second_source(self, isolated_db):
        calls = {"akshare": 0, "yahoo": 0}

        def fetch(source, code, period, start, end):
            calls[source] += 1
            if source == "akshare":
                return [], "no data"
            d = datetime(2024, 3, 4)
            return [_candle(d.replace(hour=11, minute=30), close=11.0)], None

        svc = LineageService(fetch_callback=fetch)
        gap = self._seed_with_intraday_gap(svc)

        result = svc.backfill_gaps(
            "000001", "60min", sources=["akshare", "yahoo"], strategy="fill"
        )
        assert result["status"] == "completed"
        assert gap["id"] in result["filled_gap_ids"]
        assert calls["akshare"] == 1 and calls["yahoo"] == 1

        gaps = svc.list_gaps("000001", "60min", status="filled")
        assert len(gaps) == 1
        assert gaps[0]["filled_by_batch_id"] is not None

        # 补进来的 K 线血缘指向补数子批次
        prov = svc.bar_provenance(
            "000001", "60min", datetime(2024, 3, 4, 11, 30)
        )
        assert prov["bar"]["source"] == "yahoo"

    def test_backfill_partial_when_no_source_has_data(self, isolated_db):
        def fetch(source, code, period, start, end):
            return [], "no data"

        svc = LineageService(fetch_callback=fetch)
        self._seed_with_intraday_gap(svc)
        result = svc.backfill_gaps(
            "000001", "60min", sources=["akshare", "yahoo"]
        )
        assert result["status"] == "partial"
        assert len(result["remaining_gap_ids"]) == 1
        rows = svc.list_backfills(stock_code="000001")
        assert rows[0]["status"] == "partial"

    def test_interrupted_backfill_can_resume(self, isolated_db):
        attempts = {"akshare": 0}

        def fetch(source, code, period, start, end):
            if source == "akshare":
                attempts["akshare"] += 1
                if attempts["akshare"] == 1:
                    raise RuntimeError("连接中断")
                return [], "no data"
            d = datetime(2024, 3, 4)
            return [_candle(d.replace(hour=11, minute=30), close=11.0)], None

        svc = LineageService(fetch_callback=fetch)
        gap = self._seed_with_intraday_gap(svc)

        with pytest.raises(RuntimeError):
            svc.backfill_gaps(
                "000001", "60min", sources=["akshare", "yahoo"]
            )

        rows = svc.list_backfills(stock_code="000001")
        assert rows[0]["status"] == "interrupted"

        resumed = svc.resume_backfill(rows[0]["id"])
        assert resumed["status"] == "completed"
        assert gap["id"] in resumed["filled_gap_ids"]


# ----------------------------------------------------------------------
# 分析版本化与发布
# ----------------------------------------------------------------------

class TestAnalysisVersioning:
    def _seed(self, svc_lineage=None):
        lineage = LineageService()
        candles, _ = _daily_series(datetime(2024, 3, 4), 30)
        lineage.ingest_candles("000001", "daily", candles, source="akshare")
        return lineage

    def test_recompute_creates_new_version_and_keeps_old(self, isolated_db):
        self._seed()
        service = AnalysisService()
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 31)

        v1 = service.run_analysis("000001", "daily", start, end)
        assert v1["version"] == 1
        v2 = service.run_analysis(
            "000001", "daily", start, end, recompute_reason="manual"
        )
        assert v2["version"] == 2

        versions = service.list_versions("000001", "daily")
        assert [v["version"] for v in versions] == [2, 1]
        assert versions[0]["is_current"] is True
        assert versions[1]["is_current"] is False

        old = service.get_result("000001", "daily", version=1)
        assert old["version"] == 1
        assert old["is_current"] is False
        latest = service.get_result("000001", "daily")
        assert latest["version"] == 2

    def test_published_report_is_frozen(self, isolated_db):
        self._seed()
        service = AnalysisService()
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 31)
        service.run_analysis("000001", "daily", start, end)
        published = service.publish_version("000001", "daily", 1)
        assert published["is_published"] is True

        service.run_analysis(
            "000001", "daily", start, end, recompute_reason="backfill"
        )
        v1 = service.get_result("000001", "daily", version=1)
        assert v1["is_published"] is True  # 已发布报告未被破坏

    def test_analysis_carries_lineage_and_staleness(self, isolated_db):
        lineage = self._seed()
        service = AnalysisService()
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 31)
        result = service.run_analysis("000001", "daily", start, end)
        assert result["data_lineage"]["batch_ids"]
        assert result["affected_by_open_gaps"] is False

        # 再来一个新批次（旧数据迟到），最新结果应被识别为陈旧
        lineage.ingest_candles(
            "000001", "daily",
            [_candle(datetime(2024, 3, 4), close=10.5)],
            source="akshare", kind="manual", on_conflict="trusted",
        )
        versions = service.list_versions("000001", "daily")
        assert versions[0]["is_stale"] is True

    def test_affected_artifacts_after_backfill(self, isolated_db):
        lineage = self._seed()
        service = AnalysisService()
        start, end = datetime(2024, 3, 1), datetime(2024, 3, 31)
        service.run_analysis("000001", "daily", start, end)

        fill = lineage.ingest_candles(
            "000001", "daily",
            [_candle(datetime(2024, 3, 4), close=10.5)],
            source="akshare", kind="backfill", on_conflict="trusted",
        )
        affected = lineage.affected_artifacts(
            "000001", "daily",
            datetime(2024, 3, 4), datetime(2024, 3, 4),
            min_batch_id=fill["batch_id"],
        )
        assert len(affected["stale_analyses"]) == 1
        assert "批次" in affected["stale_analyses"][0]["reason"]


# ----------------------------------------------------------------------
# 回测血缘与重跑
# ----------------------------------------------------------------------

class TestBacktestVersioning:
    def test_backtest_lineage_and_rerun(self, isolated_db):
        lineage = LineageService()
        # 使用距今一年内的工作日数据，避免默认时间窗问题
        start_day = datetime(2026, 8, 3)
        candles = []
        day = start_day
        while len(candles) < 40:
            if day.weekday() < 5:
                candles.append(_candle(
                    datetime(day.year, day.month, day.day),
                    close=10.0 + len(candles) * 0.2,
                ))
            day += timedelta(days=1)
        lineage.ingest_candles("000001", "daily", candles, source="akshare")

        service = BacktestService()
        start = candles[0].timestamp
        end = candles[-1].timestamp
        report = service.run_backtest("000001", "daily", start, end)
        assert report["data_lineage"]["batch_ids"]
        assert report["analysis_version_id"] is not None
        old_id = report["id"]

        # 补数（修正区间内一根）后重跑
        lineage.ingest_candles(
            "000001", "daily",
            [_candle(candles[5].timestamp, close=20.0)],
            source="akshare", kind="manual", on_conflict="trusted",
        )
        rerun = service.rerun_backtest(old_id, reason="backfill")
        assert rerun["id"] != old_id
        assert rerun["supersedes"] == old_id

        old_report = service.get_result(old_id)
        assert old_report["superseded_by_id"] == rerun["id"]
        # 旧报告内容仍可查询
        assert old_report["status"] == "completed"


# ----------------------------------------------------------------------
# 接口冒烟
# ----------------------------------------------------------------------

class TestLineageAPI:
    def test_lineage_endpoints(self, isolated_db):
        svc = LineageService()
        d = datetime(2024, 3, 4)
        svc.ingest_candles(
            "000001", "60min",
            [_candle(d.replace(hour=10, minute=30))],
            source="akshare",
        )
        from app.main import app
        client = TestClient(app)

        # 批次列表
        resp = client.get("/api/lineage/batches", params={"stock_code": "000001"})
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        # 单根血缘
        resp = client.get(
            "/api/lineage/000001/60min/bars/provenance",
            params={"timestamp": "2024-03-04T10:30:00"},
        )
        assert resp.status_code == 200
        assert resp.json()["bar"]["source"] == "akshare"

        # 快照
        resp = client.get("/api/lineage/000001/60min/snapshot")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        # 缺口扫描
        resp = client.post(
            "/api/lineage/000001/60min/gaps/scan",
            params={"start_date": "2024-03-04", "end_date": "2024-03-04"},
        )
        assert resp.status_code == 200
        # 当天缺 11:30/14:00/15:00，同日槽位聚为一个缺口段
        assert resp.json()["count"] == 1
        assert resp.json()["gaps"][0]["missing_count"] == 3

        # 手工导入
        resp = client.post(
            "/api/lineage/000001/60min/ingest",
            json={
                "candles": [{
                    "timestamp": "2024-03-04T11:30:00",
                    "open": 10, "high": 11, "low": 9, "close": 10,
                }],
                "source": "manual",
                "kind": "manual",
                "on_conflict": "trusted",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["inserted"] == 1

        # 冲突列表
        resp = client.get("/api/lineage/conflicts")
        assert resp.status_code == 200

    def test_analysis_version_endpoints(self, isolated_db):
        svc = LineageService()
        candles, _ = _daily_series(datetime(2024, 3, 4), 20)
        svc.ingest_candles("000001", "daily", candles, source="akshare")
        service = AnalysisService()
        service.run_analysis(
            "000001", "daily",
            datetime(2024, 3, 1), datetime(2024, 3, 31),
        )
        from app.main import app
        client = TestClient(app)

        resp = client.get("/api/analysis/000001/versions", params={"period": "daily"})
        assert resp.status_code == 200
        assert resp.json()["versions"][0]["version"] == 1

        resp = client.post(
            "/api/analysis/000001/publish",
            params={"period": "daily", "version": 1},
        )
        assert resp.status_code == 200
        assert resp.json()["is_published"] is True
