"""缺口→补数→血缘→分析版本化→发布 的 HTTP 端到端测试。"""

import pytest
from datetime import datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine

import app.config as config
from app.main import app


@pytest.fixture
def client(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/lineage_api.db")
    config._engine = engine
    config._SessionLocal = None
    with TestClient(app) as c:
        yield c
    config._engine = None
    config._SessionLocal = None


def _trading_days(start, n):
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _candle_payload(day, close):
    return {
        "timestamp": day.isoformat(),
        "open": close - 0.1, "high": close + 0.2,
        "low": close - 0.2, "close": close, "volume": 1000.0,
    }


def test_gap_backfill_lineage_and_publish_flow(client):
    code = "sz000001"

    # 1) 导入带缺口的序列（1/2 与 1/8 之间缺 1/3、1/4、1/5）
    payload = {
        "stock_code": code, "period": "daily", "source": "akshare",
        "candles": [
            _candle_payload(datetime(2024, 1, 2), 10.0),
            _candle_payload(datetime(2024, 1, 8), 11.0),
        ],
    }
    r = client.post("/api/lineage/ingest", json=payload)
    assert r.status_code == 200, r.text
    ingest1 = r.json()
    assert ingest1["data_version_no"] == 1
    assert len(ingest1["gaps_detected"]) == 1

    # 2) 缺口列表
    r = client.get("/api/lineage/gaps", params={"stock_code": code, "period": "daily"})
    gaps = r.json()["gaps"]
    assert r.json()["count"] == 1
    gap_id = gaps[0]["id"]
    assert gaps[0]["status"] == "open"

    # 3) 重复导入：幂等，不产生新版本
    r = client.post("/api/lineage/ingest", json=payload)
    assert r.json()["counts"]["duplicate"] == 2
    assert r.json()["data_changed"] is False

    # 4) 不同来源冲突（yahoo 优先级低，默认策略拒绝）
    conflict_payload = {
        "stock_code": code, "period": "daily", "source": "yahoo",
        "candles": [_candle_payload(datetime(2024, 1, 2), 99.0)],
    }
    r = client.post("/api/lineage/ingest", json=conflict_payload)
    assert r.json()["counts"]["conflict_rejected"] == 1
    r = client.get("/api/lineage/conflicts", params={"status": "rejected"})
    assert len(r.json()["conflicts"]) == 1

    # 5) 补数：产生 backfill 批次，缺口被补齐，序列版本推进
    r = client.post(f"/api/lineage/gaps/{gap_id}/backfill", json={
        "source": "akshare",
        "candles": [_candle_payload(datetime(2024, 1, d), 10.5) for d in (3, 4, 5)],
    })
    backfill = r.json()
    assert backfill["gap"]["status"] == "filled"
    assert backfill["data_version_no"] == 2

    # 6) 批次可查，补数批次关联缺口
    r = client.get("/api/lineage/batches", params={"batch_type": "backfill"})
    batches = r.json()["batches"]
    assert len(batches) == 1
    assert batches[0]["trigger_gap_id"] == gap_id

    # 7) 单根K线血缘可追溯到批次
    r = client.get("/api/lineage/candles/lineage", params={
        "stock_code": code, "period": "daily",
        "timestamp": "2024-01-03T00:00:00",
    })
    versions = r.json()["versions"]
    assert len(versions) == 1
    assert versions[0]["import_batch_id"] == backfill["batch_id"]
    assert versions[0]["change_reason"] == "backfill"

    # 8) 批次血缘反查
    r = client.get(f"/api/lineage/batches/{backfill['batch_id']}/lineage")
    assert len(r.json()["candles"]) == 3

    # 9) 核验缺口
    r = client.post(f"/api/lineage/gaps/{gap_id}/verify")
    assert r.json()["status"] == "verified"


def test_analysis_version_publish_and_recompute_api(client):
    code = "sz000001"
    days = _trading_days(datetime(2024, 1, 2), 22)
    missing = {datetime(2024, 1, 3), datetime(2024, 1, 4), datetime(2024, 1, 5)}
    days = [d for d in days if d not in missing]

    client.post("/api/lineage/ingest", json={
        "stock_code": code, "period": "daily", "source": "akshare",
        "candles": [
            _candle_payload(d, 10.0 + (i % 7) * 0.3)
            for i, d in enumerate(days)
        ],
    })

    # 跑分析（显式日期范围）
    r = client.post(f"/api/analysis/{code}/run", params={
        "start_date": "2024-01-01", "end_date": "2024-02-01",
    })
    assert r.status_code == 200, r.text
    assert r.json()["data_version_no"] == 1

    # 版本列表
    r = client.get(f"/api/analysis/{code}/versions")
    versions = r.json()["versions"]
    assert len(versions) == 1 and versions[0]["result_version"] == 1
    assert len(versions[0]["affected_gap_ids"]) == 1
    gap_id = versions[0]["affected_gap_ids"][0]

    # 发布 v1：固化快照
    r = client.post(f"/api/analysis/{code}/publish", params={"result_version": 1})
    assert r.json()["status"] == "published"
    snapshot_name = r.json()["snapshot_name"]

    # 补数
    client.post(f"/api/lineage/gaps/{gap_id}/backfill", json={
        "source": "manual",
        "candles": [_candle_payload(datetime(2024, 1, d), 50.0) for d in (3, 4, 5)],
    })

    # 已发布的 v1 仍可按版本读取且保留快照名
    r = client.get(f"/api/analysis/{code}/versions/1")
    v1 = r.json()
    assert v1["status"] == "published"
    assert v1["snapshot_name"] == snapshot_name

    # 按策略重算：缺口已补齐 → 产生 v2，v1 不受影响
    r = client.post(f"/api/analysis/{code}/recompute", params={"policy": "on_gap_filled"})
    body = r.json()
    assert body["recomputed"] is True
    assert body["previous_result_version"] == 1

    r = client.get(f"/api/analysis/{code}/versions")
    versions = r.json()["versions"]
    assert [v["result_version"] for v in versions] == [2, 1]
    assert versions[1]["status"] == "published"  # 旧报告不可变
    assert versions[0]["data_version_no"] == 2

    # 快照列表包含发布时固化的快照
    r = client.get("/api/lineage/snapshots")
    names = [s["name"] for s in r.json()["snapshots"]]
    assert snapshot_name in names
