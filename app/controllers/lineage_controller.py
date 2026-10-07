"""缺口登记、补数批次、来源血缘、快照的查询与管理接口。"""

from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.lineage_service import DataLineageService
from app.services.stock_service import StockService
from app.chan.models import RawCandle

router = APIRouter(prefix="/api/lineage", tags=["lineage"])
lineage_service = DataLineageService()
stock_service = StockService()


class CandleIn(BaseModel):
    timestamp: str  # ISO 格式
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


class BackfillRequest(BaseModel):
    source: str = "akshare"
    conflict_policy: str = "trust_priority"
    candles: List[CandleIn] = []


class ManualGapRequest(BaseModel):
    stock_code: str
    period: str
    gap_start: str
    gap_end: str


class IngestRequest(BaseModel):
    stock_code: str
    period: str
    source: str = "manual"
    batch_type: str = "manual"
    conflict_policy: str = "trust_priority"
    candles: List[CandleIn]


class ConflictResolveRequest(BaseModel):
    decision: str  # accept / reject / manual
    values: Optional[dict] = None


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _to_raw(items: List[CandleIn]) -> List[RawCandle]:
    return [
        RawCandle(
            timestamp=_parse_ts(i.timestamp),
            open=i.open, high=i.high, low=i.low,
            close=i.close, volume=i.volume,
        )
        for i in items
    ]


# ------------------------------------------------------------------
# 缺口
# ------------------------------------------------------------------

@router.get("/gaps")
async def list_gaps(
    stock_code: Optional[str] = None,
    period: Optional[str] = None,
    status: Optional[str] = Query(
        None, description="open/partial/filled/verified/ignored"
    ),
):
    gaps = lineage_service.list_gaps(
        stock_code=stock_code, period=period, status=status
    )
    return {"count": len(gaps), "gaps": gaps}


@router.post("/gaps")
async def register_gap(request: ManualGapRequest):
    """研究员手工登记缺口（例如服务商明确告知某段缺失）。"""
    gap = lineage_service.register_gap_manual(
        request.stock_code, request.period,
        _parse_ts(request.gap_start), _parse_ts(request.gap_end),
    )
    return gap


@router.post("/gaps/{gap_id}/ignore")
async def ignore_gap(gap_id: int, reason: str = Query("manual ignore")):
    return lineage_service.ignore_gap(gap_id, reason)


@router.post("/gaps/{gap_id}/verify")
async def verify_gap(gap_id: int):
    """补数后核验：区间K线数达到预期才置为 verified。"""
    return lineage_service.verify_gap(gap_id)


# ------------------------------------------------------------------
# 补数批次
# ------------------------------------------------------------------

@router.post("/gaps/{gap_id}/backfill")
async def backfill_gap(gap_id: int, request: BackfillRequest):
    """对缺口补数（数据来自手工录入或上游抓取后的调用方）。

    产生 batch_type=backfill 的批次；若中途中断，批次保留 interrupted 状态，
    重新调用会产生新批次而不会静默覆盖旧记录。
    """
    return lineage_service.backfill_gap(
        gap_id,
        _to_raw(request.candles),
        source=request.source,
        conflict_policy=request.conflict_policy,
    )


@router.post("/gaps/{gap_id}/backfill-from-provider")
async def backfill_gap_from_provider(
    gap_id: int,
    source: Optional[str] = Query(None, description="指定行情源，默认按优先级轮询"),
    conflict_policy: str = Query("trust_priority"),
):
    """登记缺口后直接向行情商重新抓取该区间。服务商仍缺时不写数据。"""
    try:
        return stock_service.backfill_gap_from_provider(
            gap_id, source=source, conflict_policy=conflict_policy
        )
    except KeyError:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"error": f"gap {gap_id} not found"})


@router.get("/batches")
async def list_batches(
    stock_code: Optional[str] = None,
    period: Optional[str] = None,
    status: Optional[str] = None,
    batch_type: Optional[str] = None,
    gap_id: Optional[int] = None,
    limit: int = 50,
):
    return {
        "batches": lineage_service.list_batches(
            stock_code=stock_code, period=period, status=status,
            batch_type=batch_type, gap_id=gap_id, limit=limit,
        )
    }


@router.get("/batches/{batch_id}")
async def get_batch(batch_id: str):
    return lineage_service.get_batch(batch_id)


@router.get("/batches/{batch_id}/lineage")
async def get_batch_lineage(batch_id: str):
    """追溯该批次写入/修订了哪些K线。"""
    return lineage_service.get_batch_lineage(batch_id)


# ------------------------------------------------------------------
# 手工/外部导入（带批次、去重、乱序与冲突处理）
# ------------------------------------------------------------------

@router.post("/ingest")
async def ingest(request: IngestRequest):
    summary = lineage_service.ingest(
        _to_raw(request.candles),
        stock_code=request.stock_code,
        period=request.period,
        source=request.source,
        batch_type=request.batch_type,
        conflict_policy=request.conflict_policy,
    )
    return summary


# ------------------------------------------------------------------
# 来源冲突
# ------------------------------------------------------------------

@router.get("/conflicts")
async def list_conflicts(
    stock_code: Optional[str] = None,
    period: Optional[str] = None,
    status: Optional[str] = Query(None, description="pending/accepted/rejected"),
):
    return {"conflicts": lineage_service.list_conflicts(
        stock_code=stock_code, period=period, status=status
    )}


@router.post("/conflicts/{conflict_id}/resolve")
async def resolve_conflict(conflict_id: int, request: ConflictResolveRequest):
    """对 pending 冲突做人工仲裁，仲裁本身也是一个可追溯批次。"""
    return lineage_service.resolve_conflict(
        conflict_id, request.decision, request.values
    )


# ------------------------------------------------------------------
# 血缘查询与快照
# ------------------------------------------------------------------

@router.get("/candles/lineage")
async def candle_lineage(
    stock_code: str,
    period: str,
    timestamp: str = Query(..., description="ISO 时间戳，查询单根K线的版本链"),
):
    """追溯每一根数据来自哪次批次：返回该K线的全部历史版本。"""
    return lineage_service.get_candle_lineage(
        stock_code, period, _parse_ts(timestamp)
    )


@router.get("/data-versions")
async def data_versions(stock_code: str, period: str):
    return {"versions": lineage_service.get_data_versions(stock_code, period)}


class SnapshotRequest(BaseModel):
    stock_code: str
    period: str
    name: str
    note: Optional[str] = None


@router.post("/snapshots")
async def create_snapshot(request: SnapshotRequest):
    """为仍在使用的历史分析固化当前数据快照。"""
    return lineage_service.create_snapshot(
        request.stock_code, request.period, request.name, request.note
    )


@router.get("/snapshots")
async def list_snapshots(
    stock_code: Optional[str] = None, period: Optional[str] = None
):
    return {"snapshots": lineage_service.list_snapshots(stock_code, period)}
