"""缺口登记、补数批次、来源血缘的接口。"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.lineage_service import LineageService
from app.services.stock_service import StockService
from app.chan.models import RawCandle

router = APIRouter(prefix="/api/lineage", tags=["lineage"])

# 复用带真实取数回调的服务实例
_stock_service = StockService()
lineage_service = LineageService(fetch_callback=_stock_service._fetch_callback)


def _parse_ts(value: Optional[str], field: str) -> Optional[datetime]:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"{field} 时间格式应为 YYYY-MM-DD 或 YYYY-MM-DDTHH:MM:SS")


def _parse_end_ts(value: Optional[str], field: str) -> Optional[datetime]:
    """结束日期只给日期时，取当天最后一刻，覆盖当日全部分钟槽位。"""
    if not value:
        return None
    if len(value.strip()) == 10:
        try:
            day = datetime.strptime(value.strip(), "%Y-%m-%d")
            return day.replace(hour=23, minute=59, second=59)
        except ValueError:
            pass
    return _parse_ts(value, field)


class CandleIn(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    amount: Optional[float] = None


class IngestRequest(BaseModel):
    candles: List[CandleIn]
    source: str = "manual"
    kind: str = "manual"
    on_conflict: str = "suspend"
    created_by: Optional[str] = None


class BackfillRequest(BaseModel):
    period: str
    gap_ids: Optional[List[int]] = None
    sources: Optional[List[str]] = None
    strategy: str = "fill"
    recompute: str = "manual"
    requested_by: Optional[str] = None


class ConflictResolveRequest(BaseModel):
    resolution: str  # keep_existing / use_incoming
    resolved_by: Optional[str] = None
    note: Optional[str] = None


# ----------------------------------------------------------------------
# 批次
# ----------------------------------------------------------------------

@router.get("/batches")
async def list_batches(
    stock_code: Optional[str] = None,
    period: Optional[str] = None,
    kind: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
):
    """业务模块说明。"""
    rows = lineage_service.list_batches(
        stock_code=stock_code, period=period, kind=kind,
        status=status, limit=limit,
    )
    return {"count": len(rows), "batches": rows}


# ----------------------------------------------------------------------
# 血缘与快照
# ----------------------------------------------------------------------

@router.get("/{code}/{period}/bars/provenance")
async def bar_provenance(
    code: str,
    period: str,
    timestamp: str = Query(..., description="K线时间戳"),
):
    """查询单根 K 线来自哪个批次、经过哪些版本。"""
    ts = _parse_ts(timestamp, "timestamp")
    result = lineage_service.bar_provenance(code, period, ts)
    if result is None:
        return {"found": False}
    return {"found": True, **result}


@router.get("/{code}/{period}/snapshot")
async def get_snapshot(
    code: str,
    period: str,
    as_of_batch_id: Optional[int] = None,
    as_of_time: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """按批次或时刻复现历史快照，不影响正在使用的当前数据。"""
    versions = lineage_service.snapshot(
        code, period,
        as_of_batch_id=as_of_batch_id,
        as_of_time=_parse_ts(as_of_time, "as_of_time"),
        start=_parse_ts(start_date, "start_date"),
        end=_parse_ts(end_date, "end_date"),
    )
    return {"count": len(versions), "bars": versions}


# ----------------------------------------------------------------------
# 缺口
# ----------------------------------------------------------------------

@router.get("/{code}/{period}/gaps")
async def list_gaps(
    code: str,
    period: str,
    status: Optional[str] = None,
):
    """业务模块说明。"""
    gaps = lineage_service.list_gaps(code, period, status)
    return {"count": len(gaps), "gaps": gaps}


@router.post("/{code}/{period}/gaps/scan")
async def scan_gaps(
    code: str,
    period: str,
    start_date: str = Query(...),
    end_date: str = Query(...),
):
    """显式按交易时段网格扫描并登记缺口。"""
    gaps = lineage_service.scan_gaps(
        code, period,
        _parse_ts(start_date, "start_date"),
        _parse_end_ts(end_date, "end_date"),
    )
    return {"count": len(gaps), "gaps": gaps}


@router.post("/gaps/{gap_id}/ignore")
async def ignore_gap(
    gap_id: int,
    reason: Optional[str] = Query(default=None),
):
    """将缺口标记为忽略（如法定休市造成的误报）。"""
    return lineage_service.ignore_gap(gap_id, reason)


# ----------------------------------------------------------------------
# 补数批次
# ----------------------------------------------------------------------

@router.post("/{code}/backfills")
async def create_backfill(code: str, request: BackfillRequest):
    """按策略对登记缺口发起补数。"""
    result = lineage_service.backfill_gaps(
        stock_code=code,
        period=request.period,
        gap_ids=request.gap_ids,
        sources=request.sources,
        strategy=request.strategy,
        recompute=request.recompute,
        requested_by=request.requested_by,
    )
    return result


@router.post("/backfills/{backfill_id}/resume")
async def resume_backfill(backfill_id: int):
    """补数中断后从断点续跑。"""
    return lineage_service.resume_backfill(backfill_id)


@router.get("/{code}/backfills")
async def list_backfills(code: str, status: Optional[str] = None):
    """业务模块说明。"""
    rows = lineage_service.list_backfills(stock_code=code, status=status)
    return {"count": len(rows), "backfills": rows}


# ----------------------------------------------------------------------
# 来源冲突
# ----------------------------------------------------------------------

@router.get("/conflicts")
async def list_conflicts(
    stock_code: Optional[str] = None,
    period: Optional[str] = None,
    status: str = "pending",
):
    """业务模块说明。"""
    rows = lineage_service.list_conflicts(stock_code, period, status)
    return {"count": len(rows), "conflicts": rows}


@router.post("/conflicts/{conflict_id}/resolve")
async def resolve_conflict(conflict_id: int, request: ConflictResolveRequest):
    """显式裁决来源冲突：保留现值或采纳来值（产生新版本）。"""
    return lineage_service.resolve_conflict(
        conflict_id, request.resolution, request.resolved_by, request.note
    )


# ----------------------------------------------------------------------
# 手工导入 / 受影响面
# ----------------------------------------------------------------------

@router.post("/{code}/{period}/ingest")
async def manual_ingest(code: str, period: str, request: IngestRequest):
    """手工导入/修正数据，同样走批次、版本与冲突通道。"""
    candles = [
        RawCandle(
            timestamp=_parse_ts(c.timestamp, "timestamp"),
            open=c.open, high=c.high, low=c.low, close=c.close,
            volume=c.volume,
        )
        for c in request.candles
    ]
    return lineage_service.ingest_candles(
        stock_code=code,
        period=period,
        candles=candles,
        source=request.source,
        kind=request.kind,
        trigger="manual",
        on_conflict=request.on_conflict,
        created_by=request.created_by,
    )


@router.get("/{code}/{period}/affected")
async def affected_artifacts(
    code: str,
    period: str,
    start_date: str = Query(...),
    end_date: str = Query(...),
    new_batch_id: Optional[int] = None,
):
    """查询某数据区间的补数影响了哪些分析与回测。"""
    return lineage_service.affected_artifacts(
        code, period,
        _parse_ts(start_date, "start_date"),
        _parse_end_ts(end_date, "end_date"),
        min_batch_id=new_batch_id,
    )
