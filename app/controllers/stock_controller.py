"""业务模块说明。"""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Query

from app.services.stock_service import StockService

router = APIRouter(prefix="/api/stocks", tags=["stocks"])
stock_service = StockService()


@router.get("/{code}/candles")
async def get_candles(
    code: str,
    period: str = Query(default="daily", description="K线周期: daily, 60min, 30min"),
    start_date: Optional[str] = Query(default=None, description="开始日期 YYYY-MM-DD"),
    end_date: Optional[str] = Query(default=None, description="结束日期 YYYY-MM-DD"),
):
    """业务模块说明。"""
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    
    candles = stock_service.get_candles(code, period, start, end)
    
    return {
        "stock_code": code,
        "period": period,
        "count": len(candles),
        "candles": [
            {
                "timestamp": c.timestamp.isoformat(),
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in candles
        ],
    }


@router.post("/{code}/fetch")
async def fetch_candles(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
    start_date: Optional[str] = Query(default=None, description="开始日期"),
    end_date: Optional[str] = Query(default=None, description="结束日期"),
):
    """业务模块说明。"""
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

    result = stock_service.fetch_and_update(code, period, start, end)
    return result


@router.get("/{code}/candles/lineage")
async def get_candles_with_lineage(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
):
    """读取K线并返回每根数据的来源批次、序列版本与区间内缺口。"""
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    data = stock_service.get_candles_with_lineage(code, period, start, end)
    # RawCandle 不能直接 JSON 序列化，转为字典
    data["candles"] = [
        {
            "timestamp": c.timestamp.isoformat(),
            "open": c.open, "high": c.high, "low": c.low,
            "close": c.close, "volume": c.volume,
        }
        for c in data["candles"]
    ]
    return data
