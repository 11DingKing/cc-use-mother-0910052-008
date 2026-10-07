"""业务模块说明。"""

from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Query

from app.services.analysis_service import AnalysisService

router = APIRouter(prefix="/api/analysis", tags=["analysis"])
analysis_service = AnalysisService()


@router.get("/{code}")
async def get_analysis(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
    version: Optional[int] = Query(default=None, description="分析版本号，缺省取当前版本"),
):
    """业务模块说明。"""
    return analysis_service.get_result(code, period, version=version)


@router.get("/{code}/versions")
async def list_analysis_versions(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
    limit: int = Query(default=20, le=100),
):
    """列出分析版本与陈旧状态（已发布版本不会被删除或覆盖）。"""
    return {
        "versions": analysis_service.list_versions(code, period, limit=limit),
    }


@router.post("/{code}/publish")
async def publish_analysis(
    code: str,
    period: str = Query(default="daily"),
    version: int = Query(..., description="要发布为报告的版本号"),
):
    """发布指定分析版本；发布后冻结，补数重算只会产生新版本。"""
    return analysis_service.publish_version(code, period, version)


@router.post("/{code}/run")
async def run_analysis(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
    start_date: Optional[str] = Query(default=None, description="开始日期"),
    end_date: Optional[str] = Query(default=None, description="结束日期"),
):
    """业务模块说明。"""
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    
    return analysis_service.run_analysis(code, period, start, end)


@router.get("/{code}/signals")
async def get_signals(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
):
    """业务模块说明。"""
    return {
        "stock_code": code,
        "period": period,
        "signals": analysis_service.get_signals(code, period),
    }


@router.get("/{code}/full")
async def get_full_analysis(
    code: str,
    period: str = Query(default="daily", description="K线周期"),
):
    """业务模块说明。"""
    return analysis_service.get_full_result(code, period)


@router.post("/{code}/multi-level")
async def run_multi_level_analysis(
    code: str,
    primary_period: str = Query(default="daily", description="主周期（定方向）"),
    secondary_periods: Optional[str] = Query(
        default="60min,30min",
        description="辅助周期（找买点），逗号分隔",
    ),
    start_date: Optional[str] = Query(default=None, description="开始日期"),
    end_date: Optional[str] = Query(default=None, description="结束日期"),
):
    """业务模块说明。"""
    start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
    
    sec_periods: List[str] = []
    if secondary_periods:
        sec_periods = [p.strip() for p in secondary_periods.split(",") if p.strip()]
    
    return analysis_service.run_multi_level_analysis(
        code, primary_period, sec_periods or None, start, end
    )


@router.get("/{code}/multi-level/recommendation")
async def get_multi_level_recommendation(
    code: str,
    primary_period: str = Query(default="daily", description="主周期"),
    secondary_periods: Optional[str] = Query(
        default="60min,30min",
        description="辅助周期，逗号分隔",
    ),
):
    """业务模块说明。"""
    sec_periods: List[str] = []
    if secondary_periods:
        sec_periods = [p.strip() for p in secondary_periods.split(",") if p.strip()]
    
    result = analysis_service.run_multi_level_analysis(
        code, primary_period, sec_periods or None
    )
    
    return {
        "stock_code": code,
        "primary_period": primary_period,
        "secondary_periods": sec_periods,
        "recommendation": result["summary"]["recommendation"],
        "strong_confirmations": result["summary"]["strong_confirmations"],
        "conflicts": result["summary"]["conflicts"],
    }
