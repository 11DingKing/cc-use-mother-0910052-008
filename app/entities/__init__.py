"""业务模块说明。"""

from app.entities.stock import StockCandle
from app.entities.analysis_result import AnalysisResult
from app.entities.watchlist import WatchlistItem
from app.entities.backtest import BacktestResult
from app.entities.lineage import (
    IngestionBatch,
    DataGap,
    BackfillBatch,
    SourceConflict,
    CandleVersion,
)

__all__ = [
    "StockCandle",
    "AnalysisResult",
    "WatchlistItem",
    "BacktestResult",
    "IngestionBatch",
    "DataGap",
    "BackfillBatch",
    "SourceConflict",
    "CandleVersion",
]
