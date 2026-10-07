"""业务模块说明。"""

import json
from datetime import datetime
from typing import Optional, Dict, Any
import logging

from app.config import db_session_scope
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.backtest.engine import BacktestEngine, BacktestConfig, BacktestResult
from app.backtest.report import BacktestReportGenerator
from app.services.stock_service import StockService
from app.services.analysis_service import AnalysisService
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import NotFoundException, AnalysisException

logger = logging.getLogger(__name__)


class BacktestService:
    """业务模块说明。"""
    
    def __init__(self):
        self.stock_service = StockService()
        self.analysis_service = AnalysisService()
        self.engine = BacktestEngine()
        self.report_generator = BacktestReportGenerator()
    
    def run_backtest(
        self,
        stock_code: str,
        period: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        initial_capital: float = 100000.0,
        position_size: float = 1.0,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        
        # 创建回测配置
        config = BacktestConfig(
            stock_code=stock_code,
            period=period,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            position_size=position_size,
        )
        
        try:
            # 获取K线数据
            candles = self.stock_service.get_candles(
                stock_code, period, start_date, end_date
            )

            if not candles:
                raise AnalysisException(
                    message="No candle data available for backtest",
                    stock_code=stock_code,
                    period=period,
                )

            # 数据血缘
            lineage = self._collect_lineage(
                stock_code, period, start_date, end_date, candles
            )

            # 执行分析获取信号（使用完整数据范围以获得更多信号）
            analysis_summary = self.analysis_service.run_analysis(stock_code, period, None, None)

            # 获取信号
            with db_session_scope() as session:
                from app.mappers.analysis_mapper import AnalysisMapper
                mapper = AnalysisMapper(session)
                analysis_result = mapper.get_latest(stock_code, period)

                if not analysis_result:
                    raise AnalysisException(
                        message="No analysis result available",
                        stock_code=stock_code,
                        period=period,
                    )

                signals = mapper.load_signals(analysis_result)
                analysis_version_id = analysis_result.id

            # 执行回测
            result = self.engine.run(config, candles, signals)

            # 保存结果
            result_id = self._save_result(
                result,
                data_batch_ids=lineage["batch_ids"],
                data_gaps=lineage["gaps"],
                analysis_version_id=analysis_version_id,
            )

            # 生成报告
            report = self.report_generator.generate(result)
            report["id"] = result_id
            report["data_lineage"] = lineage
            report["analysis_version_id"] = analysis_version_id
            report["analysis_run"] = {
                "version": analysis_summary.get("version") if analysis_summary else None,
            }

            return report
            
        except AnalysisException:
            raise
        except Exception as e:
            logger.error(f"Backtest error: {e}", exc_info=True)
            raise AnalysisException(
                message=f"Backtest failed: {str(e)}",
                stock_code=stock_code,
                period=period,
            )
    
    def _collect_lineage(self, stock_code, period, start_date, end_date, candles):
        """汇总本次回测数据窗口的批次血缘与缺口状态。"""
        batch_ids = sorted({
            getattr(c, "source_batch_id", None)
            for c in candles
            if getattr(c, "source_batch_id", None) is not None
        })
        try:
            from app.services.lineage_service import LineageService
            window = LineageService().data_lineage_for_window(
                stock_code, period, start_date, end_date
            )
            window["batch_ids"] = sorted(set(batch_ids) | set(window["batch_ids"]))
            window["latest_batch_id"] = (
                max(window["batch_ids"]) if window["batch_ids"] else None
            )
            return window
        except Exception as e:
            logger.warning(f"Lineage lookup error: {e}")
            return {
                "batch_ids": batch_ids,
                "latest_batch_id": max(batch_ids) if batch_ids else None,
                "gaps": [],
            }

    def _save_result(
        self,
        result: BacktestResult,
        data_batch_ids=None,
        data_gaps=None,
        analysis_version_id=None,
        superseded_by_id=None,
    ) -> int:
        """业务模块说明。"""
        try:
            with db_session_scope() as session:
                entity = BacktestResultEntity(
                    stock_code=result.config.stock_code,
                    period=result.config.period,
                    start_date=result.config.start_date,
                    end_date=result.config.end_date,
                    initial_capital=result.config.initial_capital,
                    final_capital=result.final_capital,
                    total_return=result.total_return,
                    annual_return=result.annual_return,
                    max_drawdown=result.max_drawdown,
                    win_rate=result.win_rate,
                    profit_loss_ratio=result.profit_loss_ratio,
                    sharpe_ratio=result.sharpe_ratio,
                    total_trades=result.total_trades,
                    winning_trades=result.winning_trades,
                    losing_trades=result.losing_trades,
                    trades_json=json.dumps([
                        {
                            "entry_time": t.entry_time.isoformat(),
                            "entry_price": t.entry_price,
                            "entry_signal": t.entry_signal.value if t.entry_signal else None,
                            "exit_time": t.exit_time.isoformat() if t.exit_time else None,
                            "exit_price": t.exit_price,
                            "exit_signal": t.exit_signal.value if t.exit_signal else None,
                            "shares": t.shares,
                            "profit": t.profit,
                            "profit_pct": t.profit_pct,
                        }
                        for t in result.trades
                    ]),
                    equity_curve_json=json.dumps(result.equity_curve),
                    status="completed",
                    completed_at=datetime.utcnow(),
                    data_batch_ids_json=(
                        json.dumps(data_batch_ids) if data_batch_ids is not None else None
                    ),
                    data_gaps_json=json.dumps(data_gaps) if data_gaps is not None else None,
                    analysis_version_id=analysis_version_id,
                    superseded_by_id=superseded_by_id,
                )
                session.add(entity)
                session.flush()
                return entity.id
        except Exception as e:
            logger.warning(f"Save backtest result error: {e}")
            return 0

    def rerun_backtest(self, result_id: int, reason: Optional[str] = None) -> Dict[str, Any]:
        """补数后按新版本数据重跑回测；旧记录标记被取代但不删除。"""
        with db_session_scope() as session:
            old = session.query(BacktestResultEntity).filter(
                BacktestResultEntity.id == result_id
            ).first()
            if not old:
                raise NotFoundException(
                    message="Backtest result not found",
                    resource_type="BacktestResult",
                    resource_id=str(result_id),
                )
            params = {
                "stock_code": old.stock_code,
                "period": old.period,
                "start_date": old.start_date,
                "end_date": old.end_date,
                "initial_capital": old.initial_capital,
            }

        report = self.run_backtest(**params)
        new_id = report.get("id")
        if new_id:
            with db_session_scope() as session:
                old = session.query(BacktestResultEntity).filter(
                    BacktestResultEntity.id == result_id
                ).first()
                old.superseded_by_id = new_id
                session.flush()
        report["rerun_reason"] = reason
        report["supersedes"] = result_id
        return report
    
    def get_result(self, result_id: int) -> Dict[str, Any]:
        """业务模块说明。"""
        with db_session_scope() as session:
            entity = session.query(BacktestResultEntity).filter(
                BacktestResultEntity.id == result_id
            ).first()
            
            if not entity:
                raise NotFoundException(
                    message="Backtest result not found",
                    resource_type="BacktestResult",
                    resource_id=str(result_id),
                )
            
            return {
                "id": entity.id,
                "summary": {
                    "stock_code": entity.stock_code,
                    "period": entity.period,
                    "start_date": entity.start_date.isoformat(),
                    "end_date": entity.end_date.isoformat(),
                    "initial_capital": entity.initial_capital,
                    "final_capital": entity.final_capital,
                },
                "performance": {
                    "total_return": entity.total_return,
                    "annual_return": entity.annual_return,
                    "max_drawdown": entity.max_drawdown,
                    "sharpe_ratio": entity.sharpe_ratio,
                    "win_rate": entity.win_rate,
                    "profit_loss_ratio": entity.profit_loss_ratio,
                    "total_trades": entity.total_trades,
                    "winning_trades": entity.winning_trades,
                    "losing_trades": entity.losing_trades,
                },
                "trades": json.loads(entity.trades_json) if entity.trades_json else [],
                "equity_curve": json.loads(entity.equity_curve_json) if entity.equity_curve_json else [],
                "status": entity.status,
                "data_lineage": {
                    "batch_ids": json.loads(entity.data_batch_ids_json)
                    if entity.data_batch_ids_json else [],
                    "gaps": json.loads(entity.data_gaps_json)
                    if entity.data_gaps_json else [],
                    "analysis_version_id": entity.analysis_version_id,
                },
                "superseded_by_id": entity.superseded_by_id,
                "created_at": entity.created_at.isoformat(),
                "completed_at": entity.completed_at.isoformat() if entity.completed_at else None,
            }
    
    def list_results(
        self,
        stock_code: Optional[str] = None,
        limit: int = 20,
    ) -> list:
        """业务模块说明。"""
        with db_session_scope() as session:
            query = session.query(BacktestResultEntity)
            
            if stock_code:
                stock_code = validate_stock_code(stock_code)
                query = query.filter(BacktestResultEntity.stock_code == stock_code)
            
            results = query.order_by(
                BacktestResultEntity.created_at.desc()
            ).limit(limit).all()
            
            return [
                {
                    "id": r.id,
                    "stock_code": r.stock_code,
                    "period": r.period,
                    "total_return": r.total_return,
                    "win_rate": r.win_rate,
                    "status": r.status,
                    "created_at": r.created_at.isoformat(),
                }
                for r in results
            ]
