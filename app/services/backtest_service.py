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
from app.services.lineage_service import DataLineageService
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import NotFoundException, AnalysisException

logger = logging.getLogger(__name__)


class BacktestService:
    """业务模块说明。"""
    
    def __init__(self):
        self.stock_service = StockService()
        self.analysis_service = AnalysisService()
        self.lineage = DataLineageService()
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
        snapshot_name: Optional[str] = None,
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
            # 获取K线数据：可指定不可变快照（复现历史报告），否则用当前数据
            if snapshot_name:
                candles = self.stock_service.get_candles_from_snapshot(snapshot_name)
                if start_date:
                    candles = [c for c in candles if c.timestamp >= start_date]
                if end_date:
                    candles = [c for c in candles if c.timestamp <= end_date]
                data_info = None
                affected_gap_ids: list = []
            else:
                candles = self.stock_service.get_candles(
                    stock_code, period, start_date, end_date
                )
                data_info = self.lineage.get_current_data_version_info(stock_code, period)
                affected_gap_ids = [
                    g["id"]
                    for g in self.lineage.get_gaps_affecting_range(
                        stock_code, period, start_date, end_date
                    )
                ]

            if not candles:
                raise AnalysisException(
                    message="No candle data available for backtest",
                    stock_code=stock_code,
                    period=period,
                )

            # 信号直接由本次回测所用K线计算（快照回测也走同一路径），
            # 避免用默认时间范围重新取数与回测区间脱节
            signals = self.analysis_service.compute_signals(
                candles, stock_code, period
            )["signals"]

            # 执行回测
            result = self.engine.run(config, candles, signals)

            # 保存结果（追加新版本，携带血缘）
            result_id = self._save_result(
                result,
                data_version_no=data_info.get("version_no") if data_info else None,
                based_on_batch_id=data_info.get("import_batch_id") if data_info else None,
                snapshot_name=snapshot_name,
                affected_gap_ids=affected_gap_ids,
            )

            # 生成报告
            report = self.report_generator.generate(result)
            report["id"] = result_id

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
    
    def _save_result(
        self,
        result: BacktestResult,
        data_version_no: Optional[int] = None,
        based_on_batch_id: Optional[str] = None,
        snapshot_name: Optional[str] = None,
        affected_gap_ids: Optional[list] = None,
    ) -> int:
        """追加回测结果版本；历史版本（含已发布）不被覆盖。"""
        try:
            with db_session_scope() as session:
                previous = (
                    session.query(BacktestResultEntity)
                    .filter(
                        BacktestResultEntity.stock_code == result.config.stock_code,
                        BacktestResultEntity.period == result.config.period,
                        BacktestResultEntity.is_current == 1,
                    )
                    .order_by(BacktestResultEntity.result_version.desc())
                    .first()
                )
                next_version = (previous.result_version + 1) if previous else 1

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
                    result_version=next_version,
                    is_current=1,
                    parent_result_id=previous.id if previous else None,
                    data_version_no=data_version_no,
                    based_on_batch_id=based_on_batch_id,
                    snapshot_name=snapshot_name,
                    affected_gap_ids_json=json.dumps(affected_gap_ids or []),
                    report_status="draft",
                )
                if previous is not None:
                    previous.is_current = 0
                session.add(entity)
                session.flush()
                return entity.id
        except Exception as e:
            logger.warning(f"Save backtest result error: {e}")
            return 0
    
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
                    "result_version": r.result_version,
                    "is_current": bool(r.is_current),
                    "report_status": r.report_status,
                    "data_version_no": r.data_version_no,
                    "snapshot_name": r.snapshot_name,
                    "total_return": r.total_return,
                    "win_rate": r.win_rate,
                    "status": r.status,
                    "created_at": r.created_at.isoformat(),
                }
                for r in results
            ]

    def publish_report(self, result_id: int) -> Dict[str, Any]:
        """发布回测报告：未绑定快照的当前结果先固化快照，再锁定为不可变。"""
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
            if entity.report_status != "published":
                snapshot_name = entity.snapshot_name
                if not snapshot_name:
                    from app.mappers.lineage_mapper import LineageMapper
                    lm = LineageMapper(session)
                    snapshot_name = f"rpt-backtest-{entity.id}-v{entity.result_version}"
                    if entity.based_on_batch_id:
                        lm.create_snapshot_as_of_batch(
                            entity.stock_code, entity.period, snapshot_name,
                            as_of_batch_id=entity.based_on_batch_id,
                            data_version_no=entity.data_version_no,
                            note=f"published backtest result {entity.id}",
                        )
                    else:
                        lm.create_snapshot(
                            entity.stock_code, entity.period, snapshot_name,
                            note=f"published backtest result {entity.id}",
                        )
                    entity.snapshot_name = snapshot_name
                entity.report_status = "published"
                entity.published_at = datetime.utcnow()
            return {
                "id": entity.id,
                "result_version": entity.result_version,
                "report_status": entity.report_status,
                "snapshot_name": entity.snapshot_name,
                "published_at": entity.published_at.isoformat() if entity.published_at else None,
            }

    def find_results_affected_by_gap(self, gap_id: int) -> list:
        """受指定缺口影响的回测版本（含已发布）。"""
        with db_session_scope() as session:
            rows = session.query(BacktestResultEntity).all()
            matched = []
            for row in rows:
                ids = json.loads(row.affected_gap_ids_json) if row.affected_gap_ids_json else []
                if gap_id in ids:
                    matched.append({
                        "id": row.id,
                        "stock_code": row.stock_code,
                        "period": row.period,
                        "result_version": row.result_version,
                        "report_status": row.report_status,
                        "data_version_no": row.data_version_no,
                        "snapshot_name": row.snapshot_name,
                    })
            return matched
