"""业务模块说明。"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BacktestResult(Base):
    """业务模块说明。"""
    __tablename__ = "backtest_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 回测配置
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False)  # daily, 60min, 30min
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    initial_capital = Column(Float, nullable=False, default=100000.0)
    
    # 策略参数（JSON序列化）
    strategy_params_json = Column(Text, nullable=True)
    
    # 回测结果指标
    final_capital = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)       # 总收益率
    annual_return = Column(Float, nullable=True)      # 年化收益率
    max_drawdown = Column(Float, nullable=True)       # 最大回撤
    win_rate = Column(Float, nullable=True)           # 胜率
    profit_loss_ratio = Column(Float, nullable=True)  # 盈亏比
    sharpe_ratio = Column(Float, nullable=True)       # 夏普比率
    
    # 交易统计
    total_trades = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    losing_trades = Column(Integer, default=0)
    
    # 交易记录（JSON序列化）
    trades_json = Column(Text, nullable=True)
    
    # 收益曲线（JSON序列化）
    equity_curve_json = Column(Text, nullable=True)
    
    # 元数据
    status = Column(String(20), default="pending")  # pending, running, completed, failed
    error_message = Column(Text, nullable=True)

    # ---- 版本化与血缘：同一配置重复回测追加版本，旧版本（含已发布）保留 ----
    result_version = Column(Integer, default=1)
    is_current = Column(Integer, default=1)
    parent_result_id = Column(Integer, nullable=True)
    # 回测消费的数据版本/批次/快照
    data_version_no = Column(Integer, nullable=True)
    based_on_batch_id = Column(String(32), nullable=True)
    snapshot_name = Column(String(128), nullable=True)
    affected_gap_ids_json = Column(Text, nullable=True)
    # 该版本是否已发布（不可变）
    report_status = Column(String(16), default="draft")
    published_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index('ix_backtest_stock_period', 'stock_code', 'period'),
        Index('ix_backtest_status', 'status'),
        # 版本号在“同一配置族”内递增（配置族由服务层按参数判定，用 parent 链串联）
        Index('ix_backtest_current', 'stock_code', 'period', 'is_current'),
        Index('ix_backtest_report_status', 'report_status'),
    )
    
    def __repr__(self):
        return (
            f"<BacktestResult(code={self.stock_code}, period={self.period}, "
            f"return={self.total_return}, status={self.status})>"
        )
    
    def to_dict(self) -> dict:
        """业务模块说明。"""
        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
            "initial_capital": self.initial_capital,
            "final_capital": self.final_capital,
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "profit_loss_ratio": self.profit_loss_ratio,
            "sharpe_ratio": self.sharpe_ratio,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "status": self.status,
            "error_message": self.error_message,
            "result_version": self.result_version,
            "is_current": bool(self.is_current),
            "data_version_no": self.data_version_no,
            "based_on_batch_id": self.based_on_batch_id,
            "snapshot_name": self.snapshot_name,
            "report_status": self.report_status,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }
