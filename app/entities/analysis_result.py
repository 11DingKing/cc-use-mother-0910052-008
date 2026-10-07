"""业务模块说明。"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AnalysisResult(Base):
    """业务模块说明。"""
    __tablename__ = "analysis_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)  # daily, 60min, 30min
    
    # 分析时间范围
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
    
    # 分析结果（JSON序列化存储）
    fractals_json = Column(Text, nullable=True)    # 分型列表
    bis_json = Column(Text, nullable=True)         # 笔列表
    duans_json = Column(Text, nullable=True)       # 段列表
    zhongshus_json = Column(Text, nullable=True)   # 中枢列表
    signals_json = Column(Text, nullable=True)     # 买卖点列表
    
    # 统计信息
    fractal_count = Column(Integer, default=0)
    bi_count = Column(Integer, default=0)
    duan_count = Column(Integer, default=0)
    zhongshu_count = Column(Integer, default=0)
    signal_count = Column(Integer, default=0)
    
    # 最新信号摘要（便于快速查询）
    latest_signal_type = Column(String(20), nullable=True)
    latest_signal_time = Column(DateTime, nullable=True)
    latest_signal_price = Column(Float, nullable=True)
    
    # 元数据
    analysis_version = Column(String(10), default="1.0")

    # ---- 数据血缘与结果版本化 ----
    # 结果版本号：同一 (股票,周期) 每次分析追加新版本，旧行保留不改写
    result_version = Column(Integer, default=1)
    # 1=当前最新结果 0=已被新版本取代
    is_current = Column(Integer, default=1)
    parent_result_id = Column(Integer, nullable=True)
    superseded_by_id = Column(Integer, nullable=True)
    superseded_at = Column(DateTime, nullable=True)
    # draft=草稿 published=已发布（不可变，固定数据快照）
    status = Column(String(16), default="draft")
    published_at = Column(DateTime, nullable=True)
    # 分析基于的数据序列版本与批次
    data_version_no = Column(Integer, nullable=True)
    based_on_batch_id = Column(String(32), nullable=True)
    # 分析区间相交的缺口ID（JSON 列表），标记哪些结论曾受缺口影响
    affected_gap_ids_json = Column(Text, nullable=True)
    # 发布时固化的数据快照名，历史报告据此复现
    snapshot_name = Column(String(128), nullable=True)
    recompute_policy = Column(String(32), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_analysis_stock_period', 'stock_code', 'period'),
        Index('ix_analysis_latest_signal', 'stock_code', 'latest_signal_type'),
        UniqueConstraint(
            'stock_code', 'period', 'result_version',
            name='uix_analysis_result_version',
        ),
        Index('ix_analysis_current', 'stock_code', 'period', 'is_current'),
        Index('ix_analysis_status', 'status'),
    )
    
    def __repr__(self):
        return (
            f"<AnalysisResult(code={self.stock_code}, period={self.period}, "
            f"signals={self.signal_count})>"
        )
    
    def to_dict(self) -> dict:
        """业务模块说明。"""
        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "fractal_count": self.fractal_count,
            "bi_count": self.bi_count,
            "duan_count": self.duan_count,
            "zhongshu_count": self.zhongshu_count,
            "signal_count": self.signal_count,
            "latest_signal_type": self.latest_signal_type,
            "latest_signal_time": self.latest_signal_time.isoformat() if self.latest_signal_time else None,
            "latest_signal_price": self.latest_signal_price,
            "analysis_version": self.analysis_version,
            "result_version": self.result_version,
            "is_current": bool(self.is_current),
            "status": self.status,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "data_version_no": self.data_version_no,
            "based_on_batch_id": self.based_on_batch_id,
            "affected_gap_ids": self.affected_gap_ids_json,
            "snapshot_name": self.snapshot_name,
            "recompute_policy": self.recompute_policy,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
