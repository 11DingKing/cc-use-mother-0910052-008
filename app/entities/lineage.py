"""行情数据血缘、缺口登记与补数批次的实体定义。

设计要点：
- ImportBatch 记录每一次导入（定时/补数/手工），批次先于数据以 running 状态落库，
  进程崩溃后可被识别为 interrupted，重跑产生新批次，旧批次留痕。
- CandleDataVersion 是 K 线的行级版本链：同一 (股票, 周期, 时间戳) 的每次修订
  追加一行，旧行置为 superseded 但永不删除，快照与已发布报告据此回溯。
- DataVersion 是单条时间序列整体的递增版本号，分析/回测记录自己基于哪个版本。
- DataSnapshot / SnapshotItem 固化某一时刻每根 K 线指向的行版本，补数后依然可读。
- DataGap 登记检测到或手工登记的缺口，随补数进度在 open/partial/filled 间流转。
- SourceConflict 记录不同来源（或同来源迟到修订）数值不一致时的仲裁过程。
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Text,
    Index,
    UniqueConstraint,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def utcnow() -> datetime:
    return datetime.utcnow()


class ImportBatch(Base):
    """一次行情导入（定时抓取、缺口补数、手工录入、冲突仲裁）。"""

    __tablename__ = "import_batches"

    # 对外暴露的批次ID（不暴露自增主键的可猜测性）
    id = Column(String(32), primary_key=True)
    # 全局递增批次号，便于排序与引用
    batch_no = Column(Integer, nullable=False, unique=True)
    # scheduled=定时抓取 backfill=缺口补数 manual=手工录入 conflict_resolution=冲突仲裁
    batch_type = Column(String(24), nullable=False)
    # 数据来源：akshare / yahoo / manual 等
    source = Column(String(32), nullable=False)
    # 补数批次对应触发它的缺口；其余批次为空
    trigger_gap_id = Column(Integer, nullable=True)
    stock_code = Column(String(20), nullable=True)
    period = Column(String(10), nullable=True)

    # running=进行中 completed=已完成 interrupted=中断(进程崩溃/异常) failed=失败
    status = Column(String(16), nullable=False, default="running")
    conflict_policy = Column(String(32), nullable=True)

    total_received = Column(Integer, default=0)   # 收到的K线总数
    inserted_count = Column(Integer, default=0)   # 新增根数
    updated_count = Column(Integer, default=0)    # 修订根数（产生了新版本）
    duplicate_count = Column(Integer, default=0)  # 完全重复、未改动
    conflict_count = Column(Integer, default=0)   # 数值冲突根数
    out_of_order_count = Column(Integer, default=0)  # 时间乱序/迟到到达根数
    gap_detected_count = Column(Integer, default=0)  # 本次新发现缺口
    gap_filled_count = Column(Integer, default=0)    # 本次补齐缺口

    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=utcnow)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_batches_gap", "trigger_gap_id"),
        Index("ix_batches_code_period", "stock_code", "period"),
        Index("ix_batches_status", "status"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "batch_no": self.batch_no,
            "batch_type": self.batch_type,
            "source": self.source,
            "trigger_gap_id": self.trigger_gap_id,
            "stock_code": self.stock_code,
            "period": self.period,
            "status": self.status,
            "conflict_policy": self.conflict_policy,
            "total_received": self.total_received,
            "inserted_count": self.inserted_count,
            "updated_count": self.updated_count,
            "duplicate_count": self.duplicate_count,
            "conflict_count": self.conflict_count,
            "out_of_order_count": self.out_of_order_count,
            "gap_detected_count": self.gap_detected_count,
            "gap_filled_count": self.gap_filled_count,
            "error_message": self.error_message,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class CandleDataVersion(Base):
    """单根K线的行级版本：每次修订追加一行，旧版本保留不删。"""

    __tablename__ = "candle_data_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 逻辑外键 -> stock_candles.id（同 (code,period,timestamp) 的行）
    candle_id = Column(Integer, nullable=False)
    # 该行内版本号，从1递增
    version_no = Column(Integer, nullable=False)

    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    timestamp = Column(DateTime, nullable=False)

    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False, default=0.0)
    amount = Column(Float, nullable=True)

    # 该版本由哪个批次、哪个来源写入
    import_batch_id = Column(String(32), nullable=True)
    source = Column(String(32), nullable=True)
    # 在导入批次中的到达序号（用于识别迟到/乱序）
    ingest_order = Column(Integer, default=0)

    # active=当前最新版本 superseded=已被更新版本取代
    status = Column(String(16), nullable=False, default="active")
    superseded_by_id = Column(Integer, nullable=True)
    # normal=正常写入 backfill=补数写入 conflict_win=冲突仲裁胜出
    change_reason = Column(String(24), default="normal")
    created_at = Column(DateTime, default=utcnow)

    __table_args__ = (
        UniqueConstraint("candle_id", "version_no", name="uix_candle_version"),
        Index("ix_cv_code_period_ts", "stock_code", "period", "timestamp"),
        Index("ix_cv_batch", "import_batch_id"),
        Index("ix_cv_status", "status"),
    )

    def value_tuple(self):
        return (
            round(float(self.open), 9),
            round(float(self.high), 9),
            round(float(self.low), 9),
            round(float(self.close), 9),
            round(float(self.volume or 0.0), 9),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "candle_id": self.candle_id,
            "version_no": self.version_no,
            "stock_code": self.stock_code,
            "period": self.period,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "amount": self.amount,
            "import_batch_id": self.import_batch_id,
            "source": self.source,
            "ingest_order": self.ingest_order,
            "status": self.status,
            "superseded_by_id": self.superseded_by_id,
            "change_reason": self.change_reason,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class DataVersion(Base):
    """单条时间序列 (股票,周期) 的整体数据版本，每次实质变更递增。"""

    __tablename__ = "data_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    version_no = Column(Integer, nullable=False)
    # creating=批次写入中 active=当前版本 superseded=已被后续版本取代
    status = Column(String(16), nullable=False, default="active")
    superseded_by_id = Column(Integer, nullable=True)

    import_batch_id = Column(String(32), nullable=True)
    candle_count = Column(Integer, default=0)
    first_time = Column(DateTime, nullable=True)
    last_time = Column(DateTime, nullable=True)
    change_summary = Column(Text, nullable=True)  # JSON: 插入/修订/缺口统计
    created_at = Column(DateTime, default=utcnow)

    __table_args__ = (
        UniqueConstraint("stock_code", "period", "version_no", name="uix_series_version"),
        Index("ix_dv_series_status", "stock_code", "period", "status"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "version_no": self.version_no,
            "status": self.status,
            "import_batch_id": self.import_batch_id,
            "candle_count": self.candle_count,
            "first_time": self.first_time.isoformat() if self.first_time else None,
            "last_time": self.last_time.isoformat() if self.last_time else None,
            "change_summary": self.change_summary,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class DataSnapshot(Base):
    """某时刻某条序列的只读快照（供已发布报告与历史回测固定使用）。"""

    __tablename__ = "data_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False, unique=True)
    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    # 快照创建时序列的整体版本号（仅作标记，精确回溯以 item 行为准）
    data_version_no = Column(Integer, nullable=True)
    created_batch_id = Column(String(32), nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utcnow)

    __table_args__ = (
        Index("ix_snapshot_series", "stock_code", "period"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "stock_code": self.stock_code,
            "period": self.period,
            "data_version_no": self.data_version_no,
            "created_batch_id": self.created_batch_id,
            "note": self.note,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class SnapshotItem(Base):
    """快照中每根K线绑定的具体行版本，补数改写当前值后仍可精确复现。"""

    __tablename__ = "data_snapshot_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_id = Column(Integer, ForeignKey("data_snapshots.id"), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    candle_version_id = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("snapshot_id", "timestamp", name="uix_snapshot_ts"),
        Index("ix_snapshot_item_version", "candle_version_id"),
    )


class DataGap(Base):
    """行情缺口登记。

    gap_start/gap_end 是缺口两侧已存在K线的时间戳，缺失的是二者之间按周期
    应当出现的 expected_count 根K线。
    """

    __tablename__ = "data_gaps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    # 同序列内缺口序号
    gap_no = Column(Integer, nullable=False)

    gap_start = Column(DateTime, nullable=False)
    gap_end = Column(DateTime, nullable=False)
    expected_count = Column(Integer, default=0)
    filled_count = Column(Integer, default=0)

    # open=完全缺失 partial=部分补齐 filled=已补齐 verified=已核验 ignored=手工忽略
    status = Column(String(16), nullable=False, default="open")
    # detected=导入时检测 manual=人工登记
    origin = Column(String(16), default="detected")

    first_seen_batch_id = Column(String(32), nullable=True)
    filled_batch_id = Column(String(32), nullable=True)
    resolution_detail = Column(Text, nullable=True)

    detected_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)
    filled_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "stock_code", "period", "gap_start", "gap_end", name="uix_gap_range"
        ),
        Index("ix_gap_series_status", "stock_code", "period", "status"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "gap_no": self.gap_no,
            "stock_code": self.stock_code,
            "period": self.period,
            "gap_start": self.gap_start.isoformat() if self.gap_start else None,
            "gap_end": self.gap_end.isoformat() if self.gap_end else None,
            "expected_count": self.expected_count,
            "filled_count": self.filled_count,
            "status": self.status,
            "origin": self.origin,
            "first_seen_batch_id": self.first_seen_batch_id,
            "filled_batch_id": self.filled_batch_id,
            "resolution_detail": self.resolution_detail,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
        }


class SourceConflict(Base):
    """同一根K线不同来源数值不一致，或迟到旧数据与当前版本不一致的仲裁记录。"""

    __tablename__ = "source_conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False)
    period = Column(String(10), nullable=False)
    timestamp = Column(DateTime, nullable=False)

    import_batch_id = Column(String(32), nullable=True)
    incoming_source = Column(String(32), nullable=False)
    existing_source = Column(String(32), nullable=True)

    incoming_values = Column(Text, nullable=False)  # JSON OHLCV
    existing_values = Column(Text, nullable=True)  # JSON OHLCV

    # pending=待处理 accepted=采用了新数据 rejected=保留了旧数据
    status = Column(String(16), nullable=False, default="pending")
    # 使用的仲裁策略
    resolution_policy = Column(String(32), nullable=True)
    winning_candle_version_id = Column(Integer, nullable=True)
    resolved_batch_id = Column(String(32), nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    resolved_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_conflict_series", "stock_code", "period", "timestamp"),
        Index("ix_conflict_status", "status"),
    )

    def to_dict(self) -> dict:
        import json

        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "import_batch_id": self.import_batch_id,
            "incoming_source": self.incoming_source,
            "existing_source": self.existing_source,
            "incoming_values": json.loads(self.incoming_values) if self.incoming_values else None,
            "existing_values": json.loads(self.existing_values) if self.existing_values else None,
            "status": self.status,
            "resolution_policy": self.resolution_policy,
            "winning_candle_version_id": self.winning_candle_version_id,
            "resolved_batch_id": self.resolved_batch_id,
            "note": self.note,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
        }
