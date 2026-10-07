"""行情缺口、补数批次与来源血缘的领域模型。

版本化思路：
- ingestion_batches 记录每次写入（常规导入 / 补数 / 手工修正），
  每一根 K 线都指向写入它的批次；
- data_gaps 登记缺口区间及其被哪个补数批次填平；
- source_conflicts 悬挂不同来源/批次对同一根 K 线的取值冲突，
  未裁决前旧值保持为当前值，不被静默覆盖；
- candle_versions 保留每根 K 线的历史取值，历史快照由此可复现。
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Text,
    Index,
    text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# 批次类型
BATCH_KIND_IMPORT = "import"
BATCH_KIND_BACKFILL = "backfill"
BATCH_KIND_MANUAL = "manual"

# 批次状态
BATCH_RUNNING = "running"
BATCH_COMPLETED = "completed"
BATCH_PARTIAL = "partial"
BATCH_INTERRUPTED = "interrupted"
BATCH_CANCELED = "canceled"
BATCH_PENDING = "pending"

# 缺口状态
GAP_OPEN = "open"
GAP_PARTIAL = "partial"
GAP_FILLED = "filled"
GAP_IGNORED = "ignored"

# 冲突状态 / 裁决
CONFLICT_PENDING = "pending"
CONFLICT_RESOLVED = "resolved"
RESOLUTION_KEEP_EXISTING = "keep_existing"
RESOLUTION_USE_INCOMING = "use_incoming"
RESOLUTION_MANUAL = "manual"

# K 线版本状态
BAR_CURRENT = "current"
BAR_SUPERSEDED = "superseded"
BAR_CANDIDATE = "candidate"

# 补数策略
STRATEGY_FILL = "fill"        # 按来源顺序，第一个填平缺口的来源胜出
STRATEGY_MERGE = "merge"      # 多来源合并，冲突挂起待裁决，来源顺序即优先级
STRATEGY_TRUSTED = "trusted"  # 仅信任指定来源，不向其他来源回退


class IngestionBatch(Base):
    """一次数据写入批次（导入 / 补数 / 手工修正）。"""

    __tablename__ = "ingestion_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_code = Column(String(24), nullable=False, unique=True, index=True)
    kind = Column(String(16), nullable=False)
    source = Column(String(32), nullable=True)
    trigger = Column(String(32), nullable=False, default="auto")

    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)
    range_start = Column(DateTime, nullable=True)
    range_end = Column(DateTime, nullable=True)

    # 写入计量
    total_received = Column(Integer, default=0)
    inserted_count = Column(Integer, default=0)
    duplicate_count = Column(Integer, default=0)     # 与现值一致的重复导入
    changed_count = Column(Integer, default=0)       # 被实际采纳的变更
    conflict_count = Column(Integer, default=0)      # 挂起冲突数
    late_arrival_count = Column(Integer, default=0)  # 时间乱序（补到旧区间）
    gap_detected_count = Column(Integer, default=0)

    status = Column(String(16), nullable=False, default=BATCH_RUNNING)
    error_message = Column(Text, nullable=True)
    extra_json = Column(Text, nullable=True)
    created_by = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_batch_key", "stock_code", "period"),
        Index("ix_batch_status", "status", "kind"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "batch_code": self.batch_code,
            "kind": self.kind,
            "source": self.source,
            "trigger": self.trigger,
            "stock_code": self.stock_code,
            "period": self.period,
            "range_start": self.range_start.isoformat() if self.range_start else None,
            "range_end": self.range_end.isoformat() if self.range_end else None,
            "total_received": self.total_received,
            "inserted_count": self.inserted_count,
            "duplicate_count": self.duplicate_count,
            "changed_count": self.changed_count,
            "conflict_count": self.conflict_count,
            "late_arrival_count": self.late_arrival_count,
            "gap_detected_count": self.gap_detected_count,
            "status": self.status,
            "error_message": self.error_message,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class DataGap(Base):
    """一根或连续一段缺失分钟线登记。"""

    __tablename__ = "data_gaps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)
    range_start = Column(DateTime, nullable=False)
    range_end = Column(DateTime, nullable=False)
    missing_count = Column(Integer, default=1)
    status = Column(String(16), nullable=False, default=GAP_OPEN, index=True)
    reason = Column(String(255), nullable=True)

    detected_by_batch_id = Column(Integer, nullable=True, index=True)
    filled_by_batch_id = Column(Integer, nullable=True, index=True)

    first_seen_at = Column(DateTime, default=datetime.utcnow)
    last_seen_at = Column(DateTime, default=datetime.utcnow)
    filled_at = Column(DateTime, nullable=True)
    created_by = Column(String(64), nullable=True)

    __table_args__ = (
        Index("ix_gap_key_status", "stock_code", "period", "status"),
        Index("ix_gap_range", "stock_code", "period", "range_start", "range_end"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "range_start": self.range_start.isoformat() if self.range_start else None,
            "range_end": self.range_end.isoformat() if self.range_end else None,
            "missing_count": self.missing_count,
            "status": self.status,
            "reason": self.reason,
            "detected_by_batch_id": self.detected_by_batch_id,
            "filled_by_batch_id": self.filled_by_batch_id,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "last_seen_at": self.last_seen_at.isoformat() if self.last_seen_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "created_by": self.created_by,
        }


class GapBackfillItem(Base):
    """缺口与补数批次的多对多关系，记录单缺口回填进展。"""

    __tablename__ = "gap_backfill_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gap_id = Column(Integer, nullable=False, index=True)
    backfill_id = Column(Integer, nullable=False, index=True)
    filled_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("uix_gap_backfill", "gap_id", "backfill_id", unique=True),
    )


class BackfillBatch(Base):
    """一次补数任务：策略、来源清单与中断恢复状态。"""

    __tablename__ = "backfill_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, nullable=False, index=True)  # 对应 ingestion_batches.id
    strategy = Column(String(16), nullable=False, default=STRATEGY_FILL)
    sources_json = Column(Text, nullable=True)             # 来源优先级清单
    processed_sources_json = Column(Text, nullable=True)   # 已成功处理的来源（断点续跑依据）

    status = Column(String(16), nullable=False, default=BATCH_PENDING, index=True)
    requested_by = Column(String(64), nullable=True)
    filled_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    def to_dict(self) -> dict:
        import json

        return {
            "id": self.id,
            "batch_id": self.batch_id,
            "strategy": self.strategy,
            "sources": json.loads(self.sources_json) if self.sources_json else [],
            "processed_sources": (
                json.loads(self.processed_sources_json)
                if self.processed_sources_json else []
            ),
            "status": self.status,
            "requested_by": self.requested_by,
            "filled_count": self.filled_count,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class SourceConflict(Base):
    """同一根 K 线在不同来源/批次间的取值冲突。"""

    __tablename__ = "source_conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)
    bar_timestamp = Column(DateTime, nullable=False, index=True)

    existing_batch_id = Column(Integer, nullable=True)
    incoming_batch_id = Column(Integer, nullable=False)
    existing_source = Column(String(32), nullable=True)
    incoming_source = Column(String(32), nullable=True)
    existing_values_json = Column(Text, nullable=True)
    incoming_values_json = Column(Text, nullable=True)

    status = Column(String(16), nullable=False, default=CONFLICT_PENDING, index=True)
    resolution = Column(String(16), nullable=True)
    applied_batch_id = Column(Integer, nullable=True)
    note = Column(Text, nullable=True)

    detected_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String(64), nullable=True)

    __table_args__ = (
        # 同一根 K 线只允许一个未裁决冲突，重复导入不再堆积
        Index(
            "uix_pending_conflict",
            "stock_code", "period", "bar_timestamp",
            unique=True,
            sqlite_where=text("status = 'pending'"),
        ),
        Index("ix_conflict_key", "stock_code", "period", "bar_timestamp"),
    )

    def to_dict(self) -> dict:
        import json

        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "bar_timestamp": self.bar_timestamp.isoformat() if self.bar_timestamp else None,
            "existing_batch_id": self.existing_batch_id,
            "incoming_batch_id": self.incoming_batch_id,
            "existing_source": self.existing_source,
            "incoming_source": self.incoming_source,
            "existing_values": json.loads(self.existing_values_json) if self.existing_values_json else None,
            "incoming_values": json.loads(self.incoming_values_json) if self.incoming_values_json else None,
            "status": self.status,
            "resolution": self.resolution,
            "applied_batch_id": self.applied_batch_id,
            "note": self.note,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by,
        }


class CandleVersion(Base):
    """单根 K 线的取值版本，支撑历史快照复现与冲突候选留痕。"""

    __tablename__ = "candle_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True)
    period = Column(String(10), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    version_number = Column(Integer, nullable=False)

    values_json = Column(Text, nullable=False)
    source = Column(String(32), nullable=True)
    producer_batch_id = Column(Integer, nullable=False, index=True)
    change_reason = Column(String(32), nullable=False, default="insert")
    status = Column(String(16), nullable=False, default=BAR_CURRENT, index=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("uix_bar_version", "stock_code", "period", "timestamp", "version_number", unique=True),
        Index("ix_bar_version_lookup", "stock_code", "period", "timestamp", "status"),
        Index("ix_bar_batch", "producer_batch_id"),
    )

    def to_dict(self) -> dict:
        import json

        return {
            "id": self.id,
            "stock_code": self.stock_code,
            "period": self.period,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "version_number": self.version_number,
            "values": json.loads(self.values_json) if self.values_json else None,
            "source": self.source,
            "producer_batch_id": self.producer_batch_id,
            "change_reason": self.change_reason,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
