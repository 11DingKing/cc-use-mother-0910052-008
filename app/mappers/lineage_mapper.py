"""缺口、批次、行版本、快照与冲突的数据访问层。"""

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.entities.lineage import (
    ImportBatch,
    CandleDataVersion,
    DataVersion,
    DataSnapshot,
    SnapshotItem,
    DataGap,
    SourceConflict,
)
from app.entities.stock import StockCandle
from app.chan.models import RawCandle


# 来源优先级：数值越大越可信（手工录入最高，主行情商次之，备用源较低）
SOURCE_PRIORITY = {
    "manual": 100,
    "akshare": 20,
    "yahoo": 10,
    "unknown": 0,
}

CONFLICT_POLICIES = ("trust_priority", "prefer_existing", "prefer_new", "manual")


def source_priority(source: Optional[str]) -> int:
    return SOURCE_PRIORITY.get((source or "unknown").lower(), 5)


class LineageMapper:
    """所有血缘相关表的读写都收敛在这里，服务层只做编排。"""

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------
    # 导入批次
    # ------------------------------------------------------------------

    def create_batch(
        self,
        batch_type: str,
        source: str,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        trigger_gap_id: Optional[int] = None,
        conflict_policy: Optional[str] = None,
    ) -> ImportBatch:
        batch_no = (self.session.query(func.coalesce(func.max(ImportBatch.batch_no), 0)).scalar() or 0) + 1
        batch = ImportBatch(
            id=uuid.uuid4().hex,
            batch_no=batch_no,
            batch_type=batch_type,
            source=(source or "unknown").lower(),
            trigger_gap_id=trigger_gap_id,
            stock_code=stock_code,
            period=period,
            status="running",
            conflict_policy=conflict_policy,
        )
        self.session.add(batch)
        self.session.flush()
        return batch

    def get_batch(self, batch_id: str) -> Optional[ImportBatch]:
        return self.session.query(ImportBatch).filter(ImportBatch.id == batch_id).first()

    def list_batches(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        status: Optional[str] = None,
        batch_type: Optional[str] = None,
        gap_id: Optional[int] = None,
        limit: int = 50,
    ) -> List[ImportBatch]:
        query = self.session.query(ImportBatch)
        if stock_code:
            query = query.filter(ImportBatch.stock_code == stock_code)
        if period:
            query = query.filter(ImportBatch.period == period)
        if status:
            query = query.filter(ImportBatch.status == status)
        if batch_type:
            query = query.filter(ImportBatch.batch_type == batch_type)
        if gap_id is not None:
            query = query.filter(ImportBatch.trigger_gap_id == gap_id)
        return query.order_by(ImportBatch.batch_no.desc()).limit(limit).all()

    def mark_interrupted_batches(self) -> List[str]:
        """进程重启时调用：残留 running 批次判定为 interrupted（补数中断留痕）。"""
        running = self.session.query(ImportBatch).filter(ImportBatch.status == "running").all()
        ids = []
        for batch in running:
            batch.status = "interrupted"
            batch.error_message = (batch.error_message or "") + "\n进程中断，批次未正常完成"
            batch.completed_at = datetime.utcnow()
            ids.append(batch.id)
        self.session.flush()
        return ids

    # ------------------------------------------------------------------
    # K线当前行 + 行版本
    # ------------------------------------------------------------------

    def get_candle_map(self, stock_code: str, period: str) -> Dict[datetime, StockCandle]:
        rows = self.session.query(StockCandle).filter(
            and_(StockCandle.stock_code == stock_code, StockCandle.period == period)
        ).all()
        return {row.timestamp: row for row in rows}

    def get_max_timestamp(self, stock_code: str, period: str) -> Optional[datetime]:
        return self.session.query(func.max(StockCandle.timestamp)).filter(
            and_(StockCandle.stock_code == stock_code, StockCandle.period == period)
        ).scalar()

    def insert_candle_with_version(
        self,
        stock_code: str,
        period: str,
        ts: datetime,
        values: Dict[str, Any],
        batch_id: str,
        source: str,
        ingest_order: int,
        change_reason: str = "normal",
    ) -> StockCandle:
        """插入新K线及其首个行版本，返回K线当前行。"""
        candle = StockCandle(
            stock_code=stock_code,
            period=period,
            timestamp=ts,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=values.get("volume", 0.0),
            amount=values.get("amount"),
            current_version_no=1,
            current_source=source,
            last_batch_id=batch_id,
        )
        self.session.add(candle)
        self.session.flush()

        version = CandleDataVersion(
            candle_id=candle.id,
            version_no=1,
            stock_code=stock_code,
            period=period,
            timestamp=ts,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume or 0.0,
            amount=candle.amount,
            import_batch_id=batch_id,
            source=source,
            ingest_order=ingest_order,
            status="active",
            change_reason=change_reason,
        )
        self.session.add(version)
        self.session.flush()
        candle.current_version_id = version.id
        self.session.flush()
        return candle

    def append_candle_version(
        self,
        candle: StockCandle,
        values: Dict[str, Any],
        batch_id: str,
        source: str,
        ingest_order: int,
        change_reason: str = "normal",
    ) -> CandleDataVersion:
        old_active = self.session.query(CandleDataVersion).filter(
            and_(
                CandleDataVersion.candle_id == candle.id,
                CandleDataVersion.status == "active",
            )
        ).first()

        new_version = CandleDataVersion(
            candle_id=candle.id,
            version_no=(candle.current_version_no or 1) + 1,
            stock_code=candle.stock_code,
            period=candle.period,
            timestamp=candle.timestamp,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=values.get("volume", 0.0),
            amount=values.get("amount"),
            import_batch_id=batch_id,
            source=source,
            ingest_order=ingest_order,
            status="active",
            change_reason=change_reason,
        )
        self.session.add(new_version)
        self.session.flush()

        if old_active is not None:
            old_active.status = "superseded"
            old_active.superseded_by_id = new_version.id

        candle.open = new_version.open
        candle.high = new_version.high
        candle.low = new_version.low
        candle.close = new_version.close
        candle.volume = new_version.volume
        candle.amount = new_version.amount
        candle.current_version_id = new_version.id
        candle.current_version_no = new_version.version_no
        candle.current_source = source
        candle.last_batch_id = batch_id
        self.session.flush()
        return new_version

    def get_active_version(self, candle: StockCandle) -> Optional[CandleDataVersion]:
        if candle.current_version_id is None:
            return None
        return self.session.query(CandleDataVersion).filter(
            CandleDataVersion.id == candle.current_version_id
        ).first()

    def list_candle_versions(
        self, stock_code: str, period: str, ts: datetime
    ) -> List[CandleDataVersion]:
        return self.session.query(CandleDataVersion).filter(
            and_(
                CandleDataVersion.stock_code == stock_code,
                CandleDataVersion.period == period,
                CandleDataVersion.timestamp == ts,
            )
        ).order_by(CandleDataVersion.version_no.asc()).all()

    def get_version_by_id(self, version_id: int) -> Optional[CandleDataVersion]:
        return self.session.query(CandleDataVersion).filter(
            CandleDataVersion.id == version_id
        ).first()

    def list_versions_by_batch(self, batch_id: str) -> List[CandleDataVersion]:
        return self.session.query(CandleDataVersion).filter(
            CandleDataVersion.import_batch_id == batch_id
        ).order_by(CandleDataVersion.ingest_order.asc()).all()

    # ------------------------------------------------------------------
    # 序列整体版本
    # ------------------------------------------------------------------

    def get_current_data_version(
        self, stock_code: str, period: str
    ) -> Optional[DataVersion]:
        return self.session.query(DataVersion).filter(
            and_(
                DataVersion.stock_code == stock_code,
                DataVersion.period == period,
                DataVersion.status == "active",
            )
        ).order_by(DataVersion.version_no.desc()).first()

    def bump_data_version(
        self,
        stock_code: str,
        period: str,
        batch_id: str,
        summary: Dict[str, Any],
    ) -> DataVersion:
        current = self.get_current_data_version(stock_code, period)
        next_no = (current.version_no + 1) if current else 1

        candles = self.get_candle_map(stock_code, period)
        timestamps = sorted(candles.keys())

        version = DataVersion(
            stock_code=stock_code,
            period=period,
            version_no=next_no,
            status="active",
            import_batch_id=batch_id,
            candle_count=len(timestamps),
            first_time=timestamps[0] if timestamps else None,
            last_time=timestamps[-1] if timestamps else None,
            change_summary=json.dumps(summary, ensure_ascii=False),
        )
        self.session.add(version)
        self.session.flush()

        if current is not None:
            current.status = "superseded"
            current.superseded_by_id = version.id
        return version

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------

    def create_snapshot(
        self,
        stock_code: str,
        period: str,
        name: str,
        batch_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> DataSnapshot:
        current = self.get_current_data_version(stock_code, period)
        snapshot = DataSnapshot(
            name=name,
            stock_code=stock_code,
            period=period,
            data_version_no=current.version_no if current else None,
            created_batch_id=batch_id,
            note=note,
        )
        self.session.add(snapshot)
        self.session.flush()

        candles = self.session.query(StockCandle).filter(
            and_(StockCandle.stock_code == stock_code, StockCandle.period == period)
        ).order_by(StockCandle.timestamp.asc()).all()

        items = [
            SnapshotItem(
                snapshot_id=snapshot.id,
                timestamp=c.timestamp,
                candle_version_id=c.current_version_id,
            )
            for c in candles
            if c.current_version_id is not None
        ]
        self.session.bulk_save_objects(items)
        self.session.flush()
        return snapshot

    def create_snapshot_as_of_batch(
        self,
        stock_code: str,
        period: str,
        name: str,
        as_of_batch_id: str,
        data_version_no: Optional[int] = None,
        note: Optional[str] = None,
    ) -> DataSnapshot:
        """创建快照：每根K线取截至指定批次（含）当时生效的行版本。

        这样即使补数后当前版本已被取代，发布报告仍能读到当时的数值。
        """
        as_of_batch = self.get_batch(as_of_batch_id)
        as_of_no = as_of_batch.batch_no if as_of_batch else 0

        rows = (
            self.session.query(CandleDataVersion, ImportBatch)
            .outerjoin(ImportBatch, CandleDataVersion.import_batch_id == ImportBatch.id)
            .filter(
                CandleDataVersion.stock_code == stock_code,
                CandleDataVersion.period == period,
            )
        ).all()

        # 每个时间戳取“截至该批次可见”的最大版本号
        best: Dict[datetime, CandleDataVersion] = {}
        for version, batch in rows:
            if batch is not None and batch.batch_no > as_of_no:
                continue
            current = best.get(version.timestamp)
            if current is None or version.version_no > current.version_no:
                best[version.timestamp] = version

        snapshot = DataSnapshot(
            name=name,
            stock_code=stock_code,
            period=period,
            data_version_no=data_version_no,
            created_batch_id=as_of_batch_id,
            note=note,
        )
        self.session.add(snapshot)
        self.session.flush()

        items = [
            SnapshotItem(snapshot_id=snapshot.id, timestamp=ts, candle_version_id=v.id)
            for ts, v in sorted(best.items())
        ]
        self.session.bulk_save_objects(items)
        self.session.flush()
        return snapshot

    def get_snapshot(self, name: Optional[str] = None, snapshot_id: Optional[int] = None):
        query = self.session.query(DataSnapshot)
        if snapshot_id is not None:
            return query.filter(DataSnapshot.id == snapshot_id).first()
        return query.filter(DataSnapshot.name == name).first()

    def list_snapshots(self, stock_code: Optional[str] = None, period: Optional[str] = None):
        query = self.session.query(DataSnapshot)
        if stock_code:
            query = query.filter(DataSnapshot.stock_code == stock_code)
        if period:
            query = query.filter(DataSnapshot.period == period)
        return query.order_by(DataSnapshot.created_at.desc()).all()

    def get_snapshot_candles(self, snapshot_id: int) -> List[RawCandle]:
        rows = (
            self.session.query(CandleDataVersion, SnapshotItem)
            .join(SnapshotItem, SnapshotItem.candle_version_id == CandleDataVersion.id)
            .filter(SnapshotItem.snapshot_id == snapshot_id)
            .order_by(SnapshotItem.timestamp.asc())
        ).all()
        return [
            RawCandle(
                timestamp=v.timestamp,
                open=v.open,
                high=v.high,
                low=v.low,
                close=v.close,
                volume=v.volume,
            )
            for v, _ in rows
        ]

    # ------------------------------------------------------------------
    # 缺口登记
    # ------------------------------------------------------------------

    def find_gap(
        self, stock_code: str, period: str, start: datetime, end: datetime
    ) -> Optional[DataGap]:
        return self.session.query(DataGap).filter(
            and_(
                DataGap.stock_code == stock_code,
                DataGap.period == period,
                DataGap.gap_start == start,
                DataGap.gap_end == end,
            )
        ).first()

    def overlapping_gap(
        self, stock_code: str, period: str, start: datetime, end: datetime
    ) -> Optional[DataGap]:
        """是否已存在与 (start,end) 相交且未忽略/未核验的缺口登记。

        部分补数会在原缺口内部产生新的相邻K线对，不应被重复登记成新缺口。
        """
        return (
            self.session.query(DataGap)
            .filter(
                DataGap.stock_code == stock_code,
                DataGap.period == period,
                DataGap.gap_start < end,
                DataGap.gap_end > start,
                DataGap.status.in_(("open", "partial", "filled")),
            )
            .first()
        )

    def list_gaps(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[DataGap]:
        query = self.session.query(DataGap)
        if stock_code:
            query = query.filter(DataGap.stock_code == stock_code)
        if period:
            query = query.filter(DataGap.period == period)
        if status:
            query = query.filter(DataGap.status == status)
        return query.order_by(
            DataGap.stock_code.asc(),
            DataGap.period.asc(),
            DataGap.gap_start.asc(),
        ).limit(limit).all()

    def get_gap(self, gap_id: int) -> Optional[DataGap]:
        return self.session.query(DataGap).filter(DataGap.id == gap_id).first()

    def add_gap(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
        expected_count: int,
        batch_id: Optional[str],
        origin: str = "detected",
    ) -> DataGap:
        gap_no = (
            self.session.query(func.coalesce(func.max(DataGap.gap_no), 0))
            .filter(and_(DataGap.stock_code == stock_code, DataGap.period == period))
            .scalar()
            or 0
        ) + 1
        gap = DataGap(
            stock_code=stock_code,
            period=period,
            gap_no=gap_no,
            gap_start=start,
            gap_end=end,
            expected_count=expected_count,
            filled_count=0,
            status="open",
            origin=origin,
            first_seen_batch_id=batch_id,
        )
        self.session.add(gap)
        self.session.flush()
        return gap

    def update_gap_status(self, gap: DataGap, filled_batch_id: str) -> DataGap:
        """按当前实际落在缺口区间内的K线数刷新补齐状态。"""
        filled = self.session.query(func.count(StockCandle.id)).filter(
            and_(
                StockCandle.stock_code == gap.stock_code,
                StockCandle.period == gap.period,
                StockCandle.timestamp > gap.gap_start,
                StockCandle.timestamp < gap.gap_end,
            )
        ).scalar() or 0
        gap.filled_count = filled
        if filled <= 0:
            gap.status = "open"
            gap.filled_at = None
        elif filled >= gap.expected_count:
            gap.status = "filled"
            gap.filled_batch_id = filled_batch_id
            gap.filled_at = datetime.utcnow()
        else:
            gap.status = "partial"
            gap.filled_batch_id = filled_batch_id
        self.session.flush()
        return gap

    # ------------------------------------------------------------------
    # 来源冲突
    # ------------------------------------------------------------------

    def add_conflict(
        self,
        stock_code: str,
        period: str,
        ts: datetime,
        batch_id: str,
        incoming_source: str,
        existing_source: Optional[str],
        incoming_values: Dict[str, Any],
        existing_values: Dict[str, Any],
        status: str,
        resolution_policy: Optional[str] = None,
        winning_version_id: Optional[int] = None,
    ) -> SourceConflict:
        conflict = SourceConflict(
            stock_code=stock_code,
            period=period,
            timestamp=ts,
            import_batch_id=batch_id,
            incoming_source=incoming_source,
            existing_source=existing_source,
            incoming_values=json.dumps(incoming_values, ensure_ascii=False),
            existing_values=json.dumps(existing_values, ensure_ascii=False),
            status=status,
            resolution_policy=resolution_policy,
            winning_candle_version_id=winning_version_id,
            resolved_batch_id=batch_id if status in ("accepted", "rejected") else None,
            resolved_at=datetime.utcnow() if status in ("accepted", "rejected") else None,
        )
        self.session.add(conflict)
        self.session.flush()
        return conflict

    def list_conflicts(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[SourceConflict]:
        query = self.session.query(SourceConflict)
        if stock_code:
            query = query.filter(SourceConflict.stock_code == stock_code)
        if period:
            query = query.filter(SourceConflict.period == period)
        if status:
            query = query.filter(SourceConflict.status == status)
        return query.order_by(SourceConflict.created_at.desc()).limit(limit).all()

    def get_conflict(self, conflict_id: int) -> Optional[SourceConflict]:
        return self.session.query(SourceConflict).filter(SourceConflict.id == conflict_id).first()
