"""缺口登记、补数批次、来源血缘的数据访问层。"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.entities.stock import StockCandle
from app.entities.lineage import (
    IngestionBatch,
    DataGap,
    GapBackfillItem,
    BackfillBatch,
    SourceConflict,
    CandleVersion,
    BATCH_RUNNING,
    GAP_FILLED,
    GAP_IGNORED,
    GAP_OPEN,
    GAP_PARTIAL,
    CONFLICT_PENDING,
    CONFLICT_RESOLVED,
    BAR_CANDIDATE,
    BAR_CURRENT,
    BAR_SUPERSEDED,
)

BAR_FIELDS = ("open", "high", "low", "close", "volume", "amount")


def bar_values(candle: Any) -> Dict[str, Optional[float]]:
    return {name: getattr(candle, name, None) for name in BAR_FIELDS}


def values_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """同一根 K 线两次取值是否完全一致（重复导入判定）。"""
    for name in BAR_FIELDS:
        va, vb = a.get(name), b.get(name)
        if va is None and vb is None:
            continue
        if va is None or vb is None:
            return False
        if round(float(va), 9) != round(float(vb), 9):
            return False
    return True


class LineageMapper:
    """封装批次/版本/缺口/冲突表的所有读写。"""

    def __init__(self, session: Session):
        self.session = session

    # ------------------------------------------------------------------
    # 批次
    # ------------------------------------------------------------------

    def create_batch(
        self,
        batch_code: str,
        kind: str,
        source: Optional[str],
        stock_code: str,
        period: str,
        range_start: Optional[datetime],
        range_end: Optional[datetime],
        trigger: str = "auto",
        created_by: Optional[str] = None,
    ) -> IngestionBatch:
        batch = IngestionBatch(
            batch_code=batch_code,
            kind=kind,
            source=source,
            trigger=trigger,
            stock_code=stock_code,
            period=period,
            range_start=range_start,
            range_end=range_end,
            status=BATCH_RUNNING,
            created_by=created_by,
        )
        self.session.add(batch)
        self.session.flush()
        return batch

    def finish_batch(
        self,
        batch: IngestionBatch,
        counts: Dict[str, int],
        status: str,
        error_message: Optional[str] = None,
    ) -> IngestionBatch:
        for key in (
            "total_received", "inserted_count", "duplicate_count",
            "changed_count", "conflict_count", "late_arrival_count",
            "gap_detected_count",
        ):
            setattr(batch, key, counts.get(key, 0))
        batch.status = status
        batch.error_message = error_message
        batch.completed_at = datetime.utcnow()
        self.session.flush()
        return batch

    def get_batch(self, batch_id: int) -> Optional[IngestionBatch]:
        return self.session.query(IngestionBatch).get(batch_id)

    def get_batch_by_code(self, code: str) -> Optional[IngestionBatch]:
        return self.session.query(IngestionBatch).filter(
            IngestionBatch.batch_code == code
        ).first()

    def list_batches(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        kind: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[IngestionBatch]:
        query = self.session.query(IngestionBatch)
        if stock_code:
            query = query.filter(IngestionBatch.stock_code == stock_code)
        if period:
            query = query.filter(IngestionBatch.period == period)
        if kind:
            query = query.filter(IngestionBatch.kind == kind)
        if status:
            query = query.filter(IngestionBatch.status == status)
        return query.order_by(IngestionBatch.id.desc()).limit(limit).all()

    def max_batch_id(self) -> int:
        value = self.session.query(func.max(IngestionBatch.id)).scalar()
        return int(value or 0)

    # ------------------------------------------------------------------
    # K 线版本化写入
    # ------------------------------------------------------------------

    def get_bar(
        self, stock_code: str, period: str, ts: datetime
    ) -> Optional[StockCandle]:
        return self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
                StockCandle.timestamp == ts,
            )
        ).first()

    def get_bars_in_range(
        self,
        stock_code: str,
        period: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[StockCandle]:
        query = self.session.query(StockCandle).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        )
        if start:
            query = query.filter(StockCandle.timestamp >= start)
        if end:
            query = query.filter(StockCandle.timestamp <= end)
        return query.order_by(StockCandle.timestamp.asc()).all()

    def max_bar_timestamp(
        self, stock_code: str, period: str
    ) -> Optional[datetime]:
        return self.session.query(func.max(StockCandle.timestamp)).filter(
            and_(
                StockCandle.stock_code == stock_code,
                StockCandle.period == period,
            )
        ).scalar()

    def _next_version_number(
        self, stock_code: str, period: str, ts: datetime
    ) -> int:
        value = self.session.query(func.max(CandleVersion.version_number)).filter(
            and_(
                CandleVersion.stock_code == stock_code,
                CandleVersion.period == period,
                CandleVersion.timestamp == ts,
            )
        ).scalar()
        return int(value or 0) + 1

    def _add_version(
        self,
        stock_code: str,
        period: str,
        ts: datetime,
        values: Dict[str, Any],
        source: Optional[str],
        batch_id: int,
        reason: str,
        status: str,
    ) -> CandleVersion:
        version = CandleVersion(
            stock_code=stock_code,
            period=period,
            timestamp=ts,
            version_number=self._next_version_number(stock_code, period, ts),
            values_json=json.dumps(values, default=str),
            source=source,
            producer_batch_id=batch_id,
            change_reason=reason,
            status=status,
        )
        self.session.add(version)
        self.session.flush()
        return version

    def _supersede_current(
        self, stock_code: str, period: str, ts: datetime
    ) -> None:
        self.session.query(CandleVersion).filter(
            and_(
                CandleVersion.stock_code == stock_code,
                CandleVersion.period == period,
                CandleVersion.timestamp == ts,
                CandleVersion.status == BAR_CURRENT,
            )
        ).update({"status": BAR_SUPERSEDED}, synchronize_session=False)

    def insert_new_bar(
        self,
        stock_code: str,
        period: str,
        ts: datetime,
        values: Dict[str, Any],
        source: Optional[str],
        batch_id: int,
        reason: str = "insert",
    ) -> CandleVersion:
        version = self._add_version(
            stock_code, period, ts, values, source, batch_id, reason, BAR_CURRENT
        )
        bar = StockCandle(
            stock_code=stock_code,
            period=period,
            timestamp=ts,
            open=values["open"],
            high=values["high"],
            low=values["low"],
            close=values["close"],
            volume=values.get("volume") or 0.0,
            amount=values.get("amount"),
            source=source,
            source_batch_id=batch_id,
            current_version_id=version.id,
        )
        self.session.add(bar)
        self.session.flush()
        return version

    def adopt_new_values(
        self,
        bar: StockCandle,
        values: Dict[str, Any],
        source: Optional[str],
        batch_id: int,
        reason: str,
    ) -> CandleVersion:
        self._supersede_current(bar.stock_code, bar.period, bar.timestamp)
        version = self._add_version(
            bar.stock_code, bar.period, bar.timestamp, values,
            source, batch_id, reason, BAR_CURRENT,
        )
        bar.open = values["open"]
        bar.high = values["high"]
        bar.low = values["low"]
        bar.close = values["close"]
        bar.volume = values.get("volume") or 0.0
        bar.amount = values.get("amount")
        bar.source = source
        bar.source_batch_id = batch_id
        bar.current_version_id = version.id
        self.session.flush()
        return version

    def add_candidate_version(
        self,
        bar: StockCandle,
        values: Dict[str, Any],
        source: Optional[str],
        batch_id: int,
    ) -> CandleVersion:
        # 同一批次对同一根 K 线只留一个候选版本，重复上报不堆积
        existing_candidate = self.session.query(CandleVersion).filter(
            and_(
                CandleVersion.stock_code == bar.stock_code,
                CandleVersion.period == bar.period,
                CandleVersion.timestamp == bar.timestamp,
                CandleVersion.status == BAR_CANDIDATE,
                CandleVersion.producer_batch_id == batch_id,
            )
        ).first()
        if existing_candidate:
            return existing_candidate
        return self._add_version(
            bar.stock_code, bar.period, bar.timestamp, values,
            source, batch_id, "conflict_candidate", BAR_CANDIDATE,
        )

    # ------------------------------------------------------------------
    # 冲突
    # ------------------------------------------------------------------

    def find_pending_conflict(
        self, stock_code: str, period: str, ts: datetime
    ) -> Optional[SourceConflict]:
        return self.session.query(SourceConflict).filter(
            and_(
                SourceConflict.stock_code == stock_code,
                SourceConflict.period == period,
                SourceConflict.bar_timestamp == ts,
                SourceConflict.status == CONFLICT_PENDING,
            )
        ).first()

    def register_conflict(
        self,
        bar: StockCandle,
        existing_values: Dict[str, Any],
        incoming_values: Dict[str, Any],
        existing_source: Optional[str],
        incoming_source: Optional[str],
        incoming_batch_id: int,
    ) -> SourceConflict:
        conflict = self.find_pending_conflict(
            bar.stock_code, bar.period, bar.timestamp
        )
        if conflict:
            # 后来的取值追加为候选，冲突本身保持挂起
            conflict.incoming_batch_id = incoming_batch_id
            conflict.incoming_source = incoming_source
            conflict.incoming_values_json = json.dumps(incoming_values, default=str)
            self.session.flush()
            return conflict

        conflict = SourceConflict(
            stock_code=bar.stock_code,
            period=bar.period,
            bar_timestamp=bar.timestamp,
            existing_batch_id=bar.source_batch_id,
            incoming_batch_id=incoming_batch_id,
            existing_source=existing_source,
            incoming_source=incoming_source,
            existing_values_json=json.dumps(existing_values, default=str),
            incoming_values_json=json.dumps(incoming_values, default=str),
        )
        self.session.add(conflict)
        self.session.flush()
        return conflict

    def list_conflicts(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        status: str = CONFLICT_PENDING,
        limit: int = 100,
    ) -> List[SourceConflict]:
        query = self.session.query(SourceConflict)
        if stock_code:
            query = query.filter(SourceConflict.stock_code == stock_code)
        if period:
            query = query.filter(SourceConflict.period == period)
        if status:
            query = query.filter(SourceConflict.status == status)
        return query.order_by(SourceConflict.id.desc()).limit(limit).all()

    def get_conflict(self, conflict_id: int) -> Optional[SourceConflict]:
        return self.session.query(SourceConflict).get(conflict_id)

    def resolve_conflict(
        self,
        conflict: SourceConflict,
        resolution: str,
        resolved_by: Optional[str],
        note: Optional[str],
    ) -> Tuple[SourceConflict, Optional[CandleVersion]]:
        """裁决冲突。resolution: keep_existing / use_incoming。"""
        applied_version: Optional[CandleVersion] = None
        bar = self.get_bar(
            conflict.stock_code, conflict.period, conflict.bar_timestamp
        )
        candidate = self.session.query(CandleVersion).filter(
            and_(
                CandleVersion.stock_code == conflict.stock_code,
                CandleVersion.period == conflict.period,
                CandleVersion.timestamp == conflict.bar_timestamp,
                CandleVersion.producer_batch_id == conflict.incoming_batch_id,
                CandleVersion.status == BAR_CANDIDATE,
            )
        ).first()

        if resolution == "use_incoming":
            if bar is None or candidate is None:
                raise ValueError("冲突对应的 K 线或候选版本已不存在，无法采纳")
            values = json.loads(candidate.values_json)
            applied_version = self.adopt_new_values(
                bar,
                values,
                conflict.incoming_source,
                conflict.incoming_batch_id,
                reason="conflict_resolution",
            )
            # 候选值已转正，候选版本留痕后作废
            candidate.status = BAR_SUPERSEDED
            conflict.applied_batch_id = conflict.incoming_batch_id
        else:
            # 保留现值：候选版本留痕后作废
            if candidate is not None:
                candidate.status = BAR_SUPERSEDED
            conflict.applied_batch_id = conflict.existing_batch_id

        conflict.status = CONFLICT_RESOLVED
        conflict.resolution = resolution
        conflict.resolved_at = datetime.utcnow()
        conflict.resolved_by = resolved_by
        conflict.note = note
        self.session.flush()
        return conflict, applied_version

    # ------------------------------------------------------------------
    # 缺口
    # ------------------------------------------------------------------

    def find_open_gap(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
    ) -> Optional[DataGap]:
        """查找与 [start, end] 相交的未关闭缺口（用于合并登记）。"""
        query = self.session.query(DataGap).filter(
            and_(
                DataGap.stock_code == stock_code,
                DataGap.period == period,
                DataGap.status.in_([GAP_OPEN, GAP_PARTIAL]),
                DataGap.range_start <= end,
                DataGap.range_end >= start,
            )
        )
        return query.order_by(DataGap.range_start.asc()).first()

    def upsert_gap(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
        missing_count: int,
        detected_by_batch_id: Optional[int],
        reason: Optional[str] = None,
    ) -> DataGap:
        gap = self.find_open_gap(stock_code, period, start, end)
        now = datetime.utcnow()
        if gap:
            # 合并相交区间
            if start < gap.range_start:
                gap.range_start = start
            if end > gap.range_end:
                gap.range_end = end
            gap.missing_count = max(gap.missing_count, missing_count)
            gap.last_seen_at = now
            if detected_by_batch_id:
                gap.detected_by_batch_id = detected_by_batch_id
            self.session.flush()
            return gap

        gap = DataGap(
            stock_code=stock_code,
            period=period,
            range_start=start,
            range_end=end,
            missing_count=missing_count,
            status=GAP_OPEN,
            reason=reason,
            detected_by_batch_id=detected_by_batch_id,
            first_seen_at=now,
            last_seen_at=now,
        )
        self.session.add(gap)
        self.session.flush()
        return gap

    def list_gaps(
        self,
        stock_code: Optional[str] = None,
        period: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
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
            DataGap.range_start.desc(),
        ).limit(limit).all()

    def get_gap(self, gap_id: int) -> Optional[DataGap]:
        return self.session.query(DataGap).get(gap_id)

    def ignore_gap(self, gap: DataGap, reason: Optional[str]) -> DataGap:
        gap.status = GAP_IGNORED
        gap.filled_at = datetime.utcnow()
        if reason:
            gap.reason = reason
        self.session.flush()
        return gap

    def refresh_gap_status(
        self,
        gap: DataGap,
        present_timestamps: Sequence[datetime],
        filled_by_batch_id: Optional[int] = None,
        period: Optional[str] = None,
        holidays: Optional[Sequence[datetime]] = None,
    ) -> bool:
        """根据当前已有 K 线重新计算缺口状态，返回是否被填平。"""
        if gap.status == GAP_IGNORED:
            return False

        from app.data.gap_detector import expected_slots

        expected = expected_slots(
            period or gap.period, gap.range_start, gap.range_end, holidays
        )
        present = set(present_timestamps)
        missing = [ts for ts in expected if ts not in present]

        if not missing:
            gap.status = GAP_FILLED
            gap.filled_at = datetime.utcnow()
            if filled_by_batch_id:
                gap.filled_by_batch_id = filled_by_batch_id
            self.session.flush()
            return True

        if len(missing) < len(expected):
            gap.status = GAP_PARTIAL
        gap.last_seen_at = datetime.utcnow()
        gap.missing_count = len(missing)
        self.session.flush()
        return False

    def link_gap_backfill(self, gap_id: int, backfill_id: int, filled_count: int) -> None:
        item = self.session.query(GapBackfillItem).filter(
            and_(
                GapBackfillItem.gap_id == gap_id,
                GapBackfillItem.backfill_id == backfill_id,
            )
        ).first()
        if item:
            item.filled_count = filled_count
        else:
            self.session.add(GapBackfillItem(
                gap_id=gap_id,
                backfill_id=backfill_id,
                filled_count=filled_count,
            ))
        self.session.flush()

    # ------------------------------------------------------------------
    # 补数批次
    # ------------------------------------------------------------------

    def create_backfill(
        self,
        batch_id: int,
        strategy: str,
        sources: List[str],
        requested_by: Optional[str],
    ) -> BackfillBatch:
        backfill = BackfillBatch(
            batch_id=batch_id,
            strategy=strategy,
            sources_json=json.dumps(sources),
            processed_sources_json=json.dumps([]),
            status=BATCH_RUNNING,
            requested_by=requested_by,
            started_at=datetime.utcnow(),
        )
        self.session.add(backfill)
        self.session.flush()
        return backfill

    def get_backfill(self, backfill_id: int) -> Optional[BackfillBatch]:
        return self.session.query(BackfillBatch).get(backfill_id)

    def list_backfills(
        self,
        stock_code: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[BackfillBatch]:
        query = self.session.query(BackfillBatch)
        if stock_code:
            query = query.join(
                IngestionBatch, IngestionBatch.id == BackfillBatch.batch_id
            ).filter(IngestionBatch.stock_code == stock_code)
        if status:
            query = query.filter(BackfillBatch.status == status)
        return query.order_by(BackfillBatch.id.desc()).limit(limit).all()

    def update_backfill_progress(
        self,
        backfill: BackfillBatch,
        processed_source: str,
        filled_count: int,
    ) -> None:
        processed = json.loads(backfill.processed_sources_json or "[]")
        if processed_source not in processed:
            processed.append(processed_source)
        backfill.processed_sources_json = json.dumps(processed)
        backfill.filled_count = (backfill.filled_count or 0) + filled_count
        self.session.flush()

    def finish_backfill(
        self,
        backfill: BackfillBatch,
        status: str,
        error_message: Optional[str] = None,
    ) -> BackfillBatch:
        backfill.status = status
        backfill.error_message = error_message
        backfill.completed_at = datetime.utcnow()
        self.session.flush()
        return backfill

    def get_open_gaps_for_backfill(
        self,
        stock_code: str,
        period: str,
        gap_ids: Optional[List[int]] = None,
    ) -> List[DataGap]:
        query = self.session.query(DataGap).filter(
            and_(
                DataGap.stock_code == stock_code,
                DataGap.period == period,
                DataGap.status.in_([GAP_OPEN, GAP_PARTIAL]),
            )
        )
        if gap_ids:
            query = query.filter(DataGap.id.in_(gap_ids))
        return query.order_by(DataGap.range_start.asc()).all()

    # ------------------------------------------------------------------
    # 血缘查询与历史快照
    # ------------------------------------------------------------------

    def bar_provenance(
        self, stock_code: str, period: str, ts: datetime
    ) -> Optional[Dict[str, Any]]:
        bar = self.get_bar(stock_code, period, ts)
        if not bar:
            return None
        versions = self.session.query(CandleVersion).filter(
            and_(
                CandleVersion.stock_code == stock_code,
                CandleVersion.period == period,
                CandleVersion.timestamp == ts,
            )
        ).order_by(CandleVersion.version_number.asc()).all()
        batch_map = {
            b.id: b for b in self.session.query(IngestionBatch).filter(
                IngestionBatch.id.in_({v.producer_batch_id for v in versions})
            ).all()
        }
        return {
            "bar": bar.to_dict(),
            "versions": [
                {
                    **v.to_dict(),
                    "batch_code": batch_map[v.producer_batch_id].batch_code
                    if v.producer_batch_id in batch_map else None,
                }
                for v in versions
            ],
        }

    def snapshot_versions(
        self,
        stock_code: str,
        period: str,
        as_of_batch_id: Optional[int] = None,
        as_of_time: Optional[datetime] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """复现历史快照：每根 K 线取截至指定批次/时刻的最新版本。"""
        query = self.session.query(CandleVersion).filter(
            and_(
                CandleVersion.stock_code == stock_code,
                CandleVersion.period == period,
                CandleVersion.status.in_([BAR_CURRENT, BAR_SUPERSEDED]),
            )
        )
        if start:
            query = query.filter(CandleVersion.timestamp >= start)
        if end:
            query = query.filter(CandleVersion.timestamp <= end)
        if as_of_batch_id is not None:
            query = query.filter(CandleVersion.producer_batch_id <= as_of_batch_id)
        if as_of_time is not None:
            batch_ids = [
                row[0] for row in self.session.query(IngestionBatch.id).filter(
                    IngestionBatch.created_at <= as_of_time
                ).all()
            ]
            if not batch_ids:
                return []
            query = query.filter(CandleVersion.producer_batch_id.in_(batch_ids))

        # 每根 K 线取版本号最大的那条（截至点内的最新取值）
        latest_by_ts: Dict[datetime, CandleVersion] = {}
        for version in query.all():
            current = latest_by_ts.get(version.timestamp)
            if current is None or version.version_number > current.version_number:
                latest_by_ts[version.timestamp] = version

        result = [v.to_dict() for v in latest_by_ts.values()]
        result.sort(key=lambda v: v["timestamp"])
        return result
