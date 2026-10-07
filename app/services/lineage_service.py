"""行情导入编排：批次原子性、去重/乱序识别、来源冲突仲裁、
缺口登记与版本推进。所有写库操作通过 LineageMapper 完成。
"""

import logging
from datetime import datetime, time
from typing import Any, Dict, List, Optional

from app.config import db_session_scope
from app.mappers.lineage_mapper import (
    LineageMapper,
    CONFLICT_POLICIES,
    source_priority,
)
from app.data import grid
from app.chan.models import RawCandle

logger = logging.getLogger(__name__)

VALUE_EPS = 1e-9


def _values_of(candle: RawCandle) -> Dict[str, Any]:
    return {
        "open": float(candle.open),
        "high": float(candle.high),
        "low": float(candle.low),
        "close": float(candle.close),
        "volume": float(candle.volume or 0.0),
        "amount": getattr(candle, "amount", None),
    }


def _same_values(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    for key in ("open", "high", "low", "close", "volume"):
        if abs(float(a.get(key) or 0.0) - float(b.get(key) or 0.0)) > VALUE_EPS:
            return False
    return True


def _snap_end_of_window(period: str, end: datetime) -> datetime:
    if period != "daily" and end.time() == time.min:
        return datetime.combine(end.date(), time(15, 0))
    return end


class DataLineageService:
    """行情写入的唯一入口：定时抓取、补数、手工录入、冲突仲裁都走这里。"""

    # ------------------------------------------------------------------
    # 启动时对账：进程崩溃留下的 running 批次标记为 interrupted
    # ------------------------------------------------------------------

    def reconcile_interrupted(self) -> List[str]:
        with db_session_scope() as session:
            return LineageMapper(session).mark_interrupted_batches()

    # ------------------------------------------------------------------
    # 核心导入
    # ------------------------------------------------------------------

    def ingest(
        self,
        candles: List[RawCandle],
        stock_code: str,
        period: str,
        source: str,
        batch_type: str = "scheduled",
        conflict_policy: str = "trust_priority",
        trigger_gap_id: Optional[int] = None,
        requested_end: Optional[datetime] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        if conflict_policy not in CONFLICT_POLICIES:
            raise ValueError(f"unknown conflict policy: {conflict_policy}")

        # 批次先独立提交：即使随后处理失败/进程被杀，中断也有迹可循
        with db_session_scope() as session:
            batch = LineageMapper(session).create_batch(
                batch_type=batch_type,
                source=source,
                stock_code=stock_code,
                period=period,
                trigger_gap_id=trigger_gap_id,
                conflict_policy=conflict_policy,
            )
            batch_id = batch.id

        try:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                summary = self._ingest_session(
                    mapper=mapper,
                    candles=candles,
                    stock_code=stock_code,
                    period=period,
                    source=source.lower(),
                    batch_id=batch_id,
                    batch_type=batch_type,
                    conflict_policy=conflict_policy,
                    trigger_gap_id=trigger_gap_id,
                    requested_end=requested_end,
                    note=note,
                )

            summary["batch_id"] = batch_id
            return summary
        except Exception as exc:
            # 正常异常路径：立刻把批次置为 interrupted，保留缺口/数据原状
            logger.error("ingest batch %s interrupted: %s", batch_id, exc, exc_info=True)
            self._mark_batch(batch_id, "interrupted", str(exc))
            raise

    def _ingest_session(
        self,
        mapper: LineageMapper,
        candles: List[RawCandle],
        stock_code: str,
        period: str,
        source: str,
        batch_id: str,
        batch_type: str,
        conflict_policy: str,
        trigger_gap_id: Optional[int],
        requested_end: Optional[datetime],
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        batch = mapper.get_batch(batch_id)
        batch.total_received = len(candles)

        existing_map = mapper.get_candle_map(stock_code, period)
        previous_max_ts = max(existing_map.keys()) if existing_map else None
        watermark = previous_max_ts

        counts = {
            "inserted": 0,
            "updated": 0,
            "duplicate": 0,
            "duplicate_in_batch": 0,
            "out_of_order": 0,
            "late_arrival": 0,
            "conflict_accepted": 0,
            "conflict_rejected": 0,
            "conflict_pending": 0,
        }
        conflict_records = []
        seen_in_batch = set()

        # 乱序/迟到按“到达顺序”判定，因此不预先排序
        for order, candle in enumerate(candles):
            ts = candle.timestamp
            if ts in seen_in_batch:
                # 同一批次内重复时间戳：重复导入
                counts["duplicate_in_batch"] += 1
                continue
            seen_in_batch.add(ts)

            if watermark is not None and ts < watermark:
                counts["out_of_order"] += 1
            if previous_max_ts is not None and ts <= previous_max_ts:
                counts["late_arrival"] += 1
            watermark = ts if (watermark is None or ts > watermark) else watermark

            incoming = _values_of(candle)
            existing = existing_map.get(ts)

            if existing is None:
                new_candle = mapper.insert_candle_with_version(
                    stock_code, period, ts, incoming,
                    batch_id=batch_id, source=source, ingest_order=order,
                    change_reason="backfill" if batch_type == "backfill" else "normal",
                )
                existing_map[ts] = new_candle
                counts["inserted"] += 1
                continue

            active = mapper.get_active_version(existing)
            existing_values = {
                "open": existing.open,
                "high": existing.high,
                "low": existing.low,
                "close": existing.close,
                "volume": existing.volume or 0.0,
                "amount": existing.amount,
            }
            existing_source = active.source if active else existing.current_source

            if _same_values(incoming, existing_values):
                counts["duplicate"] += 1
                continue

            # 数值不一致：来源冲突（含迟到旧数据修订当前值）
            decision = self._resolve_conflict(
                conflict_policy, source, existing_source
            )

            if decision == "pending":
                mapper.add_conflict(
                    stock_code, period, ts, batch_id,
                    incoming_source=source,
                    existing_source=existing_source,
                    incoming_values=incoming,
                    existing_values=existing_values,
                    status="pending",
                    resolution_policy=conflict_policy,
                )
                counts["conflict_pending"] += 1
                conflict_records.append({"timestamp": ts.isoformat(), "decision": "pending"})
                continue

            if decision == "accept":
                reason = "backfill" if batch_type == "backfill" else "conflict_win"
                new_version = mapper.append_candle_version(
                    existing, incoming, batch_id, source, order,
                    change_reason=reason,
                )
                counts["updated"] += 1
                counts["conflict_accepted"] += 1
                mapper.add_conflict(
                    stock_code, period, ts, batch_id,
                    incoming_source=source,
                    existing_source=existing_source,
                    incoming_values=incoming,
                    existing_values=existing_values,
                    status="accepted",
                    resolution_policy=conflict_policy,
                    winning_version_id=new_version.id,
                )
                conflict_records.append({"timestamp": ts.isoformat(), "decision": "accepted"})
            else:
                counts["conflict_rejected"] += 1
                mapper.add_conflict(
                    stock_code, period, ts, batch_id,
                    incoming_source=source,
                    existing_source=existing_source,
                    incoming_values=incoming,
                    existing_values=existing_values,
                    status="rejected",
                    resolution_policy=conflict_policy,
                    winning_version_id=active.id if active else None,
                )
                conflict_records.append({"timestamp": ts.isoformat(), "decision": "rejected"})

        # ---- 缺口登记与状态刷新 ----
        new_gaps: List[Dict[str, Any]] = []
        if counts["inserted"] > 0 or counts["updated"] > 0 or not existing_map:
            new_gaps = self._detect_and_refresh_gaps(
                mapper, stock_code, period, batch_id,
                previous_max_ts=previous_max_ts,
                requested_end=requested_end,
                trigger_gap_id=trigger_gap_id,
            )

        # ---- 推进序列整体版本（仅在数据实质变化时）----
        data_version_no: Optional[int] = None
        changed = counts["inserted"] + counts["updated"] > 0
        if changed:
            summary_payload = {
                "batch_type": batch_type,
                "source": source,
                "inserted": counts["inserted"],
                "updated": counts["updated"],
                "duplicate": counts["duplicate"] + counts["duplicate_in_batch"],
                "out_of_order": counts["out_of_order"],
                "late_arrival": counts["late_arrival"],
                "conflicts": {
                    "accepted": counts["conflict_accepted"],
                    "rejected": counts["conflict_rejected"],
                    "pending": counts["conflict_pending"],
                },
                "gaps_detected": len(new_gaps),
                "note": note,
            }
            dv = mapper.bump_data_version(
                stock_code, period, batch_id, summary_payload
            )
            data_version_no = dv.version_no

        # ---- 批次收尾 ----
        batch.status = "completed"
        batch.completed_at = datetime.utcnow()
        batch.inserted_count = counts["inserted"]
        batch.updated_count = counts["updated"]
        batch.duplicate_count = counts["duplicate"] + counts["duplicate_in_batch"]
        batch.conflict_count = (
            counts["conflict_accepted"]
            + counts["conflict_rejected"]
            + counts["conflict_pending"]
        )
        batch.out_of_order_count = counts["out_of_order"]
        batch.gap_detected_count = len(new_gaps)
        batch.gap_filled_count = sum(1 for g in new_gaps if g["status"] == "filled")

        return {
            "stock_code": stock_code,
            "period": period,
            "source": source,
            "batch_type": batch_type,
            "total_received": len(candles),
            "data_changed": changed,
            "data_version_no": data_version_no,
            "counts": {
                "inserted": counts["inserted"],
                "updated": counts["updated"],
                "duplicate": counts["duplicate"] + counts["duplicate_in_batch"],
                "out_of_order": counts["out_of_order"],
                "late_arrival": counts["late_arrival"],
                "conflict_accepted": counts["conflict_accepted"],
                "conflict_rejected": counts["conflict_rejected"],
                "conflict_pending": counts["conflict_pending"],
            },
            "conflicts": conflict_records,
            "gaps_detected": new_gaps,
        }

    @staticmethod
    def _resolve_conflict(
        policy: str, incoming_source: str, existing_source: Optional[str]
    ) -> str:
        """返回 accept / reject / pending。"""
        if policy == "prefer_new":
            return "accept"
        if policy == "prefer_existing":
            return "reject"
        if policy == "manual":
            return "pending"
        # trust_priority：来源可信度高者胜；同来源（迟到修订）默认信任新数据
        if source_priority(incoming_source) >= source_priority(existing_source):
            return "accept"
        return "reject"

    # ------------------------------------------------------------------
    # 缺口检测
    # ------------------------------------------------------------------

    def _detect_and_refresh_gaps(
        self,
        mapper: LineageMapper,
        stock_code: str,
        period: str,
        batch_id: str,
        previous_max_ts: Optional[datetime],
        requested_end: Optional[datetime],
        trigger_gap_id: Optional[int],
    ) -> List[Dict[str, Any]]:
        # 1) 刷新所有未结缺口（本次插入可能把它们补齐或部分补齐）
        open_gaps = mapper.list_gaps(stock_code, period, status=None, limit=10000)
        for gap in open_gaps:
            if gap.status in ("open", "partial"):
                mapper.update_gap_status(gap, batch_id)

        result: List[Dict[str, Any]] = []

        # 2) 新领土内部相邻K线之间的缺口（不重复扫描已登记区间）
        candles = mapper.get_candle_map(stock_code, period)
        timestamps = sorted(candles.keys())
        if len(timestamps) >= 2:
            start_idx = 0
            if previous_max_ts is not None:
                # 只扫描右边界超过导入前最新时间戳的相邻对
                while start_idx < len(timestamps) - 1 and timestamps[start_idx + 1] <= previous_max_ts:
                    start_idx += 1
            for i in range(max(1, start_idx), len(timestamps)):
                left, right = timestamps[i - 1], timestamps[i]
                expected = grid.expected_slot_count(period, left, right)
                if expected <= 0:
                    continue
                if mapper.find_gap(stock_code, period, left, right):
                    continue
                # 已被更大范围的未结缺口覆盖（部分补数产生的内部相邻对）：不重复登记
                if mapper.overlapping_gap(stock_code, period, left, right):
                    continue
                gap = mapper.add_gap(
                    stock_code, period, left, right, expected, batch_id
                )
                mapper.update_gap_status(gap, batch_id)
                result.append(gap.to_dict())

        # 3) 尾部缺口：最后一根K线落后于本次请求窗口终点
        if requested_end and timestamps:
            window_end = _snap_end_of_window(period, requested_end)
            last_ts = timestamps[-1]
            if window_end > last_ts and grid.expected_slot_count(period, last_ts, window_end) > 0:
                if mapper.find_gap(stock_code, period, last_ts, window_end):
                    pass
                elif mapper.overlapping_gap(stock_code, period, last_ts, window_end):
                    pass
                else:
                    gap = mapper.add_gap(
                        stock_code, period, last_ts, window_end,
                        grid.expected_slot_count(period, last_ts, window_end),
                        batch_id,
                    )
                    result.append(gap.to_dict())

        return result

    # ------------------------------------------------------------------
    # 批次状态
    # ------------------------------------------------------------------

    def _mark_batch(self, batch_id: str, status: str, error: Optional[str] = None) -> None:
        try:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                batch = mapper.get_batch(batch_id)
                if batch and batch.status == "running":
                    batch.status = status
                    batch.error_message = error
                    batch.completed_at = datetime.utcnow()
        except Exception as inner:  # noqa: BLE001
            logger.error("failed to mark batch %s: %s", batch_id, inner)

    def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        with db_session_scope() as session:
            batch = LineageMapper(session).get_batch(batch_id)
            return batch.to_dict() if batch else None

    def list_batches(self, **kwargs) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            return [b.to_dict() for b in LineageMapper(session).list_batches(**kwargs)]

    # ------------------------------------------------------------------
    # 缺口查询/登记/忽略/核验
    # ------------------------------------------------------------------

    def list_gaps(self, stock_code=None, period=None, status=None) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            return [g.to_dict() for g in LineageMapper(session).list_gaps(
                stock_code=stock_code, period=period, status=status
            )]

    def get_gaps_affecting_range(
        self,
        stock_code: str,
        period: str,
        start: Optional[datetime],
        end: Optional[datetime],
        include_filled: bool = True,
    ) -> List[Dict[str, Any]]:
        """返回与查询区间相交的缺口；已补齐的缺口默认保留，标记“曾受影响”。"""
        with db_session_scope() as session:
            gaps = LineageMapper(session).list_gaps(
                stock_code=stock_code, period=period, status=None, limit=10000
            )
            result = []
            for gap in gaps:
                if gap.status == "ignored":
                    continue
                if not include_filled and gap.status in ("filled", "verified"):
                    continue
                if start and gap.gap_end <= start:
                    continue
                if end and gap.gap_start >= end:
                    continue
                item = gap.to_dict()
                item["affected"] = gap.status in ("open", "partial")
                result.append(item)
            return result

    def get_gap_dict(self, gap_id: int) -> Optional[Dict[str, Any]]:
        with db_session_scope() as session:
            gap = LineageMapper(session).get_gap(gap_id)
            return gap.to_dict() if gap else None

    def register_gap_manual(        self, stock_code: str, period: str, gap_start: datetime, gap_end: datetime
    ) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            existing = mapper.find_gap(stock_code, period, gap_start, gap_end)
            if existing:
                return existing.to_dict()
            expected = grid.expected_slot_count(period, gap_start, gap_end)
            gap = mapper.add_gap(
                stock_code, period, gap_start, gap_end,
                max(expected, 1), None, origin="manual",
            )
            return gap.to_dict()

    def _set_gap_status(self, gap_id: int, status: str, detail: Optional[str]) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            gap = mapper.get_gap(gap_id)
            if gap is None:
                raise KeyError(f"gap {gap_id} not found")
            gap.status = status
            gap.resolution_detail = detail
            if status == "filled":
                gap.filled_at = datetime.utcnow()
            return gap.to_dict()

    def ignore_gap(self, gap_id: int, reason: str = "manual ignore") -> Dict[str, Any]:
        return self._set_gap_status(gap_id, "ignored", reason)

    def verify_gap(self, gap_id: int) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            gap = mapper.get_gap(gap_id)
            if gap is None:
                raise KeyError(f"gap {gap_id} not found")
            mapper.update_gap_status(gap, gap.filled_batch_id)
            if gap.status == "filled":
                gap.status = "verified"
                gap.resolution_detail = "补数后核验通过"
            return gap.to_dict()

    # ------------------------------------------------------------------
    # 补数
    # ------------------------------------------------------------------

    def backfill_gap(
        self,
        gap_id: int,
        candles: List[RawCandle],
        source: str,
        conflict_policy: str = "trust_priority",
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """对登记缺口执行补数。中断后重跑会产生新批次，旧批次保留留痕。"""
        with db_session_scope() as session:
            gap = LineageMapper(session).get_gap(gap_id)
            if gap is None:
                raise KeyError(f"gap {gap_id} not found")
            gap_info = gap.to_dict()
            if gap.status in ("verified", "ignored"):
                return {"gap": gap_info, "skipped": True,
                        "reason": f"gap already {gap.status}"}
            stock_code, period = gap.stock_code, gap.period

        summary = self.ingest(
            candles,
            stock_code=stock_code,
            period=period,
            source=source,
            batch_type="backfill",
            conflict_policy=conflict_policy,
            trigger_gap_id=gap_id,
            note=note or f"backfill gap #{gap_id}",
        )

        with db_session_scope() as session:
            mapper = LineageMapper(session)
            gap = mapper.get_gap(gap_id)
            mapper.update_gap_status(gap, summary["batch_id"])
            gap_after = gap.to_dict()

        summary["gap"] = gap_after
        return summary

    # ------------------------------------------------------------------
    # 冲突查询/人工仲裁
    # ------------------------------------------------------------------

    def list_conflicts(self, stock_code=None, period=None, status=None) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            return [c.to_dict() for c in LineageMapper(session).list_conflicts(
                stock_code=stock_code, period=period, status=status
            )]

    def resolve_conflict(
        self,
        conflict_id: int,
        decision: str,
        values: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """人工仲裁 pending 冲突：accept=采用新值 reject=保留现值 manual=录入指定值。"""
        if decision not in ("accept", "reject", "manual"):
            raise ValueError("decision must be accept/reject/manual")

        with db_session_scope() as session:
            mapper = LineageMapper(session)
            conflict = mapper.get_conflict(conflict_id)
            if conflict is None:
                raise KeyError(f"conflict {conflict_id} not found")

            batch = mapper.create_batch(
                batch_type="conflict_resolution",
                source="manual",
                stock_code=conflict.stock_code,
                period=conflict.period,
                conflict_policy="manual",
            )
            candle_map = mapper.get_candle_map(conflict.stock_code, conflict.period)
            candle = candle_map.get(conflict.timestamp)

            winning_id = None
            if decision in ("accept", "manual") and candle is not None:
                import json
                chosen = values if decision == "manual" and values else json.loads(conflict.incoming_values)
                new_version = mapper.append_candle_version(
                    candle, chosen, batch.id, "manual", 0, change_reason="conflict_win"
                )
                winning_id = new_version.id
                conflict.status = "accepted"
                mapper.bump_data_version(
                    conflict.stock_code, conflict.period, batch.id,
                    {"batch_type": "conflict_resolution", "conflict_id": conflict_id,
                     "decision": decision},
                )
            else:
                active = mapper.get_active_version(candle) if candle else None
                winning_id = active.id if active else None
                conflict.status = "rejected"

            conflict.resolution_policy = "manual"
            conflict.winning_candle_version_id = winning_id
            conflict.resolved_batch_id = batch.id
            conflict.resolved_at = datetime.utcnow()
            batch.status = "completed"
            batch.completed_at = datetime.utcnow()
            batch.conflict_count = 1
            return conflict.to_dict()

    # ------------------------------------------------------------------
    # 血缘查询
    # ------------------------------------------------------------------

    def get_candle_lineage(
        self, stock_code: str, period: str, ts: datetime
    ) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            versions = mapper.list_candle_versions(stock_code, period, ts)
            return {
                "stock_code": stock_code,
                "period": period,
                "timestamp": ts.isoformat(),
                "versions": [v.to_dict() for v in versions],
            }

    def get_batch_lineage(self, batch_id: str) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            batch = mapper.get_batch(batch_id)
            if batch is None:
                raise KeyError(f"batch {batch_id} not found")
            rows = mapper.list_versions_by_batch(batch_id)
            return {
                "batch": batch.to_dict(),
                "candles": [
                    {
                        "stock_code": r.stock_code,
                        "period": r.period,
                        "timestamp": r.timestamp.isoformat(),
                        "version_no": r.version_no,
                        "change_reason": r.change_reason,
                        "source": r.source,
                    }
                    for r in rows
                ],
            }

    def get_data_versions(self, stock_code: str, period: str) -> List[Dict[str, Any]]:
        from app.entities.lineage import DataVersion
        with db_session_scope() as session:
            rows = session.query(DataVersion).filter(
                DataVersion.stock_code == stock_code,
                DataVersion.period == period,
            ).order_by(DataVersion.version_no.asc()).all()
            return [r.to_dict() for r in rows]

    # ------------------------------------------------------------------
    # 快照
    # ------------------------------------------------------------------

    def create_snapshot(
        self, stock_code: str, period: str, name: str, note: Optional[str] = None
    ) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            current = mapper.get_current_data_version(stock_code, period)
            snapshot = mapper.create_snapshot(
                stock_code, period, name,
                batch_id=current.import_batch_id if current else None,
                note=note,
            )
            return snapshot.to_dict()

    def list_snapshots(self, stock_code=None, period=None) -> List[Dict[str, Any]]:
        with db_session_scope() as session:
            return [s.to_dict() for s in LineageMapper(session).list_snapshots(
                stock_code=stock_code, period=period
            )]

    def get_snapshot_candles(self, snapshot_name: str) -> List[RawCandle]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            snapshot = mapper.get_snapshot(name=snapshot_name)
            if snapshot is None:
                raise KeyError(f"snapshot {snapshot_name} not found")
            return mapper.get_snapshot_candles(snapshot.id)

    def get_current_data_version_no(self, stock_code: str, period: str) -> Optional[int]:
        with db_session_scope() as session:
            dv = LineageMapper(session).get_current_data_version(stock_code, period)
            return dv.version_no if dv else None

    def get_current_data_version_info(
        self, stock_code: str, period: str
    ) -> Optional[Dict[str, Any]]:
        with db_session_scope() as session:
            dv = LineageMapper(session).get_current_data_version(stock_code, period)
            if dv is None:
                return None
            return {
                "version_no": dv.version_no,
                "import_batch_id": dv.import_batch_id,
                "candle_count": dv.candle_count,
                "created_at": dv.created_at.isoformat() if dv.created_at else None,
            }
