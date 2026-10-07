"""缺口登记、补数批次与来源血缘的业务编排。

关键约定：
- 每次写入都在独立事务中先落一条 running 批次记录；处理失败时批次标记
  interrupted 而处理事务整体回滚，因此不会留下半成品 K 线，可凭批次断点重跑。
- 同一根 K 线取值不同默认不覆盖：挂起源冲突并保留候选版本，等待显式裁决。
- 补数完成后只报告受影响的分析/回测清单与重算策略，是否自动重算由
  recompute 参数显式决定（默认 manual，不隐式改写已发布报告）。
"""

import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from app.config import db_session_scope
from app.entities.lineage import (
    DataGap,
    BATCH_KIND_IMPORT,
    BATCH_KIND_BACKFILL,
    BATCH_KIND_MANUAL,
    BATCH_RUNNING,
    BATCH_COMPLETED,
    BATCH_PARTIAL,
    BATCH_INTERRUPTED,
    GAP_OPEN,
    GAP_PARTIAL,
    GAP_IGNORED,
    STRATEGY_FILL,
    STRATEGY_MERGE,
    STRATEGY_TRUSTED,
    CONFLICT_PENDING,
    CONFLICT_RESOLVED,
    RESOLUTION_KEEP_EXISTING,
    RESOLUTION_USE_INCOMING,
)
from app.entities.analysis_result import AnalysisResult
from app.entities.backtest import BacktestResult as BacktestResultEntity
from app.middleware.exception_handler import NotFoundException
from app.mappers.lineage_mapper import (
    LineageMapper,
    bar_values,
    values_equal,
)
from app.data.gap_detector import detect_missing_between_bars, detect_missing_groups
from app.utils.validators import validate_stock_code, validate_period

logger = logging.getLogger(__name__)

# 取数回调：(source, stock_code, period, start, end) -> (candles, error)
FetchCallback = Callable[[str, str, str, Optional[datetime], Optional[datetime]], Tuple[Sequence, Optional[str]]]


def _normalize_key(stock_code: str, period: str) -> Tuple[str, str]:
    """统一标的/周期的规范化，保证写入键与查询键一致。"""
    return validate_stock_code(stock_code), validate_period(period)


class LineageService:
    """缺口/批次/血缘的统一入口。"""

    def __init__(self, fetch_callback: Optional[FetchCallback] = None):
        self.fetch_callback = fetch_callback

    # ------------------------------------------------------------------
    # 批次编号
    # ------------------------------------------------------------------

    @staticmethod
    def _new_batch_code(mapper: LineageMapper, kind: str) -> str:
        prefix = {
            BATCH_KIND_IMPORT: "IMP",
            BATCH_KIND_BACKFILL: "BKF",
            BATCH_KIND_MANUAL: "MAN",
        }.get(kind, "BAT")
        seq = mapper.max_batch_id() + 1
        return f"{prefix}{datetime.utcnow():%Y%m%d%H%M%S}{seq:06d}"

    # ------------------------------------------------------------------
    # 导入：去重 / 乱序 / 冲突 / 缺口登记
    # ------------------------------------------------------------------

    def ingest_candles(
        self,
        stock_code: str,
        period: str,
        candles: Sequence,
        source: Optional[str],
        kind: str = BATCH_KIND_IMPORT,
        trigger: str = "auto",
        range_start: Optional[datetime] = None,
        range_end: Optional[datetime] = None,
        on_conflict: str = "suspend",
        source_priority: Optional[List[str]] = None,
        created_by: Optional[str] = None,
        detect_gaps: bool = True,
    ) -> Dict[str, Any]:
        """写入一批 K 线并返回批次计量。

        on_conflict:
          suspend         差异挂起源冲突，保留现值（默认）
          source_priority 按 source_priority 顺序，高优先级来源直接生效
          trusted         本来路即为可信来源（如手工修正），直接生效
        """
        if not candles:
            return {
                "batch_id": None,
                "status": "empty",
                "inserted": 0, "duplicate": 0, "changed": 0,
                "conflicts": 0, "late_arrivals": 0, "gaps_detected": [],
            }

        stock_code, period = _normalize_key(stock_code, period)
        if kind not in (BATCH_KIND_IMPORT, BATCH_KIND_BACKFILL, BATCH_KIND_MANUAL):
            raise ValueError(
                f"kind 必须是 import / backfill / manual，收到 {kind!r}"
            )
        if on_conflict not in ("suspend", "source_priority", "trusted"):
            raise ValueError(
                f"on_conflict 必须是 suspend / source_priority / trusted，"
                f"收到 {on_conflict!r}"
            )
        ordered = sorted(candles, key=lambda c: c.timestamp)
        if range_start is None:
            range_start = ordered[0].timestamp
        if range_end is None:
            range_end = ordered[-1].timestamp

        # 1) 独立事务建立 running 批次，中断后仍可查
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            batch = mapper.create_batch(
                self._new_batch_code(mapper, kind),
                kind, source, stock_code, period,
                range_start, range_end, trigger, created_by,
            )
            batch_id = batch.id
            batch_code = batch.batch_code
            existing_max_ts = mapper.max_bar_timestamp(stock_code, period)

        # 2) 处理事务：失败则整体回滚，批次另行标记 interrupted
        try:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                batch = mapper.get_batch(batch_id)

                counts = {
                    "total_received": len(ordered),
                    "inserted_count": 0,
                    "duplicate_count": 0,
                    "changed_count": 0,
                    "conflict_count": 0,
                    "late_arrival_count": 0,
                    "gap_detected_count": 0,
                }
                conflict_ids: List[int] = []
                seen_ts = set()

                for candle in ordered:
                    ts = candle.timestamp
                    if ts in seen_ts:
                        # 同一批次内的重复行，直接忽略
                        continue
                    seen_ts.add(ts)

                    incoming = bar_values(candle)
                    bar = mapper.get_bar(stock_code, period, ts)

                    if bar is None:
                        mapper.insert_new_bar(
                            stock_code, period, ts, incoming, source, batch_id,
                            reason="backfill" if kind == BATCH_KIND_BACKFILL else "insert",
                        )
                        counts["inserted_count"] += 1
                        if existing_max_ts and ts < existing_max_ts:
                            counts["late_arrival_count"] += 1
                        continue

                    existing = bar_values(bar)
                    if values_equal(existing, incoming):
                        counts["duplicate_count"] += 1
                        continue

                    decision = self._conflict_decision(
                        bar.source, source, on_conflict, source_priority
                    )
                    if decision == "adopt":
                        mapper.adopt_new_values(
                            bar, incoming, source, batch_id,
                            reason="manual_correction" if kind == BATCH_KIND_MANUAL
                            else "source_priority",
                        )
                        counts["changed_count"] += 1
                    else:
                        conflict = mapper.register_conflict(
                            bar, existing, incoming,
                            bar.source, source, batch_id,
                        )
                        mapper.add_candidate_version(
                            bar, incoming, source, batch_id
                        )
                        conflict_ids.append(conflict.id)
                        counts["conflict_count"] += 1

                # 3) 缺口登记：只在已有相邻 K 线之间检测，边界缺失由显式扫描负责
                gap_ids: List[int] = []
                if detect_gaps:
                    present = [
                        c.timestamp for c in
                        mapper.get_bars_in_range(stock_code, period)
                    ]
                    groups = detect_missing_between_bars(period, present)
                    for g_start, g_end, missing_count in groups:
                        gap = mapper.upsert_gap(
                            stock_code, period, g_start, g_end,
                            missing_count, batch_id,
                            reason="detected_during_import",
                        )
                        gap_ids.append(gap.id)
                    counts["gap_detected_count"] = len(gap_ids)

                # 4) 迟到数据可能正好填平此前登记的缺口：刷新相交缺口状态
                overlapping = session.query(DataGap).filter(
                    DataGap.stock_code == stock_code,
                    DataGap.period == period,
                    DataGap.status.in_([GAP_OPEN, GAP_PARTIAL]),
                    DataGap.range_start <= range_end,
                    DataGap.range_end >= range_start,
                ).all()
                present_full = [
                    c.timestamp for c in mapper.get_bars_in_range(stock_code, period)
                ]
                for open_gap in overlapping:
                    mapper.refresh_gap_status(
                        open_gap, present_full,
                        filled_by_batch_id=batch_id, period=period,
                    )

                status = BATCH_PARTIAL if conflict_ids else BATCH_COMPLETED
                mapper.finish_batch(batch, counts, status)
                payload = {
                    "batch_id": batch_id,
                    "batch_code": batch_code,
                    "status": status,
                    "inserted": counts["inserted_count"],
                    "duplicate": counts["duplicate_count"],
                    "changed": counts["changed_count"],
                    "conflicts": counts["conflict_count"],
                    "conflict_ids": conflict_ids,
                    "late_arrivals": counts["late_arrival_count"],
                    "gaps_detected": gap_ids,
                }
        except Exception as exc:
            self._mark_batch_interrupted(batch_id, str(exc))
            raise

        return payload

    @staticmethod
    def _conflict_decision(
        existing_source: Optional[str],
        incoming_source: Optional[str],
        on_conflict: str,
        source_priority: Optional[List[str]],
    ) -> str:
        if on_conflict == "trusted":
            return "adopt"
        if on_conflict == "source_priority" and source_priority and incoming_source:
            priority = [s for s in source_priority]
            if incoming_source in priority and (
                existing_source not in priority
                or priority.index(incoming_source) < priority.index(existing_source)
            ):
                return "adopt"
        return "suspend"

    def _mark_batch_interrupted(self, batch_id: int, error: str) -> None:
        try:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                batch = mapper.get_batch(batch_id)
                if batch is not None and batch.status == "running":
                    mapper.finish_batch(batch, {}, BATCH_INTERRUPTED, error[:1000])
        except Exception:
            logger.error("无法标记中断批次 %s", batch_id, exc_info=True)

    # ------------------------------------------------------------------
    # 显式缺口扫描
    # ------------------------------------------------------------------

    def scan_gaps(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
        holidays: Optional[Sequence[datetime]] = None,
    ) -> List[Dict[str, Any]]:
        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            present = [
                c.timestamp for c in mapper.get_bars_in_range(
                    stock_code, period, start, end
                )
            ]
            groups = detect_missing_groups(period, present, start, end, holidays)
            result = []
            for g_start, g_end, count in groups:
                gap = mapper.upsert_gap(
                    stock_code, period, g_start, g_end, count, None,
                    reason="explicit_scan",
                )
                result.append(gap.to_dict())
            return result

    def list_gaps(self, stock_code=None, period=None, status=None):
        if stock_code is not None and period is not None:
            stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return [g.to_dict() for g in mapper.list_gaps(stock_code, period, status)]

    def ignore_gap(self, gap_id: int, reason: Optional[str] = None) -> Dict[str, Any]:
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            gap = mapper.get_gap(gap_id)
            if gap is None:
                raise NotFoundException(
                    message=f"gap {gap_id} 不存在",
                    resource_type="DataGap",
                    resource_id=str(gap_id),
                )
            mapper.ignore_gap(gap, reason)
            return gap.to_dict()

    # ------------------------------------------------------------------
    # 补数批次（支持中断恢复）
    # ------------------------------------------------------------------

    def backfill_gaps(
        self,
        stock_code: str,
        period: str,
        gap_ids: Optional[List[int]] = None,
        sources: Optional[List[str]] = None,
        strategy: str = STRATEGY_FILL,
        recompute: str = "manual",
        requested_by: Optional[str] = None,
        backfill_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """对登记缺口发起补数。

        strategy: fill（首个填平缺口的来源胜出）/ merge（多来源合并，冲突挂起）/
                  trusted（仅信任指定来源，差异直接生效）
        recompute: manual（仅登记受影响清单）/ analysis（自动重算分析新版本）/
                   analysis_and_backtest（分析、回测均产生新版本）
        """
        sources = sources or ["akshare", "yahoo"]
        if strategy not in (STRATEGY_FILL, STRATEGY_MERGE, STRATEGY_TRUSTED):
            raise ValueError(
                f"strategy 必须是 fill / merge / trusted，收到 {strategy!r}"
            )
        if recompute not in ("manual", "analysis", "analysis_and_backtest"):
            raise ValueError(
                f"recompute 必须是 manual / analysis / analysis_and_backtest，"
                f"收到 {recompute!r}"
            )
        if strategy == STRATEGY_TRUSTED and not sources:
            raise ValueError("trusted 策略必须指定可信来源")

        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            gaps = mapper.get_open_gaps_for_backfill(stock_code, period, gap_ids)
            if gap_ids and len(gaps) != len(set(gap_ids)):
                raise ValueError("部分缺口不存在、已关闭或不属于该标的/周期")
            if not gaps:
                return {
                    "status": "nothing_to_fill",
                    "backfill_id": backfill_id,
                    "filled_gap_ids": [], "remaining_gap_ids": [],
                    "conflicts": [], "batch_id": None,
                }
            range_start = min(g.range_start for g in gaps)
            range_end = max(g.range_end for g in gaps)

            if backfill_id is None:
                batch = mapper.create_batch(
                    self._new_batch_code(mapper, BATCH_KIND_BACKFILL),
                    BATCH_KIND_BACKFILL, None, stock_code, period,
                    range_start, range_end, "gap_backfill", requested_by,
                )
                backfill_row = mapper.create_backfill(
                    batch.id, strategy, sources, requested_by
                )
                backfill_id = backfill_row.id
                batch_id = batch.id
            else:
                backfill_row = mapper.get_backfill(backfill_id)
                if backfill_row is None:
                    raise NotFoundException(message=f"补数批次 {backfill_id} 不存在", resource_type="BackfillBatch", resource_id=str(backfill_id))
                batch_id = backfill_row.batch_id
                sources = json.loads(backfill_row.sources_json or "[]")
                strategy = backfill_row.strategy

        try:
            result = self._run_backfill_sources(
                backfill_id, batch_id, stock_code, period,
                sources, strategy, range_start, range_end,
            )
        except Exception as exc:
            self._mark_backfill_interrupted(backfill_id, batch_id, str(exc))
            raise

        # 重算策略：补数成功后按显式策略处理下游
        result["recompute"] = self._apply_recompute_policy(
            recompute, stock_code, period,
            range_start, range_end, result["batch_id"],
        )
        return result

    def _run_backfill_sources(
        self,
        backfill_id: int,
        batch_id: int,
        stock_code: str,
        period: str,
        sources: List[str],
        strategy: str,
        range_start: datetime,
        range_end: datetime,
    ) -> Dict[str, Any]:
        if self.fetch_callback is None:
            raise RuntimeError("未配置取数回调，无法执行补数")

        all_conflict_ids: List[int] = []
        filled_gap_ids: List[int] = []
        child_batch_ids: List[int] = []

        for source in sources:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                backfill_row = mapper.get_backfill(backfill_id)
                processed = json.loads(backfill_row.processed_sources_json or "[]")
                gaps = mapper.get_open_gaps_for_backfill(stock_code, period)
            if source in processed:
                continue
            if strategy == STRATEGY_FILL and not gaps:
                break

            candles, error = self.fetch_callback(
                source, stock_code, period, range_start, range_end
            )
            if not candles:
                # 该来源拿不到数据：记录进度后继续下一来源
                with db_session_scope() as session:
                    mapper = LineageMapper(session)
                    mapper.update_backfill_progress(
                        mapper.get_backfill(backfill_id), source, 0
                    )
                continue

            ingest_summary = self.ingest_candles(
                stock_code, period, candles, source,
                kind=BATCH_KIND_BACKFILL, trigger="gap_backfill",
                range_start=range_start, range_end=range_end,
                on_conflict="trusted" if strategy == STRATEGY_TRUSTED else "suspend",
                source_priority=sources if strategy == STRATEGY_FILL else None,
                detect_gaps=False,
            )
            if ingest_summary.get("batch_id"):
                child_batch_ids.append(ingest_summary["batch_id"])
            all_conflict_ids.extend(ingest_summary.get("conflict_ids", []))

            with db_session_scope() as session:
                mapper = LineageMapper(session)
                backfill_row = mapper.get_backfill(backfill_id)
                just_filled = 0
                # 导入通道已自动填平缺口：收集被本子批次填平的缺口
                filled_now = session.query(DataGap).filter(
                    DataGap.stock_code == stock_code,
                    DataGap.period == period,
                    DataGap.filled_by_batch_id == ingest_summary["batch_id"],
                ).all()
                for gap in filled_now:
                    present = [
                        c.timestamp for c in mapper.get_bars_in_range(
                            stock_code, period, gap.range_start, gap.range_end
                        )
                    ]
                    mapper.link_gap_backfill(
                        gap.id, backfill_id,
                        len([t for t in present
                             if gap.range_start <= t <= gap.range_end]),
                    )
                    filled_gap_ids.append(gap.id)
                    just_filled += 1

                # 仍开放的缺口：兜底重算一次状态（部分填充场景）
                still_open = []
                for gap in mapper.get_open_gaps_for_backfill(stock_code, period):
                    present = [
                        c.timestamp for c in mapper.get_bars_in_range(
                            stock_code, period, gap.range_start, gap.range_end
                        )
                    ]
                    became_filled = mapper.refresh_gap_status(
                        gap, present, filled_by_batch_id=ingest_summary["batch_id"]
                    )
                    if became_filled:
                        mapper.link_gap_backfill(gap.id, backfill_id, len(present))
                        filled_gap_ids.append(gap.id)
                        just_filled += 1
                    else:
                        still_open.append(gap.id)
                mapper.update_backfill_progress(
                    backfill_row, source, ingest_summary.get("inserted", 0)
                )

            if strategy == STRATEGY_FILL and not still_open:
                break

        with db_session_scope() as session:
            mapper = LineageMapper(session)
            backfill_row = mapper.get_backfill(backfill_id)
            remaining = [
                g.id for g in mapper.get_open_gaps_for_backfill(stock_code, period)
            ]
            status = BATCH_COMPLETED if not remaining else BATCH_PARTIAL
            mapper.finish_backfill(backfill_row, status)
            batch = mapper.get_batch(batch_id)
            if batch is not None and batch.status != BATCH_INTERRUPTED:
                # 汇总各来源子批次写入的 K 线数
                children = mapper.list_batches(
                    stock_code=stock_code, period=period, kind=BATCH_KIND_BACKFILL,
                )
                inserted = sum(
                    c.inserted_count for c in children if c.id in child_batch_ids
                )
                mapper.finish_batch(
                    batch,
                    {
                        "inserted_count": inserted,
                        "conflict_count": len(all_conflict_ids),
                    },
                    BATCH_PARTIAL if (remaining or all_conflict_ids)
                    else BATCH_COMPLETED,
                )

        return {
            "status": status,
            "backfill_id": backfill_id,
            "batch_id": batch_id,
            "filled_gap_ids": sorted(set(filled_gap_ids)),
            "remaining_gap_ids": remaining,
            "conflicts": sorted(set(all_conflict_ids)),
        }

    def resume_backfill(self, backfill_id: int) -> Dict[str, Any]:
        """从中断点继续补数：跳过已处理来源，继续未完成缺口。"""
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            backfill_row = mapper.get_backfill(backfill_id)
            if backfill_row is None:
                raise NotFoundException(message=f"补数批次 {backfill_id} 不存在", resource_type="BackfillBatch", resource_id=str(backfill_id))
            if backfill_row.status not in (BATCH_INTERRUPTED, BATCH_PARTIAL):
                return {
                    "status": backfill_row.status,
                    "backfill_id": backfill_id,
                    "message": "该批次无需恢复",
                }
            batch_id = backfill_row.batch_id
            batch = mapper.get_batch(batch_id)
            stock_code, period = batch.stock_code, batch.period
            strategy = backfill_row.strategy
            sources = json.loads(backfill_row.sources_json or "[]")
            processed = set(json.loads(backfill_row.processed_sources_json or "[]"))
            range_start, range_end = batch.range_start, batch.range_end
            backfill_row.status = "running"
            backfill_row.error_message = None
            # 父批次恢复为运行中，完成后重新落最终状态
            batch.status = BATCH_RUNNING
            batch.completed_at = None

        # 未处理来源先跑；已处理来源若仍留下开放缺口则重试
        pending_sources = [s for s in sources if s not in processed]
        pending_sources += [s for s in sources if s in processed]

        try:
            result = self._run_backfill_sources(
                backfill_id, batch_id,
                stock_code, period,
                pending_sources, strategy, range_start, range_end,
            )
        except Exception as exc:
            self._mark_backfill_interrupted(backfill_id, batch_id, str(exc))
            raise
        result["recompute"] = self._apply_recompute_policy(
            "manual", stock_code, period, range_start, range_end,
            result["batch_id"],
        )
        return result

    def _mark_backfill_interrupted(self, backfill_id, batch_id, error):
        try:
            with db_session_scope() as session:
                mapper = LineageMapper(session)
                backfill_row = mapper.get_backfill(backfill_id)
                if backfill_row is not None and backfill_row.status == "running":
                    mapper.finish_backfill(
                        backfill_row, BATCH_INTERRUPTED, error[:1000]
                    )
                batch = mapper.get_batch(batch_id)
                if batch is not None and batch.status == "running":
                    mapper.finish_batch(batch, {}, BATCH_INTERRUPTED, error[:1000])
        except Exception:
            logger.error("无法标记中断补数 %s", backfill_id, exc_info=True)

    # ------------------------------------------------------------------
    # 冲突裁决
    # ------------------------------------------------------------------

    def resolve_conflict(
        self,
        conflict_id: int,
        resolution: str,
        resolved_by: Optional[str] = None,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        if resolution not in (RESOLUTION_KEEP_EXISTING, RESOLUTION_USE_INCOMING):
            raise ValueError(
                f"resolution 必须是 {RESOLUTION_KEEP_EXISTING} 或 "
                f"{RESOLUTION_USE_INCOMING}"
            )
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            conflict = mapper.get_conflict(conflict_id)
            if conflict is None:
                raise NotFoundException(message=f"冲突 {conflict_id} 不存在", resource_type="SourceConflict", resource_id=str(conflict_id))
            if conflict.status == CONFLICT_RESOLVED:
                return conflict.to_dict()
            mapper.resolve_conflict(conflict, resolution, resolved_by, note)
            result = conflict.to_dict()
            stock_code = conflict.stock_code
            period = conflict.period
            ts = conflict.bar_timestamp
            applied_batch = conflict.applied_batch_id

        if resolution == RESOLUTION_USE_INCOMING:
            affected = self.affected_artifacts(
                stock_code, period, ts, ts, min_batch_id=applied_batch
            )
            result["affected"] = affected
        return result

    def list_conflicts(self, stock_code=None, period=None, status=CONFLICT_PENDING):
        if stock_code is not None and period is not None:
            stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return [c.to_dict() for c in
                    mapper.list_conflicts(stock_code, period, status)]

    # ------------------------------------------------------------------
    # 血缘查询 / 历史快照
    # ------------------------------------------------------------------

    def bar_provenance(self, stock_code, period, ts):
        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return mapper.bar_provenance(stock_code, period, ts)

    def snapshot(
        self, stock_code, period,
        as_of_batch_id=None, as_of_time=None, start=None, end=None,
    ):
        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return mapper.snapshot_versions(
                stock_code, period, as_of_batch_id, as_of_time, start, end
            )

    def list_batches(self, **kwargs):
        if kwargs.get("stock_code") is not None:
            kwargs["stock_code"] = validate_stock_code(kwargs["stock_code"])
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return [b.to_dict() for b in mapper.list_batches(**kwargs)]

    def list_backfills(self, **kwargs):
        if kwargs.get("stock_code") is not None:
            kwargs["stock_code"] = validate_stock_code(kwargs["stock_code"])
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            return [b.to_dict() for b in mapper.list_backfills(**kwargs)]

    def data_lineage_for_window(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
    ) -> Dict[str, Any]:
        """分析/回测读取数据时获取血缘：批次集合、缺口窗口。"""
        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            mapper = LineageMapper(session)
            bars = mapper.get_bars_in_range(stock_code, period, start, end)
            batch_ids = sorted({b.source_batch_id for b in bars
                                if b.source_batch_id is not None})
            gaps = [
                {
                    "range_start": g.range_start.isoformat(),
                    "range_end": g.range_end.isoformat(),
                    "status": g.status,
                    "missing_count": g.missing_count,
                }
                for g in mapper.list_gaps(stock_code, period)
                if g.range_start <= end and g.range_end >= start
                and g.status != GAP_IGNORED
            ]
            return {
                "batch_ids": batch_ids,
                "latest_batch_id": max(batch_ids) if batch_ids else None,
                "gaps": gaps,
            }

    # ------------------------------------------------------------------
    # 受影响分析/回测识别与重算策略
    # ------------------------------------------------------------------

    def affected_artifacts(
        self,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
        min_batch_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """找出时间窗重叠且数据血缘早于新批次的分析/回测。"""
        stock_code, period = _normalize_key(stock_code, period)
        with db_session_scope() as session:
            analyses = session.query(AnalysisResult).filter(
                AnalysisResult.stock_code == stock_code,
                AnalysisResult.period == period,
                AnalysisResult.is_current == 1,
                AnalysisResult.start_time <= end,
                AnalysisResult.end_time >= start,
            ).all()
            stale_analyses = [
                {
                    "id": a.id,
                    "version": a.version,
                    "is_published": bool(a.is_published),
                    "latest_data_batch_id": a.latest_data_batch_id,
                    "reason": self._staleness_reason(
                        a.latest_data_batch_id, min_batch_id
                    ),
                }
                for a in analyses
                if min_batch_id is None
                or a.latest_data_batch_id is None
                or a.latest_data_batch_id < min_batch_id
            ]

            backtests = session.query(BacktestResultEntity).filter(
                BacktestResultEntity.stock_code == stock_code,
                BacktestResultEntity.period == period,
                BacktestResultEntity.status == "completed",
                BacktestResultEntity.superseded_by_id.is_(None),
                BacktestResultEntity.start_date <= end,
                BacktestResultEntity.end_date >= start,
            ).all()
            stale_backtests = []
            for bt in backtests:
                ids = json.loads(bt.data_batch_ids_json or "[]")
                latest = max(ids) if ids else None
                if min_batch_id is None or latest is None or latest < min_batch_id:
                    stale_backtests.append({
                        "id": bt.id,
                        "latest_data_batch_id": latest,
                        "reason": self._staleness_reason(latest, min_batch_id),
                    })

            return {
                "window": {
                    "start": start.isoformat(), "end": end.isoformat(),
                },
                "new_batch_id": min_batch_id,
                "stale_analyses": stale_analyses,
                "stale_backtests": stale_backtests,
                "published_analysis_versions": [
                    a for a in stale_analyses if a["is_published"]
                ],
            }

    @staticmethod
    def _staleness_reason(latest_known, new_batch_id):
        if latest_known is None:
            return "该结果没有数据血缘记录"
        if new_batch_id is None:
            return "数据窗口发生变化，建议重算"
        return f"结果基于批次 {latest_known}，早于新数据批次 {new_batch_id}"

    def _apply_recompute_policy(
        self,
        policy: str,
        stock_code: str,
        period: str,
        start: datetime,
        end: datetime,
        batch_id: Optional[int],
    ) -> Dict[str, Any]:
        affected = self.affected_artifacts(
            stock_code, period, start, end, min_batch_id=batch_id
        )
        outcome: Dict[str, Any] = {"policy": policy, "affected": affected}

        if policy in ("analysis", "analysis_and_backtest") and affected["stale_analyses"]:
            # 延迟导入避免循环依赖
            from app.services.analysis_service import AnalysisService
            service = AnalysisService()
            new_version = service.run_analysis(
                stock_code, period, start, end,
                recompute_reason=f"backfill:batch{batch_id}",
            )
            outcome["new_analysis_version"] = new_version.get("version")
            outcome["new_analysis_id"] = new_version.get("id")

        if policy == "analysis_and_backtest" and affected["stale_backtests"]:
            from app.services.backtest_service import BacktestService
            service = BacktestService()
            rerun_ids = []
            for item in affected["stale_backtests"]:
                rerun = service.rerun_backtest(
                    item["id"], reason=f"backfill:batch{batch_id}"
                )
                rerun_ids.append(rerun["id"])
            outcome["new_backtest_ids"] = rerun_ids

        return outcome
