"""业务模块说明。"""

from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from app.config import db_session_scope
from app.entities.analysis_result import AnalysisResult
from app.mappers.analysis_mapper import AnalysisMapper
from app.services.stock_service import StockService
from app.chan.kline_processor import KLineProcessor
from app.chan.fractal_detector import FractalDetector
from app.chan.bi_detector import BiDetector
from app.chan.duan_detector import DuanDetector
from app.chan.zhongshu_detector import ZhongshuDetector
from app.chan.signal_detector import SignalDetector
from app.chan.multi_level import MultiLevelLinkage, CombinedSignal
from app.chan.models import Fractal, Bi, Duan, Zhongshu, Signal
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import AnalysisException, NotFoundException
from app.services.lineage_service import DataLineageService

logger = logging.getLogger(__name__)


# 补数后重算策略：
# always         — 数据版本有变化就重算
# on_gap_filled  — 仅当原结果区间内存在“曾受影响且现已补齐”的缺口时重算
# on_open_gap    — 仅当区间内仍有未结缺口时才重算（保守，不建议自动）
# manual         — 不自动重算，只登记待处理
RECOMPUTE_POLICIES = ("always", "on_gap_filled", "on_open_gap", "manual")


class AnalysisService:
    """业务模块说明。"""
    stock_service = StockService()

    def __init__(self):
        self.stock_service = self.__class__.stock_service
        self.lineage = DataLineageService()
        self.kline_processor = KLineProcessor()
        self.fractal_detector = FractalDetector()
        self.bi_detector = BiDetector()
        self.duan_detector = DuanDetector()
        self.zhongshu_detector = ZhongshuDetector()
        self.multi_level_linkage = MultiLevelLinkage()
    
    def compute_signals(
        self,
        candles: List,
        stock_code: str,
        period: str,
    ) -> Dict[str, Any]:
        """对给定K线（可为历史快照）执行缠论计算，不落库。"""
        cleaned, _ = self.kline_processor.clean(candles)
        merged = self.kline_processor.merge(cleaned)
        fractals = self.fractal_detector.detect(merged)
        bis = self.bi_detector.detect(fractals, merged)
        duans = self.duan_detector.detect(bis)
        zhongshus = self.zhongshu_detector.detect(bis)
        signal_detector = SignalDetector(stock_code, period)
        signals = signal_detector.detect_all(bis, duans, zhongshus)
        return {
            "fractals": fractals,
            "bis": bis,
            "duans": duans,
            "zhongshus": zhongshus,
            "signals": signals,
        }

    def run_analysis(
        self,
        stock_code: str,
        period: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        
        try:
            # 获取K线数据
            candles = self.stock_service.get_candles(
                stock_code, period, start_date, end_date
            )
            
            if not candles:
                raise AnalysisException(
                    message="No candle data available",
                    stock_code=stock_code,
                    period=period,
                )
            
            # 清洗和合并K线
            cleaned, _ = self.kline_processor.clean(candles)
            merged = self.kline_processor.merge(cleaned)
            
            # 识别分型
            fractals = self.fractal_detector.detect(merged)
            
            # 识别笔
            bis = self.bi_detector.detect(fractals, merged)
            
            # 识别段
            duans = self.duan_detector.detect(bis)
            
            # 识别中枢
            zhongshus = self.zhongshu_detector.detect(bis)
            
            # 识别买卖点
            signal_detector = SignalDetector(stock_code, period)
            signals = signal_detector.detect_all(bis, duans, zhongshus)
            
            # 保存结果（携带数据版本、批次与缺口影响范围）
            data_info = self.lineage.get_current_data_version_info(
                stock_code, period
            )
            affected_gaps = self.lineage.get_gaps_affecting_range(
                stock_code, period, start_date, end_date
            )
            affected_gap_ids = [g["id"] for g in affected_gaps]

            self._save_result(
                stock_code, period, start_date, end_date,
                fractals, bis, duans, zhongshus, signals,
                data_version_no=data_info.get("version_no") if data_info else None,
                based_on_batch_id=data_info.get("import_batch_id") if data_info else None,
                affected_gap_ids=affected_gap_ids,
            )

            return {
                "stock_code": stock_code,
                "period": period,
                "candle_count": len(candles),
                "merged_count": len(merged),
                "fractal_count": len(fractals),
                "bi_count": len(bis),
                "duan_count": len(duans),
                "zhongshu_count": len(zhongshus),
                "signal_count": len(signals),
                "latest_signal": signals[-1].signal_type.value if signals else None,
                "data_version_no": data_info.get("version_no") if data_info else None,
                "affected_open_gaps": [
                    {"id": g["id"], "gap_start": g["gap_start"], "gap_end": g["gap_end"],
                     "status": g["status"]}
                    for g in affected_gaps if g["affected"]
                ],
            }
            
        except AnalysisException:
            raise
        except Exception as e:
            logger.error(f"Analysis error: {e}", exc_info=True)
            raise AnalysisException(
                message=f"Analysis failed: {str(e)}",
                stock_code=stock_code,
                period=period,
            )
    
    def _save_result(
        self,
        stock_code: str,
        period: str,
        start_date: datetime,
        end_date: datetime,
        fractals: List[Fractal],
        bis: List[Bi],
        duans: List[Duan],
        zhongshus: List[Zhongshu],
        signals: List[Signal],
        data_version_no: Optional[int] = None,
        based_on_batch_id: Optional[str] = None,
        affected_gap_ids: Optional[List[int]] = None,
    ) -> int:
        """业务模块说明。"""
        try:
            with db_session_scope() as session:
                mapper = AnalysisMapper(session)
                result = mapper.save_analysis(
                    stock_code, period, start_date, end_date,
                    fractals, bis, duans, zhongshus, signals,
                    data_version_no=data_version_no,
                    based_on_batch_id=based_on_batch_id,
                    affected_gap_ids=affected_gap_ids,
                )
                return result.id
        except Exception as e:
            logger.warning(f"Save result error: {e}")
            return 0
    
    def get_result(
        self,
        stock_code: str,
        period: str,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)

        with db_session_scope() as session:
            mapper = AnalysisMapper(session)
            result = mapper.get_latest(stock_code, period)

            if not result:
                raise NotFoundException(
                    message="Analysis result not found",
                    resource_type="AnalysisResult",
                    resource_id=f"{stock_code}:{period}",
                )

            return self._result_summary(result)
    
    def get_signals(
        self,
        stock_code: str,
        period: str,
    ) -> List[Dict[str, Any]]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        
        with db_session_scope() as session:
            mapper = AnalysisMapper(session)
            result = mapper.get_latest(stock_code, period)
            
            if not result:
                return []
            
            signals = mapper.load_signals(result)
            return [
                {
                    "signal_type": s.signal_type.value,
                    "timestamp": s.timestamp.isoformat(),
                    "price": s.price,
                    "strength": s.strength,
                    "level": s.level,
                }
                for s in signals
            ]
    
    def get_full_result(
        self,
        stock_code: str,
        period: str,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        
        with db_session_scope() as session:
            mapper = AnalysisMapper(session)
            result = mapper.get_latest(stock_code, period)
            
            if not result:
                raise NotFoundException(
                    message="Analysis result not found",
                    resource_type="AnalysisResult",
                    resource_id=f"{stock_code}:{period}",
                )
            
            data = mapper.load_all(result)
            
            return {
                "stock_code": result.stock_code,
                "period": result.period,
                "fractals": [self._fractal_to_dict(f) for f in data["fractals"]],
                "bis": [self._bi_to_dict(b) for b in data["bis"]],
                "duans": [self._duan_to_dict(d) for d in data["duans"]],
                "zhongshus": [self._zhongshu_to_dict(z) for z in data["zhongshus"]],
                "signals": [self._signal_to_dict(s) for s in data["signals"]],
            }
    
    def _fractal_to_dict(self, f: Fractal) -> Dict[str, Any]:
        return {
            "type": f.type.value,
            "timestamp": f.timestamp.isoformat(),
            "price": f.price,
            "index": f.candle_index,
        }
    
    def _bi_to_dict(self, b: Bi) -> Dict[str, Any]:
        return {
            "direction": b.direction.value,
            "start_time": b.start_fractal.timestamp.isoformat(),
            "end_time": b.end_fractal.timestamp.isoformat(),
            "start_price": b.start_price,
            "end_price": b.end_price,
        }
    
    def _duan_to_dict(self, d: Duan) -> Dict[str, Any]:
        return {
            "direction": d.direction.value,
            "bi_count": len(d.bi_list),
            "start_time": d.bi_list[0].start_fractal.timestamp.isoformat() if d.bi_list else None,
            "end_time": d.bi_list[-1].end_fractal.timestamp.isoformat() if d.bi_list else None,
        }
    
    def _zhongshu_to_dict(self, z: Zhongshu) -> Dict[str, Any]:
        return {
            "high": z.high,
            "low": z.low,
            "bi_count": len(z.bi_list),
            "start_time": z.start_time.isoformat() if z.start_time else (
                z.bi_list[0].start_fractal.timestamp.isoformat() if z.bi_list else None
            ),
            "end_time": z.end_time.isoformat() if z.end_time else (
                z.bi_list[-1].end_fractal.timestamp.isoformat() if z.bi_list else None
            ),
        }
    
    def _signal_to_dict(self, s: Signal) -> Dict[str, Any]:
        return {
            "signal_type": s.signal_type.value,
            "timestamp": s.timestamp.isoformat(),
            "price": s.price,
            "strength": s.strength,
            "level": s.level,
        }
    
    def run_multi_level_analysis(
        self,
        stock_code: str,
        primary_period: str = "daily",
        secondary_periods: Optional[List[str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """业务模块说明。"""
        stock_code = validate_stock_code(stock_code)
        primary_period = validate_period(primary_period)
        start_date, end_date = validate_time_range(start_date, end_date)
        
        if secondary_periods is None:
            secondary_periods = ["60min", "30min"]
        
        secondary_periods = [validate_period(p) for p in secondary_periods]
        all_periods = [primary_period] + secondary_periods
        
        try:
            signals_by_period: Dict[str, List[Signal]] = {}
            results_by_period: Dict[str, Dict[str, Any]] = {}
            
            for period in all_periods:
                candles = self.stock_service.get_candles(
                    stock_code, period, start_date, end_date
                )
                
                if not candles:
                    logger.warning(f"No candle data for {stock_code} {period}")
                    continue
                
                cleaned, _ = self.kline_processor.clean(candles)
                merged = self.kline_processor.merge(cleaned)
                fractals = self.fractal_detector.detect(merged)
                bis = self.bi_detector.detect(fractals, merged)
                duans = self.duan_detector.detect(bis)
                zhongshus = self.zhongshu_detector.detect(bis)
                
                signal_detector = SignalDetector(stock_code, period)
                signals = signal_detector.detect_all(bis, duans, zhongshus)
                
                signals_by_period[period] = signals
                results_by_period[period] = {
                    "candle_count": len(candles),
                    "merged_count": len(merged),
                    "fractal_count": len(fractals),
                    "bi_count": len(bis),
                    "duan_count": len(duans),
                    "zhongshu_count": len(zhongshus),
                    "signal_count": len(signals),
                    "latest_signal": signals[-1].signal_type.value if signals else None,
                    "signals": [self._signal_to_dict(s) for s in signals],
                }

            if primary_period not in results_by_period:
                raise AnalysisException(
                    message="No candle data available for primary period",
                    stock_code=stock_code,
                    period=primary_period,
                )
            
            combined_signals = self.multi_level_linkage.generate_combined_signal(
                signals_by_period
            )
            
            strong_confirmations = [s for s in combined_signals if s.is_strong_confirmation]
            conflicts = [s for s in combined_signals if s.has_conflict]
            
            return {
                "stock_code": stock_code,
                "primary_period": primary_period,
                "secondary_periods": secondary_periods,
                "period_results": results_by_period,
                "combined_signals": [
                    self._combined_signal_to_dict(s) for s in combined_signals
                ],
                "summary": {
                    "total_combined_signals": len(combined_signals),
                    "strong_confirmations": len(strong_confirmations),
                    "conflicts": len(conflicts),
                    "recommendation": self._generate_recommendation(
                        combined_signals, primary_period
                    ),
                },
            }
            
        except AnalysisException:
            raise
        except Exception as e:
            logger.error(f"Multi-level analysis error: {e}", exc_info=True)
            raise AnalysisException(
                message=f"多周期联立分析失败: {str(e)}",
                stock_code=stock_code,
                period=primary_period,
            )
    
    def _combined_signal_to_dict(self, s: CombinedSignal) -> Dict[str, Any]:
        """业务模块说明。"""
        return {
            "stock_code": s.stock_code,
            "signal_type": s.signal_type.value,
            "timestamp": s.timestamp.isoformat(),
            "price": s.price,
            "primary_level": s.primary_level,
            "secondary_levels": s.secondary_levels,
            "strength": s.strength,
            "is_strong_confirmation": s.is_strong_confirmation,
            "has_conflict": s.has_conflict,
            "conflict_details": s.conflict_details,
        }
    
    def _generate_recommendation(
        self,
        combined_signals: List[CombinedSignal],
        primary_period: str,
    ) -> str:
        """业务模块说明。"""
        if not combined_signals:
            return "暂无明确信号，建议观望"
        
        latest = combined_signals[-1]
        
        if latest.is_strong_confirmation:
            signal_type = "买入" if latest.signal_type.value.startswith("buy") else "卖出"
            return f"【强确认】{primary_period}级别第一类{signal_type}点 + 次级别第二类{signal_type}点共振，信号强度: {latest.strength:.2f}"
        
        if latest.has_conflict:
            primary_dir = latest.conflict_details.get("primary_direction", "")
            return f"【冲突】大级别看{primary_dir}，但次级别存在反向信号，建议等待确认"
        
        if latest.strength >= 0.7:
            signal_type = "买入" if latest.signal_type.value.startswith("buy") else "卖出"
            return f"【正常】{primary_period}级别{signal_type}信号，多周期共振，信号强度: {latest.strength:.2f}"

        return f"信号强度较弱 ({latest.strength:.2f})，建议观望或轻仓"

    # ------------------------------------------------------------------
    # 版本化结果：历史版本读取 / 发布 / 影响面 / 补数后重算
    # ------------------------------------------------------------------

    @staticmethod
    def _result_summary(result) -> Dict[str, Any]:
        import json as _json
        gap_ids = []
        if result.affected_gap_ids_json:
            try:
                gap_ids = _json.loads(result.affected_gap_ids_json)
            except ValueError:
                gap_ids = []
        return {
            "id": result.id,
            "stock_code": result.stock_code,
            "period": result.period,
            "result_version": result.result_version,
            "is_current": bool(result.is_current),
            "status": result.status,
            "published_at": result.published_at.isoformat() if result.published_at else None,
            "snapshot_name": result.snapshot_name,
            "start_time": result.start_time.isoformat(),
            "end_time": result.end_time.isoformat(),
            "fractal_count": result.fractal_count,
            "bi_count": result.bi_count,
            "duan_count": result.duan_count,
            "zhongshu_count": result.zhongshu_count,
            "signal_count": result.signal_count,
            "latest_signal_type": result.latest_signal_type,
            "latest_signal_time": result.latest_signal_time.isoformat() if result.latest_signal_time else None,
            "latest_signal_price": result.latest_signal_price,
            "data_version_no": result.data_version_no,
            "based_on_batch_id": result.based_on_batch_id,
            "affected_gap_ids": gap_ids,
            "updated_at": result.updated_at.isoformat(),
        }

    def get_result_version(
        self, stock_code: str, period: str, result_version: int
    ) -> Dict[str, Any]:
        """按版本号读取历史结果；已发布报告永远可读，不受补数影响。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        with db_session_scope() as session:
            result = AnalysisMapper(session).get_version(
                stock_code, period, result_version
            )
            if not result:
                raise NotFoundException(
                    message="Analysis result version not found",
                    resource_type="AnalysisResult",
                    resource_id=f"{stock_code}:{period}:v{result_version}",
                )
            return self._result_summary(result)

    def list_result_versions(
        self, stock_code: str, period: str, limit: int = 50
    ) -> List[Dict[str, Any]]:
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        with db_session_scope() as session:
            rows = AnalysisMapper(session).list_versions(stock_code, period, limit=limit)
            return [self._result_summary(r) for r in rows]

    def publish_report(
        self, stock_code: str, period: str, result_version: Optional[int] = None
    ) -> Dict[str, Any]:
        """发布报告：按其依据的批次时点固化数据快照，报告行变为不可变。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        with db_session_scope() as session:
            mapper = AnalysisMapper(session)
            result = (
                mapper.get_version(stock_code, period, result_version)
                if result_version is not None
                else mapper.get_latest(stock_code, period)
            )
            if not result:
                raise NotFoundException(
                    message="Analysis result not found",
                    resource_type="AnalysisResult",
                    resource_id=f"{stock_code}:{period}",
                )
            if result.status == "published":
                return self._result_summary(result)

            snapshot_name = f"rpt-analysis-{result.id}-v{result.result_version}"
            from app.mappers.lineage_mapper import LineageMapper
            lineage_mapper = LineageMapper(mapper.session)
            if result.based_on_batch_id:
                snap = lineage_mapper.create_snapshot_as_of_batch(
                    stock_code, period, snapshot_name,
                    as_of_batch_id=result.based_on_batch_id,
                    data_version_no=result.data_version_no,
                    note=f"published analysis result {result.id}",
                )
            else:
                snap = lineage_mapper.create_snapshot(
                    stock_code, period, snapshot_name,
                    note=f"published analysis result {result.id}",
                )
            mapper.publish(result, snap.name)
            return self._result_summary(result)

    def find_results_affected_by_gap(self, gap_id: int) -> List[Dict[str, Any]]:
        """哪些分析结果把该缺口计入过影响范围（含已发布报告）。"""
        import json as _json
        with db_session_scope() as session:
            all_rows = session.query(AnalysisResult).all()
            matched = []
            for row in all_rows:
                ids = []
                if row.affected_gap_ids_json:
                    try:
                        ids = _json.loads(row.affected_gap_ids_json)
                    except ValueError:
                        ids = []
                if gap_id in ids:
                    matched.append(self._result_summary(row))
            return matched

    def recompute_after_backfill(
        self,
        stock_code: str,
        period: str,
        policy: str = "on_gap_filled",
    ) -> Dict[str, Any]:
        """补数后按明确策略决定是否重算。已发布报告不被覆盖，只产生新版本。

        - always: 数据版本领先于结果依据版本即重算
        - on_gap_filled: 原结果标注的缺口中，已有在其后被补齐的才重算
        - on_open_gap: 区间仍有未结缺口时重算
        - manual: 只返回判断结果，不执行重算
        """
        if policy not in RECOMPUTE_POLICIES:
            raise ValueError(f"policy must be one of {RECOMPUTE_POLICIES}")

        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)

        with db_session_scope() as session:
            mapper = AnalysisMapper(session)
            latest = mapper.get_latest(stock_code, period)
            if latest is None:
                return {"recomputed": False, "reason": "no_existing_result"}
            from app.mappers.lineage_mapper import LineageMapper
            lm = LineageMapper(session)
            current_dv = lm.get_current_data_version(stock_code, period)
            current_version_no = current_dv.version_no if current_dv else None
            based_on = latest.data_version_no
            affected_ids = []
            if latest.affected_gap_ids_json:
                import json as _json
                try:
                    affected_ids = _json.loads(latest.affected_gap_ids_json)
                except ValueError:
                    affected_ids = []
            start_time, end_time = latest.start_time, latest.end_time

            data_advanced = (
                current_version_no is not None
                and based_on is not None
                and current_version_no > based_on
            )

            if not data_advanced:
                should, reason = False, "up_to_date"
            elif policy == "always":
                should, reason = True, "data_version_advanced"
            elif policy == "manual":
                should, reason = False, "data_version_advanced_manual_policy"
            elif policy == "on_gap_filled":
                filled_after = [
                    gid for gid in affected_ids
                    if (gap := lm.get_gap(gid)) and gap.status in ("filled", "verified")
                ]
                should = bool(filled_after)
                reason = "affected_gap_filled" if should else "no_affected_gap_filled"
            else:  # on_open_gap
                open_gaps = [
                    g for g in self.lineage.get_gaps_affecting_range(
                        stock_code, period, start_time, end_time
                    ) if g["affected"]
                ]
                should = bool(open_gaps)
                reason = "open_gap_in_range" if should else "no_open_gap"

            latest_version = latest.result_version

        if policy == "manual":
            return {
                "recomputed": False,
                "should_recompute": data_advanced,
                "reason": reason,
                "policy": policy,
            }

        if not should:
            return {"recomputed": False, "reason": reason, "policy": policy,
                    "current_result_version": latest_version}

        new_summary = self.run_analysis(stock_code, period, start_time, end_time)
        return {
            "recomputed": True,
            "reason": reason,
            "policy": policy,
            "previous_result_version": latest_version,
            "new_result": new_summary,
        }
