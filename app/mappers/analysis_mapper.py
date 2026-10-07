"""业务模块说明。"""

import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.entities.analysis_result import AnalysisResult
from app.chan.serializer import ChanSerializer
from app.chan.models import Fractal, Bi, Duan, Zhongshu, Signal


class AnalysisMapper:
    """业务模块说明。"""
    
    def __init__(self, session: Session):
        self.session = session
        self.serializer = ChanSerializer()
    
    def create(self, result: AnalysisResult) -> AnalysisResult:
        """业务模块说明。"""
        self.session.add(result)
        self.session.flush()
        return result
    
    def get_by_id(self, result_id: int) -> Optional[AnalysisResult]:
        """业务模块说明。"""
        return self.session.query(AnalysisResult).filter(
            AnalysisResult.id == result_id
        ).first()
    
    def get_latest(
        self,
        stock_code: str,
        period: str,
    ) -> Optional[AnalysisResult]:
        """业务模块说明。"""
        return self.session.query(AnalysisResult).filter(
            and_(
                AnalysisResult.stock_code == stock_code,
                AnalysisResult.period == period,
                AnalysisResult.is_current == 1,
            )
        ).order_by(AnalysisResult.result_version.desc()).first()

    def get_version(
        self, stock_code: str, period: str, result_version: int
    ) -> Optional[AnalysisResult]:
        """按版本号读取历史结果（含已发布报告），不随补数变化。"""
        return self.session.query(AnalysisResult).filter(
            and_(
                AnalysisResult.stock_code == stock_code,
                AnalysisResult.period == period,
                AnalysisResult.result_version == result_version,
            )
        ).first()

    def list_versions(
        self, stock_code: str, period: str, limit: Optional[int] = None
    ) -> List[AnalysisResult]:
        query = self.session.query(AnalysisResult).filter(
            and_(
                AnalysisResult.stock_code == stock_code,
                AnalysisResult.period == period,
            )
        ).order_by(AnalysisResult.result_version.desc())

        if limit:
            query = query.limit(limit)

        return query.all()
    
    def update(self, result: AnalysisResult) -> AnalysisResult:
        """业务模块说明。"""
        self.session.merge(result)
        self.session.flush()
        return result
    
    def delete(self, result_id: int) -> bool:
        """业务模块说明。"""
        result = self.get_by_id(result_id)
        if result:
            self.session.delete(result)
            self.session.flush()
            return True
        return False
    
    def delete_by_stock_period(self, stock_code: str, period: str) -> int:
        """业务模块说明。"""
        count = self.session.query(AnalysisResult).filter(
            and_(
                AnalysisResult.stock_code == stock_code,
                AnalysisResult.period == period,
            )
        ).delete(synchronize_session=False)
        self.session.flush()
        return count
    
    def save_analysis(
        self,
        stock_code: str,
        period: str,
        start_time: datetime,
        end_time: datetime,
        fractals: List[Fractal],
        bis: List[Bi],
        duans: List[Duan],
        zhongshus: List[Zhongshu],
        signals: List[Signal],
        data_version_no: Optional[int] = None,
        based_on_batch_id: Optional[str] = None,
        affected_gap_ids: Optional[List[int]] = None,
        recompute_policy: Optional[str] = None,
    ) -> AnalysisResult:
        """追加一个新的分析结果版本；旧版本（含已发布报告）原样保留。"""
        fractals_json = json.dumps([self.serializer.serialize(f) for f in fractals])
        bis_json = json.dumps([self.serializer.serialize(b) for b in bis])
        duans_json = json.dumps([self.serializer.serialize(d) for d in duans])
        zhongshus_json = json.dumps([self.serializer.serialize(z) for z in zhongshus])
        signals_json = json.dumps([self.serializer.serialize(s) for s in signals])

        latest_signal = signals[-1] if signals else None

        previous = self.get_latest(stock_code, period)
        next_version = (previous.result_version + 1) if previous else 1

        result = AnalysisResult(
            stock_code=stock_code,
            period=period,
            start_time=start_time,
            end_time=end_time,
            fractals_json=fractals_json,
            bis_json=bis_json,
            duans_json=duans_json,
            zhongshus_json=zhongshus_json,
            signals_json=signals_json,
            fractal_count=len(fractals),
            bi_count=len(bis),
            duan_count=len(duans),
            zhongshu_count=len(zhongshus),
            signal_count=len(signals),
            result_version=next_version,
            is_current=1,
            parent_result_id=previous.id if previous else None,
            status="draft",
            data_version_no=data_version_no,
            based_on_batch_id=based_on_batch_id,
            affected_gap_ids_json=(
                json.dumps(affected_gap_ids or [], ensure_ascii=False)
            ),
            recompute_policy=recompute_policy,
        )

        if latest_signal:
            result.latest_signal_type = latest_signal.signal_type.value
            result.latest_signal_time = latest_signal.timestamp
            result.latest_signal_price = latest_signal.price

        if previous is not None:
            previous.is_current = 0
            previous.superseded_by_id = None  # 新行flush后再回填
        self.session.add(result)
        self.session.flush()

        if previous is not None:
            previous.superseded_by_id = result.id
            previous.superseded_at = datetime.utcnow()
        self.session.flush()
        return result

    def publish(self, result: AnalysisResult, snapshot_name: str) -> AnalysisResult:
        """发布报告：绑定数据快照并锁定，后续补数/重算均不再改动该行。"""
        result.status = "published"
        result.published_at = datetime.utcnow()
        result.snapshot_name = snapshot_name
        self.session.flush()
        return result
    
    def load_fractals(self, result: AnalysisResult) -> List[Fractal]:
        """业务模块说明。"""
        if not result.fractals_json:
            return []
        data = json.loads(result.fractals_json)
        return [self.serializer.deserialize(d, Fractal) for d in data]
    
    def load_bis(self, result: AnalysisResult) -> List[Bi]:
        """业务模块说明。"""
        if not result.bis_json:
            return []
        data = json.loads(result.bis_json)
        return [self.serializer.deserialize(d, Bi) for d in data]
    
    def load_duans(self, result: AnalysisResult) -> List[Duan]:
        """业务模块说明。"""
        if not result.duans_json:
            return []
        data = json.loads(result.duans_json)
        return [self.serializer.deserialize(d, Duan) for d in data]
    
    def load_zhongshus(self, result: AnalysisResult) -> List[Zhongshu]:
        """业务模块说明。"""
        if not result.zhongshus_json:
            return []
        data = json.loads(result.zhongshus_json)
        return [self.serializer.deserialize(d, Zhongshu) for d in data]
    
    def load_signals(self, result: AnalysisResult) -> List[Signal]:
        """业务模块说明。"""
        if not result.signals_json:
            return []
        data = json.loads(result.signals_json)
        return [self.serializer.deserialize(d, Signal) for d in data]
    
    def load_all(self, result: AnalysisResult) -> Dict[str, Any]:
        """业务模块说明。"""
        return {
            "fractals": self.load_fractals(result),
            "bis": self.load_bis(result),
            "duans": self.load_duans(result),
            "zhongshus": self.load_zhongshus(result),
            "signals": self.load_signals(result),
        }
    
    def get_stocks_with_signals(
        self,
        signal_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[AnalysisResult]:
        """业务模块说明。"""
        query = self.session.query(AnalysisResult).filter(
            AnalysisResult.latest_signal_type.isnot(None)
        )
        
        if signal_type:
            query = query.filter(AnalysisResult.latest_signal_type == signal_type)
        
        return query.order_by(
            AnalysisResult.latest_signal_time.desc()
        ).limit(limit).all()
