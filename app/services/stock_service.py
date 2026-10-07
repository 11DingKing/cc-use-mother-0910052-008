from datetime import datetime
from typing import List, Optional, Dict, Any
import logging
from app.config import db_session_scope
from app.mappers.stock_mapper import StockMapper
from app.mappers.lineage_mapper import LineageMapper
from app.data.fetcher import FetchResult
from app.data.akshare_fetcher import AKShareFetcher
from app.data.yahoo_fetcher import YahooFetcher
from app.chan.models import RawCandle
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import DataFetchException
from app.services.lineage_service import DataLineageService
logger = logging.getLogger(__name__)
class StockService:
    def __init__(self):
        self.fetchers = self._init_fetchers()
        self.lineage = DataLineageService()
    def _init_fetchers(self):
        fetchers = {}
        try:
            akshare = AKShareFetcher()
            if akshare.is_available():
                fetchers['akshare'] = akshare
        except Exception as e:
            logger.warning(f'AKShare not available: {e}')
        try:
            yahoo = YahooFetcher()
            if yahoo.is_available():
                fetchers['yahoo'] = yahoo
        except Exception as e:
            logger.warning(f'Yahoo Finance not available: {e}')
        return fetchers
    def get_candles(self, stock_code, period, start_date=None, end_date=None, use_cache=True):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        # 只有调用方明确给出窗口终点时才据此检测尾部缺口，
        # 避免“抓取至今”把尚未到达的最新分钟线误登记为缺口
        explicit_end = end_date is not None
        start_date, end_date = validate_time_range(start_date, end_date)
        if use_cache:
            cached = self._get_from_cache(stock_code, period, start_date, end_date)
            if cached:
                return cached
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        if not result.success:
            raise DataFetchException(message=result.error_message or 'Failed', stock_code=stock_code, source=result.source)
        if result.candles:
            self._save_to_cache(
                result.candles, stock_code, period, result.source,
                requested_end=end_date if explicit_end else None,
            )
        return result.candles
    def get_candles_with_lineage(
        self, stock_code, period, start_date=None, end_date=None
    ) -> Dict[str, Any]:
        """读取当前K线，并附带每根数据的血缘信息、区间内缺口与序列版本。"""
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        with db_session_scope() as session:
            mapper = StockMapper(session)
            entities = mapper.get_candles(stock_code, period, start_date, end_date)
            candles = mapper.to_raw_candles(entities)
            lineage = [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "version_no": e.current_version_no,
                    "source": e.current_source,
                    "batch_id": e.last_batch_id,
                }
                for e in entities
            ]
            dv = LineageMapper(session).get_current_data_version(stock_code, period)
            data_version_no = dv.version_no if dv else None
        gaps = self.lineage.get_gaps_affecting_range(
            stock_code, period, start_date, end_date
        )
        return {
            "stock_code": stock_code,
            "period": period,
            "data_version_no": data_version_no,
            "count": len(candles),
            "candles": candles,
            "lineage": lineage,
            "gaps": gaps,
        }
    def get_candles_from_snapshot(self, snapshot_name: str) -> List[RawCandle]:
        """按不可变快照读取历史K线，补数不影响已发布报告。"""
        return self.lineage.get_snapshot_candles(snapshot_name)
    def _get_from_cache(self, stock_code, period, start_date, end_date):
        try:
            with db_session_scope() as session:
                mapper = StockMapper(session)
                candles = mapper.get_candles(stock_code, period, start_date, end_date)
                if not candles:
                    return None
                return mapper.to_raw_candles(candles)
        except Exception as e:
            logger.warning(f'Cache read error: {e}')
            return None
    def _save_to_cache(self, candles, stock_code, period, source='unknown', requested_end=None):
        try:
            return self.lineage.ingest(
                candles,
                stock_code=stock_code,
                period=period,
                source=source or 'unknown',
                batch_type='scheduled',
                requested_end=requested_end,
            )
        except Exception as e:
            logger.warning(f'Cache write error: {e}')
            return None
    def _fetch_from_source(self, stock_code, period, start_date, end_date):
        sources = ['akshare', 'yahoo'] if stock_code[0].isdigit() else ['yahoo', 'akshare']
        last_error = None
        for source_name in sources:
            fetcher = self.fetchers.get(source_name)
            if not fetcher:
                continue
            try:
                result = fetcher.fetch_candles(stock_code, period, start_date, end_date)
                if result.success and result.candles:
                    return result
                last_error = result.error_message
            except Exception as e:
                last_error = str(e)
        return FetchResult([], stock_code, period, start_date, end_date, 'none', False, last_error)
    def backfill_gap_from_provider(
        self,
        gap_id: int,
        source: Optional[str] = None,
        conflict_policy: str = "trust_priority",
    ) -> Dict[str, Any]:
        """登记缺口后直接向行情商抓取缺口区间补数。

        抓不到（离线/服务商仍缺）时不写数据，只返回抓取失败信息，
        缺口维持原状态，便于研究员稍后重试（重试产生新批次）。
        """
        gap = self.lineage.get_gap_dict(gap_id)
        if gap is None:
            raise KeyError(f"gap {gap_id} not found")

        result = self._fetch_gap(gap, source)

        if not result.success or not result.candles:
            return {
                "gap_id": gap_id,
                "fetched": False,
                "source": result.source,
                "error": result.error_message,
            }

        return self.lineage.backfill_gap(
            gap_id, result.candles,
            source=result.source, conflict_policy=conflict_policy,
        )

    def _fetch_gap(self, gap: Dict[str, Any], preferred_source: Optional[str]):
        start = datetime.fromisoformat(gap["gap_start"])
        end = datetime.fromisoformat(gap["gap_end"])
        if preferred_source and preferred_source in self.fetchers:
            return self.fetchers[preferred_source].fetch_candles(
                gap["stock_code"], gap["period"], start, end
            )
        return self._fetch_from_source(
            gap["stock_code"], gap["period"], start, end
        )

    def fetch_and_update(self, stock_code, period, start_date=None, end_date=None):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        ingest_summary = None
        if result.success and result.candles:
            ingest_summary = self._save_to_cache(
                result.candles, stock_code, period, result.source, end_date
            )
        return {
            'stock_code': stock_code,
            'period': period,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
            'success': result.success,
            'count': len(result.candles),
            'source': result.source,
            'error': result.error_message,
            'ingest': ingest_summary,
        }
