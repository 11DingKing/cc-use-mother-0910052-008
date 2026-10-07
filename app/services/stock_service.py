import logging
from app.config import db_session_scope
from app.mappers.stock_mapper import StockMapper
from app.data.fetcher import FetchResult
from app.data.akshare_fetcher import AKShareFetcher
from app.data.yahoo_fetcher import YahooFetcher
from app.services.lineage_service import LineageService
from app.utils.validators import validate_stock_code, validate_time_range, validate_period
from app.middleware.exception_handler import DataFetchException
logger = logging.getLogger(__name__)
class StockService:
    def __init__(self):
        self.fetchers = self._init_fetchers()
        self.lineage_service = LineageService(fetch_callback=self._fetch_callback)
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
        start_date, end_date = validate_time_range(start_date, end_date)
        if use_cache:
            cached = self._get_from_cache(stock_code, period, start_date, end_date)
            if cached:
                return cached
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        if not result.success:
            raise DataFetchException(message=result.error_message or 'Failed', stock_code=stock_code, source=result.source)
        if result.candles:
            self._ingest_fetched(result, stock_code, period)
        # 以库中去重/裁决后的版本为准，避免把未采纳的冲突值当成最新数据
        cached = self._get_from_cache(stock_code, period, start_date, end_date)
        return cached or result.candles
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
    def _ingest_fetched(self, result: FetchResult, stock_code: str, period: str):
        """常规导入经血缘通道落库：去重、乱序计数、来源冲突挂起、缺口登记。"""
        try:
            self.lineage_service.ingest_candles(
                stock_code=stock_code,
                period=period,
                candles=result.candles,
                source=result.source,
                kind="import",
                trigger="scheduled",
            )
        except Exception as e:
            logger.warning(f'Lineage ingest error: {e}')
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
    def _fetch_callback(self, source_name, stock_code, period, start_date, end_date):
        """补数通道使用的单来源取数回调。"""
        fetcher = self.fetchers.get(source_name)
        if fetcher is None:
            return None, f"数据源 {source_name} 不可用"
        try:
            result = fetcher.fetch_candles(stock_code, period, start_date, end_date)
            return result.candles, (None if result.success else result.error_message)
        except Exception as e:
            return None, str(e)
    def fetch_and_update(self, stock_code, period, start_date=None, end_date=None):
        stock_code = validate_stock_code(stock_code)
        period = validate_period(period)
        start_date, end_date = validate_time_range(start_date, end_date)
        result = self._fetch_from_source(stock_code, period, start_date, end_date)
        ingest_summary = None
        if result.success and result.candles:
            ingest_summary = self._ingest_fetched_sync(result, stock_code, period)
        return {'stock_code': stock_code, 'period': period, 'start_date': start_date.isoformat(), 'end_date': end_date.isoformat(), 'success': result.success, 'count': len(result.candles), 'source': result.source, 'error': result.error_message, 'ingestion': ingest_summary}
    def _ingest_fetched_sync(self, result: FetchResult, stock_code: str, period: str):
        return self.lineage_service.ingest_candles(
            stock_code=stock_code,
            period=period,
            candles=result.candles,
            source=result.source,
            kind="import",
            trigger="manual_fetch",
        )
