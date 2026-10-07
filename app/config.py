"""业务模块说明。"""

import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager


# 项目根目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 数据库配置
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'chan_trading.db'}"
)

# SQLAlchemy 引擎和会话
_engine = None
_SessionLocal = None


def get_engine():
    """业务模块说明。"""
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
            echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        )
    return _engine


def get_session_factory():
    """业务模块说明。"""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_engine(),
        )
    return _SessionLocal


def get_db_session() -> Session:
    """业务模块说明。"""
    SessionLocal = get_session_factory()
    return SessionLocal()


@contextmanager
def db_session_scope():
    """业务模块说明。"""
    session = get_db_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_database():
    """业务模块说明。"""
    from app.entities.stock import Base as StockBase
    from app.entities.analysis_result import Base as AnalysisBase
    from app.entities.watchlist import Base as WatchlistBase
    from app.entities.backtest import Base as BacktestBase
    from app.entities.lineage import Base as LineageBase

    engine = get_engine()

    # 创建所有表
    StockBase.metadata.create_all(bind=engine)
    AnalysisBase.metadata.create_all(bind=engine)
    WatchlistBase.metadata.create_all(bind=engine)
    BacktestBase.metadata.create_all(bind=engine)
    LineageBase.metadata.create_all(bind=engine)

    # 对既有 SQLite/其他库补齐新增列（开发期轻量迁移，等价于列级 ADD COLUMN）
    _ensure_columns(engine)


def _ensure_columns(engine):
    """为旧库补加新版本引入的列；新库的 create_all 已包含，跳过即可。"""
    from sqlalchemy import text, inspect

    required = {
        "stock_candles": [
            ("current_version_id", "INTEGER"),
            ("current_version_no", "INTEGER DEFAULT 1"),
            ("current_source", "VARCHAR(32)"),
            ("last_batch_id", "VARCHAR(32)"),
        ],
        "analysis_results": [
            ("result_version", "INTEGER DEFAULT 1"),
            ("is_current", "INTEGER DEFAULT 1"),
            ("parent_result_id", "INTEGER"),
            ("superseded_by_id", "INTEGER"),
            ("superseded_at", "DATETIME"),
            ("status", "VARCHAR(16) DEFAULT 'draft'"),
            ("published_at", "DATETIME"),
            ("data_version_no", "INTEGER"),
            ("based_on_batch_id", "VARCHAR(32)"),
            ("affected_gap_ids_json", "TEXT"),
            ("snapshot_name", "VARCHAR(128)"),
            ("recompute_policy", "VARCHAR(32)"),
        ],
        "backtest_results": [
            ("result_version", "INTEGER DEFAULT 1"),
            ("is_current", "INTEGER DEFAULT 1"),
            ("parent_result_id", "INTEGER"),
            ("data_version_no", "INTEGER"),
            ("based_on_batch_id", "VARCHAR(32)"),
            ("snapshot_name", "VARCHAR(128)"),
            ("affected_gap_ids_json", "TEXT"),
            ("report_status", "VARCHAR(16) DEFAULT 'draft'"),
            ("published_at", "DATETIME"),
        ],
    }

    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    with engine.begin() as conn:
        for table, columns in required.items():
            if table not in existing_tables:
                continue
            present = {col["name"] for col in inspector.get_columns(table)}
            for name, ddl in columns:
                if name not in present:
                    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}"))

        # 旧库已有的K线没有行版本：补建 source=legacy 的 v1 并回填指针，
        # 使历史数据同样可追溯、可进入快照
        if "stock_candles" in existing_tables:
            conn.execute(text(
                """
                INSERT INTO candle_data_versions
                  (candle_id, version_no, stock_code, period, timestamp,
                   open, high, low, close, volume, amount,
                   import_batch_id, source, ingest_order, status, change_reason, created_at)
                SELECT s.id, 1, s.stock_code, s.period, s.timestamp,
                       s.open, s.high, s.low, s.close, COALESCE(s.volume, 0), s.amount,
                       NULL, 'legacy', 0, 'active', 'normal', CURRENT_TIMESTAMP
                FROM stock_candles s
                WHERE s.current_version_id IS NULL
                """
            ))
            conn.execute(text(
                """
                UPDATE stock_candles SET
                  current_version_id = (
                    SELECT cdv.id FROM candle_data_versions cdv
                    WHERE cdv.candle_id = stock_candles.id AND cdv.version_no = 1
                  ),
                  current_source = COALESCE(current_source, 'legacy'),
                  current_version_no = COALESCE(current_version_no, 1)
                WHERE current_version_id IS NULL
                """
            ))

# 数据源配置
DATA_SOURCE_CONFIG = {
    # 默认数据源：akshare, yahoo
    "default_source": os.getenv("DEFAULT_DATA_SOURCE", "akshare"),

    # AKShare 配置
    "akshare": {
        "enabled": True,
        "timeout": int(os.getenv("AKSHARE_TIMEOUT", "30")),
    },

    # Yahoo Finance 配置
    "yahoo": {
        "enabled": True,
        "timeout": int(os.getenv("YAHOO_TIMEOUT", "30")),
    },
}

# 支持的K线周期
SUPPORTED_PERIODS = ["daily", "60min", "30min"]

# FastAPI 应用配置
APP_CONFIG = {
    "title": "缠论量化交易系统",
    "description": "基于缠论的全栈量化交易平台 API",
    "version": "1.0.0",
    "host": os.getenv("APP_HOST", "0.0.0.0"),
    "port": int(os.getenv("APP_PORT", "8000")),
    "debug": os.getenv("APP_DEBUG", "true").lower() == "true",
}

# 日志配置
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
}

# CORS 配置
CORS_CONFIG = {
    "allow_origins": os.getenv("CORS_ORIGINS", "*").split(","),
    "allow_credentials": True,
    "allow_methods": ["*"],
    "allow_headers": ["*"],
}
