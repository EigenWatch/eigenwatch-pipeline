# analytics_pipeline/resources.py
"""
Dagster Resources for database connections and configuration
"""
from dagster import ConfigurableResource
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker
from redis import Redis
from contextlib import contextmanager
import pandas as pd
import dagster as dg
import os


class SubgraphDBResource(ConfigurableResource):
    """Connection to subgraph PostgreSQL database containing raw blockchain data"""

    db_uri: str
    deployment_hash: str

    def get_engine(self) -> Engine:
        return create_engine(self.db_uri, pool_pre_ping=True, pool_recycle=3600)

    def execute_query(self, query: str) -> pd.DataFrame:
        # Replace placeholder with actual deployment hash
        resolved_query = query.format(deployment_hash=self.deployment_hash)
        with self.get_engine().connect() as conn:
            return pd.read_sql(resolved_query, conn)


class AnalyticsDBResource(ConfigurableResource):
    """Connection to analytics PostgreSQL database for calculated metrics"""

    db_uri: str

    def get_engine(self) -> Engine:
        return create_engine(self.db_uri, pool_pre_ping=True, pool_recycle=3600)

    def store_dataframe(
        self, df: pd.DataFrame, table_name: str, if_exists: str = "replace"
    ):
        with self.get_engine().connect() as conn:
            df.to_sql(
                table_name, conn, if_exists=if_exists, index=False, method="multi"
            )

    def execute_query(self, query: str) -> pd.DataFrame:
        with self.get_engine().connect() as conn:
            return pd.read_sql(query, conn)

    def create_tables(self):
        """Create all tables using SQLAlchemy metadata"""
        # from db.schema import Base

        # engine = self.get_engine()
        # Base.metadata.create_all(engine)


class RedisResource(ConfigurableResource):
    """Redis connection for caching real-time metrics"""

    host: str = "localhost"
    port: int = 6379
    db: int = 0

    def get_client(self) -> Redis:
        return Redis(host=self.host, port=self.port, db=self.db, decode_responses=True)

    def invalidate_pattern(self, pattern: str):
        """Invalidate cache keys matching pattern"""
        redis_client = self.get_client()
        keys = redis_client.keys(pattern)
        if keys:
            redis_client.delete(*keys)


# Resource instances
subgraph_db = SubgraphDBResource(
    db_uri="postgresql://graph-node:eigenwatch-2k25@localhost:5433/graph-node",
    deployment_hash="sgd2",  # TODO: Find a way to make this dynamic so we do not have to change it with every new deployment
)

analytics_db = AnalyticsDBResource(
    db_uri="postgresql+psycopg2://postgres:secret@localhost:5432/analytics"
)


class DatabaseResource(ConfigurableResource):
    """Database resource for managing connections to both event and analytics databases"""

    events_db_url: str = os.getenv("EVENTS_DB_URL")
    analytics_db_url: str = os.getenv("ANALYTICS_DB_URL")

    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30

    def __init__(self, **data):
        super().__init__(**data)
        self._events_engine = None
        self._analytics_engine = None
        self._EventsSessionLocal = None
        self._AnalyticsSessionLocal = None

    @property
    def events_engine(self):
        """Lazy initialization of events database engine"""
        if self._events_engine is None:
            self._events_engine = create_engine(
                self.events_db_url,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                echo=False,
            )
        return self._events_engine

    @property
    def analytics_engine(self):
        """Lazy initialization of analytics database engine"""
        if self._analytics_engine is None:
            self._analytics_engine = create_engine(
                self.analytics_db_url,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                echo=False,
            )
        return self._analytics_engine

    @property
    def EventsSessionLocal(self):
        """Session factory for events database"""
        if self._EventsSessionLocal is None:
            self._EventsSessionLocal = sessionmaker(
                bind=self.events_engine,
                expire_on_commit=False,
            )
        return self._EventsSessionLocal

    @property
    def AnalyticsSessionLocal(self):
        """Session factory for analytics database"""
        if self._AnalyticsSessionLocal is None:
            self._AnalyticsSessionLocal = sessionmaker(
                bind=self.analytics_engine,
                expire_on_commit=False,
            )
        return self._AnalyticsSessionLocal

    @contextmanager
    def get_events_session(self):
        """Context manager for events database session"""
        session = self.EventsSessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @contextmanager
    def get_analytics_session(self):
        """Context manager for analytics database session"""
        session = self.AnalyticsSessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: dict = None, db: str = "events"):
        """Execute a raw SQL query and return results"""
        engine = self.events_engine if db == "events" else self.analytics_engine
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result.fetchall()

    def execute_update(self, query: str, params: dict = None, db: str = "analytics"):
        """Execute an UPDATE/INSERT/DELETE query"""
        engine = self.events_engine if db == "events" else self.analytics_engine
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            return result.rowcount


class ConfigResource(ConfigurableResource):
    """Configuration resource for pipeline settings"""

    # Checkpoint settings
    checkpoint_table: str = "pipeline_checkpoints"
    checkpoint_key: str = "analytics_pipeline_v1"

    # Safety buffer (blocks to lag behind latest to avoid race conditions)
    safety_buffer_blocks: int = 10
    safety_buffer_seconds: int = 60

    # Batch processing
    max_operators_per_batch: int = 100
    max_blocks_per_run: int = 1000

    # Snapshot settings
    snapshot_hour_utc: int = 0  # Create snapshots at midnight UTC

    # Analytics settings
    volatility_windows: list = [7, 30, 90]  # Days
    min_data_points_for_analytics: int = 7

    # Monitoring
    enable_detailed_logging: bool = True
    log_batch_progress_every: int = 10  # Log every N operators

    # Performance
    use_bulk_operations: bool = True
    commit_batch_size: int = 50

    def get_checkpoint_query(self) -> str:
        """Get query for retrieving checkpoint"""
        return f"""
            SELECT 
                last_processed_at,
                last_processed_block,
                operators_processed_count,
                run_metadata
            FROM {self.checkpoint_table}
            WHERE pipeline_name = :pipeline_name
        """

    def get_update_checkpoint_query(self) -> str:
        """Get query for updating checkpoint"""
        return f"""
            INSERT INTO {self.checkpoint_table} (
                pipeline_name,
                last_processed_at,
                last_processed_block,
                operators_processed_count,
                total_events_processed,
                run_duration_seconds,
                run_metadata
            ) VALUES (
                :pipeline_name,
                :last_processed_at,
                :last_processed_block,
                :operators_processed_count,
                :total_events_processed,
                :run_duration_seconds,
                :run_metadata
            )
            ON CONFLICT (pipeline_name) 
            DO UPDATE SET
                last_processed_at = EXCLUDED.last_processed_at,
                last_processed_block = EXCLUDED.last_processed_block,
                operators_processed_count = EXCLUDED.operators_processed_count,
                total_events_processed = EXCLUDED.total_events_processed,
                run_duration_seconds = EXCLUDED.run_duration_seconds,
                run_metadata = EXCLUDED.run_metadata
        """
