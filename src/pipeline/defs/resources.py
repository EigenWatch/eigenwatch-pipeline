from dagster import ConfigurableResource
from sqlalchemy import create_engine, Engine
from redis import Redis
import pandas as pd
import dagster as dg
from sqlmodel import SQLModel, Session


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


class SQLModelAnalyticsDBResource(AnalyticsDBResource):
    """Enhanced analytics database resource with SQLModel support"""

    def create_tables(self):
        """Create all SQLModel tables"""
        engine = self.get_engine()
        SQLModel.metadata.create_all(engine)

    def validate_and_store_dataframe(
        self, df: pd.DataFrame, model_class: SQLModel, validate_rows: bool = False
    ):
        """Store DataFrame with optional row-by-row validation"""

        if validate_rows:
            # Slower but more robust - validates each row
            validated_records = []
            errors = []

            for idx, row in df.iterrows():
                try:
                    # Convert row to dict and validate through SQLModel
                    row_dict = row.to_dict()
                    validated_record = model_class(**row_dict)
                    validated_records.append(validated_record)
                except Exception as e:
                    errors.append(f"Row {idx}: {str(e)}")

            if errors:
                raise ValueError(f"Validation errors: {'; '.join(errors[:5])}")

            # Bulk insert validated records
            engine = self.get_engine()
            with Session(engine) as session:
                session.add_all(validated_records)
                session.commit()

        else:
            # Faster bulk insert using pandas
            engine = self.get_engine()
            df.to_sql(
                model_class.__tablename__,
                engine,
                if_exists="append",
                index=False,
                method="multi",
            )


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


subgraph_db = SubgraphDBResource(
    db_uri="postgresql://graph-node:eigenwatch-2k25@localhost:5433/graph-node",
    deployment_hash="sgd2",  # TODO: Find a way to make this dynamic so we do not have to change it with every new deployment
)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "subgraph_db": subgraph_db,
            # "analytics_db": AnalyticsDBResource(db_uri=""),
            # "redis": RedisResource(),
        }
    )
