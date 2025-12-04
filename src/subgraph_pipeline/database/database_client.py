"""
Database client resource for Dagster.
Handles connection pooling and session management.
"""

from contextlib import contextmanager
from typing import Generator

import dagster as dg
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session


class DatabaseClient(dg.ConfigurableResource):
    """
    SQLAlchemy database client for Postgres.

    Config:
        connection_string: PostgreSQL connection string
        pool_size: Connection pool size (default: 5)
        max_overflow: Max overflow connections (default: 10)
    """

    connection_string: str
    pool_size: int = 5
    max_overflow: int = 10

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialize engine and session factory."""
        self._engine = create_engine(
            self.connection_string,
            pool_pre_ping=True,  # Verify connections before using
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            echo=True,  # Set to True for SQL debugging
        )
        self._session_factory = sessionmaker(bind=self._engine)

        context.log.info(f"Database client initialized with pool_size={self.pool_size}")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.

        Usage:
            with db_client.get_session() as session:
                session.execute(...)
                session.commit()
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    @property
    def engine(self) -> Engine:
        """Direct access to SQLAlchemy engine for utilities."""
        return self._engine

    def execute_query(self, query: str, params: dict = None):
        """
        Execute a raw SQL query and return results.
        Useful for debugging or one-off queries.
        """
        with self.get_session() as session:
            result = session.execute(query, params or {})
            return result.fetchall()

    def teardown_after_execution(self, context: dg.InitResourceContext) -> None:
        """Clean up connections."""
        if hasattr(self, "_engine"):
            self._engine.dispose()
            context.log.info("Database connections disposed")
