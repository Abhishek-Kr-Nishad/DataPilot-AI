"""SQLAlchemy 2.0 engine, session factory, and schema initialization."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from datapilot.core.config import AppSettings
from datapilot.core.logging import get_logger
from datapilot.models.orm import Base

logger = get_logger("datapilot.database")


class DatabaseManager:
    """Manages SQLAlchemy Engine, SessionLocal, and automatic table creation."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.is_sqlite = database_url.startswith("sqlite")
        self.engine = self._build_engine(database_url)
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
        self.create_tables()

    @staticmethod
    def _build_engine(database_url: str) -> Engine:
        if database_url.startswith("sqlite"):
            if ":memory:" in database_url:
                return create_engine(
                    database_url,
                    connect_args={"check_same_thread": False},
                    poolclass=StaticPool,
                )
            # Ensure SQLite parent directory exists and enable WAL for concurrent API reads
            raw_path = database_url.replace("sqlite:///", "", 1)
            Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine(
                database_url,
                connect_args={"check_same_thread": False, "timeout": 30},
            )

            @event.listens_for(engine, "connect")
            def _configure_sqlite_pragmas(dbapi_conn, _connection_record) -> None:
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA busy_timeout=5000;")
                cursor.close()

            return engine
        return create_engine(database_url, pool_pre_ping=True)

    def create_tables(self) -> None:
        """Create ORM tables if they do not exist."""
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """Provide a transactional SQLAlchemy session scope."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def create_database_manager(settings: AppSettings) -> DatabaseManager:
    """Create a DatabaseManager configured for PostgreSQL or local SQLite."""
    db_url = settings.metadata.database_url
    logger.info("Initializing SQLAlchemy DatabaseManager (dialect=%s)", db_url.split(":", 1)[0])
    return DatabaseManager(db_url)
