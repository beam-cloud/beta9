"""Database connection and session management."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .environment import Base


class Database:
    """Database connection manager."""

    def __init__(self, database_url: str):
        """
        Initialize database connection.

        Args:
            database_url: PostgreSQL connection URL
                          e.g., postgresql://user:pass@localhost:5432/provisioning
        """
        self.engine = create_engine(
            database_url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # Check connection health
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        Base.metadata.create_all(bind=self.engine)

    def drop_tables(self) -> None:
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(bind=self.engine)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Context manager for database sessions.

        Usage:
            with db.session() as session:
                env = session.query(Environment).filter_by(id=env_id).first()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# Global database instance (initialized in app startup)
_db: Database | None = None


def init_db(database_url: str) -> Database:
    """
    Initialize global database instance.

    Args:
        database_url: PostgreSQL connection URL

    Returns:
        Database instance
    """
    global _db
    _db = Database(database_url)
    _db.create_tables()
    return _db


def get_db() -> Database:
    """
    Get global database instance.

    Returns:
        Database instance

    Raises:
        RuntimeError: If database not initialized
    """
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db
