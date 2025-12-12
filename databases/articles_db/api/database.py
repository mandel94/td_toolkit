"""
Database connection and session management
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator
import logging

from config import get_settings

logger = logging.getLogger(__name__)

# Create declarative base for models
Base = declarative_base()


class DatabaseManager:
    """Manages database connections and sessions"""
    
    def __init__(self):
        """Initialize database engine and session factory"""
        settings = get_settings()
        
        self.engine = create_engine(
            settings.database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )
        
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info("Database engine initialized")
    
    def get_session(self) -> Session:
        """Get a new database session"""
        return self.SessionLocal()
    
    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope for database operations
        
        Usage:
            with db_manager.session_scope() as session:
                # perform database operations
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def create_tables(self):
        """Create all tables in the database"""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created")
    
    def dispose(self):
        """Dispose of the database engine"""
        self.engine.dispose()
        logger.info("Database engine disposed")


# Global database manager instance
db_manager = DatabaseManager()


def get_db() -> Generator[Session, None, None]:
    """
    Dependency for FastAPI endpoints to get database sessions
    
    Usage in FastAPI:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            # use db session
    """
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()
