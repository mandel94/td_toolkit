"""Database loader for dimensional model (star schema)."""
import sys
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from contextlib import contextmanager
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

# Import models from the API (reuse existing ORM models)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "databases", "articles_db", "api")))
from models import DimWeek, DimArticle, DimAuthor, DimCategory, FactWeeklyMetrics
from database import Base

from etl.articles_db_pipeline.config.database import DATABASE_URL
from etl.articles_db_pipeline.models.article import ProcessedWeeklyBatch


class DatabaseLoader:
    """Handle database operations for dimensional model."""
    
    def __init__(self, database_url: str = DATABASE_URL):
        self.database_url = database_url
        self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=300)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.engine)
        
        logger.info("Initialized DatabaseLoader for dimensional model")
    
    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {str(e)}")
            raise
        finally:
            session.close()
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False
    
    def load_weekly_batch(self, batch: ProcessedWeeklyBatch) -> Dict[str, Any]:
        """Load a complete weekly batch into dimensional tables.
        
        Args:
            batch: ProcessedWeeklyBatch with all dimensional data
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading weekly batch {batch.batch_id}")
        
        stats = {
            'weeks_loaded': 0,
            'articles_loaded': 0,
            'authors_loaded': 0,
            'categories_loaded': 0,
            'metrics_loaded': 0,
            'errors': 0
        }
        
        try:
            with self.get_session() as session:
                # Load dimensions first (order matters due to foreign keys)
                stats['weeks_loaded'] = self._load_dim_weeks(session, batch.weeks)
                stats['authors_loaded'] = self._load_dim_authors(session, batch.authors)
                stats['categories_loaded'] = self._load_dim_categories(session, batch.categories)
                stats['articles_loaded'] = self._load_dim_articles(session, batch.articles)
                
                # Load fact table
                stats['metrics_loaded'] = self._load_fact_metrics(session, batch.metrics)
                
                logger.success(f"Batch {batch.batch_id} loaded successfully: {stats}")
                
        except Exception as e:
            logger.error(f"Failed to load batch: {str(e)}")
            stats['errors'] += 1
            raise
        
        return stats
    
    def _load_dim_weeks(self, session: Session, weeks: List) -> int:
        """Load week dimension with upsert logic."""
        loaded = 0
        for week_data in weeks:
            try:
                # Check if week exists
                existing = session.query(DimWeek).filter_by(week_id=week_data.week_id).first()
                
                if not existing:
                    week = DimWeek(
                        week_id=week_data.week_id,
                        year=week_data.year,
                        week_of_year=week_data.week_of_year,
                        week_start_date=week_data.week_start_date,
                        week_end_date=week_data.week_end_date,
                        quarter=week_data.quarter,
                        month=week_data.month,
                        year_week=week_data.year_week
                    )
                    session.add(week)
                    loaded += 1
                    
            except Exception as e:
                logger.warning(f"Failed to load week {week_data.week_id}: {str(e)}")
                continue
        
        session.flush()
        logger.info(f"Loaded {loaded} new weeks")
        return loaded
    
    def _load_dim_authors(self, session: Session, authors: List) -> int:
        """Load author dimension with upsert logic."""
        loaded = 0
        for author_data in authors:
            try:
                # Check if author exists
                existing = session.query(DimAuthor).filter_by(author_name=author_data.author_name).first()
                
                if not existing:
                    author = DimAuthor(author_name=author_data.author_name)
                    session.add(author)
                    loaded += 1
                    
            except Exception as e:
                logger.warning(f"Failed to load author {author_data.author_name}: {str(e)}")
                continue
        
        session.flush()
        logger.info(f"Loaded {loaded} new authors")
        return loaded
    
    def _load_dim_categories(self, session: Session, categories: List) -> int:
        """Load category dimension with upsert logic."""
        loaded = 0
        for category_data in categories:
            try:
                # Check if category exists
                existing = session.query(DimCategory).filter_by(category_name=category_data.category_name).first()
                
                if not existing:
                    category = DimCategory(category_name=category_data.category_name)
                    session.add(category)
                    loaded += 1
                    
            except Exception as e:
                logger.warning(f"Failed to load category {category_data.category_name}: {str(e)}")
                continue
        
        session.flush()
        logger.info(f"Loaded {loaded} new categories")
        return loaded
    
    def _load_dim_articles(self, session: Session, articles: List) -> int:
        """Load article dimension with upsert logic."""
        loaded = 0
        for article_data in articles:
            try:
                # Check if article exists
                existing = session.query(DimArticle).filter_by(page_path=article_data.page_path).first()
                
                if not existing:
                    article = DimArticle(
                        page_path=article_data.page_path,
                        title=article_data.title,
                        publication_date=article_data.publication_date
                    )
                    session.add(article)
                    loaded += 1
                    
            except Exception as e:
                logger.warning(f"Failed to load article {article_data.page_path}: {str(e)}")
                continue
        
        session.flush()
        logger.info(f"Loaded {loaded} new articles")
        return loaded
    
    def _load_fact_metrics(self, session: Session, metrics: List) -> int:
        """Load fact weekly metrics with upsert logic."""
        loaded = 0
        
        for metric_data in metrics:
            try:
                # Get dimension IDs
                article = session.query(DimArticle).filter_by(page_path=metric_data.page_path).first()
                author = session.query(DimAuthor).filter_by(author_name=metric_data.author_name).first()
                category = session.query(DimCategory).filter_by(category_name=metric_data.category_name).first()
                
                if not all([article, author, category]):
                    logger.warning(f"Missing dimension for metric: {metric_data.page_path}")
                    continue
                
                # Check if fact record exists
                existing = session.query(FactWeeklyMetrics).filter_by(
                    article_id=article.article_id,
                    week_id=metric_data.week_id
                ).first()
                
                if existing:
                    # Update existing metrics
                    existing.screen_page_views = metric_data.screen_page_views
                    existing.engaged_sessions = metric_data.engaged_sessions
                    existing.sessions = metric_data.sessions
                    existing.engagement_rate = metric_data.engagement_rate
                    existing.average_session_duration = metric_data.average_session_duration
                else:
                    # Insert new metrics
                    fact = FactWeeklyMetrics(
                        article_id=article.article_id,
                        author_id=author.author_id,
                        category_id=category.category_id,
                        week_id=metric_data.week_id,
                        screen_page_views=metric_data.screen_page_views,
                        engaged_sessions=metric_data.engaged_sessions,
                        sessions=metric_data.sessions,
                        engagement_rate=metric_data.engagement_rate,
                        average_session_duration=metric_data.average_session_duration
                    )
                    session.add(fact)
                
                loaded += 1
                
            except Exception as e:
                logger.warning(f"Failed to load metric for {metric_data.page_path}: {str(e)}")
                continue
        
        session.flush()
        logger.info(f"Loaded {loaded} weekly metrics")
        return loaded
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with self.get_session() as session:
                stats = {
                    'total_weeks': session.query(DimWeek).count(),
                    'total_articles': session.query(DimArticle).count(),
                    'total_authors': session.query(DimAuthor).count(),
                    'total_categories': session.query(DimCategory).count(),
                    'total_metrics': session.query(FactWeeklyMetrics).count()
                }
                
                logger.info(f"Database stats: {stats}")
                return stats
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get database stats: {str(e)}")
            return {'error': str(e)}