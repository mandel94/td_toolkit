"""Database loader with CRUD operations for articles."""
import sys
import os
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Date, Numeric, DateTime, select, insert, update, delete
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from contextlib import contextmanager
from logging import logger

# Add project root to Python path  
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from etl.articles_pipeline.config.database import DATABASE_URL, BATCH_SIZE
from etl.articles_pipeline.models.article import ProcessedArticle

class DatabaseLoader:
    """Handle database operations for articles."""
    
    def __init__(self, database_url: str = DATABASE_URL):
        self.database_url = database_url
        self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=300)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.metadata = MetaData()
        self._define_table_schema()
        logger.info("Initialized DatabaseLoader")
    
    def _define_table_schema(self):
        """Define the articles table schema using SQLAlchemy Core."""
        self.articles_table = Table(
            'articles',
            self.metadata,
            Column('article_id', Integer, primary_key=True, autoincrement=True),
            Column('title', String(500)),
            Column('author', String(200)),
            Column('category', String(100)),
            Column('screen_page_views', Integer, default=0),
            Column('sessions', Integer, default=0),
            Column('engaged_sessions', Integer, default=0),
            Column('engagement_rate', Numeric(5, 4), default=0.0000),
            Column('average_session_duration', Numeric(10, 2), default=0.00),
            Column('publication_date', Date),
            Column('page_path', String(1000)),
            Column('url', String(1000)),
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow)
        )
    
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
    
    def load_batch(
        self,
        articles: List[ProcessedArticle],
        batch_size: int = BATCH_SIZE,
        upsert: bool = True
    ) -> Dict[str, int]:
        """Load a batch of articles into the database.
        
        Args:
            articles: List of ProcessedArticle objects
            batch_size: Number of articles to process in each sub-batch
            upsert: Whether to update existing records or skip them
            
        Returns:
            Dictionary with counts of inserted, updated, and failed records
        """
        logger.info(f"Loading {len(articles)} articles to database (batch_size={batch_size})")
        
        stats = {'inserted': 0, 'updated': 0, 'failed': 0, 'skipped': 0}
        
        # Process in smaller batches
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            batch_stats = self._load_sub_batch(batch, upsert)
            
            # Aggregate stats
            for key in stats:
                stats[key] += batch_stats.get(key, 0)
        
        logger.success(
            f"Batch load completed: {stats['inserted']} inserted, "
            f"{stats['updated']} updated, {stats['skipped']} skipped, "
            f"{stats['failed']} failed"
        )
        
        return stats
    
    def _load_sub_batch(
        self,
        articles: List[ProcessedArticle],
        upsert: bool = True
    ) -> Dict[str, int]:
        """Load a sub-batch of articles."""
        stats = {'inserted': 0, 'updated': 0, 'failed': 0, 'skipped': 0}
        
        try:
            with self.get_session() as session:
                for article in articles:
                    try:
                        result = self._upsert_article(session, article) if upsert else self._insert_article(session, article)
                        stats[result] += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to process article {article.page_path}: {str(e)}")
                        stats['failed'] += 1
                        continue
                        
        except Exception as e:
            logger.error(f"Sub-batch load failed: {str(e)}")
            stats['failed'] += len(articles)
        
        return stats
    
    def _insert_article(self, session: Session, article: ProcessedArticle) -> str:
        """Insert a new article (will fail if exists)."""
        article_data = self._article_to_dict(article)
        
        stmt = insert(self.articles_table).values(**article_data)
        session.execute(stmt)
        
        return 'inserted'
    
    def _upsert_article(self, session: Session, article: ProcessedArticle) -> str:
        """Insert or update an article based on page_path."""
        # Check if article exists
        existing = session.execute(
            select(self.articles_table.c.article_id)
            .where(self.articles_table.c.page_path == article.page_path)
        ).first()
        
        article_data = self._article_to_dict(article)
        
        if existing:
            # Update existing article
            article_data['updated_at'] = datetime.utcnow()
            stmt = (
                update(self.articles_table)
                .where(self.articles_table.c.page_path == article.page_path)
                .values(**article_data)
            )
            session.execute(stmt)
            return 'updated'
        else:
            # Insert new article
            stmt = insert(self.articles_table).values(**article_data)
            session.execute(stmt)
            return 'inserted'
    
    def _article_to_dict(self, article: ProcessedArticle) -> Dict[str, Any]:
        """Convert ProcessedArticle to dictionary for database insertion."""
        return {
            'title': article.title,
            'author': article.author,
            'category': article.category,
            'screen_page_views': article.screen_page_views,
            'sessions': article.sessions,
            'engaged_sessions': article.engaged_sessions,
            'engagement_rate': float(article.engagement_rate),
            'average_session_duration': float(article.average_session_duration),
            'publication_date': article.publication_date,
            'page_path': article.page_path,
            'url': article.url
        }
    
    def get_article_by_path(self, page_path: str) -> Optional[Dict[str, Any]]:
        """Get an article by its page path."""
        try:
            with self.get_session() as session:
                result = session.execute(
                    select(self.articles_table)
                    .where(self.articles_table.c.page_path == page_path)
                ).first()
                
                if result:
                    return dict(result._mapping)
                return None
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get article by path {page_path}: {str(e)}")
            return None
    
    def get_articles_by_date_range(
        self,
        start_date: date,
        end_date: date,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get articles within a date range."""
        try:
            with self.get_session() as session:
                stmt = (
                    select(self.articles_table)
                    .where(
                        self.articles_table.c.publication_date >= start_date,
                        self.articles_table.c.publication_date <= end_date
                    )
                    .order_by(self.articles_table.c.screen_page_views.desc())
                )
                
                if limit:
                    stmt = stmt.limit(limit)
                
                results = session.execute(stmt).fetchall()
                return [dict(row._mapping) for row in results]
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get articles by date range: {str(e)}")
            return []
    
    def delete_articles_by_date_range(self, start_date: date, end_date: date) -> int:
        """Delete articles within a date range (for reprocessing)."""
        try:
            with self.get_session() as session:
                stmt = delete(self.articles_table).where(
                    self.articles_table.c.publication_date >= start_date,
                    self.articles_table.c.publication_date <= end_date
                )
                result = session.execute(stmt)
                deleted_count = result.rowcount
                
                logger.info(f"Deleted {deleted_count} articles from {start_date} to {end_date}")
                return deleted_count
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to delete articles: {str(e)}")
            return 0
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            with self.get_session() as session:
                # Total articles
                total_count = session.execute(
                    select(text('COUNT(*)')).select_from(self.articles_table)
                ).scalar()
                
                # Articles by category
                category_stats = session.execute(
                    select(
                        self.articles_table.c.category,
                        text('COUNT(*) as count')
                    )
                    .group_by(self.articles_table.c.category)
                    .order_by(text('count DESC'))
                ).fetchall()
                
                # Latest articles
                latest_date = session.execute(
                    select(text('MAX(created_at)')).select_from(self.articles_table)
                ).scalar()
                
                return {
                    'total_articles': total_count,
                    'categories': [dict(row._mapping) for row in category_stats],
                    'latest_load': latest_date
                }
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get database stats: {str(e)}")
            return {'error': str(e)}