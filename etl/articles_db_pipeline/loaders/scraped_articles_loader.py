"""Loader for scraped articles data into PostgreSQL."""
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy import create_engine, text, Table, MetaData, Column, Integer, String, Text, Date, TIMESTAMP
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from contextlib import contextmanager
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from etl.articles_db_pipeline.config.database import DATABASE_URL
from etl.articles_db_pipeline.models.scraped_article import (
    EnrichedScrapedArticle,
    ScrapingBatchResult
)


class ScrapedArticlesLoader:
    """Load scraped article data into PostgreSQL database."""
    
    def __init__(self, database_url: str = DATABASE_URL):
        """Initialize loader.
        
        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url
        self.engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=300)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # Initialize schema
        self._initialize_schema()
        
        logger.info("Initialized ScrapedArticlesLoader")
    
    def _initialize_schema(self):
        """Create tables if they don't exist."""
        # Read and execute the SQL schema file
        schema_file = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..",
            "articles_db", "sql", "03_create_scraped_articles.sql"
        )
        
        if os.path.exists(schema_file):
            try:
                with open(schema_file, 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                
                with self.engine.connect() as conn:
                    # Execute schema creation
                    conn.execute(text(schema_sql))
                    conn.commit()
                
                logger.info("Scraped articles schema initialized")
            except Exception as e:
                logger.warning(f"Schema initialization failed (may already exist): {str(e)}")
        else:
            logger.warning(f"Schema file not found: {schema_file}")
    
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
    
    def load_scraped_article(
        self,
        article: EnrichedScrapedArticle,
        session: Optional[Session] = None
    ) -> bool:
        """Load a single scraped article into database.
        
        Args:
            article: EnrichedScrapedArticle object
            session: Optional SQLAlchemy session (creates new if None)
            
        Returns:
            True if successful, False otherwise
        """
        article_data = {
            'page_path': article.page_path,
            'url': article.url,
            'title': article.title,
            'subtitle': article.subtitle,
            'author': article.author,
            'category': article.category,
            'publication_date': article.publication_date,
            'published_text': article.published_text,
            'body_html': article.body_html,
            'body_text': article.body_text,
            'archive_scraped_at': datetime.fromisoformat(article.archive_scraped_at) if article.archive_scraped_at else None,
            'detail_scraped_at': datetime.fromisoformat(article.detail_scraped_at) if article.detail_scraped_at else None,
        }
        
        try:
            if session:
                # Use provided session
                self._insert_article(session, article_data)
                return True
            else:
                # Create new session
                with self.get_session() as sess:
                    self._insert_article(sess, article_data)
                    return True
        except Exception as e:
            logger.error(f"Failed to load article {article.page_path}: {str(e)}")
            return False
    
    def _insert_article(self, session: Session, article_data: Dict[str, Any]):
        """Insert article data into database.
        
        Args:
            session: SQLAlchemy session
            article_data: Dictionary with article data
        """
        insert_sql = text("""
            INSERT INTO scraped_articles_raw (
                page_path, url, title, subtitle, author, category,
                publication_date, published_text, body_html, body_text,
                archive_scraped_at, detail_scraped_at
            ) VALUES (
                :page_path, :url, :title, :subtitle, :author, :category,
                :publication_date, :published_text, :body_html, :body_text,
                :archive_scraped_at, :detail_scraped_at
            )
        """)
        
        session.execute(insert_sql, article_data)
    
    def load_batch(self, batch: ScrapingBatchResult) -> Dict[str, Any]:
        """Load a complete batch of scraped articles.
        
        Args:
            batch: ScrapingBatchResult with articles to load
            
        Returns:
            Dictionary with load statistics
        """
        logger.info(f"Loading batch {batch.batch_id} with {len(batch.articles)} articles")
        
        stats = {
            'batch_id': batch.batch_id,
            'total_articles': len(batch.articles),
            'loaded': 0,
            'failed': 0,
            'errors': []
        }
        
        try:
            with self.get_session() as session:
                for article in batch.articles:
                    try:
                        self._insert_article(session, {
                            'page_path': article.page_path,
                            'url': article.url,
                            'title': article.title,
                            'subtitle': article.subtitle,
                            'author': article.author,
                            'category': article.category,
                            'publication_date': article.publication_date,
                            'published_text': article.published_text,
                            'body_html': article.body_html,
                            'body_text': article.body_text,
                            'archive_scraped_at': datetime.fromisoformat(article.archive_scraped_at) if article.archive_scraped_at else None,
                            'detail_scraped_at': datetime.fromisoformat(article.detail_scraped_at) if article.detail_scraped_at else None,
                        })
                        stats['loaded'] += 1
                    except Exception as e:
                        stats['failed'] += 1
                        error_msg = f"Failed to load {article.page_path}: {str(e)}"
                        stats['errors'].append(error_msg)
                        logger.warning(error_msg)
                
                # Commit all at once
                session.commit()
                
                logger.success(
                    f"Batch {batch.batch_id} loaded: "
                    f"{stats['loaded']} successful, {stats['failed']} failed"
                )
        
        except Exception as e:
            logger.error(f"Failed to load batch {batch.batch_id}: {str(e)}")
            stats['errors'].append(str(e))
            raise
        
        return stats
    
    def update_dim_tables_from_scraped(self) -> Dict[str, int]:
        """Update dimensional tables (dim_articles, dim_authors, dim_categories) from scraped data.
        
        This method syncs the scraped data with the dimensional model used by analytics.
        
        Returns:
            Dictionary with counts of updated records
        """
        logger.info("Updating dimensional tables from scraped data")
        
        stats = {
            'dim_articles': 0,
            'dim_authors': 0,
            'dim_categories': 0
        }
        
        try:
            with self.engine.connect() as conn:
                # Update dim_articles
                result = conn.execute(text("""
                    INSERT INTO dim_articles (page_path, title, publication_date)
                    SELECT DISTINCT
                        page_path,
                        title,
                        publication_date
                    FROM latest_scraped_articles
                    WHERE page_path IS NOT NULL
                    ON CONFLICT (page_path) DO UPDATE SET
                        title = EXCLUDED.title,
                        publication_date = EXCLUDED.publication_date
                """))
                stats['dim_articles'] = result.rowcount
                
                # Update dim_authors
                result = conn.execute(text("""
                    INSERT INTO dim_authors (author_name)
                    SELECT DISTINCT author
                    FROM latest_scraped_articles
                    WHERE author IS NOT NULL AND author != ''
                    ON CONFLICT (author_name) DO NOTHING
                """))
                stats['dim_authors'] = result.rowcount
                
                # Update dim_categories
                result = conn.execute(text("""
                    INSERT INTO dim_categories (category_name)
                    SELECT DISTINCT category
                    FROM latest_scraped_articles
                    WHERE category IS NOT NULL AND category != ''
                    ON CONFLICT (category_name) DO NOTHING
                """))
                stats['dim_categories'] = result.rowcount
                
                conn.commit()
                
                logger.success(f"Dimensional tables updated: {stats}")
        
        except Exception as e:
            logger.error(f"Failed to update dimensional tables: {str(e)}")
            raise
        
        return stats
    
    def get_scraped_articles_count(self) -> int:
        """Get total count of scraped articles."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM scraped_articles_raw"))
                count = result.scalar()
                return count
        except Exception as e:
            logger.error(f"Failed to get article count: {str(e)}")
            return 0
    
    def get_latest_scrape_date(self) -> Optional[datetime]:
        """Get the timestamp of the most recent scrape."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT MAX(created_at) FROM scraped_articles_raw"
                ))
                latest = result.scalar()
                return latest
        except Exception as e:
            logger.error(f"Failed to get latest scrape date: {str(e)}")
            return None
