"""
PostgreSQL storage for text mining features
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import logging
from typing import Optional
from datetime import datetime

from etl.text_mining.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresStorage:
    """
    PostgreSQL storage for text mining data
    
    Responsibilities:
    - Create schema and tables
    - Store raw HTML content
    - Store extracted features
    - Version control (append-only)
    """
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None
    ):
        self.host = host or config.POSTGRES_HOST
        self.port = port or config.POSTGRES_PORT
        self.database = database or config.POSTGRES_DB
        self.user = user or config.POSTGRES_USER
        self.password = password or config.POSTGRES_PASSWORD
        
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def create_schema(self):
        """Create text mining schema and tables"""
        create_tables_sql = """
        -- Schema for text mining
        CREATE SCHEMA IF NOT EXISTS text_mining;
        
        -- Raw HTML storage (versioned)
        CREATE TABLE IF NOT EXISTS text_mining.articles_raw (
            id SERIAL PRIMARY KEY,
            article_id VARCHAR(255),
            pagepath TEXT NOT NULL,
            html_content TEXT,
            scraped_at TIMESTAMP,
            sample_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(article_id, sample_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_articles_raw_pagepath 
            ON text_mining.articles_raw(pagepath);
        CREATE INDEX IF NOT EXISTS idx_articles_raw_sample_id 
            ON text_mining.articles_raw(sample_id);
        
        -- Extracted features (versioned)
        CREATE TABLE IF NOT EXISTS text_mining.articles_features (
            id SERIAL PRIMARY KEY,
            article_id VARCHAR(255),
            pagepath TEXT NOT NULL,
            word_count INTEGER,
            char_count INTEGER,
            paragraph_count INTEGER,
            pageviews INTEGER,
            engaged_sessions INTEGER,
            avg_session_duration NUMERIC(10, 2),
            engagement_rate NUMERIC(5, 4),
            editorial_score NUMERIC(10, 6),
            date_range_start DATE,
            date_range_end DATE,
            processing_version VARCHAR(50),
            processing_date TIMESTAMP,
            sample_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_articles_features_pagepath 
            ON text_mining.articles_features(pagepath);
        CREATE INDEX IF NOT EXISTS idx_articles_features_sample_id 
            ON text_mining.articles_features(sample_id);
        CREATE INDEX IF NOT EXISTS idx_articles_features_processing_version 
            ON text_mining.articles_features(processing_version);
        
        -- Sample metadata
        CREATE TABLE IF NOT EXISTS text_mining.samples (
            sample_id VARCHAR(255) PRIMARY KEY,
            generated_at TIMESTAMP,
            articles_count INTEGER,
            processing_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_tables_sql)
                self.conn.commit()
            logger.info("Schema and tables created successfully")
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            self.conn.rollback()
            raise
    
    def store_raw_articles(self, scraped_data: dict, sample_id: str):
        """Store raw HTML content"""
        articles = scraped_data.get('articles', [])
        
        if not articles:
            logger.warning("No articles to store")
            return
        
        insert_sql = """
        INSERT INTO text_mining.articles_raw 
            (article_id, pagepath, html_content, scraped_at, sample_id)
        VALUES %s
        ON CONFLICT (article_id, sample_id) DO NOTHING
        """
        
        values = [
            (
                self._generate_article_id(article['pagepath']),
                article['pagepath'],
                article['html_content'],
                article['scraped_at'],
                sample_id
            )
            for article in articles
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
                self.conn.commit()
            logger.info(f"Stored {len(values)} raw articles for sample {sample_id}")
        except Exception as e:
            logger.error(f"Error storing raw articles: {e}")
            self.conn.rollback()
            raise
    
    def store_features(self, df: pd.DataFrame):
        """Store extracted features from DataFrame"""
        if df.empty:
            logger.warning("No features to store")
            return
        
        # Add article_id if not present
        if 'article_id' not in df.columns:
            df['article_id'] = df['pagepath'].apply(self._generate_article_id)
        
        insert_sql = """
        INSERT INTO text_mining.articles_features 
            (article_id, pagepath, word_count, char_count, paragraph_count,
             pageviews, engaged_sessions, avg_session_duration, engagement_rate,
             editorial_score, date_range_start, date_range_end,
             processing_version, processing_date, sample_id)
        VALUES %s
        """
        
        # Prepare values
        values = []
        for _, row in df.iterrows():
            values.append((
                row.get('article_id'),
                row.get('pagepath'),
                row.get('word_count'),
                row.get('char_count'),
                row.get('paragraph_count'),
                row.get('pageviews'),
                row.get('engaged_sessions'),
                row.get('avg_session_duration'),
                row.get('engagement_rate'),
                row.get('editorial_score'),
                row.get('date_range_start'),
                row.get('date_range_end'),
                row.get('processing_version'),
                row.get('processing_date'),
                row.get('sample_id')
            ))
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
                self.conn.commit()
            logger.info(f"Stored {len(values)} feature records")
        except Exception as e:
            logger.error(f"Error storing features: {e}")
            self.conn.rollback()
            raise
    
    def store_sample_metadata(self, sample_id: str, generated_at: datetime, articles_count: int):
        """Store sample metadata"""
        insert_sql = """
        INSERT INTO text_mining.samples 
            (sample_id, generated_at, articles_count, processing_status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (sample_id) DO UPDATE SET
            processing_status = EXCLUDED.processing_status
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(insert_sql, (sample_id, generated_at, articles_count, 'completed'))
                self.conn.commit()
            logger.info(f"Stored metadata for sample {sample_id}")
        except Exception as e:
            logger.error(f"Error storing sample metadata: {e}")
            self.conn.rollback()
            raise
    
    def _generate_article_id(self, pagepath: str) -> str:
        """Generate unique article ID from pagepath"""
        # Extract date and slug from pagepath
        # Example: /2025/01/15/article-title/ -> 2025-01-15-article-title
        parts = pagepath.strip('/').split('/')
        if len(parts) >= 4:
            return f"{parts[0]}-{parts[1]}-{parts[2]}-{parts[3]}"
        return pagepath.strip('/').replace('/', '-')
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed PostgreSQL connection")


if __name__ == "__main__":
    # Test storage
    storage = PostgresStorage()
    storage.create_schema()
    storage.close()
