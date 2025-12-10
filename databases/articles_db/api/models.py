"""
SQLAlchemy models for Articles Analytics Database
"""
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

from .database import Base


class DimAuthor(Base):
    """Dimension table for article authors"""
    __tablename__ = "dim_authors"
    
    author_id = Column(Integer, primary_key=True, autoincrement=True)
    author_name = Column(String(255), unique=True, nullable=False)
    
    # Relationships
    weekly_metrics = relationship("FactWeeklyMetrics", back_populates="author")
    
    def __repr__(self):
        return f"<Author(id={self.author_id}, name='{self.author_name}')>"


class DimCategory(Base):
    """Dimension table for content categories"""
    __tablename__ = "dim_categories"
    
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(100), unique=True, nullable=False)
    
    # Relationships
    weekly_metrics = relationship("FactWeeklyMetrics", back_populates="category")
    
    def __repr__(self):
        return f"<Category(id={self.category_id}, name='{self.category_name}')>"


class DimDate(Base):
    """Dimension table for date hierarchy"""
    __tablename__ = "dim_dates"
    
    date_id = Column(Integer, primary_key=True)  # YYYYMMDD format
    full_date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    week_of_year = Column(Integer, nullable=False)
    day_of_month = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    is_weekend = Column(Boolean, nullable=False)
    
    def __repr__(self):
        return f"<Date(id={self.date_id}, date={self.full_date})>"


class DimArticle(Base):
    """Dimension table for article master data"""
    __tablename__ = "dim_articles"
    
    article_id = Column(Integer, primary_key=True, autoincrement=True)
    page_path = Column(String(1024), unique=True, nullable=False)
    title = Column(String(500))
    publication_date = Column(Date)
    created_at = Column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    weekly_metrics = relationship("FactWeeklyMetrics", back_populates="article")
    
    def __repr__(self):
        return f"<Article(id={self.article_id}, title='{self.title}')>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "article_id": self.article_id,
            "page_path": self.page_path,
            "title": self.title,
            "publication_date": self.publication_date.isoformat() if self.publication_date else None,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }


class FactWeeklyMetrics(Base):
    """Fact table for weekly article performance metrics"""
    __tablename__ = "fact_weekly_metrics"
    
    article_id = Column(Integer, ForeignKey("dim_articles.article_id"), primary_key=True)
    author_id = Column(Integer, ForeignKey("dim_authors.author_id"), nullable=False)
    category_id = Column(Integer, ForeignKey("dim_categories.category_id"), nullable=False)
    year = Column(Integer, primary_key=True, nullable=False)
    week_of_year = Column(Integer, primary_key=True, nullable=False)
    
    # Metrics
    screen_page_views = Column(Integer)
    engaged_sessions = Column(Integer)
    sessions = Column(Integer)
    engagement_rate = Column(Numeric(10, 4))
    average_session_duration = Column(Numeric(10, 4))
    
    # Relationships
    article = relationship("DimArticle", back_populates="weekly_metrics")
    author = relationship("DimAuthor", back_populates="weekly_metrics")
    category = relationship("DimCategory", back_populates="weekly_metrics")
    
    # Indexes
    __table_args__ = (
        Index("idx_fact_weekly_metrics_year_week", "year", "week_of_year"),
    )
    
    def __repr__(self):
        return f"<WeeklyMetrics(article_id={self.article_id}, year={self.year}, week={self.week_of_year})>"
    
    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "article_id": self.article_id,
            "author_id": self.author_id,
            "category_id": self.category_id,
            "year": self.year,
            "week_of_year": self.week_of_year,
            "screen_page_views": self.screen_page_views,
            "engaged_sessions": self.engaged_sessions,
            "sessions": self.sessions,
            "engagement_rate": float(self.engagement_rate) if self.engagement_rate else None,
            "average_session_duration": float(self.average_session_duration) if self.average_session_duration else None
        }
