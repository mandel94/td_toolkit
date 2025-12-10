"""
Service layer for analytics business logic
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, and_
from typing import List, Optional, Tuple
from datetime import date, datetime
import logging

from .models import (
    DimArticle, DimAuthor, DimCategory, 
    FactWeeklyMetrics
)
from .schemas import (
    TopArticleResponse, AuthorPerformanceResponse,
    CategoryPerformanceResponse, EngagementTrendResponse
)

logger = logging.getLogger(__name__)


class AnalyticsService:
    """Service class for analytics operations"""
    
    def __init__(self, db: Session):
        """Initialize with database session"""
        self.db = db
    
    def get_top_articles(
        self,
        limit: int = 50,
        category_id: Optional[int] = None,
        author_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[TopArticleResponse]:
        """
        Get top performing articles by engagement metrics
        
        Args:
            limit: Number of top articles to return
            category_id: Filter by category
            author_id: Filter by author
            start_date: Filter by start date
            end_date: Filter by end date
            
        Returns:
            List of top article responses with aggregated metrics
        """
        query = (
            self.db.query(
                FactWeeklyMetrics.article_id,
                DimArticle.title,
                DimArticle.page_path,
                DimAuthor.author_name,
                DimCategory.category_name,
                func.sum(FactWeeklyMetrics.screen_page_views).label('total_views'),
                func.sum(FactWeeklyMetrics.engaged_sessions).label('total_engaged_sessions'),
                func.avg(FactWeeklyMetrics.engagement_rate).label('avg_engagement_rate'),
                func.avg(FactWeeklyMetrics.average_session_duration).label('avg_session_duration')
            )
            .join(DimArticle, FactWeeklyMetrics.article_id == DimArticle.article_id)
            .join(DimAuthor, FactWeeklyMetrics.author_id == DimAuthor.author_id)
            .join(DimCategory, FactWeeklyMetrics.category_id == DimCategory.category_id)
        )
        
        # Apply filters
        if category_id:
            query = query.filter(FactWeeklyMetrics.category_id == category_id)
        
        if author_id:
            query = query.filter(FactWeeklyMetrics.author_id == author_id)
        
        if start_date:
            query = query.filter(DimArticle.publication_date >= start_date)
        
        if end_date:
            query = query.filter(DimArticle.publication_date <= end_date)
        
        # Group, order and limit
        results = (
            query
            .group_by(
                FactWeeklyMetrics.article_id,
                DimArticle.title,
                DimArticle.page_path,
                DimAuthor.author_name,
                DimCategory.category_name
            )
            .order_by(desc('total_engaged_sessions'))
            .limit(limit)
            .all()
        )
        
        logger.info(f"Retrieved {len(results)} top articles")
        
        return [
            TopArticleResponse(
                article_id=r.article_id,
                title=r.title,
                page_path=r.page_path,
                author_name=r.author_name,
                category_name=r.category_name,
                total_views=r.total_views or 0,
                total_engaged_sessions=r.total_engaged_sessions or 0,
                avg_engagement_rate=float(r.avg_engagement_rate or 0),
                avg_session_duration=float(r.avg_session_duration or 0)
            )
            for r in results
        ]
    
    def get_author_performance(self) -> List[AuthorPerformanceResponse]:
        """
        Get performance metrics aggregated by author
        
        Returns:
            List of author performance metrics
        """
        results = (
            self.db.query(
                DimAuthor.author_id,
                DimAuthor.author_name,
                func.count(func.distinct(FactWeeklyMetrics.article_id)).label('total_articles'),
                func.sum(FactWeeklyMetrics.screen_page_views).label('total_views'),
                func.sum(FactWeeklyMetrics.sessions).label('total_sessions'),
                func.avg(FactWeeklyMetrics.engagement_rate).label('avg_engagement_rate'),
                func.avg(FactWeeklyMetrics.average_session_duration).label('avg_session_duration')
            )
            .join(FactWeeklyMetrics, DimAuthor.author_id == FactWeeklyMetrics.author_id)
            .group_by(DimAuthor.author_id, DimAuthor.author_name)
            .order_by(desc('total_views'))
            .all()
        )
        
        logger.info(f"Retrieved performance data for {len(results)} authors")
        
        return [
            AuthorPerformanceResponse(
                author_id=r.author_id,
                author_name=r.author_name,
                total_articles=r.total_articles,
                total_views=r.total_views or 0,
                total_sessions=r.total_sessions or 0,
                avg_engagement_rate=float(r.avg_engagement_rate or 0),
                avg_session_duration=float(r.avg_session_duration or 0)
            )
            for r in results
        ]
    
    def get_category_performance(self) -> List[CategoryPerformanceResponse]:
        """
        Get performance metrics aggregated by category
        
        Returns:
            List of category performance metrics
        """
        results = (
            self.db.query(
                DimCategory.category_id,
                DimCategory.category_name,
                func.count(func.distinct(FactWeeklyMetrics.article_id)).label('total_articles'),
                func.sum(FactWeeklyMetrics.screen_page_views).label('total_views'),
                func.sum(FactWeeklyMetrics.sessions).label('total_sessions'),
                func.avg(FactWeeklyMetrics.engagement_rate).label('avg_engagement_rate'),
                func.avg(FactWeeklyMetrics.average_session_duration).label('avg_session_duration')
            )
            .join(FactWeeklyMetrics, DimCategory.category_id == FactWeeklyMetrics.category_id)
            .group_by(DimCategory.category_id, DimCategory.category_name)
            .order_by(desc('total_views'))
            .all()
        )
        
        logger.info(f"Retrieved performance data for {len(results)} categories")
        
        return [
            CategoryPerformanceResponse(
                category_id=r.category_id,
                category_name=r.category_name,
                total_articles=r.total_articles,
                total_views=r.total_views or 0,
                total_sessions=r.total_sessions or 0,
                avg_engagement_rate=float(r.avg_engagement_rate or 0),
                avg_session_duration=float(r.avg_session_duration or 0)
            )
            for r in results
        ]
    
    def get_engagement_trends(
        self,
        article_id: Optional[int] = None,
        limit: int = 100
    ) -> List[EngagementTrendResponse]:
        """
        Get engagement rate trends over time
        
        Args:
            article_id: Specific article to analyze (optional)
            limit: Maximum number of results
            
        Returns:
            List of engagement trend data points
        """
        query = (
            self.db.query(
                FactWeeklyMetrics.article_id,
                DimArticle.title,
                DimArticle.publication_date,
                FactWeeklyMetrics.year,
                FactWeeklyMetrics.week_of_year,
                FactWeeklyMetrics.engagement_rate,
                FactWeeklyMetrics.sessions
            )
            .join(DimArticle, FactWeeklyMetrics.article_id == DimArticle.article_id)
            .filter(FactWeeklyMetrics.engagement_rate.isnot(None))
        )
        
        if article_id:
            query = query.filter(FactWeeklyMetrics.article_id == article_id)
        
        results = (
            query
            .order_by(
                FactWeeklyMetrics.article_id,
                FactWeeklyMetrics.year,
                FactWeeklyMetrics.week_of_year
            )
            .limit(limit)
            .all()
        )
        
        logger.info(f"Retrieved {len(results)} engagement trend data points")
        
        trends = []
        for r in results:
            # Calculate weeks since publication
            if r.publication_date:
                # Simplified calculation - can be enhanced
                pub_week = r.publication_date.isocalendar()[1]
                pub_year = r.publication_date.year
                weeks_since = (r.year - pub_year) * 52 + (r.week_of_year - pub_week)
            else:
                weeks_since = 0
            
            trends.append(
                EngagementTrendResponse(
                    article_id=r.article_id,
                    title=r.title,
                    week=r.week_of_year,
                    year=r.year,
                    engagement_rate=float(r.engagement_rate),
                    sessions=r.sessions or 0,
                    weeks_since_publication=max(0, weeks_since)
                )
            )
        
        return trends
    
    def get_article_by_id(self, article_id: int) -> Optional[DimArticle]:
        """Get article by ID"""
        return self.db.query(DimArticle).filter(DimArticle.article_id == article_id).first()
    
    def get_all_authors(self) -> List[DimAuthor]:
        """Get all authors"""
        return self.db.query(DimAuthor).order_by(DimAuthor.author_name).all()
    
    def get_all_categories(self) -> List[DimCategory]:
        """Get all categories"""
        return self.db.query(DimCategory).order_by(DimCategory.category_name).all()
