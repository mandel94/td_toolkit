"""
Main FastAPI application for Articles Analytics API
"""
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
import logging

from config import get_settings
from database import get_db
from services import AnalyticsService
from schemas import (
    TopArticleResponse,
    AuthorPerformanceResponse,
    CategoryPerformanceResponse,
    EngagementTrendResponse,
    ArticleResponse
)
from models import DimAuthor, DimCategory

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get settings
settings = get_settings()

# Create FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "description": settings.api_description,
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "top_articles": "/api/v1/analytics/top-articles",
            "author_performance": "/api/v1/analytics/author-performance",
            "category_performance": "/api/v1/analytics/category-performance",
            "engagement_trends": "/api/v1/analytics/engagement-trends",
            "authors": "/api/v1/dimensions/authors",
            "categories": "/api/v1/dimensions/categories"
        }
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "articles-analytics-api"}


# Analytics Endpoints

@app.get(
    "/api/v1/analytics/top-articles",
    response_model=List[TopArticleResponse],
    summary="Get top performing articles",
    description="Retrieve top articles ranked by engagement metrics with optional filters"
)
def get_top_articles(
    limit: int = Query(default=50, ge=1, le=500, description="Number of top articles to return"),
    category_id: Optional[int] = Query(default=None, description="Filter by category ID"),
    author_id: Optional[int] = Query(default=None, description="Filter by author ID"),
    start_date: Optional[date] = Query(default=None, description="Filter by start publication date"),
    end_date: Optional[date] = Query(default=None, description="Filter by end publication date"),
    db: Session = Depends(get_db)
):
    """
    Get top performing articles by engagement metrics.
    
    **Use Case 1**: Identify Top-Performing Articles
    
    Returns articles ranked by engaged sessions with aggregated metrics including:
    - Total views
    - Total engaged sessions
    - Average engagement rate
    - Average session duration
    """
    try:
        service = AnalyticsService(db)
        return service.get_top_articles(
            limit=limit,
            category_id=category_id,
            author_id=author_id,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        logger.error(f"Error fetching top articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/analytics/author-performance",
    response_model=List[AuthorPerformanceResponse],
    summary="Get author performance metrics",
    description="Retrieve performance metrics aggregated by author"
)
def get_author_performance(db: Session = Depends(get_db)):
    """
    Get author performance scorecards.
    
    **Use Case 5**: Author Performance Scorecards
    
    Returns aggregated metrics per author including:
    - Total articles written
    - Total views generated
    - Average engagement rate
    - Average session duration
    """
    try:
        service = AnalyticsService(db)
        return service.get_author_performance()
    except Exception as e:
        logger.error(f"Error fetching author performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/analytics/category-performance",
    response_model=List[CategoryPerformanceResponse],
    summary="Get category performance metrics",
    description="Retrieve performance metrics aggregated by content category"
)
def get_category_performance(db: Session = Depends(get_db)):
    """
    Get category portfolio performance.
    
    **Use Case 7**: Category Portfolio Management
    
    Returns aggregated metrics per category including:
    - Total articles in category
    - Total views
    - Average engagement rate
    - Average session duration
    """
    try:
        service = AnalyticsService(db)
        return service.get_category_performance()
    except Exception as e:
        logger.error(f"Error fetching category performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/analytics/engagement-trends",
    response_model=List[EngagementTrendResponse],
    summary="Get engagement rate trends",
    description="Analyze engagement trends over time"
)
def get_engagement_trends(
    article_id: Optional[int] = Query(default=None, description="Filter by specific article ID"),
    limit: int = Query(default=100, ge=1, le=1000, description="Maximum number of data points"),
    db: Session = Depends(get_db)
):
    """
    Get engagement rate trend analysis.
    
    **Use Case 3**: Engagement Rate Trend Monitoring
    
    Returns time series data showing how engagement evolves:
    - Engagement rate by week
    - Sessions count
    - Weeks since publication
    """
    try:
        service = AnalyticsService(db)
        return service.get_engagement_trends(article_id=article_id, limit=limit)
    except Exception as e:
        logger.error(f"Error fetching engagement trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Dimension Endpoints

@app.get(
    "/api/v1/dimensions/authors",
    summary="Get all authors",
    description="Retrieve list of all authors in the system"
)
def get_authors(db: Session = Depends(get_db)):
    """Get all authors for filtering and reference"""
    try:
        service = AnalyticsService(db)
        authors = service.get_all_authors()
        return [
            {"author_id": a.author_id, "author_name": a.author_name}
            for a in authors
        ]
    except Exception as e:
        logger.error(f"Error fetching authors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/dimensions/categories",
    summary="Get all categories",
    description="Retrieve list of all content categories"
)
def get_categories(db: Session = Depends(get_db)):
    """Get all categories for filtering and reference"""
    try:
        service = AnalyticsService(db)
        categories = service.get_all_categories()
        return [
            {"category_id": c.category_id, "category_name": c.category_name}
            for c in categories
        ]
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/api/v1/articles/{article_id}",
    response_model=ArticleResponse,
    summary="Get article by ID",
    description="Retrieve article details by ID"
)
def get_article(article_id: int, db: Session = Depends(get_db)):
    """Get specific article details"""
    try:
        service = AnalyticsService(db)
        article = service.get_article_by_id(article_id)
        if not article:
            raise HTTPException(status_code=404, detail=f"Article {article_id} not found")
        return article
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching article {article_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
