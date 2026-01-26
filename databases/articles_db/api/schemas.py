"""
Pydantic schemas for request/response validation
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import date, datetime


class ArticleBase(BaseModel):
    """Base schema for article data"""
    page_path: str
    title: Optional[str] = None
    publication_date: Optional[date] = None


class ArticleResponse(ArticleBase):
    """Schema for article API responses"""
    article_id: int
    created_at: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


class WeekBase(BaseModel):
    """Base schema for week dimension"""
    year: int
    week_of_year: int
    week_start_date: date
    week_end_date: date
    quarter: int
    month: int
    year_week: str


class WeekResponse(WeekBase):
    """Schema for week dimension API responses"""
    week_id: int
    
    model_config = ConfigDict(from_attributes=True)


class WeeklyMetricsBase(BaseModel):
    """Base schema for weekly metrics"""
    week_id: int
    screen_page_views: Optional[int] = None
    engaged_sessions: Optional[int] = None
    sessions: Optional[int] = None
    engagement_rate: Optional[float] = None
    average_session_duration: Optional[float] = None


class WeeklyMetricsResponse(WeeklyMetricsBase):
    """Schema for weekly metrics API responses"""
    article_id: int
    author_id: int
    category_id: int
    
    model_config = ConfigDict(from_attributes=True)


class TopArticleResponse(BaseModel):
    """Schema for top performing articles"""
    article_id: int
    title: Optional[str]
    page_path: str
    author_name: str
    category_name: str
    total_views: int
    total_engaged_sessions: int
    avg_engagement_rate: float
    avg_session_duration: float
    
    model_config = ConfigDict(from_attributes=True)


class AuthorPerformanceResponse(BaseModel):
    """Schema for author performance metrics"""
    author_id: int
    author_name: str
    total_articles: int
    total_views: int
    total_sessions: int
    avg_engagement_rate: float
    avg_session_duration: float
    
    model_config = ConfigDict(from_attributes=True)


class CategoryPerformanceResponse(BaseModel):
    """Schema for category performance metrics"""
    category_id: int
    category_name: str
    total_articles: int
    total_views: int
    total_sessions: int
    avg_engagement_rate: float
    avg_session_duration: float
    
    model_config = ConfigDict(from_attributes=True)


class EngagementTrendResponse(BaseModel):
    """Schema for engagement trend analysis"""
    article_id: int
    title: Optional[str]
    week: int
    year: int
    engagement_rate: float
    sessions: int
    weeks_since_publication: int
    
    model_config = ConfigDict(from_attributes=True)


class PaginationParams(BaseModel):
    """Schema for pagination parameters"""
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    
    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class PaginatedResponse(BaseModel):
    """Generic paginated response schema"""
    total: int
    page: int
    page_size: int
    total_pages: int
    data: list
    
    model_config = ConfigDict(from_attributes=True)
