"""Data models for dimensional ETL pipeline."""
from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field, validator
from decimal import Decimal


class RawWeeklyData(BaseModel):
    """Raw weekly article data from GA4 API."""
    page_path: str = Field(..., alias='pagePath')
    year: int
    week: int
    screen_page_views: int = Field(..., alias='screenPageViews')
    sessions: int = Field(default=0)
    engaged_sessions: int = Field(..., alias='engagedSessions')
    engagement_rate: float = Field(..., alias='engagementRate')
    average_session_duration: float = Field(..., alias='averageSessionDuration')
    
    class Config:
        allow_population_by_field_name = True
        
    @validator('screen_page_views', 'sessions', 'engaged_sessions')
    def validate_positive_integers(cls, v):
        return max(0, int(v)) if v is not None else 0
        
    @validator('engagement_rate')
    def validate_engagement_rate(cls, v):
        return max(0.0, min(1.0, float(v))) if v is not None else 0.0
        
    @validator('average_session_duration')
    def validate_duration(cls, v):
        return max(0.0, float(v)) if v is not None else 0.0


class DimWeekData(BaseModel):
    """Week dimension data."""
    week_id: int
    year: int
    week_of_year: int
    week_start_date: date
    week_end_date: date
    quarter: int
    month: int
    year_week: str


class DimArticleData(BaseModel):
    """Article dimension data."""
    page_path: str
    title: Optional[str] = None
    publication_date: Optional[date] = None


class DimAuthorData(BaseModel):
    """Author dimension data."""
    author_name: str


class DimCategoryData(BaseModel):
    """Category dimension data."""
    category_name: str


class FactWeeklyMetricsData(BaseModel):
    """Fact table weekly metrics data."""
    page_path: str  # Will be mapped to article_id
    author_name: str  # Will be mapped to author_id
    category_name: str  # Will be mapped to category_id
    week_id: int
    screen_page_views: int
    engaged_sessions: int
    sessions: int
    engagement_rate: Decimal
    average_session_duration: Decimal
    
    @validator('engagement_rate', pre=True)
    def validate_engagement_rate_decimal(cls, v):
        if v is None:
            return Decimal('0.0000')
        return Decimal(str(round(float(v), 4)))
        
    @validator('average_session_duration', pre=True)
    def validate_duration_decimal(cls, v):
        if v is None:
            return Decimal('0.00')
        return Decimal(str(round(float(v), 2)))


class ProcessedWeeklyBatch(BaseModel):
    """Batch of processed weekly data ready for loading."""
    weeks: List[DimWeekData]
    articles: List[DimArticleData]
    authors: List[DimAuthorData]
    categories: List[DimCategoryData]
    metrics: List[FactWeeklyMetricsData]
    batch_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('batch_id', pre=True, always=True)
    def generate_batch_id(cls, v):
        if v:
            return v
        return f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"