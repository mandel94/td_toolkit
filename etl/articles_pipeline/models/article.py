"""Article data models for ETL pipeline."""
from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field, validator
from decimal import Decimal

class RawArticleData(BaseModel):
    """Raw article data from GA4 API."""
    page_path: str = Field(..., alias='pagePath')
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

class ArticleMetadata(BaseModel):
    """Simplified article metadata (no scraping)."""
    title: Optional[str] = None
    author: Optional[str] = None  # Always None for now
    publication_date: Optional[date] = None  # Always None for now

class ProcessedArticle(BaseModel):
    """Processed article ready for database insertion."""
    title: Optional[str] = None  # Not populated for now
    author: Optional[str] = None  # Always None
    category: Optional[str] = None
    screen_page_views: int = 0
    sessions: int = 0
    engaged_sessions: int = 0
    engagement_rate: Decimal = Field(default=Decimal('0.0000'))
    average_session_duration: Decimal = Field(default=Decimal('0.00'))
    publication_date: Optional[date] = None  # Always None for now
    page_path: str  # Main field we care about
    url: Optional[str] = None  # Optional for now
    
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
        
    # URL building removed for simplicity
        
class ArticleBatch(BaseModel):
    """Batch of articles for processing."""
    articles: List[ProcessedArticle]
    batch_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('batch_id', pre=True, always=True)
    def generate_batch_id(cls, v):
        if v:
            return v
        return f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"