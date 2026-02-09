"""
Event schemas for the text mining pipeline
"""
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import uuid

class ArticleMetadata(BaseModel):
    """Article metadata with GA4 KPIs"""
    pagepath: str
    pageviews: int
    engaged_sessions: int
    avg_session_duration: float
    engagement_rate: float
    editorial_score: Optional[float] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None

class GA4SampleReadyEvent(BaseModel):
    """Event published when GA4 sample is ready"""
    sample_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    articles: List[ArticleMetadata]
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ArticleScrapedContent(BaseModel):
    """Scraped article content"""
    pagepath: str
    html_content: str
    scraped_at: datetime = Field(default_factory=datetime.utcnow)

class ArticleHTMLScrapedEvent(BaseModel):
    """Event published when articles are scraped"""
    sample_id: str
    json_path: str
    articles_count: int
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
