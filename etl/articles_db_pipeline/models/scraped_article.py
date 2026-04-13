"""Data models for web scraping pipeline."""
from datetime import datetime, date
from typing import Optional, List
from pydantic import BaseModel, Field, validator


class ScrapedArchiveArticle(BaseModel):
    """Article metadata scraped from archive page."""
    page_path: str = Field(..., description="Relative page path (e.g., /article-slug)")
    title: Optional[str] = Field(None, description="Article title")
    category: Optional[str] = Field(None, description="Article category")
    url: str = Field(..., description="Full article URL")
    published_text: Optional[str] = Field(None, description="Publication date text (Italian format)")
    scraped_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    
    @validator('page_path')
    def validate_page_path(cls, v):
        """Ensure page_path starts with /."""
        if not v.startswith('/'):
            return f"/{v}"
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "page_path": "/news/titolo-articolo",
                "title": "Titolo dell'articolo",
                "category": "News",
                "url": "https://www.taxidrivers.it/news/titolo-articolo",
                "published_text": "2 ore fa"
            }
        }


class ScrapedArticleDetail(BaseModel):
    """Detailed article content scraped from individual article page."""
    page_path: str = Field(..., description="Relative page path (e.g., /article-slug)")
    title: Optional[str] = Field(None, description="Article title")
    subtitle: Optional[str] = Field(None, description="Article subtitle/excerpt")
    author: Optional[str] = Field(None, description="Article author")
    publication_date: Optional[str] = Field(None, description="Publication date in ISO format (YYYY-MM-DD)")
    body_html: Optional[str] = Field(None, description="Full HTML body content")
    body_text: Optional[str] = Field(None, description="Plain text body content")
    scraped_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    
    @validator('publication_date', pre=True)
    def validate_publication_date(cls, v):
        """Validate and normalize publication date."""
        if v is None:
            return None
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, str):
            # Try to parse and return ISO format
            try:
                dt = datetime.fromisoformat(v.replace('Z', '+00:00'))
                return dt.date().isoformat()
            except:
                return v
        return v
    
    @validator('page_path')
    def validate_page_path(cls, v):
        """Ensure page_path starts with /."""
        if not v.startswith('/'):
            return f"/{v}"
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "page_path": "/news/titolo-articolo",
                "title": "Titolo dell'articolo",
                "subtitle": "Sottotitolo o estratto dell'articolo",
                "author": "Nome Autore",
                "publication_date": "2025-02-13",
                "body_html": "<div>...</div>",
                "body_text": "Testo completo dell'articolo..."
            }
        }


class EnrichedScrapedArticle(BaseModel):
    """Complete article combining archive and detail data."""
    # Primary key
    page_path: str = Field(..., description="Relative page path")
    
    # From archive scraping
    url: str = Field(..., description="Full article URL")
    published_text: Optional[str] = Field(None, description="Publication date text (Italian)")
    
    # From detail scraping (can override archive data)
    title: Optional[str] = Field(None, description="Article title")
    subtitle: Optional[str] = Field(None, description="Article subtitle/excerpt")
    author: Optional[str] = Field(None, description="Article author")
    category: Optional[str] = Field(None, description="Article category")
    publication_date: Optional[date] = Field(None, description="Parsed publication date")
    body_html: Optional[str] = Field(None, description="Full HTML body")
    body_text: Optional[str] = Field(None, description="Plain text body")
    
    # Metadata
    archive_scraped_at: Optional[str] = None
    detail_scraped_at: Optional[str] = None
    
    @validator('publication_date', pre=True)
    def parse_publication_date(cls, v):
        """Parse publication date from various formats."""
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00')).date()
            except:
                return None
        return None
    
    @validator('page_path')
    def validate_page_path(cls, v):
        """Ensure page_path starts with /."""
        if not v.startswith('/'):
            return f"/{v}"
        return v
    
    @validator('author', 'category')
    def normalize_text_fields(cls, v):
        """Normalize text fields (strip whitespace, handle None)."""
        if v is None:
            return None
        return v.strip() if isinstance(v, str) else v
    
    class Config:
        json_schema_extra = {
            "example": {
                "page_path": "/news/titolo-articolo",
                "url": "https://www.taxidrivers.it/news/titolo-articolo",
                "title": "Titolo dell'articolo",
                "subtitle": "Sottotitolo o estratto",
                "author": "Nome Autore",
                "category": "News",
                "publication_date": "2025-02-13",
                "body_html": "<div>...</div>",
                "body_text": "Testo completo..."
            }
        }


class ScrapingBatchResult(BaseModel):
    """Result of a scraping batch operation."""
    batch_id: str = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    articles: List[EnrichedScrapedArticle]
    total_scraped: int
    successful: int
    failed: int
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    @validator('total_scraped', 'successful', 'failed', pre=True, always=True)
    def compute_stats(cls, v, values):
        """Compute statistics from articles list."""
        if 'articles' in values:
            articles = values['articles']
            if isinstance(articles, list):
                return len(articles)
        return v or 0
    
    def mark_completed(self):
        """Mark batch as completed."""
        self.completed_at = datetime.now()
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate batch duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "20250213_143000",
                "total_scraped": 100,
                "successful": 95,
                "failed": 5,
                "articles": []
            }
        }
