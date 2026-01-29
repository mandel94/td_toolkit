"""
Configuration for Text Mining Pipeline
"""
import os
from pydantic_settings import BaseSettings
from pydantic import Field

class TextMiningConfig(BaseSettings):
    """Central configuration for text mining pipeline"""
    
    # GA4 Configuration
    GA4_PROPERTY_ID: str = Field(default="394327334")
    GA4_CREDENTIALS_PATH: str = Field(default="../ga4_api/client_secret_722854453271-t3dg269vqsvjjhbmpkh2a5etk0mmf6ve.apps.googleusercontent.com.json")
    
    # Sampling Configuration
    SAMPLE_SIZE: int = Field(default=10, description="Number of articles to sample")
    DATE_START: str = Field(default="2025-01-01")
    DATE_END: str = Field(default="today")
    
    # Redis Configuration
    REDIS_HOST: str = Field(default="localhost")
    REDIS_PORT: int = Field(default=6379)
    REDIS_DB: int = Field(default=0)
    REDIS_STREAM_GA4: str = Field(default="stream:ga4_sample_ready")
    REDIS_STREAM_SCRAPED: str = Field(default="stream:article_html_scraped")
    
    # PostgreSQL Configuration
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_DB: str = Field(default="articles_db")
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: str = Field(default="postgres123")
    
    # Scraping Configuration
    SCRAPER_DOMAIN: str = Field(default="https://www.taxidrivers.it")
    SCRAPER_DELAY: float = Field(default=1.0, description="Delay between requests in seconds")
    SCRAPER_CONTENT_SELECTOR: str = Field(default="div#mvp-content-main")
    
    # Storage Configuration
    SCRAPED_DATA_DIR: str = Field(default="./data/scraped")
    
    # Processing Configuration
    PROCESSING_VERSION: str = Field(default="tm_v0.1")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra fields from .env

config = TextMiningConfig()
