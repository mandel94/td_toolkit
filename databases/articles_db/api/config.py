"""
Configuration settings for the Articles Analytics API
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Database Configuration
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "articles_db"
    db_user: str = "postgres"
    db_password: str = "postgres123"
    
    # API Configuration
    api_title: str = "Articles Analytics API"
    api_version: str = "1.0.0"
    api_description: str = "Backend API for Taxi Drivers content analytics"
    
    # CORS Configuration
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8000"]
    
    # Pagination Defaults
    default_page_size: int = 50
    max_page_size: int = 500
    
    @property
    def database_url(self) -> str:
        """Generate database connection URL"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()
