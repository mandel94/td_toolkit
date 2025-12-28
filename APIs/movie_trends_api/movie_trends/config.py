"""Configuration management using Pydantic Settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: PostgresDsn = Field(
        default="postgresql+asyncpg://postgres:password@localhost:5432/movie_trends",
        description="PostgreSQL connection string",
    )
    database_pool_size: int = Field(default=10, ge=1, le=100)
    database_max_overflow: int = Field(default=20, ge=0, le=100)

    # TMDb API
    tmdb_api_key: str = Field(default="", description="TMDb API key")
    tmdb_base_url: str = Field(default="https://api.themoviedb.org/3")
    tmdb_rate_limit_per_second: int = Field(default=40, ge=1, le=50)

    # Redis
    redis_url: str = Field(default="redis://localhost:6379/0")
    cache_ttl_seconds: int = Field(default=3600, ge=60)

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000, ge=1024, le=65535)
    api_workers: int = Field(default=4, ge=1, le=16)
    api_reload: bool = Field(default=False)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")

    # Trend Scoring
    trend_formula_version: str = Field(default="1.0")
    recency_lambda_days: int = Field(default=75, ge=30, le=180)
    popularity_weight: float = Field(default=0.6, ge=0.0, le=1.0)
    vote_velocity_weight: float = Field(default=0.4, ge=0.0, le=1.0)
    volatility_periods: int = Field(default=4, ge=2, le=10)

    # Prefect
    prefect_api_url: str = Field(default="http://localhost:4200/api")

    @property
    def database_url_str(self) -> str:
        """Get database URL as string."""
        return str(self.database_url)


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
