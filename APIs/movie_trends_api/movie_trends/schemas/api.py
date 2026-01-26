"""Pydantic schemas for API requests and responses."""

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class TimeWindow(str, Enum):
    """Time window for trend analysis."""

    DAILY = "daily"
    WEEKLY = "weekly"


class TrendClassification(str, Enum):
    """Trend classification categories."""

    EMERGING = "EMERGING"
    PEAKING = "PEAKING"
    STABLE = "STABLE"
    DECLINING = "DECLINING"


class MovieBase(BaseModel):
    """Base movie information."""

    movie_id: int
    title: str
    release_date: date | None = None
    genres: list[str] = []


class TrendMetrics(BaseModel):
    """Trend metrics breakdown."""

    trend_score: float = Field(..., description="Overall trend score (0-100)")
    trend_classification: TrendClassification = Field(
        ..., description="Trend classification category"
    )
    popularity_growth: float | None = Field(None, description="Relative popularity growth")
    vote_velocity: float | None = Field(None, description="Vote count velocity")
    recency_factor: float = Field(..., description="Recency adjustment factor")
    stability_factor: float = Field(..., description="Stability adjustment factor")


class TrendHistory(BaseModel):
    """Historical trend data."""

    previous_score: float | None = None
    delta: float | None = None


class TrendTimePoint(BaseModel):
    """Single point in trend time series."""

    period: str = Field(..., description="Time period identifier (e.g., '2025-W07')")
    trend_score: float


class MovieTrend(BaseModel):
    """Complete movie trend data."""

    movie: MovieBase
    trend_metrics: TrendMetrics
    trend_history: TrendHistory


class MovieTrendDetailed(MovieTrend):
    """Detailed movie trend with time series."""

    trend_timeseries: list[TrendTimePoint] = []


class APIMetadata(BaseModel):
    """API response metadata."""

    api_version: str = "v1"
    time_window: TimeWindow
    as_of: date
    trend_definition_version: str


class TrendsListResponse(BaseModel):
    """Response for trends list endpoint."""

    meta: APIMetadata
    data: list[MovieTrend]


class TrendDetailResponse(BaseModel):
    """Response for single trend detail endpoint."""

    meta: APIMetadata
    data: MovieTrendDetailed


class APIError(BaseModel):
    """API error response."""

    code: str
    message: str
    details: dict[str, Any] | None = None
