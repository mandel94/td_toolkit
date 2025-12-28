"""Schema exports."""

from movie_trends.schemas.api import (
    APIError,
    APIMetadata,
    MovieTrend,
    MovieTrendDetailed,
    TimeWindow,
    TrendClassification,
    TrendDetailResponse,
    TrendsListResponse,
)
from movie_trends.schemas.tmdb import (
    TMDbError,
    TMDbMovieBase,
    TMDbMovieDetailed,
    TMDbTrendingResponse,
)

__all__ = [
    "APIError",
    "APIMetadata",
    "MovieTrend",
    "MovieTrendDetailed",
    "TimeWindow",
    "TrendClassification",
    "TrendDetailResponse",
    "TrendsListResponse",
    "TMDbError",
    "TMDbMovieBase",
    "TMDbMovieDetailed",
    "TMDbTrendingResponse",
]
