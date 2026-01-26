"""Trends API endpoints."""

from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from movie_trends.database import get_db
from movie_trends.database.models import DimMovie, FactMovieTrendsWeekly
from movie_trends.logging_config import get_logger
from movie_trends.repositories import TrendsRepository
from movie_trends.schemas.api import (
    APIError,
    APIMetadata,
    MovieBase,
    MovieTrend,
    MovieTrendDetailed,
    TimeWindow,
    TrendClassification,
    TrendDetailResponse,
    TrendHistory,
    TrendMetrics,
    TrendsListResponse,
    TrendTimePoint,
)
from movie_trends.services.transformation import get_week_bounds

logger = get_logger(__name__)

router = APIRouter()


def get_latest_week_start() -> date:
    """Get the start date of the most recent complete week."""
    today = date.today()
    last_week = today - timedelta(days=7)
    week_start, _ = get_week_bounds(last_week)
    return week_start


async def get_trends_repository(
    db: Annotated[AsyncSession, Depends(get_db)]
) -> TrendsRepository:
    """Dependency for trends repository."""
    return TrendsRepository(db)


async def build_movie_trend(
    trend_fact: FactMovieTrendsWeekly,
    movie: DimMovie,
    previous_trend: FactMovieTrendsWeekly | None = None,
) -> MovieTrend:
    """Build MovieTrend response from database entities."""
    movie_base = MovieBase(
        movie_id=movie.movie_id,
        title=movie.title,
        release_date=movie.release_date,
        genres=movie.genres or [],
    )
    
    trend_metrics = TrendMetrics(
        trend_score=trend_fact.trend_score,
        trend_classification=TrendClassification(trend_fact.trend_classification),
        popularity_growth=trend_fact.popularity_growth,
        vote_velocity=trend_fact.vote_velocity,
        recency_factor=trend_fact.recency_factor,
        stability_factor=trend_fact.stability_factor,
    )
    
    trend_history = TrendHistory(
        previous_score=previous_trend.trend_score if previous_trend else None,
        delta=(trend_fact.trend_score - previous_trend.trend_score)
        if previous_trend
        else None,
    )
    
    return MovieTrend(
        movie=movie_base,
        trend_metrics=trend_metrics,
        trend_history=trend_history,
    )


@router.get(
    "/trends/movies",
    response_model=TrendsListResponse,
    summary="Get trending movies",
    description="Get list of trending movies for a specific time window with trend metrics",
)
async def get_trending_movies(
    db: Annotated[AsyncSession, Depends(get_db)],
    time_window: Annotated[
        TimeWindow, Query(description="Time window for trends")
    ] = TimeWindow.WEEKLY,
    genre: Annotated[str | None, Query(description="Filter by genre")] = None,
    classification: Annotated[
        TrendClassification | None, Query(description="Filter by trend classification")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum results")] = 20,
) -> TrendsListResponse:
    """
    Get trending movies with filters.
    
    Returns movies sorted by trend score with full metric breakdown.
    """
    trends_repo = TrendsRepository(db)
    
    # Determine target week
    target_week_start = get_latest_week_start()
    
    # Fetch trends
    trends = await trends_repo.get_trends(
        week_start_date=target_week_start,
        limit=limit,
        genre=genre,
        classification=classification.value if classification else None,
    )
    
    if not trends:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "NO_DATA",
                "message": "No trend data available for the requested period",
                "details": {"week_start": target_week_start.isoformat()},
            },
        )
    
    # Build response
    movie_trends = []
    for trend in trends:
        # Get movie details
        movie = await db.get(DimMovie, trend.movie_key)
        if not movie:
            continue
        
        # Get previous week's trend for delta
        previous_week = target_week_start - timedelta(days=7)
        from sqlalchemy import and_, select
        
        stmt = select(FactMovieTrendsWeekly).where(
            and_(
                FactMovieTrendsWeekly.movie_key == trend.movie_key,
                FactMovieTrendsWeekly.week_start_date == previous_week,
            )
        )
        result = await db.execute(stmt)
        previous_trend = result.scalar_one_or_none()
        
        movie_trend = await build_movie_trend(trend, movie, previous_trend)
        movie_trends.append(movie_trend)
    
    metadata = APIMetadata(
        api_version="v1",
        time_window=time_window,
        as_of=target_week_start,
        trend_definition_version="1.0",
    )
    
    logger.info(
        "trends_requested",
        time_window=time_window.value,
        genre=genre,
        classification=classification.value if classification else None,
        results=len(movie_trends),
    )
    
    return TrendsListResponse(meta=metadata, data=movie_trends)


@router.get(
    "/trends/movies/{movie_id}",
    response_model=TrendDetailResponse,
    summary="Get movie trend details",
    description="Get detailed trend information and history for a specific movie",
)
async def get_movie_trend(
    movie_id: int,
    db: Annotated[AsyncSession, Depends(get_db)],
    time_window: Annotated[
        TimeWindow, Query(description="Time window for trends")
    ] = TimeWindow.WEEKLY,
) -> TrendDetailResponse:
    """
    Get detailed trend data for a specific movie including time series.
    """
    from sqlalchemy import and_, select
    
    # Get current movie
    stmt = select(DimMovie).where(
        and_(DimMovie.movie_id == movie_id, DimMovie.is_current == True)
    )
    result = await db.execute(stmt)
    movie = result.scalar_one_or_none()
    
    if not movie:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "MOVIE_NOT_FOUND",
                "message": f"Movie with ID {movie_id} not found",
            },
        )
    
    # Get trend history
    trends_repo = TrendsRepository(db)
    trend_history = await trends_repo.get_movie_trend_history(
        movie_key=movie.movie_key,
        limit=12,
    )
    
    if not trend_history:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "NO_TREND_DATA",
                "message": f"No trend data available for movie {movie_id}",
            },
        )
    
    # Latest trend is first (descending order)
    latest_trend = trend_history[0]
    previous_trend = trend_history[1] if len(trend_history) > 1 else None
    
    # Build basic trend
    movie_trend = await build_movie_trend(latest_trend, movie, previous_trend)
    
    # Add time series
    timeseries = [
        TrendTimePoint(
            period=f"{trend.week_start_date.year}-W{trend.week_start_date.isocalendar()[1]:02d}",
            trend_score=trend.trend_score,
        )
        for trend in reversed(trend_history)  # Oldest to newest
    ]
    
    detailed_trend = MovieTrendDetailed(
        movie=movie_trend.movie,
        trend_metrics=movie_trend.trend_metrics,
        trend_history=movie_trend.trend_history,
        trend_timeseries=timeseries,
    )
    
    metadata = APIMetadata(
        api_version="v1",
        time_window=time_window,
        as_of=latest_trend.week_start_date,
        trend_definition_version=latest_trend.formula_version,
    )
    
    logger.info(
        "movie_trend_requested",
        movie_id=movie_id,
        periods=len(timeseries),
    )
    
    return TrendDetailResponse(meta=metadata, data=detailed_trend)


@router.get(
    "/trends/compare",
    summary="Compare trends for multiple movies",
    description="Compare trend trajectories for multiple movies",
)
async def compare_movie_trends(
    ids: Annotated[str, Query(description="Comma-separated movie IDs (e.g., '123,456')")],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict:
    """
    Compare trends for multiple movies.
    
    Returns trend data for multiple movies for comparison.
    """
    try:
        movie_ids = [int(id.strip()) for id in ids.split(",")]
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "INVALID_PARAMETER",
                "message": "Invalid movie IDs format",
                "details": {"expected": "Comma-separated integers"},
            },
        )
    
    if len(movie_ids) > 10:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "TOO_MANY_IDS",
                "message": "Maximum 10 movies can be compared at once",
            },
        )
    
    comparisons = []
    for movie_id in movie_ids:
        try:
            trend_response = await get_movie_trend(movie_id, db)
            comparisons.append(trend_response.data)
        except HTTPException:
            # Skip movies without data
            continue
    
    return {
        "meta": {
            "api_version": "v1",
            "comparison_count": len(comparisons),
        },
        "data": comparisons,
    }
