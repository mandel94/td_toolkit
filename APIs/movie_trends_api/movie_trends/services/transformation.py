"""Transformation service for calculating weekly trends."""

from datetime import date, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from movie_trends.config import get_settings
from movie_trends.database.models import DimMovie
from movie_trends.logging_config import get_logger
from movie_trends.repositories import PopularityRepository, TrendsRepository
from movie_trends.services.trend_scoring import MovieMetrics, TrendScoringEngine

logger = get_logger(__name__)


def get_week_bounds(target_date: date) -> tuple[date, date]:
    """
    Get start and end dates for the week containing target_date.
    
    Args:
        target_date: Date to find week for
        
    Returns:
        Tuple of (week_start, week_end)
    """
    # ISO week starts on Monday
    days_since_monday = target_date.weekday()
    week_start = target_date - timedelta(days=days_since_monday)
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


class TransformationService:
    """Service for transforming raw data into trend metrics."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.popularity_repo = PopularityRepository(session)
        self.trends_repo = TrendsRepository(session)
        self.scoring_engine = TrendScoringEngine()

    async def calculate_weekly_trends(self, target_date: date | None = None) -> dict[str, Any]:
        """
        Calculate weekly trend scores for all movies.
        
        Args:
            target_date: Date to calculate trends for (defaults to last week)
            
        Returns:
            Calculation summary
        """
        settings = get_settings()
        
        if target_date is None:
            # Default to last complete week
            target_date = date.today() - timedelta(days=7)
        
        # Get week bounds
        current_week_start, current_week_end = get_week_bounds(target_date)
        previous_week_start = current_week_start - timedelta(days=7)
        previous_week_end = current_week_start - timedelta(days=1)
        
        logger.info(
            "calculating_weekly_trends",
            current_week_start=current_week_start.isoformat(),
            current_week_end=current_week_end.isoformat(),
            previous_week_start=previous_week_start.isoformat(),
            previous_week_end=previous_week_end.isoformat(),
        )
        
        # Fetch weekly aggregates
        current_aggregates = await self.popularity_repo.get_weekly_aggregates(
            current_week_start,
            current_week_end,
        )
        
        previous_aggregates = await self.popularity_repo.get_weekly_aggregates(
            previous_week_start,
            previous_week_end,
        )
        
        if not current_aggregates:
            logger.warning("no_data_for_week", week_start=current_week_start.isoformat())
            return {
                "week_start": current_week_start.isoformat(),
                "movies_processed": 0,
                "status": "no_data",
            }
        
        # Convert to MovieMetrics
        current_metrics = [
            MovieMetrics(
                movie_key=row["movie_key"],
                avg_popularity=float(row["avg_popularity"]),
                avg_vote_count=int(row["avg_vote_count"]),
                avg_vote_average=float(row["avg_vote_average"]),
                popularity_stddev=float(row["popularity_stddev"])
                if row["popularity_stddev"]
                else None,
            )
            for row in current_aggregates
        ]
        
        previous_metrics = [
            MovieMetrics(
                movie_key=row["movie_key"],
                avg_popularity=float(row["avg_popularity"]),
                avg_vote_count=int(row["avg_vote_count"]),
                avg_vote_average=float(row["avg_vote_average"]),
                popularity_stddev=float(row["popularity_stddev"])
                if row["popularity_stddev"]
                else None,
            )
            for row in previous_aggregates
        ]
        
        # Get release dates
        movie_keys = [m.movie_key for m in current_metrics]
        
        from sqlalchemy import select
        
        stmt = select(DimMovie.movie_key, DimMovie.release_date).where(
            DimMovie.movie_key.in_(movie_keys)
        )
        result = await self.session.execute(stmt)
        release_dates = {row.movie_key: row.release_date for row in result}
        
        # Calculate trends
        trend_components = self.scoring_engine.calculate_batch_trends(
            current_period_metrics=current_metrics,
            previous_period_metrics=previous_metrics,
            movie_release_dates=release_dates,
            current_date=current_week_end,
        )
        
        # Save to database
        movies_saved = 0
        for movie_key, components in trend_components.items():
            # Get base metrics for this movie
            current_metric = next(m for m in current_metrics if m.movie_key == movie_key)
            
            trend_data = {
                "avg_popularity": current_metric.avg_popularity,
                "avg_vote_count": current_metric.avg_vote_count,
                "avg_vote_average": current_metric.avg_vote_average,
                "popularity_growth": components.popularity_growth,
                "vote_velocity": components.vote_velocity,
                "norm_popularity_growth": components.norm_popularity_growth,
                "norm_vote_velocity": components.norm_vote_velocity,
                "recency_factor": components.recency_factor,
                "stability_factor": components.stability_factor,
                "volatility": components.volatility,
                "trend_score": components.trend_score,
                "trend_classification": components.trend_classification,
            }
            
            await self.trends_repo.save_weekly_trend(
                movie_key=movie_key,
                week_start_date=current_week_start,
                week_end_date=current_week_end,
                trend_data=trend_data,
                formula_version=settings.trend_formula_version,
            )
            
            movies_saved += 1
        
        await self.session.commit()
        
        summary = {
            "week_start": current_week_start.isoformat(),
            "week_end": current_week_end.isoformat(),
            "movies_processed": movies_saved,
            "formula_version": settings.trend_formula_version,
            "status": "completed",
        }
        
        logger.info("weekly_trends_calculated", **summary)
        
        return summary
