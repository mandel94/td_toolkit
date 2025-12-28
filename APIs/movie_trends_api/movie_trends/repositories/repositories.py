"""Repository pattern for data access."""

from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import Select, and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from movie_trends.database.models import (
    DimMovie,
    FactMoviePopularityDaily,
    FactMovieTrendsWeekly,
    RawTMDbMovie,
    RawTMDbTrending,
)
from movie_trends.logging_config import get_logger

logger = get_logger(__name__)


class RawDataRepository:
    """Repository for raw staging data."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_raw_trending(
        self,
        time_window: str,
        media_type: str,
        payload: dict[str, Any],
        batch_id: str,
    ) -> RawTMDbTrending:
        """Save raw trending data."""
        raw_trending = RawTMDbTrending(
            time_window=time_window,
            media_type=media_type,
            fetched_at=datetime.utcnow(),
            payload=payload,
            import_batch_id=batch_id,
            import_source="tmdb_api",
        )
        self.session.add(raw_trending)
        await self.session.flush()
        return raw_trending

    async def save_raw_movie(
        self,
        movie_id: int,
        payload: dict[str, Any],
        batch_id: str,
    ) -> RawTMDbMovie:
        """Save raw movie details."""
        raw_movie = RawTMDbMovie(
            movie_id=movie_id,
            fetched_at=datetime.utcnow(),
            payload=payload,
            import_batch_id=batch_id,
            import_source="tmdb_api",
        )
        self.session.add(raw_movie)
        await self.session.flush()
        return raw_movie

    async def get_latest_trending_batch(self, time_window: str) -> str | None:
        """Get latest batch ID for trending data."""
        stmt = (
            select(RawTMDbTrending.import_batch_id)
            .where(RawTMDbTrending.time_window == time_window)
            .order_by(desc(RawTMDbTrending.fetched_at))
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()


class MovieRepository:
    """Repository for movie dimension data."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_current_movie(self, movie_id: int) -> DimMovie | None:
        """Get current version of a movie."""
        stmt = select(DimMovie).where(
            and_(DimMovie.movie_id == movie_id, DimMovie.is_current == True)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert_movie(
        self,
        movie_id: int,
        title: str,
        original_title: str,
        original_language: str,
        release_date: date | None,
        overview: str | None,
        genres: list[str],
        production_countries: list[str],
    ) -> DimMovie:
        """Insert or update movie using SCD Type 2."""
        current = await self.get_current_movie(movie_id)
        
        now = datetime.utcnow()
        
        # Check if update needed
        if current and (
            current.title != title
            or current.original_title != original_title
            or current.release_date != release_date
        ):
            # Expire current record
            current.is_current = False
            current.valid_to = now
        
        # Create new record
        new_movie = DimMovie(
            movie_id=movie_id,
            title=title,
            original_title=original_title,
            original_language=original_language,
            release_date=release_date,
            overview=overview,
            genres=genres,
            production_countries=production_countries,
            valid_from=now,
            valid_to=None,
            is_current=True,
        )
        self.session.add(new_movie)
        await self.session.flush()
        
        return new_movie


class PopularityRepository:
    """Repository for popularity facts."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_daily_popularity(
        self,
        movie_key: int,
        date_key: date,
        popularity: float,
        vote_count: int,
        vote_average: float,
    ) -> FactMoviePopularityDaily:
        """Insert or update daily popularity fact."""
        # Check if exists
        stmt = select(FactMoviePopularityDaily).where(
            and_(
                FactMoviePopularityDaily.movie_key == movie_key,
                FactMoviePopularityDaily.date_key == date_key,
            )
        )
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            existing.popularity = popularity
            existing.vote_count = vote_count
            existing.vote_average = vote_average
            return existing
        else:
            new_fact = FactMoviePopularityDaily(
                movie_key=movie_key,
                date_key=date_key,
                popularity=popularity,
                vote_count=vote_count,
                vote_average=vote_average,
            )
            self.session.add(new_fact)
            await self.session.flush()
            return new_fact

    async def get_weekly_aggregates(
        self,
        week_start: date,
        week_end: date,
    ) -> Sequence[dict[str, Any]]:
        """Get weekly aggregated metrics."""
        stmt = (
            select(
                FactMoviePopularityDaily.movie_key,
                func.avg(FactMoviePopularityDaily.popularity).label("avg_popularity"),
                func.avg(FactMoviePopularityDaily.vote_count).label("avg_vote_count"),
                func.avg(FactMoviePopularityDaily.vote_average).label("avg_vote_average"),
                func.stddev(FactMoviePopularityDaily.popularity).label("popularity_stddev"),
            )
            .where(
                and_(
                    FactMoviePopularityDaily.date_key >= week_start,
                    FactMoviePopularityDaily.date_key <= week_end,
                )
            )
            .group_by(FactMoviePopularityDaily.movie_key)
        )
        result = await self.session.execute(stmt)
        return result.mappings().all()


class TrendsRepository:
    """Repository for trend facts."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_weekly_trend(
        self,
        movie_key: int,
        week_start_date: date,
        week_end_date: date,
        trend_data: dict[str, Any],
        formula_version: str,
    ) -> FactMovieTrendsWeekly:
        """Save weekly trend fact."""
        # Check if exists
        stmt = select(FactMovieTrendsWeekly).where(
            and_(
                FactMovieTrendsWeekly.movie_key == movie_key,
                FactMovieTrendsWeekly.week_start_date == week_start_date,
            )
        )
        result = await self.session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            # Update existing
            for key, value in trend_data.items():
                setattr(existing, key, value)
            return existing
        else:
            new_trend = FactMovieTrendsWeekly(
                movie_key=movie_key,
                week_start_date=week_start_date,
                week_end_date=week_end_date,
                formula_version=formula_version,
                **trend_data,
            )
            self.session.add(new_trend)
            await self.session.flush()
            return new_trend

    async def get_trends(
        self,
        week_start_date: date,
        limit: int = 20,
        genre: str | None = None,
        classification: str | None = None,
    ) -> Sequence[FactMovieTrendsWeekly]:
        """Get trending movies with filters."""
        stmt = (
            select(FactMovieTrendsWeekly)
            .where(FactMovieTrendsWeekly.week_start_date == week_start_date)
            .order_by(desc(FactMovieTrendsWeekly.trend_score))
            .limit(limit)
        )
        
        if classification:
            stmt = stmt.where(FactMovieTrendsWeekly.trend_classification == classification)
        
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_movie_trend_history(
        self,
        movie_key: int,
        limit: int = 12,
    ) -> Sequence[FactMovieTrendsWeekly]:
        """Get trend history for a movie."""
        stmt = (
            select(FactMovieTrendsWeekly)
            .where(FactMovieTrendsWeekly.movie_key == movie_key)
            .order_by(desc(FactMovieTrendsWeekly.week_start_date))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()
