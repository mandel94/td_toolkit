"""SQLAlchemy ORM models for raw and analytics layers."""

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Date,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from movie_trends.database.base import AuditMixin, Base, TimestampMixin


# ============================================================================
# RAW LAYER - Staging tables with JSONB payloads
# ============================================================================


class RawTMDbTrending(Base, AuditMixin):
    """Raw trending data from TMDb API."""

    __tablename__ = "raw_tmdb_trending"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    time_window: Mapped[str] = mapped_column(String(10), nullable=False)  # 'day' or 'week'
    media_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 'movie'
    fetched_at: Mapped[datetime] = mapped_column(nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index("idx_raw_trending_fetch_time", "fetched_at", "time_window"),
    )


class RawTMDbMovie(Base, AuditMixin):
    """Raw movie details from TMDb API."""

    __tablename__ = "raw_tmdb_movies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    movie_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fetched_at: Mapped[datetime] = mapped_column(nullable=False, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index("idx_raw_movie_id_fetch", "movie_id", "fetched_at"),
    )


# ============================================================================
# ANALYTICS LAYER - Dimensional Model
# ============================================================================


class DimDate(Base):
    """Date dimension table."""

    __tablename__ = "dim_date"

    date_key: Mapped[date] = mapped_column(Date, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    week: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_year: Mapped[int] = mapped_column(Integer, nullable=False)
    is_weekend: Mapped[bool] = mapped_column(nullable=False)


class DimGenre(Base, TimestampMixin):
    """Genre dimension table."""

    __tablename__ = "dim_genre"

    genre_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    genre_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)


class DimMovie(Base, TimestampMixin):
    """Movie dimension table (SCD Type 2)."""

    __tablename__ = "dim_movie"

    movie_key: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    movie_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    original_title: Mapped[str] = mapped_column(String(500), nullable=True)
    original_language: Mapped[str] = mapped_column(String(10), nullable=True)
    release_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    overview: Mapped[str | None] = mapped_column(Text, nullable=True)
    genres: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    production_countries: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    
    # SCD Type 2 fields
    valid_from: Mapped[datetime] = mapped_column(nullable=False, index=True)
    valid_to: Mapped[datetime | None] = mapped_column(nullable=True)
    is_current: Mapped[bool] = mapped_column(nullable=False, default=True, index=True)

    __table_args__ = (
        Index("idx_movie_id_current", "movie_id", "is_current"),
        Index("idx_movie_validity", "valid_from", "valid_to"),
    )


class FactMoviePopularityDaily(Base, TimestampMixin):
    """Daily popularity facts."""

    __tablename__ = "fact_movie_popularity_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    movie_key: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dim_movie.movie_key"),
        nullable=False,
        index=True,
    )
    date_key: Mapped[date] = mapped_column(
        Date,
        ForeignKey("dim_date.date_key"),
        nullable=False,
        index=True,
    )
    
    # Raw metrics from TMDb
    popularity: Mapped[float] = mapped_column(Float, nullable=False)
    vote_count: Mapped[int] = mapped_column(Integer, nullable=False)
    vote_average: Mapped[float] = mapped_column(Float, nullable=False)
    
    # Derived metrics
    popularity_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    
    movie: Mapped[DimMovie] = relationship("DimMovie")
    date: Mapped[DimDate] = relationship("DimDate")

    __table_args__ = (
        UniqueConstraint("movie_key", "date_key", name="uq_movie_date"),
        Index("idx_date_popularity", "date_key", "popularity"),
    )


class FactMovieTrendsWeekly(Base, TimestampMixin):
    """Weekly trend metrics and scores."""

    __tablename__ = "fact_movie_trends_weekly"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    movie_key: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dim_movie.movie_key"),
        nullable=False,
        index=True,
    )
    week_start_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    week_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    
    # Base metrics (weekly aggregates)
    avg_popularity: Mapped[float] = mapped_column(Float, nullable=False)
    avg_vote_count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_vote_average: Mapped[float] = mapped_column(Float, nullable=False)
    
    # Growth metrics
    popularity_growth: Mapped[float | None] = mapped_column(Float, nullable=True)
    vote_velocity: Mapped[float | None] = mapped_column(Float, nullable=True)
    
    # Normalized components
    norm_popularity_growth: Mapped[float | None] = mapped_column(Float, nullable=True)
    norm_vote_velocity: Mapped[float | None] = mapped_column(Float, nullable=True)
    
    # Adjustment factors
    recency_factor: Mapped[float] = mapped_column(Float, nullable=False)
    stability_factor: Mapped[float] = mapped_column(Float, nullable=False)
    volatility: Mapped[float | None] = mapped_column(Float, nullable=True)
    
    # Final score and classification
    trend_score: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    trend_classification: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True,
    )  # EMERGING, PEAKING, STABLE, DECLINING
    
    # Metadata
    formula_version: Mapped[str] = mapped_column(String(10), nullable=False)
    
    movie: Mapped[DimMovie] = relationship("DimMovie")

    __table_args__ = (
        UniqueConstraint("movie_key", "week_start_date", name="uq_movie_week"),
        Index("idx_week_trend_score", "week_start_date", "trend_score"),
        Index("idx_trend_classification_score", "trend_classification", "trend_score"),
    )
