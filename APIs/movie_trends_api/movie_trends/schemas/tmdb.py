"""Pydantic schemas for TMDb API responses."""

from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class TMDbMovieBase(BaseModel):
    """Base TMDb movie schema."""

    id: int
    title: str
    original_title: str
    original_language: str
    overview: str | None = None
    release_date: date | None = None
    popularity: float
    vote_count: int
    vote_average: float
    adult: bool = False
    video: bool = False
    backdrop_path: str | None = None
    poster_path: str | None = None


class TMDbGenre(BaseModel):
    """TMDb genre schema."""

    id: int
    name: str


class TMDbProductionCountry(BaseModel):
    """TMDb production country schema."""

    iso_3166_1: str = Field(..., alias="iso_3166_1")
    name: str


class TMDbMovieDetailed(TMDbMovieBase):
    """Detailed TMDb movie schema."""

    genres: list[TMDbGenre] = []
    production_countries: list[TMDbProductionCountry] = []
    budget: int = 0
    revenue: int = 0
    runtime: int | None = None
    status: str | None = None
    tagline: str | None = None
    homepage: str | None = None

    model_config = {"populate_by_name": True}


class TMDbTrendingResponse(BaseModel):
    """TMDb trending API response."""

    page: int
    results: list[TMDbMovieBase]
    total_pages: int
    total_results: int


class TMDbError(BaseModel):
    """TMDb API error response."""

    status_code: int
    status_message: str
    success: bool = False
