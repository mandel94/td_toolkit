"""Repository exports."""

from movie_trends.repositories.repositories import (
    MovieRepository,
    PopularityRepository,
    RawDataRepository,
    TrendsRepository,
)

__all__ = [
    "RawDataRepository",
    "MovieRepository",
    "PopularityRepository",
    "TrendsRepository",
]
