"""API client exports."""

from movie_trends.clients.tmdb_client import TMDbClient, TMDbAPIError

__all__ = ["TMDbClient", "TMDbAPIError"]
