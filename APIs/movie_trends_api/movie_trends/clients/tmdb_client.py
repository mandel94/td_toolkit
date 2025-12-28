"""TMDb API client with async support and rate limiting."""

import asyncio
from datetime import datetime
from typing import Any

import httpx
from httpx import AsyncClient, Response

from movie_trends.config import get_settings
from movie_trends.logging_config import get_logger
from movie_trends.schemas.tmdb import (
    TMDbError,
    TMDbMovieDetailed,
    TMDbTrendingResponse,
)

logger = get_logger(__name__)


class RateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(self, rate_limit: int):
        """
        Initialize rate limiter.
        
        Args:
            rate_limit: Maximum requests per second
        """
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_update = asyncio.get_event_loop().time()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Acquire a token, waiting if necessary."""
        async with self.lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate_limit, self.tokens + elapsed * self.rate_limit)
            self.last_update = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_limit
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class TMDbAPIError(Exception):
    """TMDb API error exception."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"TMDb API Error {status_code}: {message}")


class TMDbClient:
    """Async TMDb API client with rate limiting and retry logic."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        rate_limit: int | None = None,
    ):
        """
        Initialize TMDb client.
        
        Args:
            api_key: TMDb API key (defaults to config)
            base_url: Base URL for TMDb API (defaults to config)
            rate_limit: Rate limit per second (defaults to config)
        """
        settings = get_settings()
        self.api_key = api_key or settings.tmdb_api_key
        self.base_url = base_url or settings.tmdb_base_url
        self.rate_limiter = RateLimiter(rate_limit or settings.tmdb_rate_limit_per_second)
        self.client: AsyncClient | None = None

    async def __aenter__(self) -> "TMDbClient":
        """Async context manager entry."""
        self.client = AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            params={"api_key": self.api_key},
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        if self.client:
            await self.client.aclose()

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> Response:
        """
        Make rate-limited HTTP request with retry logic.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            max_retries: Maximum retry attempts
            
        Returns:
            HTTP response
            
        Raises:
            TMDbAPIError: On API error
        """
        if not self.client:
            raise RuntimeError("Client not initialized. Use async context manager.")

        await self.rate_limiter.acquire()

        for attempt in range(max_retries):
            try:
                response = await self.client.request(method, endpoint, params=params)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limit
                    wait_time = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        "rate_limit_exceeded",
                        wait_time=wait_time,
                        attempt=attempt + 1,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    error_data = response.json()
                    raise TMDbAPIError(
                        status_code=response.status_code,
                        message=error_data.get("status_message", "Unknown error"),
                    )
            except httpx.RequestError as e:
                logger.error("request_error", error=str(e), attempt=attempt + 1)
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)

        raise TMDbAPIError(500, "Max retries exceeded")

    async def get_trending(
        self,
        media_type: str = "movie",
        time_window: str = "week",
        page: int = 1,
    ) -> TMDbTrendingResponse:
        """
        Get trending movies.
        
        Args:
            media_type: Media type ('movie', 'tv', 'all')
            time_window: Time window ('day', 'week')
            page: Page number
            
        Returns:
            Trending response
        """
        endpoint = f"/trending/{media_type}/{time_window}"
        response = await self._request("GET", endpoint, params={"page": page})
        
        logger.info(
            "trending_fetched",
            media_type=media_type,
            time_window=time_window,
            page=page,
            results=len(response.json()["results"]),
        )
        
        return TMDbTrendingResponse(**response.json())

    async def get_movie_details(self, movie_id: int) -> TMDbMovieDetailed:
        """
        Get detailed movie information.
        
        Args:
            movie_id: TMDb movie ID
            
        Returns:
            Detailed movie data
        """
        endpoint = f"/movie/{movie_id}"
        response = await self._request("GET", endpoint)
        
        logger.info("movie_details_fetched", movie_id=movie_id)
        
        return TMDbMovieDetailed(**response.json())

    async def get_all_trending_pages(
        self,
        media_type: str = "movie",
        time_window: str = "week",
        max_pages: int = 5,
    ) -> list[TMDbTrendingResponse]:
        """
        Get multiple pages of trending data.
        
        Args:
            media_type: Media type
            time_window: Time window
            max_pages: Maximum pages to fetch
            
        Returns:
            List of trending responses
        """
        results = []
        
        # Get first page to determine total pages
        first_page = await self.get_trending(media_type, time_window, page=1)
        results.append(first_page)
        
        total_pages = min(first_page.total_pages, max_pages)
        
        # Fetch remaining pages concurrently
        if total_pages > 1:
            tasks = [
                self.get_trending(media_type, time_window, page=page)
                for page in range(2, total_pages + 1)
            ]
            remaining_results = await asyncio.gather(*tasks)
            results.extend(remaining_results)
        
        logger.info(
            "all_trending_pages_fetched",
            media_type=media_type,
            time_window=time_window,
            pages=len(results),
            total_movies=sum(len(r.results) for r in results),
        )
        
        return results
