"""Data ingestion service for TMDb data."""

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from movie_trends.clients import TMDbClient
from movie_trends.logging_config import get_logger
from movie_trends.repositories import MovieRepository, PopularityRepository, RawDataRepository
from movie_trends.schemas.tmdb import TMDbMovieDetailed

logger = get_logger(__name__)


class IngestionService:
    """Service for ingesting TMDb data into database."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.raw_repo = RawDataRepository(session)
        self.movie_repo = MovieRepository(session)
        self.popularity_repo = PopularityRepository(session)

    async def ingest_trending_data(
        self,
        time_window: str = "week",
        max_pages: int = 5,
    ) -> dict[str, Any]:
        """
        Ingest trending data from TMDb.
        
        Args:
            time_window: 'day' or 'week'
            max_pages: Maximum pages to fetch
            
        Returns:
            Ingestion summary
        """
        batch_id = str(uuid.uuid4())
        logger.info(
            "ingestion_started",
            batch_id=batch_id,
            time_window=time_window,
        )
        
        movies_processed = 0
        movies_new = 0
        
        async with TMDbClient() as client:
            # Fetch trending pages
            trending_pages = await client.get_all_trending_pages(
                media_type="movie",
                time_window=time_window,
                max_pages=max_pages,
            )
            
            today = date.today()
            
            for page_response in trending_pages:
                # Save raw response
                await self.raw_repo.save_raw_trending(
                    time_window=time_window,
                    media_type="movie",
                    payload=page_response.model_dump(),
                    batch_id=batch_id,
                )
                
                # Process each movie
                for movie in page_response.results:
                    try:
                        # Get detailed movie info
                        detailed_movie = await client.get_movie_details(movie.id)
                        
                        # Save raw movie data
                        await self.raw_repo.save_raw_movie(
                            movie_id=movie.id,
                            payload=detailed_movie.model_dump(),
                            batch_id=batch_id,
                        )
                        
                        # Upsert to dimension
                        current_movie = await self.movie_repo.get_current_movie(movie.id)
                        is_new = current_movie is None
                        
                        dim_movie = await self._upsert_movie_dimension(detailed_movie)
                        
                        # Insert daily popularity fact
                        await self.popularity_repo.upsert_daily_popularity(
                            movie_key=dim_movie.movie_key,
                            date_key=today,
                            popularity=movie.popularity,
                            vote_count=movie.vote_count,
                            vote_average=movie.vote_average,
                        )
                        
                        movies_processed += 1
                        if is_new:
                            movies_new += 1
                            
                    except Exception as e:
                        logger.error(
                            "movie_processing_failed",
                            movie_id=movie.id,
                            error=str(e),
                        )
                        continue
            
            await self.session.commit()
            
            summary = {
                "batch_id": batch_id,
                "time_window": time_window,
                "pages_fetched": len(trending_pages),
                "movies_processed": movies_processed,
                "movies_new": movies_new,
                "completed_at": datetime.utcnow().isoformat(),
            }
            
            logger.info("ingestion_completed", **summary)
            
            return summary

    async def _upsert_movie_dimension(self, movie: TMDbMovieDetailed) -> Any:
        """Upsert movie to dimension table."""
        genres = [g.name for g in movie.genres]
        countries = [c.iso_3166_1 for c in movie.production_countries]
        
        return await self.movie_repo.upsert_movie(
            movie_id=movie.id,
            title=movie.title,
            original_title=movie.original_title,
            original_language=movie.original_language,
            release_date=movie.release_date,
            overview=movie.overview,
            genres=genres,
            production_countries=countries,
        )
