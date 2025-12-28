"""Example usage scripts for the Movie Trends API."""

import asyncio
from datetime import date, timedelta

from movie_trends.clients import TMDbClient
from movie_trends.database import AsyncSessionLocal
from movie_trends.services import IngestionService, TransformationService


async def example_ingest_data():
    """Example: Ingest trending data from TMDb."""
    print("🎬 Ingesting trending data...")
    
    async with AsyncSessionLocal() as session:
        service = IngestionService(session)
        summary = await service.ingest_trending_data(
            time_window="week",
            max_pages=3,
        )
        print(f"✓ Ingested {summary['movies_processed']} movies")
        print(f"  Batch ID: {summary['batch_id']}")


async def example_calculate_trends():
    """Example: Calculate weekly trends."""
    print("📊 Calculating weekly trends...")
    
    # Calculate for last week
    target_date = date.today() - timedelta(days=7)
    
    async with AsyncSessionLocal() as session:
        service = TransformationService(session)
        summary = await service.calculate_weekly_trends(target_date)
        print(f"✓ Calculated trends for {summary['movies_processed']} movies")
        print(f"  Week: {summary['week_start']} to {summary['week_end']}")


async def example_tmdb_client():
    """Example: Use TMDb client directly."""
    print("🔍 Fetching from TMDb API...")
    
    async with TMDbClient() as client:
        # Get trending movies
        trending = await client.get_trending(media_type="movie", time_window="week")
        print(f"✓ Found {len(trending.results)} trending movies")
        
        # Get details for first movie
        if trending.results:
            movie = trending.results[0]
            details = await client.get_movie_details(movie.id)
            print(f"  Top movie: {details.title} ({details.release_date})")
            print(f"  Popularity: {details.popularity}")
            print(f"  Genres: {[g.name for g in details.genres]}")


async def example_api_client():
    """Example: Use the API as a client."""
    import httpx
    
    print("🌐 Calling Movie Trends API...")
    
    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        # Health check
        response = await client.get("/health")
        print(f"✓ Health: {response.json()}")
        
        # Get trending movies
        response = await client.get("/v1/trends/movies?limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Found {len(data['data'])} trending movies")
            
            for movie_trend in data['data']:
                movie = movie_trend['movie']
                metrics = movie_trend['trend_metrics']
                print(f"  {movie['title']}")
                print(f"    Score: {metrics['trend_score']:.1f}")
                print(f"    Classification: {metrics['trend_classification']}")
        else:
            print(f"✗ Error: {response.status_code}")


async def example_backfill():
    """Example: Backfill historical trends."""
    print("⏮️  Backfilling historical trends...")
    
    start_date = date(2024, 12, 1)
    end_date = date(2024, 12, 31)
    
    current = start_date
    weeks_processed = 0
    
    async with AsyncSessionLocal() as session:
        service = TransformationService(session)
        
        while current <= end_date:
            try:
                summary = await service.calculate_weekly_trends(current)
                weeks_processed += 1
                print(f"  ✓ Week {summary['week_start']}: {summary['movies_processed']} movies")
            except Exception as e:
                print(f"  ✗ Week {current}: {str(e)}")
            
            current += timedelta(days=7)
    
    print(f"✓ Backfilled {weeks_processed} weeks")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("Movie Trends API - Usage Examples")
    print("=" * 60)
    print()
    
    # Example 1: TMDb Client
    await example_tmdb_client()
    print()
    
    # Example 2: Ingest Data
    # await example_ingest_data()
    # print()
    
    # Example 3: Calculate Trends
    # await example_calculate_trends()
    # print()
    
    # Example 4: API Client
    # await example_api_client()
    # print()
    
    # Example 5: Backfill
    # await example_backfill()
    # print()
    
    print("=" * 60)
    print("✓ Examples completed")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
