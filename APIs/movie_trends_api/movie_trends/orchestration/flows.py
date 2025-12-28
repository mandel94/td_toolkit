"""Prefect flows for orchestrating data pipelines."""

from datetime import date, datetime, timedelta

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from movie_trends.database import AsyncSessionLocal
from movie_trends.logging_config import get_logger
from movie_trends.services import IngestionService, TransformationService

logger = get_logger(__name__)


@task(name="ingest-trending-data", retries=3, retry_delay_seconds=60)
async def ingest_trending_task(time_window: str = "week", max_pages: int = 5) -> dict:
    """
    Task for ingesting trending data from TMDb.
    
    Args:
        time_window: 'day' or 'week'
        max_pages: Maximum pages to fetch
        
    Returns:
        Ingestion summary
    """
    async with AsyncSessionLocal() as session:
        service = IngestionService(session)
        summary = await service.ingest_trending_data(
            time_window=time_window,
            max_pages=max_pages,
        )
    
    logger.info("ingest_task_completed", **summary)
    return summary


@task(name="calculate-weekly-trends", retries=2, retry_delay_seconds=120)
async def calculate_trends_task(target_date: date | None = None) -> dict:
    """
    Task for calculating weekly trend scores.
    
    Args:
        target_date: Date to calculate trends for
        
    Returns:
        Calculation summary
    """
    async with AsyncSessionLocal() as session:
        service = TransformationService(session)
        summary = await service.calculate_weekly_trends(target_date)
    
    logger.info("trends_task_completed", **summary)
    return summary


@flow(
    name="daily-trending-ingestion",
    description="Daily ingestion of trending data from TMDb",
    task_runner=ConcurrentTaskRunner(),
)
async def daily_ingestion_flow() -> dict:
    """
    Daily flow to ingest trending data.
    
    Runs:
    1. Ingest daily trending
    2. Ingest weekly trending
    
    Returns:
        Flow summary
    """
    logger.info("daily_ingestion_flow_started")
    
    # Ingest both daily and weekly trending
    daily_summary = await ingest_trending_task(time_window="day", max_pages=3)
    weekly_summary = await ingest_trending_task(time_window="week", max_pages=5)
    
    summary = {
        "flow": "daily-ingestion",
        "completed_at": datetime.utcnow().isoformat(),
        "daily_ingestion": daily_summary,
        "weekly_ingestion": weekly_summary,
    }
    
    logger.info("daily_ingestion_flow_completed", **summary)
    return summary


@flow(
    name="weekly-trend-calculation",
    description="Calculate weekly trend scores for all movies",
)
async def weekly_trends_flow(target_date: date | None = None) -> dict:
    """
    Weekly flow to calculate trend scores.
    
    Args:
        target_date: Date to calculate trends for (defaults to last week)
        
    Returns:
        Flow summary
    """
    logger.info("weekly_trends_flow_started", target_date=target_date)
    
    # Calculate trends for target week
    trends_summary = await calculate_trends_task(target_date)
    
    summary = {
        "flow": "weekly-trends",
        "completed_at": datetime.utcnow().isoformat(),
        "trends_calculation": trends_summary,
    }
    
    logger.info("weekly_trends_flow_completed", **summary)
    return summary


@flow(
    name="full-pipeline",
    description="Complete pipeline: ingest data and calculate trends",
    task_runner=ConcurrentTaskRunner(),
)
async def full_pipeline_flow(
    ingest_time_window: str = "week",
    calculate_target_date: date | None = None,
) -> dict:
    """
    Full pipeline flow combining ingestion and trend calculation.
    
    Args:
        ingest_time_window: Time window for ingestion
        calculate_target_date: Date for trend calculation
        
    Returns:
        Complete pipeline summary
    """
    logger.info("full_pipeline_flow_started")
    
    # Step 1: Ingest data
    ingestion_summary = await ingest_trending_task(
        time_window=ingest_time_window,
        max_pages=5,
    )
    
    # Step 2: Calculate trends
    trends_summary = await calculate_trends_task(calculate_target_date)
    
    summary = {
        "flow": "full-pipeline",
        "completed_at": datetime.utcnow().isoformat(),
        "ingestion": ingestion_summary,
        "trends": trends_summary,
    }
    
    logger.info("full_pipeline_flow_completed", **summary)
    return summary


@flow(
    name="backfill-trends",
    description="Backfill trend calculations for historical weeks",
)
async def backfill_trends_flow(
    start_date: date,
    end_date: date | None = None,
) -> dict:
    """
    Backfill trend calculations for a date range.
    
    Args:
        start_date: Start date for backfill
        end_date: End date for backfill (defaults to last week)
        
    Returns:
        Backfill summary
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=7)
    
    logger.info(
        "backfill_flow_started",
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    
    weeks_processed = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            summary = await calculate_trends_task(current_date)
            weeks_processed.append(summary)
        except Exception as e:
            logger.error(
                "backfill_week_failed",
                date=current_date.isoformat(),
                error=str(e),
            )
        
        current_date += timedelta(days=7)
    
    backfill_summary = {
        "flow": "backfill-trends",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "weeks_processed": len(weeks_processed),
        "completed_at": datetime.utcnow().isoformat(),
    }
    
    logger.info("backfill_flow_completed", **backfill_summary)
    return backfill_summary


if __name__ == "__main__":
    import asyncio
    
    # Example: Run full pipeline
    asyncio.run(full_pipeline_flow())
