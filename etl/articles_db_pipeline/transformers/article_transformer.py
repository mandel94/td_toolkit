"""Weekly data transformation module for dimensional model."""
import sys
import os
from typing import List, Set, Dict
from datetime import date, timedelta
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from map_ga4_categories import map_ga4_categories
from etl.articles_db_pipeline.models.article import (
    RawWeeklyData, DimWeekData, DimArticleData, DimAuthorData,
    DimCategoryData, FactWeeklyMetricsData, ProcessedWeeklyBatch
)


class ArticleTransformer:
    """Transform raw weekly data into dimensional format."""
    
    def __init__(self):
        logger.info("Initialized ArticleTransformer for dimensional model")
    
    def transform_weekly_batch(
        self,
        raw_weekly_data: List[RawWeeklyData]
    ) -> ProcessedWeeklyBatch:
        """Transform raw weekly data into dimensional format.
        
        Args:
            raw_weekly_data: List of raw weekly data from GA4
            
        Returns:
            ProcessedWeeklyBatch with all dimensional data ready for loading
        """
        logger.info(f"Transforming {len(raw_weekly_data)} weekly records")
        
        # Collect unique dimensions
        unique_weeks: Set[tuple] = set()
        unique_articles: Set[str] = set()
        unique_authors: Set[str] = set()
        unique_categories: Set[str] = set()
        
        # Prepare fact table data
        metrics_data: List[FactWeeklyMetricsData] = []
        
        for record in raw_weekly_data:
            try:
                # Map category
                category = self._map_category(record.page_path)
                
                # Extract author (default to "Unknown" for now)
                author = "Unknown"
                
                # Add to unique sets
                unique_weeks.add((record.year, record.week))
                unique_articles.add(record.page_path)
                unique_authors.add(author)
                unique_categories.add(category)
                
                # Create week_id
                week_id = record.year * 100 + record.week
                
                # Create fact metrics
                metric = FactWeeklyMetricsData(
                    page_path=record.page_path,
                    author_name=author,
                    category_name=category,
                    week_id=week_id,
                    screen_page_views=record.screen_page_views,
                    engaged_sessions=record.engaged_sessions,
                    sessions=record.sessions,
                    engagement_rate=record.engagement_rate,
                    average_session_duration=record.average_session_duration
                )
                metrics_data.append(metric)
                
            except Exception as e:
                logger.error(f"Failed to transform record: {str(e)}")
                continue
        
        # Create dimension data
        weeks = [self._create_week_dimension(year, week) for year, week in sorted(unique_weeks)]
        articles = [DimArticleData(page_path=path) for path in sorted(unique_articles)]
        authors = [DimAuthorData(author_name=name) for name in sorted(unique_authors)]
        categories = [DimCategoryData(category_name=name) for name in sorted(unique_categories)]
        
        batch = ProcessedWeeklyBatch(
            weeks=weeks,
            articles=articles,
            authors=authors,
            categories=categories,
            metrics=metrics_data,
            batch_id=f"weekly_batch_{len(metrics_data)}"
        )
        
        logger.success(f"Transformed batch: {len(weeks)} weeks, {len(articles)} articles, "
                      f"{len(authors)} authors, {len(categories)} categories, {len(metrics_data)} metrics")
        return batch
    
    def _create_week_dimension(self, year: int, week: int) -> DimWeekData:
        """Create week dimension data with calculated dates."""
        # Calculate week_start_date (Monday of the week)
        # ISO week date: week 1 is the week with the first Thursday
        jan_4 = date(year, 1, 4)
        week_1_monday = jan_4 - timedelta(days=jan_4.weekday())
        week_start = week_1_monday + timedelta(weeks=week - 1)
        week_end = week_start + timedelta(days=6)
        
        # Calculate quarter and primary month
        month = week_start.month
        quarter = (month - 1) // 3 + 1
        
        week_id = year * 100 + week
        year_week = f"{year}-W{week:02d}"
        
        return DimWeekData(
            week_id=week_id,
            year=year,
            week_of_year=week,
            week_start_date=week_start,
            week_end_date=week_end,
            quarter=quarter,
            month=month,
            year_week=year_week
        )
    
    def _map_category(self, page_path: str) -> str:
        """Map page path to article category."""
        try:
            # Use existing category mapping function
            category = map_ga4_categories(page_path)
            
            # Handle "Si farà" special case
            if "si-fara" in page_path.lower():
                category = "Si farà"
            
            # Merge "Recensioni / In Sala" with "Recensioni"
            if category in ["Recensioni / In Sala", "Recensioni"]:
                category = "Recensioni"
            
            return category if category else "Uncategorized"
            
        except Exception as e:
            logger.warning(f"Failed to map category for {page_path}: {str(e)}")
            return "Uncategorized"