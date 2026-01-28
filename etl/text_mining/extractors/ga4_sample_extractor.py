"""
GA4 Sample Extractor
Extracts random sample of articles with KPI metrics from GA4
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from typing import List
import pandas as pd
from datetime import datetime
import logging

from ga4_api.ga4_api import Ga4Client
from etl.text_mining.config import config
from etl.text_mining.events import GA4SampleReadyEvent, ArticleMetadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GA4SampleExtractor:
    """
    Extract random sample of articles with GA4 KPIs
    
    Responsibilities:
    - Query GA4 for article performance data
    - Apply random sampling
    - Calculate editorial_score (if applicable)
    - Create GA4SampleReadyEvent
    """
    
    def __init__(self):
        self.ga4_client = Ga4Client(credentials_file=config.GA4_CREDENTIALS_PATH)
        self.property_id = config.GA4_PROPERTY_ID
    
    def extract_sample(
        self,
        sample_size: int = None,
        start_date: str = None,
        end_date: str = None
    ) -> GA4SampleReadyEvent:
        """
        Extract a random sample of articles with KPI metrics
        
        Args:
            sample_size: Number of articles to sample (default from config)
            start_date: Start date for data range (default from config)
            end_date: End date for data range (default from config)
            
        Returns:
            GA4SampleReadyEvent with sampled articles and metrics
        """
        sample_size = sample_size or config.SAMPLE_SIZE
        start_date = start_date or config.DATE_START
        end_date = end_date if end_date and end_date != "today" else datetime.now().strftime("%Y-%m-%d")
        
        logger.info(f"Extracting GA4 data from {start_date} to {end_date}")
        
        # Query GA4 with required dimensions and metrics
        df = self.ga4_client.run_query(
            property_id=self.property_id,
            dimensions=["pagePath"],
            metrics=[
                "screenPageViews",
                "engagedSessions",
                "averageSessionDuration",
                "engagementRate"
            ],
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.warning("No data returned from GA4")
            return GA4SampleReadyEvent(articles=[])
        
        # Filter only article pages that begin with /{numbers]contain  pattern
        df = df[df['pagePath'].str.match(r"^/\d+")]

        if len(df) == 0:
            logger.warning("No article pages found after filtering")
            return GA4SampleReadyEvent(articles=[])
        
        # Convert metrics to proper types
        df['screenPageViews'] = pd.to_numeric(df['screenPageViews'], errors='coerce').fillna(0).astype(int)
        df['engagedSessions'] = pd.to_numeric(df['engagedSessions'], errors='coerce').fillna(0).astype(int)
        df['averageSessionDuration'] = pd.to_numeric(df['averageSessionDuration'], errors='coerce').fillna(0.0)
        df['engagementRate'] = pd.to_numeric(df['engagementRate'], errors='coerce').fillna(0.0)
        
        # Calculate editorial_score (simple weighted average for now)
        # This can be replaced with more sophisticated scoring
        df['editorial_score'] = self._calculate_editorial_score(df)
        
        # Random sampling
        if len(df) > sample_size:
            df_sample = df.sample(n=sample_size, random_state=42)
            logger.info(f"Sampled {sample_size} articles from {len(df)} total")
        else:
            df_sample = df
            logger.info(f"Using all {len(df)} articles (less than sample size)")
        
        # Create event
        articles = []
        for _, row in df_sample.iterrows():
            article = ArticleMetadata(
                pagepath=row['pagePath'],
                pageviews=int(row['screenPageViews']),
                engaged_sessions=int(row['engagedSessions']),
                avg_session_duration=float(row['averageSessionDuration']),
                engagement_rate=float(row['engagementRate']),
                editorial_score=float(row['editorial_score'])
            )
            articles.append(article)
        
        event = GA4SampleReadyEvent(articles=articles)
        logger.info(f"Created GA4SampleReadyEvent with sample_id={event.sample_id}")
        
        return event
    
    def _calculate_editorial_score(self, df: pd.DataFrame) -> pd.Series:
        """
        Calculate editorial score from GA4 metrics
        
        Simple weighted average for MVP:
        - page_views: 40%
        - engaged_sessions (normalized): 30%
        - avg_session_duration (normalized): 30%
        """
        # Normalize metrics to 0-1 range

        page_views_norm = (df['screenPageViews'] - df['screenPageViews'].min()) / (
            df['screenPageViews'].max() - df['screenPageViews'].min() + 1e-10
        )
        engaged_norm = (df['engagedSessions'] - df['engagedSessions'].min()) / (
            df['engagedSessions'].max() - df['engagedSessions'].min() + 1e-10
        )
        duration_norm = (df['averageSessionDuration'] - df['averageSessionDuration'].min()) / (
            df['averageSessionDuration'].max() - df['averageSessionDuration'].min() + 1e-10
        )
        
        # Weighted score
        score = (
            0.4 * page_views_norm +
            0.3 * engaged_norm +
            0.3 * duration_norm
        )
        
        return score.fillna(0.0)


if __name__ == "__main__":
    # Test extraction
    extractor = GA4SampleExtractor()
    event = extractor.extract_sample(sample_size=5)
    print(event.model_dump_json(indent=2))
