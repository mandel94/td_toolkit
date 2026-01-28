"""
Text Mining Pipeline Orchestrator
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import logging
import json
from pathlib import Path
from datetime import datetime
import time

from etl.text_mining.config import config
from etl.text_mining.extractors.ga4_sample_extractor import GA4SampleExtractor
from etl.text_mining.scrapers.content_scraper import ContentScraper
from etl.text_mining.processors.text_feature_extractor import TextFeatureExtractor
from etl.text_mining.storage.postgres_storage import PostgresStorage
from etl.text_mining.messaging.redis_queue import RedisQueue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TextMiningOrchestrator:
    """
    Main orchestrator for text mining pipeline
    
    Modes:
    - synchronous: Run entire pipeline sequentially (MVP)
    - event-driven: Use Redis for async event-driven processing (future)
    """
    
    def __init__(self, mode: str = "synchronous"):
        self.mode = mode
        
        # Initialize components
        self.ga4_extractor = GA4SampleExtractor()
        self.scraper = ContentScraper()
        self.feature_extractor = TextFeatureExtractor()
        self.storage = PostgresStorage()
        
        if mode == "event-driven":
            self.queue = RedisQueue()
        else:
            self.queue = None
        
        logger.info(f"TextMiningOrchestrator initialized in {mode} mode")
    
    def run_pipeline(self, sample_size: int = None):
        """
        Run complete pipeline
        
        Args:
            sample_size: Number of articles to process (default from config)
        """
        logger.info("=" * 80)
        logger.info("STARTING TEXT MINING PIPELINE")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # Step 1: Extract GA4 sample
            logger.info("\n[STEP 1/5] Extracting GA4 sample...")
            ga4_event = self.ga4_extractor.extract_sample(sample_size=sample_size)
            
            if not ga4_event.articles:
                logger.warning("No articles found in GA4 sample. Exiting.")
                return
            
            logger.info(f"✓ Extracted {len(ga4_event.articles)} articles from GA4")
            
            # Store sample metadata
            self.storage.store_sample_metadata(
                sample_id=ga4_event.sample_id,
                generated_at=ga4_event.generated_at,
                articles_count=len(ga4_event.articles)
            )
            
            # Step 2: Scrape content
            logger.info("\n[STEP 2/5] Scraping article content...")
            scraped_event = self.scraper.scrape_sample(ga4_event)
            logger.info(f"✓ Scraped {scraped_event.articles_count} articles")
            
            # Step 3: Store raw HTML
            logger.info("\n[STEP 3/5] Storing raw HTML content...")
            with open(scraped_event.json_path, 'r', encoding='utf-8') as f:
                scraped_data = json.load(f)
            self.storage.store_raw_articles(scraped_data, ga4_event.sample_id)
            logger.info("✓ Raw content stored in database")
            
            # Step 4: Extract features
            logger.info("\n[STEP 4/5] Extracting text features...")
            
            # Build GA4 metadata mapping
            ga4_metadata = {
                article.pagepath: {
                    'pageviews': article.pageviews,
                    'engaged_sessions': article.engaged_sessions,
                    'avg_session_duration': article.avg_session_duration,
                    'engagement_rate': article.engagement_rate,
                    'editorial_score': article.editorial_score
                }
                for article in ga4_event.articles
            }
            
            features_df = self.feature_extractor.process_scraped_articles(
                scraped_event,
                ga4_metadata
            )
            logger.info(f"✓ Extracted features for {len(features_df)} articles")
            
            # Step 5: Store features
            logger.info("\n[STEP 5/5] Storing features in database...")
            self.storage.store_features(features_df)
            logger.info("✓ Features stored in database")
            
            # Summary
            elapsed = time.time() - start_time
            logger.info("\n" + "=" * 80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"Sample ID: {ga4_event.sample_id}")
            logger.info(f"Articles processed: {len(ga4_event.articles)}")
            logger.info(f"Processing version: {config.PROCESSING_VERSION}")
            logger.info(f"Elapsed time: {elapsed:.2f} seconds")
            logger.info("=" * 80)
            
            # Print sample features
            if not features_df.empty:
                logger.info("\nSample features (first article):")
                first_row = features_df.iloc[0]
                for col in ['pagepath', 'word_count', 'pageviews', 'engagement_rate', 'editorial_score']:
                    if col in first_row:
                        logger.info(f"  {col}: {first_row[col]}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.storage:
            self.storage.close()
        if self.queue:
            self.queue.close()
        logger.info("Resources cleaned up")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Text Mining Pipeline Orchestrator")
    parser.add_argument(
        '--sample-size',
        type=int,
        default=10,
        help='Number of articles to sample (default: 10)'
    )
    parser.add_argument(
        '--mode',
        choices=['synchronous', 'event-driven'],
        default='synchronous',
        help='Pipeline execution mode (default: synchronous)'
    )
    
    args = parser.parse_args()
    
    # Initialize storage schema
    logger.info("Initializing database schema...")
    storage = PostgresStorage()
    storage.create_schema()
    storage.close()
    
    # Run pipeline
    orchestrator = TextMiningOrchestrator(mode=args.mode)
    orchestrator.run_pipeline(sample_size=args.sample_size)


if __name__ == "__main__":
    main()
