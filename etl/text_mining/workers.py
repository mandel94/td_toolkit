"""
Asynchronous Workers for Text Mining Pipeline

Event-driven workers that consume from Redis Streams and process in parallel
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import logging
import json
import time
from datetime import datetime

from etl.text_mining.config import config
from etl.text_mining.messaging.redis_queue import RedisQueue
from etl.text_mining.extractors.ga4_sample_extractor import GA4SampleExtractor
from etl.text_mining.scrapers.content_scraper import ContentScraper
from etl.text_mining.processors.text_feature_extractor import TextFeatureExtractor
from etl.text_mining.storage.postgres_storage import PostgresStorage
from etl.text_mining.events import (
    GA4SampleReadyEvent,
    ArticleHTMLScrapedEvent
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GA4PublisherWorker:
    """
    Worker that extracts GA4 samples and publishes events to Redis
    """
    
    def __init__(self):
        self.extractor = GA4SampleExtractor()
        self.queue = RedisQueue()
        logger.info("GA4PublisherWorker initialized")
    
    def publish_sample(self, sample_size: int = None):
        """
        Extract GA4 sample and publish to Redis stream
        
        Args:
            sample_size: Number of articles to sample
            
        Returns:
            sample_id of the published event
        """
        logger.info(f"Extracting GA4 sample (size={sample_size or config.SAMPLE_SIZE})")
        
        # Extract sample
        ga4_event = self.extractor.extract_sample(sample_size=sample_size)
        
        if not ga4_event.articles:
            logger.warning("No articles found in GA4 sample")
            return None
        
        # Publish to Redis stream
        event_data = ga4_event.model_dump()
        message_id = self.queue.publish_event(
            config.REDIS_STREAM_GA4,
            event_data
        )
        
        logger.info(f"✓ Published GA4 event: sample_id={ga4_event.sample_id}, articles={len(ga4_event.articles)}")
        
        return ga4_event.sample_id
    
    def close(self):
        self.queue.close()


class ScraperWorker:
    """
    Worker that consumes GA4 events, scrapes content, and publishes scraped events
    """
    
    def __init__(self, worker_id: str = "scraper-1"):
        self.worker_id = worker_id
        self.scraper = ContentScraper()
        self.queue = RedisQueue()
        self.storage = PostgresStorage()
        logger.info(f"ScraperWorker {worker_id} initialized")
    
    def run(self, blocking: bool = True, max_iterations: int = None):
        """
        Run worker loop consuming from GA4 stream
        
        Args:
            blocking: If True, blocks waiting for messages
            max_iterations: Max number of iterations (None = infinite)
        """
        logger.info(f"ScraperWorker {self.worker_id} starting...")
        
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                # Consume event from Redis
                events = self.queue.consume_events(
                    stream_name=config.REDIS_STREAM_GA4,
                    consumer_group="scrapers",
                    consumer_name=self.worker_id,
                    block_ms=5000 if blocking else 1,
                    count=1
                )
                
                if not events:
                    if not blocking:
                        logger.info("No events available, exiting...")
                        break
                    continue
                
                for event_data in events:
                    self._process_ga4_event(event_data)
                    
                    # Acknowledge message
                    self.queue.acknowledge_message(
                        config.REDIS_STREAM_GA4,
                        "scrapers",
                        event_data['_message_id']
                    )
                
                iteration += 1
                
            except KeyboardInterrupt:
                logger.info("Worker interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(5)  # Backoff on error
    
    def _process_ga4_event(self, event_data: dict):
        """Process a single GA4 event"""
        logger.info(f"Processing GA4 event: sample_id={event_data.get('sample_id')}")
        
        # Reconstruct GA4SampleReadyEvent
        ga4_event = GA4SampleReadyEvent(**event_data)
        
        # Store sample metadata
        self.storage.store_sample_metadata(
            sample_id=ga4_event.sample_id,
            generated_at=ga4_event.generated_at,
            articles_count=len(ga4_event.articles)
        )
        
        # Scrape content
        logger.info(f"Scraping {len(ga4_event.articles)} articles...")
        scraped_event = self.scraper.scrape_sample(ga4_event)
        
        # Store raw HTML
        with open(scraped_event.json_path, 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        self.storage.store_raw_articles(scraped_data, ga4_event.sample_id)
        
        # Publish scraped event to next stage
        scraped_event_data = scraped_event.model_dump()
        # Include GA4 metadata for feature extraction
        scraped_event_data['ga4_articles'] = [art.model_dump() for art in ga4_event.articles]
        
        self.queue.publish_event(
            config.REDIS_STREAM_SCRAPED,
            scraped_event_data
        )
        
        logger.info(f"✓ Scraped and published: sample_id={scraped_event.sample_id}")
    
    def close(self):
        self.queue.close()
        self.storage.close()


class FeatureWorker:
    """
    Worker that consumes scraped events, extracts features, and stores in DB
    """
    
    def __init__(self, worker_id: str = "feature-1"):
        self.worker_id = worker_id
        self.feature_extractor = TextFeatureExtractor()
        self.queue = RedisQueue()
        self.storage = PostgresStorage()
        logger.info(f"FeatureWorker {worker_id} initialized")
    
    def run(self, blocking: bool = True, max_iterations: int = None):
        """
        Run worker loop consuming from scraped stream
        
        Args:
            blocking: If True, blocks waiting for messages
            max_iterations: Max number of iterations (None = infinite)
        """
        logger.info(f"FeatureWorker {self.worker_id} starting...")
        
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                # Consume event from Redis
                events = self.queue.consume_events(
                    stream_name=config.REDIS_STREAM_SCRAPED,
                    consumer_group="features",
                    consumer_name=self.worker_id,
                    block_ms=5000 if blocking else 1,
                    count=1
                )
                
                if not events:
                    if not blocking:
                        logger.info("No events available, exiting...")
                        break
                    continue
                
                for event_data in events:
                    self._process_scraped_event(event_data)
                    
                    # Acknowledge message
                    self.queue.acknowledge_message(
                        config.REDIS_STREAM_SCRAPED,
                        "features",
                        event_data['_message_id']
                    )
                
                iteration += 1
                
            except KeyboardInterrupt:
                logger.info("Worker interrupted by user")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(5)  # Backoff on error
    
    def _process_scraped_event(self, event_data: dict):
        """Process a single scraped event"""
        logger.info(f"Processing scraped event: sample_id={event_data.get('sample_id')}")
        
        # Reconstruct ArticleHTMLScrapedEvent
        ga4_articles = event_data.pop('ga4_articles', [])
        scraped_event = ArticleHTMLScrapedEvent(**event_data)
        
        # Build GA4 metadata mapping
        ga4_metadata = {}
        for article in ga4_articles:
            ga4_metadata[article['pagepath']] = {
                'pageviews': article['pageviews'],
                'engaged_sessions': article['engaged_sessions'],
                'avg_session_duration': article['avg_session_duration'],
                'engagement_rate': article['engagement_rate'],
                'editorial_score': article.get('editorial_score')
            }
        
        # Extract features
        logger.info("Extracting text features...")
        features_df = self.feature_extractor.process_scraped_articles(
            scraped_event,
            ga4_metadata
        )
        
        # Store features
        self.storage.store_features(features_df)
        
        logger.info(f"✓ Extracted and stored features: {len(features_df)} articles")
    
    def close(self):
        self.queue.close()
        self.storage.close()


# ===== CLI for running workers =====

def main():
    """Main entry point for running workers"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Text Mining Pipeline Workers")
    parser.add_argument(
        'worker_type',
        choices=['publisher', 'scraper', 'feature', 'all'],
        help='Type of worker to run'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=10,
        help='Sample size for publisher (default: 10)'
    )
    parser.add_argument(
        '--worker-id',
        type=str,
        default=None,
        help='Worker ID for identification'
    )
    parser.add_argument(
        '--non-blocking',
        action='store_true',
        help='Run in non-blocking mode (process once and exit)'
    )
    parser.add_argument(
        '--max-iterations',
        type=int,
        default=None,
        help='Max iterations before stopping (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Initialize database schema
    logger.info("Initializing database schema...")
    storage = PostgresStorage()
    storage.create_schema()
    storage.close()
    
    try:
        if args.worker_type == 'publisher':
            worker = GA4PublisherWorker()
            sample_id = worker.publish_sample(sample_size=args.sample_size)
            logger.info(f"Published sample: {sample_id}")
            worker.close()
            
        elif args.worker_type == 'scraper':
            worker_id = args.worker_id or f"scraper-{os.getpid()}"
            worker = ScraperWorker(worker_id=worker_id)
            worker.run(
                blocking=not args.non_blocking,
                max_iterations=args.max_iterations
            )
            worker.close()
            
        elif args.worker_type == 'feature':
            worker_id = args.worker_id or f"feature-{os.getpid()}"
            worker = FeatureWorker(worker_id=worker_id)
            worker.run(
                blocking=not args.non_blocking,
                max_iterations=args.max_iterations
            )
            worker.close()
            
        elif args.worker_type == 'all':
            # Run all workers in sequence (for testing)
            logger.info("Running all workers in sequence...")
            
            # 1. Publish
            publisher = GA4PublisherWorker()
            sample_id = publisher.publish_sample(sample_size=args.sample_size)
            publisher.close()
            
            if sample_id:
                # 2. Scrape (non-blocking, one iteration)
                scraper = ScraperWorker(worker_id="scraper-all")
                scraper.run(blocking=False, max_iterations=1)
                scraper.close()
                
                # 3. Extract features (non-blocking, one iteration)
                feature = FeatureWorker(worker_id="feature-all")
                feature.run(blocking=False, max_iterations=1)
                feature.close()
                
                logger.info("✓ All workers completed successfully")
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Worker failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
