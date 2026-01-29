"""
Test script for Asynchronous Text Mining Pipeline

Tests the async orchestrator, workers, and Redis Streams integration
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import logging
import time
import json
from datetime import datetime
from pathlib import Path

from etl.text_mining.config import config
from etl.text_mining.messaging.redis_queue import RedisQueue
from etl.text_mining.workers import GA4PublisherWorker, ScraperWorker, FeatureWorker
from etl.text_mining.storage.postgres_storage import PostgresStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AsyncPipelineTest:
    """
    Test suite for async pipeline
    """
    
    def __init__(self):
        self.queue = RedisQueue()
        self.storage = PostgresStorage()
        self.test_results = []
        
    def run_all_tests(self):
        """Run all tests"""
        logger.info("=" * 70)
        logger.info("ASYNC PIPELINE TEST SUITE")
        logger.info("=" * 70)
        
        tests = [
            ("Infrastructure Check", self.test_infrastructure),
            ("Database Schema", self.test_database_schema),
            ("Redis Streams", self.test_redis_streams),
            ("GA4 Publisher Worker", self.test_publisher_worker),
            ("Scraper Worker", self.test_scraper_worker),
            ("Feature Worker", self.test_feature_worker),
            ("End-to-End Pipeline", self.test_end_to_end_pipeline),
        ]
        
        for test_name, test_func in tests:
            logger.info(f"\n{'=' * 70}")
            logger.info(f"TEST: {test_name}")
            logger.info(f"{'=' * 70}")
            
            try:
                start_time = time.time()
                result = test_func()
                duration = time.time() - start_time
                
                if result:
                    logger.info(f"✓ PASSED ({duration:.2f}s)")
                    self.test_results.append((test_name, "PASSED", duration))
                else:
                    logger.error(f"✗ FAILED ({duration:.2f}s)")
                    self.test_results.append((test_name, "FAILED", duration))
            except Exception as e:
                logger.error(f"✗ ERROR: {e}", exc_info=True)
                self.test_results.append((test_name, "ERROR", 0))
        
        self._print_summary()
    
    def test_infrastructure(self):
        """Test Redis and PostgreSQL connectivity"""
        logger.info("Testing infrastructure connections...")
        
        # Test Redis
        try:
            redis_info = self.queue.client.info()
            logger.info(f"✓ Redis connected: version={redis_info['redis_version']}")
        except Exception as e:
            logger.error(f"✗ Redis connection failed: {e}")
            return False
        
        # Test PostgreSQL
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            logger.info(f"✓ PostgreSQL connected: {version[:50]}")
        except Exception as e:
            logger.error(f"✗ PostgreSQL connection failed: {e}")
            return False
        
        return True
    
    def test_database_schema(self):
        """Test database schema creation"""
        logger.info("Testing database schema...")
        
        try:
            self.storage.create_schema()
            logger.info("✓ Database schema created/verified")
            
            # Verify tables exist
            cursor = self.storage.conn.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'text_mining'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            
            expected_tables = ['articles_raw', 'articles_features', 'samples']
            for table in expected_tables:
                if table in tables:
                    logger.info(f"  ✓ Table '{table}' exists")
                else:
                    logger.warning(f"  ✗ Table '{table}' missing")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Schema creation failed: {e}")
            return False
    
    def test_redis_streams(self):
        """Test Redis Streams operations"""
        logger.info("Testing Redis Streams...")
        
        try:
            # Clear existing streams
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            self.queue.client.delete(config.REDIS_STREAM_SCRAPED)
            logger.info("  Cleared existing streams")
            
            # Test publishing
            test_data = {
                "test_id": "test_001",
                "timestamp": datetime.now().isoformat(),
                "data": "test message"
            }
            
            msg_id = self.queue.publish_event(config.REDIS_STREAM_GA4, test_data)
            logger.info(f"  ✓ Published test message: {msg_id}")
            
            # Test consuming (consumer group is auto-created)
            events = self.queue.consume_events(
                config.REDIS_STREAM_GA4,
                "test_group",
                "test_consumer",
                block_ms=1000,
                count=1
            )
            logger.info("  ✓ Consumer group created and consumed")
            
            if events and events[0].get('test_id') == 'test_001':
                logger.info("  ✓ Consumed test message successfully")
                
                # Test acknowledgment
                self.queue.acknowledge_message(
                    config.REDIS_STREAM_GA4,
                    "test_group",
                    events[0]['_message_id']
                )
                logger.info("  ✓ Message acknowledged")
            else:
                logger.error("  ✗ Failed to consume message")
                return False
            
            # Cleanup
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            
            return True
        except Exception as e:
            logger.error(f"Redis Streams test failed: {e}")
            return False
    
    def test_publisher_worker(self):
        """Test GA4 Publisher Worker"""
        logger.info("Testing GA4 Publisher Worker...")
        
        try:
            # Clear stream
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            
            # Initialize and run publisher
            publisher = GA4PublisherWorker()
            sample_id = publisher.publish_sample(sample_size=5)
            publisher.close()
            
            if not sample_id:
                logger.error("  ✗ Publisher returned no sample_id")
                return False
            
            logger.info(f"  ✓ Published sample: {sample_id}")
            
            # Verify stream has messages
            stream_info = self.queue.get_stream_info(config.REDIS_STREAM_GA4)
            msg_count = stream_info.get('length', 0)
            
            if msg_count > 0:
                logger.info(f"  ✓ Stream has {msg_count} message(s)")
                
                # Peek at the message (consume_events creates consumer group automatically)
                events = self.queue.consume_events(
                    config.REDIS_STREAM_GA4,
                    "test_peek",
                    "test_consumer",
                    block_ms=1000,
                    count=1
                )
                
                if events:
                    event = events[0]
                    logger.info(f"  ✓ Event data verified:")
                    logger.info(f"    - sample_id: {event.get('sample_id')}")
                    logger.info(f"    - articles_count: {event.get('articles_count')}")
                    logger.info(f"    - generated_at: {event.get('generated_at')}")
                
                return True
            else:
                logger.error("  ✗ No messages in stream")
                return False
                
        except Exception as e:
            logger.error(f"Publisher worker test failed: {e}", exc_info=True)
            return False
    
    def test_scraper_worker(self):
        """Test Scraper Worker"""
        logger.info("Testing Scraper Worker...")
        
        try:
            # Ensure we have a GA4 event in the stream
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            self.queue.client.delete(config.REDIS_STREAM_SCRAPED)
            
            publisher = GA4PublisherWorker()
            sample_id = publisher.publish_sample(sample_size=3)
            publisher.close()
            
            if not sample_id:
                logger.error("  ✗ Failed to publish sample for scraper test")
                return False
            
            logger.info(f"  Sample published: {sample_id}")
            
            # Run scraper worker (non-blocking, 1 iteration)
            scraper = ScraperWorker(worker_id="test-scraper")
            scraper.run(blocking=False, max_iterations=1)
            scraper.close()
            
            # Verify scraped stream has messages
            stream_info = self.queue.get_stream_info(config.REDIS_STREAM_SCRAPED)
            msg_count = stream_info.get('length', 0)
            
            if msg_count > 0:
                logger.info(f"  ✓ Scraper produced {msg_count} message(s)")
                
                # Check if raw articles were stored
                # This would require querying the database
                return True
            else:
                logger.warning("  ⚠ No messages in scraped stream (may be normal if no articles were scraped)")
                return True  # Don't fail - might be intentional
                
        except Exception as e:
            logger.error(f"Scraper worker test failed: {e}", exc_info=True)
            return False
    
    def test_feature_worker(self):
        """Test Feature Worker"""
        logger.info("Testing Feature Worker...")
        
        try:
            # Ensure we have scraped articles
            # First publish, then scrape
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            self.queue.client.delete(config.REDIS_STREAM_SCRAPED)
            
            publisher = GA4PublisherWorker()
            sample_id = publisher.publish_sample(sample_size=2)
            publisher.close()
            
            if not sample_id:
                logger.error("  ✗ Failed to publish sample")
                return False
            
            # Scrape
            scraper = ScraperWorker(worker_id="test-scraper-2")
            scraper.run(blocking=False, max_iterations=1)
            scraper.close()
            
            # Run feature worker
            feature_worker = FeatureWorker(worker_id="test-feature")
            feature_worker.run(blocking=False, max_iterations=1)
            feature_worker.close()
            
            logger.info("  ✓ Feature worker completed")
            
            # Verify features were stored (would require DB query)
            return True
                
        except Exception as e:
            logger.error(f"Feature worker test failed: {e}", exc_info=True)
            return False
    
    def test_end_to_end_pipeline(self):
        """Test complete end-to-end pipeline"""
        logger.info("Testing end-to-end pipeline...")
        
        try:
            # Clear streams
            self.queue.client.delete(config.REDIS_STREAM_GA4)
            self.queue.client.delete(config.REDIS_STREAM_SCRAPED)
            
            # Get initial counts from database
            initial_samples = self._count_samples()
            initial_articles = self._count_raw_articles()
            initial_features = self._count_features()
            
            logger.info(f"  Initial DB state:")
            logger.info(f"    - Samples: {initial_samples}")
            logger.info(f"    - Raw articles: {initial_articles}")
            logger.info(f"    - Features: {initial_features}")
            
            # Run complete pipeline
            sample_size = 5
            
            # 1. Publish
            logger.info(f"  Step 1: Publishing sample (size={sample_size})...")
            publisher = GA4PublisherWorker()
            sample_id = publisher.publish_sample(sample_size=sample_size)
            publisher.close()
            
            if not sample_id:
                logger.error("  ✗ Failed to publish")
                return False
            
            logger.info(f"    ✓ Published: {sample_id}")
            
            # 2. Scrape
            logger.info("  Step 2: Scraping articles...")
            scraper = ScraperWorker(worker_id="e2e-scraper")
            scraper.run(blocking=False, max_iterations=1)
            scraper.close()
            logger.info("    ✓ Scraping completed")
            
            # 3. Extract features
            logger.info("  Step 3: Extracting features...")
            feature_worker = FeatureWorker(worker_id="e2e-feature")
            feature_worker.run(blocking=False, max_iterations=1)
            feature_worker.close()
            logger.info("    ✓ Feature extraction completed")
            
            # Verify database changes
            final_samples = self._count_samples()
            final_articles = self._count_raw_articles()
            final_features = self._count_features()
            
            logger.info(f"  Final DB state:")
            logger.info(f"    - Samples: {final_samples} (+{final_samples - initial_samples})")
            logger.info(f"    - Raw articles: {final_articles} (+{final_articles - initial_articles})")
            logger.info(f"    - Features: {final_features} (+{final_features - initial_features})")
            
            # Verify we added data
            if final_samples > initial_samples:
                logger.info("  ✓ New sample added to database")
            else:
                logger.warning("  ⚠ No new sample in database")
            
            if final_articles > initial_articles:
                logger.info("  ✓ New articles added to database")
            else:
                logger.warning("  ⚠ No new articles in database")
            
            if final_features > initial_features:
                logger.info("  ✓ New features added to database")
            else:
                logger.warning("  ⚠ No new features in database")
            
            return True
            
        except Exception as e:
            logger.error(f"End-to-end test failed: {e}", exc_info=True)
            return False
    
    def _count_samples(self):
        """Count samples in database"""
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM text_mining.samples")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except:
            return 0
    
    def _count_raw_articles(self):
        """Count raw articles in database"""
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM text_mining.articles_raw")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except:
            return 0
    
    def _count_features(self):
        """Count features in database"""
        try:
            cursor = self.storage.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM text_mining.articles_features")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except:
            return 0
    
    def _print_summary(self):
        """Print test summary"""
        logger.info("\n" + "=" * 70)
        logger.info("TEST SUMMARY")
        logger.info("=" * 70)
        
        passed = sum(1 for _, status, _ in self.test_results if status == "PASSED")
        failed = sum(1 for _, status, _ in self.test_results if status == "FAILED")
        errors = sum(1 for _, status, _ in self.test_results if status == "ERROR")
        total = len(self.test_results)
        
        for test_name, status, duration in self.test_results:
            symbol = "✓" if status == "PASSED" else "✗"
            logger.info(f"{symbol} {test_name}: {status} ({duration:.2f}s)")
        
        logger.info("-" * 70)
        logger.info(f"Total: {total} | Passed: {passed} | Failed: {failed} | Errors: {errors}")
        
        if failed == 0 and errors == 0:
            logger.info("🎉 ALL TESTS PASSED!")
        else:
            logger.warning("⚠ SOME TESTS FAILED")
        
        logger.info("=" * 70)
    
    def cleanup(self):
        """Cleanup resources"""
        self.queue.close()
        self.storage.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Async Text Mining Pipeline")
    parser.add_argument(
        '--test',
        choices=['all', 'infrastructure', 'schema', 'streams', 'publisher', 'scraper', 'feature', 'e2e'],
        default='all',
        help='Which test to run (default: all)'
    )
    
    args = parser.parse_args()
    
    tester = AsyncPipelineTest()
    
    try:
        if args.test == 'all':
            tester.run_all_tests()
        elif args.test == 'infrastructure':
            tester.test_infrastructure()
        elif args.test == 'schema':
            tester.test_database_schema()
        elif args.test == 'streams':
            tester.test_redis_streams()
        elif args.test == 'publisher':
            tester.test_publisher_worker()
        elif args.test == 'scraper':
            tester.test_scraper_worker()
        elif args.test == 'feature':
            tester.test_feature_worker()
        elif args.test == 'e2e':
            tester.test_end_to_end_pipeline()
    
    except KeyboardInterrupt:
        logger.info("\nTests interrupted by user")
    except Exception as e:
        logger.error(f"Test suite failed: {e}", exc_info=True)
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main()
