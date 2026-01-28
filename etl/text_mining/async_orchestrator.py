"""
Async Orchestrator for Text Mining Pipeline

Coordinates multiple async workers using Redis Streams
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import logging
import asyncio
import subprocess
import time
from pathlib import Path

from etl.text_mining.config import config
from etl.text_mining.messaging.redis_queue import RedisQueue

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AsyncOrchestrator:
    """
    Orchestrate async workers for event-driven pipeline
    
    This orchestrator starts multiple worker processes that run in parallel,
    consuming from Redis Streams and processing events asynchronously.
    """
    
    def __init__(self):
        self.queue = RedisQueue()
        self.workers = []
        logger.info("AsyncOrchestrator initialized")
    
    def start_workers(self, num_scrapers: int = 2, num_features: int = 2):
        """
        Start worker processes
        
        Args:
            num_scrapers: Number of scraper workers to start
            num_features: Number of feature workers to start
        """
        workers_script = Path(__file__).parent / "workers.py"
        python_exe = sys.executable
        
        logger.info(f"Starting {num_scrapers} scraper workers...")
        for i in range(num_scrapers):
            proc = subprocess.Popen(
                [python_exe, str(workers_script), "scraper", "--worker-id", f"scraper-{i+1}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.workers.append(("scraper", proc))
            logger.info(f"  Started scraper-{i+1} (PID: {proc.pid})")
        
        logger.info(f"Starting {num_features} feature workers...")
        for i in range(num_features):
            proc = subprocess.Popen(
                [python_exe, str(workers_script), "feature", "--worker-id", f"feature-{i+1}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            self.workers.append(("feature", proc))
            logger.info(f"  Started feature-{i+1} (PID: {proc.pid})")
        
        logger.info(f"✓ All workers started ({len(self.workers)} total)")
    
    def publish_job(self, sample_size: int = None):
        """
        Publish a new job by triggering GA4 extraction
        
        Args:
            sample_size: Number of articles to sample
        """
        workers_script = Path(__file__).parent / "workers.py"
        python_exe = sys.executable
        
        logger.info(f"Publishing new job (sample_size={sample_size or config.SAMPLE_SIZE})...")
        
        result = subprocess.run(
            [python_exe, str(workers_script), "publisher", "--sample-size", str(sample_size or config.SAMPLE_SIZE)],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("✓ Job published successfully")
            return True
        else:
            logger.error(f"Failed to publish job: {result.stderr}")
            return False
    
    def monitor_streams(self, duration: int = None):
        """
        Monitor Redis streams for activity
        
        Args:
            duration: Duration to monitor in seconds (None = indefinite)
        """
        logger.info("Monitoring Redis streams...")
        logger.info("Press Ctrl+C to stop\n")
        
        start_time = time.time()
        
        try:
            while True:
                # Get stream info
                ga4_info = self.queue.get_stream_info(config.REDIS_STREAM_GA4)
                scraped_info = self.queue.get_stream_info(config.REDIS_STREAM_SCRAPED)
                
                # Display status
                print(f"\r[{time.strftime('%H:%M:%S')}] "
                      f"GA4: {ga4_info.get('length', 0)} msgs | "
                      f"Scraped: {scraped_info.get('length', 0)} msgs | "
                      f"Workers: {len(self.workers)} active", end="", flush=True)
                
                time.sleep(2)
                
                # Check duration
                if duration and (time.time() - start_time) > duration:
                    print()  # New line
                    logger.info("Monitoring duration reached")
                    break
                    
        except KeyboardInterrupt:
            print()  # New line
            logger.info("Monitoring stopped by user")
    
    def stop_workers(self):
        """Stop all worker processes"""
        logger.info("Stopping all workers...")
        
        for worker_type, proc in self.workers:
            try:
                proc.terminate()
                proc.wait(timeout=5)
                logger.info(f"  Stopped {worker_type} (PID: {proc.pid})")
            except subprocess.TimeoutExpired:
                proc.kill()
                logger.warning(f"  Force killed {worker_type} (PID: {proc.pid})")
            except Exception as e:
                logger.error(f"  Error stopping {worker_type}: {e}")
        
        self.workers.clear()
        logger.info("✓ All workers stopped")
    
    def cleanup(self):
        """Cleanup resources"""
        self.stop_workers()
        self.queue.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Async Text Mining Pipeline Orchestrator")
    parser.add_argument(
        '--sample-size',
        type=int,
        default=10,
        help='Number of articles to sample (default: 10)'
    )
    parser.add_argument(
        '--num-scrapers',
        type=int,
        default=2,
        help='Number of scraper workers (default: 2)'
    )
    parser.add_argument(
        '--num-features',
        type=int,
        default=2,
        help='Number of feature workers (default: 2)'
    )
    parser.add_argument(
        '--monitor-duration',
        type=int,
        default=None,
        help='Duration to monitor in seconds (default: indefinite)'
    )
    parser.add_argument(
        '--publish-only',
        action='store_true',
        help='Only publish job without starting workers'
    )
    
    args = parser.parse_args()
    
    orchestrator = AsyncOrchestrator()
    
    try:
        if args.publish_only:
            # Just publish a job
            orchestrator.publish_job(sample_size=args.sample_size)
        else:
            # Start workers
            orchestrator.start_workers(
                num_scrapers=args.num_scrapers,
                num_features=args.num_features
            )
            
            # Give workers time to initialize
            time.sleep(2)
            
            # Publish job
            orchestrator.publish_job(sample_size=args.sample_size)
            
            # Monitor streams
            orchestrator.monitor_streams(duration=args.monitor_duration)
    
    except KeyboardInterrupt:
        logger.info("Orchestrator interrupted by user")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}", exc_info=True)
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    main()
