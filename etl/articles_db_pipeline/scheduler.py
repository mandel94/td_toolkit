"""Scheduler for automated weekly scraping of TaxiDrivers.it articles.

This module provides scheduling capabilities for running the web scraping pipeline
automatically on a weekly basis (every Friday).
"""
import sys
import os
import schedule
import time
from datetime import datetime
from typing import Optional, Callable
from loguru import logger
import json

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline


class WebScrapingScheduler:
    """Scheduler for automated weekly article scraping."""
    
    def __init__(
        self,
        pipeline: Optional[WebScrapingPipeline] = None,
        schedule_day: str = "friday",
        schedule_time: str = "02:00",
        log_dir: Optional[str] = None
    ):
        """Initialize scheduler.
        
        Args:
            pipeline: WebScrapingPipeline instance (creates new if None)
            schedule_day: Day of week to run (default: "friday")
            schedule_time: Time to run in HH:MM format (default: "02:00")
            log_dir: Directory for execution logs (default: ./logs)
        """
        self.pipeline = pipeline or WebScrapingPipeline()
        self.schedule_day = schedule_day.lower()
        self.schedule_time = schedule_time
        
        # Setup log directory
        if log_dir is None:
            log_dir = os.path.join(
                os.path.dirname(__file__),
                "logs"
            )
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Configure loguru to also log to file
        log_file = os.path.join(self.log_dir, "scraping_scheduler_{time}.log")
        logger.add(
            log_file,
            rotation="1 week",
            retention="3 months",
            level="INFO"
        )
        
        logger.info(
            f"Initialized WebScrapingScheduler: "
            f"{schedule_day} at {schedule_time}"
        )
    
    def run_scheduled_job(self):
        """Execute the scraping pipeline and log results."""
        job_start = datetime.now()
        logger.info("=" * 80)
        logger.info(f"SCHEDULED JOB STARTED: {job_start.isoformat()}")
        logger.info("=" * 80)
        
        try:
            # Run the pipeline
            results = self.pipeline.run_full_pipeline()
            
            # Save results to JSON file
            results_file = os.path.join(
                self.log_dir,
                f"scraping_results_{job_start.strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            logger.success(f"Results saved to: {results_file}")
            
            if results['success']:
                logger.success("Scheduled job completed successfully")
            else:
                logger.error("Scheduled job completed with errors")
            
        except Exception as e:
            logger.error(f"Scheduled job failed: {str(e)}")
            raise
        finally:
            job_end = datetime.now()
            duration = (job_end - job_start).total_seconds()
            logger.info(f"Job duration: {duration:.1f} seconds")
            logger.info("=" * 80)
    
    def setup_schedule(self):
        """Setup the weekly schedule."""
        # Map day names to schedule methods
        day_map = {
            'monday': schedule.every().monday,
            'tuesday': schedule.every().tuesday,
            'wednesday': schedule.every().wednesday,
            'thursday': schedule.every().thursday,
            'friday': schedule.every().friday,
            'saturday': schedule.every().saturday,
            'sunday': schedule.every().sunday
        }
        
        if self.schedule_day not in day_map:
            raise ValueError(
                f"Invalid schedule day: {self.schedule_day}. "
                f"Must be one of: {list(day_map.keys())}"
            )
        
        # Setup schedule
        day_map[self.schedule_day].at(self.schedule_time).do(self.run_scheduled_job)
        
        logger.info(
            f"Schedule configured: Every {self.schedule_day.capitalize()} "
            f"at {self.schedule_time}"
        )
    
    def run_forever(self):
        """Run the scheduler indefinitely."""
        self.setup_schedule()
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        logger.info(f"Next run: {schedule.next_run()}")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    
    def run_once_now(self):
        """Run the scraping job once immediately (for testing)."""
        logger.info("Running job immediately (test mode)")
        self.run_scheduled_job()


def main():
    """Main entry point for scheduler."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Schedule weekly scraping of TaxiDrivers.it articles"
    )
    parser.add_argument(
        '--day',
        type=str,
        default='friday',
        choices=['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
        help='Day of week to run the scraper (default: friday)'
    )
    parser.add_argument(
        '--time',
        type=str,
        default='02:00',
        help='Time to run in HH:MM format (default: 02:00)'
    )
    parser.add_argument(
        '--run-now',
        action='store_true',
        help='Run the scraper immediately instead of scheduling'
    )
    parser.add_argument(
        '--log-dir',
        type=str,
        help='Directory for execution logs'
    )
    
    args = parser.parse_args()
    
    # Create scheduler
    scheduler = WebScrapingScheduler(
        schedule_day=args.day,
        schedule_time=args.time,
        log_dir=args.log_dir
    )
    
    if args.run_now:
        # Run immediately
        scheduler.run_once_now()
    else:
        # Run on schedule
        scheduler.run_forever()


if __name__ == '__main__':
    main()
