"""Command Line Interface for Weekly Articles ETL Pipeline."""
import argparse
import sys
import os
from datetime import date
import json
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_db_pipeline.pipeline import WeeklyArticlesETLPipeline


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    logger.remove()  # Remove default handler
    
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    
    # Console logging
    log_level = "DEBUG" if verbose else "INFO"
    logger.add(sys.stdout, format=log_format, level=log_level)
    
    # File logging
    log_file = os.path.join(os.path.dirname(__file__), "logs", "etl_pipeline.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger.add(
        log_file,
        format=log_format,
        level="DEBUG",
        rotation="10 MB",
        retention="30 days"
    )


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Load weekly article analytics from GA4 into dimensional database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all 2025 data (from Jan 1 to today)
  python cli.py
  
  # Load with custom end date
  python cli.py --end-date 2025-12-31
  
  # Load with custom start date
  python cli.py --start-date 2025-06-01
  
  # Load with verbose logging
  python cli.py --verbose
  
  # Save results to JSON file
  python cli.py --output results.json
        """
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        default='2025-01-01',
        help='Start date (YYYY-MM-DD). Default: 2025-01-01'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date (YYYY-MM-DD). Default: today'
    )
    
    parser.add_argument(
        '--min-page-views',
        type=int,
        default=30,
        help='Minimum page views per week to include article. Default: 30'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Save results to JSON file'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose (DEBUG) logging'
    )
    
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Only show database statistics, do not run ETL'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    # Show stats only
    if args.stats_only:
        pipeline = WeeklyArticlesETLPipeline()
        stats = pipeline.get_database_stats()
        print(json.dumps(stats, indent=2, default=str))
        return
    
    # Set end date to today if not provided
    if not args.end_date:
        args.end_date = date.today().strftime('%Y-%m-%d')
    
    logger.info(f"Starting weekly ETL pipeline from {args.start_date} to {args.end_date}")
    
    # Run pipeline
    pipeline = WeeklyArticlesETLPipeline()
    
    result = pipeline.run_full_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        min_page_views=args.min_page_views
    )
    
    # Save output if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Results saved to {args.output}")
    
    # Print results
    print(json.dumps(result, indent=2, default=str))
    
    # Exit with error code if pipeline failed
    if result['status'] == 'failed':
        sys.exit(1)


if __name__ == '__main__':
    main()