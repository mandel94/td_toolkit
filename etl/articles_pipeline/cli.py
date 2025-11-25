"""Command Line Interface for Articles ETL Pipeline."""
import argparse
import sys
import os
from datetime import datetime, date, timedelta
import json
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_pipeline.pipeline import ArticlesETLPipeline

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

def run_pipeline_command(args):
    """Run the full ETL pipeline."""
    logger.info(f"Running ETL pipeline from {args.start_date} to {args.end_date}")
    
    pipeline = ArticlesETLPipeline(
        ga4_property_id=args.ga4_property_id,
        database_url=args.database_url
    )
    
    result = pipeline.run_full_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        min_page_views=args.min_page_views,
        batch_size=args.batch_size,
        upsert=args.upsert
    )
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Results saved to {args.output}")
    
    print(json.dumps(result, indent=2, default=str))
    
    # Exit with error code if pipeline failed
    if result['status'] == 'failed':
        sys.exit(1)

def run_extract_command(args):
    """Run only the extraction phase."""
    logger.info(f"Running extraction from {args.start_date} to {args.end_date}")
    
    pipeline = ArticlesETLPipeline(
        ga4_property_id=args.ga4_property_id
    )
    
    result = pipeline.run_extract_only(
        start_date=args.start_date,
        end_date=args.end_date,
        min_page_views=args.min_page_views
    )
    
    print(json.dumps(result, indent=2, default=str))

def run_status_command(args):
    """Get pipeline status."""
    logger.info("Getting pipeline status")
    
    pipeline = ArticlesETLPipeline(
        database_url=args.database_url
    )
    
    result = pipeline.get_pipeline_status()
    print(json.dumps(result, indent=2, default=str))

def run_cleanup_command(args):
    """Clean up old data."""
    logger.info(f"Cleaning up data older than {args.days} days")
    
    pipeline = ArticlesETLPipeline(
        database_url=args.database_url
    )
    
    result = pipeline.cleanup_old_data(days_to_keep=args.days)
    print(json.dumps(result, indent=2, default=str))

def calculate_date_range(days_back: int = 7) -> tuple:
    """Calculate start and end dates (default: from Jan 1, 2025 to today)."""
    from etl.articles_pipeline.config.database import ETL_START_DATE
    start_date = ETL_START_DATE
    end_date = date.today().strftime('%Y-%m-%d')
    return start_date, end_date

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Articles ETL Pipeline - Extract, Transform, Load article analytics data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline for last 7 days
  python cli.py run --days 7
  
  # Run for specific date range
  python cli.py run --start-date 2025-11-01 --end-date 2025-11-07
  
  # Run extraction only (testing)
  python cli.py extract --days 3
  
  # Check pipeline status
  python cli.py status
  
  # Clean up old data
  python cli.py cleanup --days 90
        """
    )
    
    # Global arguments
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--database-url', help='Database connection URL')
    parser.add_argument('--output', '-o', help='Output file for results (JSON)')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run pipeline command
    run_parser = subparsers.add_parser('run', help='Run the full ETL pipeline')
    run_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    run_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    run_parser.add_argument('--days', type=int, default=7, help='Days back from yesterday (default: 7)')
    run_parser.add_argument('--min-page-views', type=int, default=30, help='Minimum page views threshold')
    run_parser.add_argument('--batch-size', type=int, default=100, help='Database batch size')
    run_parser.add_argument('--no-upsert', dest='upsert', action='store_false', help='Insert only, do not update existing records')
    run_parser.add_argument('--ga4-property-id', help='GA4 property ID')
    
    # Extract only command
    extract_parser = subparsers.add_parser('extract', help='Run extraction only (testing)')
    extract_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    extract_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    extract_parser.add_argument('--days', type=int, default=7, help='Days back from yesterday (default: 7)')
    extract_parser.add_argument('--min-page-views', type=int, default=30, help='Minimum page views threshold')
    extract_parser.add_argument('--ga4-property-id', help='GA4 property ID')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Get pipeline and database status')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old data')
    cleanup_parser.add_argument('--days', type=int, default=90, help='Keep data newer than N days (default: 90)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(verbose=args.verbose)
    
    # Handle date calculation for run and extract commands
    if args.command in ['run', 'extract'] and not args.start_date:
        args.start_date, args.end_date = calculate_date_range(args.days)
        logger.info(f"Calculated date range: {args.start_date} to {args.end_date}")
    
    # Execute command
    if args.command == 'run':
        run_pipeline_command(args)
    elif args.command == 'extract':
        run_extract_command(args)
    elif args.command == 'status':
        run_status_command(args)
    elif args.command == 'cleanup':
        run_cleanup_command(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()