"""Command-line interface for web scraping pipeline."""
import sys
import os
import argparse
import json
from datetime import datetime
from loguru import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    logger.remove()  # Remove default handler
    
    log_level = "DEBUG" if verbose else "INFO"
    
    # Console handler with color
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=log_level,
        colorize=True
    )


def run_scraping(args):
    """Run the web scraping pipeline."""
    logger.info("Starting web scraping pipeline")
    
    # Create pipeline
    pipeline = WebScrapingPipeline(
        base_url=args.base_url,
        delay_between_requests=args.delay,
        batch_size=args.batch_size,
        batch_pause_duration=args.batch_pause,
        timeout=args.timeout,
        max_retries=args.max_retries
    )
    
    # Run pipeline
    try:
        results = pipeline.run_full_pipeline(
            archive_url=args.archive_url,
            limit=args.limit,
            update_dim_tables=not args.no_dim_update
        )
        
        # Save results to file if requested
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to: {args.output}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Success: {results['success']}")
        print(f"Duration: {results.get('duration_seconds', 0):.1f} seconds")
        print(f"\nExtraction:")
        print(f"  - Total scraped: {results['extraction'].get('total_scraped', 0)}")
        print(f"  - Successful: {results['extraction'].get('successful', 0)}")
        print(f"  - Failed: {results['extraction'].get('failed', 0)}")
        print(f"\nLoading:")
        print(f"  - Loaded: {results['loading'].get('loaded', 0)}")
        print(f"  - Failed: {results['loading'].get('failed', 0)}")
        if results.get('dimensional_update'):
            print(f"\nDimensional Update:")
            for table, count in results['dimensional_update'].items():
                print(f"  - {table}: {count}")
        print("=" * 80)
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return 1


def run_status(args):
    """Show pipeline status."""
    pipeline = WebScrapingPipeline()
    status = pipeline.get_pipeline_status()
    
    print("\n" + "=" * 80)
    print("PIPELINE STATUS")
    print("=" * 80)
    print(f"Database connected: {status.get('database_connected', False)}")
    print(f"Total scraped articles: {status.get('total_scraped_articles', 0)}")
    print(f"Latest scrape: {status.get('latest_scrape_date', 'Never')}")
    print("=" * 80)
    
    return 0


def run_update_dim(args):
    """Update dimensional tables only."""
    logger.info("Updating dimensional tables from scraped data")
    
    pipeline = WebScrapingPipeline()
    results = pipeline.run_update_only()
    
    if results['success']:
        print("\n" + "=" * 80)
        print("DIMENSIONAL UPDATE SUMMARY")
        print("=" * 80)
        for table, count in results['dimensional_update'].items():
            print(f"{table}: {count} records")
        print("=" * 80)
        return 0
    else:
        logger.error(f"Update failed: {results.get('error')}")
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Web Scraping Pipeline for TaxiDrivers.it articles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python web_scraping_cli.py scrape
  
  # Limit to 50 articles
  python web_scraping_cli.py scrape --limit 50
  
  # Show status
  python web_scraping_cli.py status
  
  # Update dimensional tables only
  python web_scraping_cli.py update-dim
  
  # Save results to file
  python web_scraping_cli.py scrape --output results.json
        """
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Run the scraping pipeline')
    scrape_parser.add_argument(
        '--base-url',
        type=str,
        default='https://www.taxidrivers.it',
        help='Base URL of the website (default: https://www.taxidrivers.it)'
    )
    scrape_parser.add_argument(
        '--archive-url',
        type=str,
        default='/archivio',
        help='Archive page URL path (default: /archivio)'
    )
    scrape_parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of articles to process'
    )
    scrape_parser.add_argument(
        '--delay',
        type=float,
        default=2.0,
        help='Delay between requests in seconds (default: 2.0)'
    )
    scrape_parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of articles per batch (default: 100)'
    )
    scrape_parser.add_argument(
        '--batch-pause',
        type=int,
        default=120,
        help='Pause duration in seconds between batches (default: 120)'
    )
    scrape_parser.add_argument(
        '--timeout',
        type=int,
        default=30,
        help='Request timeout in seconds (default: 30)'
    )
    scrape_parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum retry attempts (default: 3)'
    )
    scrape_parser.add_argument(
        '--no-dim-update',
        action='store_true',
        help='Skip dimensional table update'
    )
    scrape_parser.add_argument(
        '--output', '-o',
        type=str,
        help='Save results to JSON file'
    )
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show pipeline status')
    
    # Update-dim command
    update_parser = subparsers.add_parser('update-dim', help='Update dimensional tables only')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Execute command
    if args.command == 'scrape':
        return run_scraping(args)
    elif args.command == 'status':
        return run_status(args)
    elif args.command == 'update-dim':
        return run_update_dim(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
