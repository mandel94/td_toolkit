"""
Test script for web scraping module - Quick validation

This script performs a quick test of the web scraping system:
1. Tests database connection
2. Scrapes 5 articles as a sample
3. Validates data quality
4. Shows results

Usage:
    python test_scraping.py
"""
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from etl.articles_db_pipeline.web_scraping_pipeline import WebScrapingPipeline
from loguru import logger


def test_database_connection():
    """Test 1: Database Connection"""
    print("\n" + "=" * 80)
    print("TEST 1: Database Connection")
    print("=" * 80)
    
    pipeline = WebScrapingPipeline()
    status = pipeline.get_pipeline_status()
    
    if status['database_connected']:
        print("✓ Database connected successfully")
        print(f"  - Total articles in DB: {status['total_scraped_articles']}")
        print(f"  - Last scrape: {status['latest_scrape_date'] or 'Never'}")
        return True
    else:
        print("✗ Database connection failed!")
        print("\nPlease start the database:")
        print("  cd articles_db")
        print("  docker-compose up -d")
        return False


def test_scraping(limit=5):
    """Test 2: Scrape Sample Articles"""
    print("\n" + "=" * 80)
    print(f"TEST 2: Scraping Sample ({limit} articles)")
    print("=" * 80)
    
    try:
        pipeline = WebScrapingPipeline(
            delay_between_requests=1.0,  # Faster for testing
            batch_size=10,
            batch_pause_duration=5       # Shorter pause for testing
        )
        
        results = pipeline.run_full_pipeline(
            limit=limit,
            update_dim_tables=False  # Skip dim update for test
        )
        
        if results['success']:
            print("\n✓ Scraping completed successfully")
            print(f"  - Extracted: {results['extraction']['successful']} articles")
            print(f"  - Loaded: {results['loading']['loaded']} articles")
            print(f"  - Failed: {results['extraction']['failed'] + results['loading']['failed']} articles")
            print(f"  - Duration: {results['duration_seconds']:.1f} seconds")
            return True, results
        else:
            print("\n✗ Scraping failed")
            print(f"  - Errors: {results.get('errors', [])}")
            return False, results
            
    except Exception as e:
        print(f"\n✗ Exception during scraping: {str(e)}")
        return False, None


def validate_data(results):
    """Test 3: Validate Data Quality"""
    print("\n" + "=" * 80)
    print("TEST 3: Data Quality Validation")
    print("=" * 80)
    
    if not results or not results.get('success'):
        print("✗ No data to validate")
        return False
    
    checks = {
        'has_extractions': results['extraction']['successful'] > 0,
        'has_loads': results['loading']['loaded'] > 0,
        'no_major_failures': results['extraction']['failed'] < results['extraction']['total_scraped'] * 0.5,
        'reasonable_duration': results['duration_seconds'] < 300  # Less than 5 minutes for 5 articles
    }
    
    all_passed = all(checks.values())
    
    for check_name, passed in checks.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {check_name.replace('_', ' ').title()}")
    
    if all_passed:
        print("\n✓ All quality checks passed!")
        return True
    else:
        print("\n⚠ Some quality checks failed")
        return False


def show_sample_data():
    """Test 4: Query Sample Data"""
    print("\n" + "=" * 80)
    print("TEST 4: Sample Data from Database")
    print("=" * 80)
    
    try:
        from etl.articles_db_pipeline.loaders.scraped_articles_loader import ScrapedArticlesLoader
        from sqlalchemy import text
        
        loader = ScrapedArticlesLoader()
        
        with loader.engine.connect() as conn:
            # Get latest 3 articles
            result = conn.execute(text("""
                SELECT 
                    title, 
                    author, 
                    categoria,
                    publication_date,
                    LENGTH(body_text) as text_length
                FROM latest_scraped_articles
                ORDER BY created_at DESC
                LIMIT 3
            """))
            
            articles = result.fetchall()
            
            if articles:
                print("\nLatest articles in database:")
                print("-" * 80)
                for i, article in enumerate(articles, 1):
                    print(f"\n{i}. {article[0] or 'No title'}")
                    print(f"   Author: {article[1] or 'Unknown'}")
                    print(f"   Category: {article[2] or 'Uncategorized'}")
                    print(f"   Published: {article[3] or 'Unknown'}")
                    print(f"   Text length: {article[4] or 0} chars")
                
                print("\n✓ Successfully retrieved sample data")
                return True
            else:
                print("⚠ No articles found in database")
                return False
                
    except Exception as e:
        print(f"✗ Failed to query sample data: {str(e)}")
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("WEB SCRAPING MODULE - TEST SUITE")
    print("=" * 80)
    print("\nThis will:")
    print("  1. Test database connection")
    print("  2. Scrape 5 sample articles")
    print("  3. Validate data quality")
    print("  4. Show sample data")
    print("\nEstimated time: 1-2 minutes")
    
    input("\nPress Enter to start tests...")
    
    # Run tests
    test_results = []
    
    # Test 1: Database
    db_ok = test_database_connection()
    test_results.append(('Database Connection', db_ok))
    
    if not db_ok:
        print("\n⚠ Cannot proceed without database. Please start the database first.")
        return 1
    
    # Test 2: Scraping
    scrape_ok, results = test_scraping(limit=5)
    test_results.append(('Scraping', scrape_ok))
    
    if scrape_ok:
        # Test 3: Validation
        valid_ok = validate_data(results)
        test_results.append(('Data Validation', valid_ok))
        
        # Test 4: Sample Data
        sample_ok = show_sample_data()
        test_results.append(('Sample Data', sample_ok))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    for test_name, passed in test_results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status} - {test_name}")
    
    all_passed = all(result[1] for result in test_results)
    
    print("\n" + "=" * 80)
    if all_passed:
        print("✓ ALL TESTS PASSED!")
        print("\nThe web scraping module is working correctly.")
        print("\nNext steps:")
        print("  - Run full scraping: python web_scraping_cli.py scrape")
        print("  - Setup scheduler: .\\Start-WebScraping-Scheduler.ps1")
        print("  - Read guide: WEB_SCRAPING_GUIDE.md")
    else:
        print("⚠ SOME TESTS FAILED")
        print("\nPlease check the errors above and:")
        print("  1. Ensure database is running")
        print("  2. Check internet connectivity")
        print("  3. Verify dependencies are installed")
        print("  4. Review logs for details")
    print("=" * 80)
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
