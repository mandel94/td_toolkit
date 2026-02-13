"""
Simple script to extract GA4 sample and save for Jupyter Notebook analysis

This script extracts a sample of articles with GA4 metrics and saves them
to CSV and JSON formats for easy analysis in Jupyter Notebook.
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import json
from datetime import datetime, timedelta
import logging

from etl.text_mining.extractors.ga4_sample_extractor import GA4SampleExtractor
from etl.text_mining.config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_sample_to_files(
    sample_size: int = None,
    days_back: int = None,
    output_dir: str = "./output"
):
    """
    Extract GA4 sample and save to CSV and JSON
    
    Args:
        sample_size: Number of articles to sample (default from config: 300)
        days_back: Number of days to look back (default from config: 30)
        output_dir: Output directory for files
    """
    # Use config defaults
    sample_size = sample_size or config.SAMPLE_SIZE
    days_back = days_back or config.DEFAULT_DAYS_BACK
    
    # Calculate date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"Extracting sample of {sample_size} articles from GA4...")
    logger.info(f"Date range: {start_date} to {end_date} ({days_back} days)")
    
    # Extract sample with date range
    extractor = GA4SampleExtractor()
    ga4_event = extractor.extract_sample(
        sample_size=sample_size,
        start_date=start_date,
        end_date=end_date
    )
    
    if not ga4_event.articles:
        logger.warning("No articles found in GA4 sample")
        return
    
    logger.info(f"✓ Extracted {len(ga4_event.articles)} articles")
    
    # Convert to DataFrame
    data = []
    for article in ga4_event.articles:
        data.append({
            'pagepath': article.pagepath,
            'pageviews': article.pageviews,
            'engaged_sessions': article.engaged_sessions,
            'avg_session_duration': article.avg_session_duration,
            'engagement_rate': article.engagement_rate,
            'editorial_score': article.editorial_score,
            'date_range_start': article.date_range_start,
            'date_range_end': article.date_range_end
        })
    
    df = pd.DataFrame(data)
    
    # Add metadata columns
    df['sample_id'] = ga4_event.sample_id
    df['extracted_at'] = ga4_event.generated_at.strftime('%Y-%m-%d %H:%M:%S')
    
    # Sort by editorial score descending
    df = df.sort_values('editorial_score', ascending=False).reset_index(drop=True)
    
    # Generate filenames
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"ga4_sample_{sample_size}articles_{timestamp}.csv"
    json_filename = f"ga4_sample_{sample_size}articles_{timestamp}.json"
    
    csv_path = os.path.join(output_dir, csv_filename)
    json_path = os.path.join(output_dir, json_filename)
    
    # Save to CSV
    df.to_csv(csv_path, index=False, encoding='utf-8')
    logger.info(f"✓ Saved CSV to: {csv_path}")
    
    # Save to JSON (with metadata)
    output_json = {
        'metadata': {
            'sample_id': ga4_event.sample_id,
            'extracted_at': ga4_event.generated_at.isoformat(),
            'sample_size': len(ga4_event.articles),
            'date_range': {
                'start': start_date,
                'end': end_date,
                'days': days_back
            }
        },
        'articles': data
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=2, ensure_ascii=False)
    logger.info(f"✓ Saved JSON to: {json_path}")
    
    # Print summary statistics
    logger.info("\n" + "="*60)
    logger.info("SAMPLE SUMMARY")
    logger.info("="*60)
    logger.info(f"Sample ID: {ga4_event.sample_id}")
    logger.info(f"Articles extracted: {len(df)}")
    logger.info(f"\nPageviews statistics:")
    logger.info(f"  Min: {df['pageviews'].min()}")
    logger.info(f"  Max: {df['pageviews'].max()}")
    logger.info(f"  Mean: {df['pageviews'].mean():.2f}")
    logger.info(f"  Median: {df['pageviews'].median():.2f}")
    logger.info(f"\nEditorial Score statistics:")
    logger.info(f"  Min: {df['editorial_score'].min():.4f}")
    logger.info(f"  Max: {df['editorial_score'].max():.4f}")
    logger.info(f"  Mean: {df['editorial_score'].mean():.4f}")
    logger.info(f"\nTop 5 articles by editorial score:")
    for idx, row in df.head(5).iterrows():
        logger.info(f"  {idx+1}. {row['pagepath'][:60]}... (score: {row['editorial_score']:.4f})")
    logger.info("="*60)
    
    return csv_path, json_path, df


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract GA4 sample for Jupyter Notebook analysis")
    parser.add_argument(
        '--sample-size',
        type=int,
        default=None,
        help='Number of articles to sample (default: 300 from config)'
    )
    parser.add_argument(
        '--days-back',
        type=int,
        default=None,
        help='Number of days to look back (default: 30 from config)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./output',
        help='Output directory (default: ./output)'
    )
    
    args = parser.parse_args()
    
    try:
        csv_path, json_path, df = extract_sample_to_files(
            sample_size=args.sample_size,
            days_back=args.days_back,
            output_dir=args.output_dir
        )
        
        logger.info(f"\n✓ Data ready for Jupyter Notebook!")
        logger.info(f"\nTo load in Jupyter:")
        logger.info(f"  import pandas as pd")
        logger.info(f"  df = pd.read_csv('{csv_path}')")
        logger.info(f"\nOr load JSON:")
        logger.info(f"  import json")
        logger.info(f"  with open('{json_path}') as f:")
        logger.info(f"      data = json.load(f)")
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
