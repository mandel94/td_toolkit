"""
Complete Text Mining Pipeline - Extract, Scrape, Process and Save

This script:
1. Loads the GA4 sample (100 articles)
2. Scrapes HTML content from each article
3. Extracts text features
4. Combines everything into a comprehensive dataset
5. Saves to CSV and JSON with full text content
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import json
from datetime import datetime
import logging
from pathlib import Path

from etl.text_mining.scrapers.content_scraper import ContentScraper
from etl.text_mining.processors.text_feature_extractor import TextFeatureExtractor
from etl.text_mining.events import GA4SampleReadyEvent, ArticleMetadata

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_ga4_sample(csv_path: str) -> GA4SampleReadyEvent:
    """Load GA4 sample from CSV and convert to event"""
    df = pd.read_csv(csv_path)
    
    # Extract date range from first row (all rows have same date_range)
    date_range_start = df['date_range_start'].iloc[0] if 'date_range_start' in df.columns else None
    date_range_end = df['date_range_end'].iloc[0] if 'date_range_end' in df.columns else None
    
    # Convert to ArticleMetadata list
    articles = []
    for _, row in df.iterrows():
        article = ArticleMetadata(
            pagepath=row['pagepath'],
            pageviews=int(row['pageviews']),
            engaged_sessions=int(row['engaged_sessions']),
            avg_session_duration=float(row['avg_session_duration']),
            engagement_rate=float(row['engagement_rate']),
            editorial_score=float(row['editorial_score']),
            date_range_start=date_range_start,
            date_range_end=date_range_end
        )
        articles.append(article)
    
    # Create event
    event = GA4SampleReadyEvent(
        sample_id=df['sample_id'].iloc[0],
        articles=articles,
        date_range_start=date_range_start,
        date_range_end=date_range_end
    )
    
    return event, df


def scrape_and_extract_features(event: GA4SampleReadyEvent, output_dir: str = "./output"):
    """
    Complete pipeline: scrape content and extract all features
    
    Args:
        event: GA4SampleReadyEvent with article metadata
        output_dir: Directory to save results
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting complete text mining pipeline for {len(event.articles)} articles")
    
    # Step 1: Scrape content
    logger.info("\n[STEP 1/3] Scraping article content...")
    scraper = ContentScraper()
    scraped_event = scraper.scrape_sample(event)
    logger.info(f"✓ Scraped {scraped_event.articles_count} articles")
    
    # Step 2: Extract features
    logger.info("\n[STEP 2/3] Extracting text features...")
    
    # Build GA4 metadata mapping
    ga4_metadata = {
        article.pagepath: {
            'pageviews': article.pageviews,
            'engaged_sessions': article.engaged_sessions,
            'avg_session_duration': article.avg_session_duration,
            'engagement_rate': article.engagement_rate,
            'editorial_score': article.editorial_score
        }
        for article in event.articles
    }
    
    feature_extractor = TextFeatureExtractor()
    features_df = feature_extractor.process_scraped_articles(
        scraped_event,
        ga4_metadata
    )
    
    logger.info(f"✓ Extracted features for {len(features_df)} articles")
    
    # Step 3: Load scraped content and add full text
    logger.info("\n[STEP 3/3] Adding full text content...")
    
    with open(scraped_event.json_path, 'r', encoding='utf-8') as f:
        scraped_data = json.load(f)
    
    # Create mapping of pagepath to full text
    text_mapping = {}
    for article in scraped_data['articles']:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(article['html_content'], 'html.parser')
        
        # Remove script and style
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Extract full text
        full_text = soup.get_text(separator='\n', strip=True)
        text_mapping[article['pagepath']] = full_text
    
    # Add full text to dataframe
    features_df['full_text'] = features_df['pagepath'].map(text_mapping)
    
    # Reorder columns for better readability
    column_order = [
        'pagepath',
        'publish_date',
        'pageviews',
        'engaged_sessions',
        'avg_session_duration',
        'engagement_rate',
        'editorial_score',
        'word_count',
        'char_count',
        'paragraph_count',
        'full_text',
        'clean_text_preview',
        'scraped_at',
        'processing_version',
        'processing_date',
        'sample_id'
    ]
    
    # Only include columns that exist
    column_order = [col for col in column_order if col in features_df.columns]
    features_df = features_df[column_order]
    
    # Step 4: Save results
    logger.info("\n[STEP 4/4] Saving complete dataset...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"complete_text_analysis_{len(features_df)}articles_{timestamp}.csv"
    json_filename = f"complete_text_analysis_{len(features_df)}articles_{timestamp}.json"
    
    csv_path = output_dir / csv_filename
    json_path = output_dir / json_filename
    
    # Save to CSV
    features_df.to_csv(csv_path, index=False, encoding='utf-8')
    logger.info(f"✓ Saved CSV to: {csv_path}")
    
    # Save to JSON (structured format)
    output_json = {
        'metadata': {
            'sample_id': event.sample_id,
            'extracted_at': datetime.utcnow().isoformat(),
            'articles_count': len(features_df),
            'date_range': {
                'start': event.date_range_start,
                'end': event.date_range_end
            },
            'processing_version': features_df['processing_version'].iloc[0] if 'processing_version' in features_df else None
        },
        'articles': features_df.to_dict('records')
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=2, ensure_ascii=False)
    logger.info(f"✓ Saved JSON to: {json_path}")
    
    # Print summary statistics
    logger.info("\n" + "="*80)
    logger.info("TEXT MINING PIPELINE - SUMMARY")
    logger.info("="*80)
    logger.info(f"Sample ID: {event.sample_id}")
    logger.info(f"Articles processed: {len(features_df)}")
    logger.info(f"\nText Statistics:")
    logger.info(f"  Average word count: {features_df['word_count'].mean():.1f}")
    logger.info(f"  Min word count: {features_df['word_count'].min()}")
    logger.info(f"  Max word count: {features_df['word_count'].max()}")
    logger.info(f"  Total words extracted: {features_df['word_count'].sum():,}")
    logger.info(f"\nPerformance Metrics:")
    logger.info(f"  Average pageviews: {features_df['pageviews'].mean():.1f}")
    logger.info(f"  Average editorial score: {features_df['editorial_score'].mean():.2f}")
    logger.info(f"  Average engagement rate: {features_df['engagement_rate'].mean():.4f}")
    
    # Top 5 by word count
    logger.info(f"\nTop 5 articles by word count:")
    top_5_words = features_df.nlargest(5, 'word_count')[['pagepath', 'word_count', 'editorial_score']]
    for idx, row in top_5_words.iterrows():
        logger.info(f"  {idx+1}. {row['word_count']} words - Score: {row['editorial_score']:.2f}")
        logger.info(f"     {row['pagepath'][:70]}...")
    
    logger.info("="*80)
    logger.info(f"\n✓ Complete dataset ready for analysis!")
    logger.info(f"\nTo load in Jupyter:")
    logger.info(f"  import pandas as pd")
    logger.info(f"  df = pd.read_csv('{csv_path}')")
    logger.info(f"\nTo analyze text:")
    logger.info(f"  # Access full text for each article")
    logger.info(f"  for idx, row in df.iterrows():")
    logger.info(f"      text = row['full_text']")
    logger.info(f"      # Perform your text analysis here")
    
    return features_df, csv_path, json_path


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Complete Text Mining Pipeline - Scrape and Extract Features")
    parser.add_argument(
        '--input-csv',
        type=str,
        required=True,
        help='Path to GA4 sample CSV file'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./output',
        help='Output directory (default: ./output)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load GA4 sample
        logger.info(f"Loading GA4 sample from {args.input_csv}")
        event, original_df = load_ga4_sample(args.input_csv)
        logger.info(f"✓ Loaded {len(event.articles)} articles")
        
        # Run complete pipeline
        features_df, csv_path, json_path = scrape_and_extract_features(
            event,
            output_dir=args.output_dir
        )
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Output files:")
        logger.info(f"  CSV: {csv_path}")
        logger.info(f"  JSON: {json_path}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
