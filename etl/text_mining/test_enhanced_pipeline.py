"""
Test script to verify enhanced pipeline with:
1. Average session duration (already in model)
2. Editorial score computation using content_scoring module
3. Date range columns
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

import pandas as pd
import json
from datetime import datetime
from pathlib import Path

# Import modules
from etl.text_mining.events import ArticleMetadata, GA4SampleReadyEvent, ArticleHTMLScrapedEvent
from etl.text_mining.processors.text_feature_extractor import TextFeatureExtractor
from etl.text_mining.storage.postgres_storage import PostgresStorage

def test_enhanced_features():
    """Test that enhanced features are properly processed"""
    
    print("=" * 80)
    print("TESTING ENHANCED PIPELINE FEATURES")
    print("=" * 80)
    
    # 1. Create mock GA4 event with all required fields
    print("\n1. Creating mock GA4 event with enhanced fields...")
    articles = [
        ArticleMetadata(
            pagepath="/2025/01/15/test-article-one/",
            pageviews=1500,
            engaged_sessions=900,
            avg_session_duration=125.5,  # Average session duration
            engagement_rate=0.75,
            editorial_score=None,  # Will be computed
            date_range_start="2025-01-01",  # Date range start
            date_range_end="2025-01-31"     # Date range end
        ),
        ArticleMetadata(
            pagepath="/2025/01/20/test-article-two/",
            pageviews=800,
            engaged_sessions=500,
            avg_session_duration=95.3,
            engagement_rate=0.62,
            editorial_score=None,
            date_range_start="2025-01-01",
            date_range_end="2025-01-31"
        )
    ]
    
    ga4_event = GA4SampleReadyEvent(
        articles=articles,
        date_range_start="2025-01-01",
        date_range_end="2025-01-31"
    )
    
    print(f"   ✓ Created GA4 event with {len(ga4_event.articles)} articles")
    print(f"   ✓ Date range: {ga4_event.date_range_start} to {ga4_event.date_range_end}")
    for art in ga4_event.articles:
        print(f"     - {art.pagepath}: {art.pageviews} views, {art.avg_session_duration}s avg session")
    
    # 2. Create mock scraped data
    print("\n2. Creating mock scraped HTML content...")
    scraped_data = {
        "sample_id": ga4_event.sample_id,
        "articles": [
            {
                "pagepath": "/2025/01/15/test-article-one/",
                "html_content": "<html><body><p>This is a test article with some content.</p><p>It has multiple paragraphs for testing.</p></body></html>",
                "scraped_at": datetime.utcnow().isoformat()
            },
            {
                "pagepath": "/2025/01/20/test-article-two/",
                "html_content": "<html><body><p>Another test article.</p><p>Also with multiple paragraphs.</p><p>And more text for counting words.</p></body></html>",
                "scraped_at": datetime.utcnow().isoformat()
            }
        ]
    }
    
    # Save to temp file
    temp_dir = Path("./data/scraped")
    temp_dir.mkdir(parents=True, exist_ok=True)
    json_path = temp_dir / f"test_{ga4_event.sample_id}.json"
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f)
    
    print(f"   ✓ Created mock scraped data: {len(scraped_data['articles'])} articles")
    print(f"   ✓ Saved to: {json_path}")
    
    # 3. Create scraped event
    scraped_event = ArticleHTMLScrapedEvent(
        sample_id=ga4_event.sample_id,
        json_path=str(json_path),
        articles_count=len(scraped_data['articles'])
    )
    
    # 4. Build GA4 metadata mapping
    print("\n3. Building GA4 metadata mapping...")
    ga4_metadata = {}
    for article in ga4_event.articles:
        ga4_metadata[article.pagepath] = {
            'pageviews': article.pageviews,
            'engaged_sessions': article.engaged_sessions,
            'avg_session_duration': article.avg_session_duration,
            'engagement_rate': article.engagement_rate,
            'editorial_score': article.editorial_score,
            'date_range_start': article.date_range_start,
            'date_range_end': article.date_range_end
        }
    
    print(f"   ✓ Mapped {len(ga4_metadata)} articles with GA4 metadata")
    
    # 5. Extract features (this should compute editorial_score)
    print("\n4. Extracting features and computing editorial scores...")
    extractor = TextFeatureExtractor(processing_version="test_v1.0")
    features_df = extractor.process_scraped_articles(scraped_event, ga4_metadata)
    
    print(f"   ✓ Extracted features for {len(features_df)} articles")
    
    # 6. Verify all required columns are present
    print("\n5. Verifying enhanced columns...")
    required_columns = [
        'pagepath', 'word_count', 'char_count', 'paragraph_count',
        'pageviews', 'engaged_sessions', 'avg_session_duration',
        'engagement_rate', 'editorial_score',
        'date_range_start', 'date_range_end',
        'processing_version', 'sample_id'
    ]
    
    missing_columns = [col for col in required_columns if col not in features_df.columns]
    
    if missing_columns:
        print(f"   ✗ Missing columns: {missing_columns}")
        return False
    else:
        print(f"   ✓ All required columns present!")
    
    # 7. Display results
    print("\n6. Feature extraction results:")
    print("=" * 80)
    for idx, row in features_df.iterrows():
        print(f"\n   Article: {row['pagepath']}")
        print(f"   - Word count: {row['word_count']}")
        print(f"   - Char count: {row['char_count']}")
        print(f"   - Paragraphs: {row['paragraph_count']}")
        print(f"   - Pageviews: {row['pageviews']}")
        print(f"   - Engaged sessions: {row['engaged_sessions']}")
        print(f"   - Avg session duration: {row['avg_session_duration']:.2f}s")
        print(f"   - Engagement rate: {row['engagement_rate']:.4f}")
        print(f"   - Editorial score: {row['editorial_score']:.6f}")
        print(f"   - Date range: {row['date_range_start']} to {row['date_range_end']}")
    
    # 8. Test database storage
    print("\n7. Testing database storage...")
    try:
        storage = PostgresStorage()
        storage.store_features(features_df)
        storage.close()
        print("   ✓ Features stored successfully in database")
    except Exception as e:
        print(f"   ✗ Database storage failed: {e}")
        return False
    
    # 9. Verify database content
    print("\n8. Verifying database content...")
    try:
        storage = PostgresStorage()
        with storage.conn.cursor() as cur:
            cur.execute("""
                SELECT pagepath, avg_session_duration, editorial_score, 
                       date_range_start, date_range_end
                FROM text_mining.articles_features
                WHERE sample_id = %s
                ORDER BY pageviews DESC
            """, (ga4_event.sample_id,))
            
            rows = cur.fetchall()
            print(f"   ✓ Retrieved {len(rows)} rows from database")
            print("\n   Database content:")
            for row in rows:
                pagepath, avg_dur, ed_score, dr_start, dr_end = row
                print(f"     - {pagepath}")
                print(f"       Avg session: {avg_dur}s, Editorial score: {ed_score:.6f}")
                print(f"       Date range: {dr_start} to {dr_end}")
        
        storage.close()
    except Exception as e:
        print(f"   ✗ Database verification failed: {e}")
        return False
    
    print("\n" + "=" * 80)
    print("✓ ALL TESTS PASSED!")
    print("=" * 80)
    print("\nEnhancements verified:")
    print("  1. ✓ Average session duration included in data model")
    print("  2. ✓ Editorial score computed using content_scoring module")
    print("  3. ✓ Date range columns (start/end) stored in database")
    
    # Cleanup
    if json_path.exists():
        json_path.unlink()
    
    return True


if __name__ == "__main__":
    success = test_enhanced_features()
    sys.exit(0 if success else 1)
