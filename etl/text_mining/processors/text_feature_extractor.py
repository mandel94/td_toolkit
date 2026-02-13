"""
Text Feature Extractor for Text Mining Pipeline
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

import json
import logging
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

from etl.text_mining.config import config
from etl.text_mining.events import ArticleHTMLScrapedEvent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextFeatureExtractor:
    """
    Extract text features from scraped HTML content
    
    Responsibilities:
    - Load scraped HTML from filesystem
    - Extract clean text from HTML
    - Calculate text features (word count, etc.)
    - Merge with GA4 metrics
    - Create flat feature table
    """
    
    def __init__(self, processing_version: str = None):
        self.processing_version = processing_version or config.PROCESSING_VERSION
        logger.info(f"TextFeatureExtractor initialized: version={self.processing_version}")
    
    def process_scraped_articles(
        self,
        event: ArticleHTMLScrapedEvent,
        ga4_metadata: Dict[str, Any] = None
    ) -> pd.DataFrame:
        """
        Process scraped articles and extract features
        
        Args:
            event: ArticleHTMLScrapedEvent with path to scraped data
            ga4_metadata: Optional dictionary mapping pagepath to GA4 metrics
            
        Returns:
            DataFrame with extracted features
        """
        logger.info(f"Processing scraped articles from {event.json_path}")
        
        # Load scraped data
        scraped_data = self._load_scraped_data(event.json_path)
        
        if not scraped_data:
            logger.warning("No scraped data found")
            return pd.DataFrame()
        
        # Extract features for each article
        features_list = []
        
        for article in scraped_data['articles']:
            features = self._extract_article_features(article)
            
            # Merge with GA4 metadata if provided
            if ga4_metadata and article['pagepath'] in ga4_metadata:
                features.update(ga4_metadata[article['pagepath']])
            
            features_list.append(features)
        
        # Create DataFrame
        df = pd.DataFrame(features_list)
        
        # Compute editorial_score using content_scoring module if we have required metrics
        if not df.empty and all(col in df.columns for col in ['pageviews', 'engagement_rate', 'avg_session_duration']):
            logger.info("Computing editorial scores using content_scoring module...")
            try:
                from etl.content_scoring.calculator import ContentScoreCalculator
                from etl.content_scoring.config import ContentScoringConfig
                
                # Map our column names to content_scoring expected names
                scoring_df = df.rename(columns={
                    'pageviews': 'screenPageViews',
                    'engagement_rate': 'engagementRate',
                    'avg_session_duration': 'averageSessionDuration'
                })
                
                scoring_config = ContentScoringConfig(
                    metrics_mapping={
                        'views': 'screenPageViews',
                        'engagement_rate': 'engagementRate',
                        'session_duration': 'averageSessionDuration'
                    },
                    strategy_name='balanced'
                )
                score_calculator = ContentScoreCalculator(config=scoring_config)
                scoring_df = score_calculator.calculate(scoring_df)
                
                # Copy editorial_score back to original dataframe
                df['editorial_score'] = scoring_df['editorial_score']
                logger.info(f"Editorial scores computed. Avg: {df['editorial_score'].mean():.6f}")
            except Exception as e:
                logger.warning(f"Could not compute editorial scores: {e}. Using GA4 scores if available.")
                # Keep the editorial_score from GA4 metadata if available
                pass
        
        # Add processing metadata
        df['processing_version'] = self.processing_version
        df['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df['sample_id'] = event.sample_id
        
        logger.info(f"Extracted features for {len(df)} articles")
        
        return df
    
    def _load_scraped_data(self, json_path: str) -> Dict[str, Any]:
        """Load scraped data from JSON file"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading scraped data from {json_path}: {e}")
            return {}
    
    def _extract_article_features(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract text features from a single article
        
        Current MVP features:
        - pagepath
        - word_count
        - char_count
        - paragraph_count
        - clean_text (first 500 chars for preview)
        """
        pagepath = article['pagepath']
        html_content = article['html_content']
        
        # Parse HTML and extract text
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get clean text
        text = soup.get_text(separator=' ', strip=True)
        
        # Calculate features
        words = text.split()
        word_count = len(words)
        char_count = len(text)
        
        # Count paragraphs (p tags)
        paragraphs = soup.find_all('p')
        paragraph_count = len(paragraphs)
        
        features = {
            'pagepath': pagepath,
            'word_count': word_count,
            'char_count': char_count,
            'paragraph_count': paragraph_count,
            'clean_text_preview': text[:500] if text else None,
            'publish_date': article.get('publish_date'),  # Include publish_date from scraping
            'scraped_at': article.get('scraped_at')
        }
        
        return features


if __name__ == "__main__":
    # Test feature extraction
    test_event = ArticleHTMLScrapedEvent(
        sample_id="test_123",
        json_path="./data/scraped/sample_test.json",
        articles_count=1
    )
    
    # Mock GA4 metadata
    ga4_metadata = {
        "/2025/01/15/test-article/": {
            "pageviews": 1000,
            "engaged_sessions": 500,
            "avg_session_duration": 120.5,
            "engagement_rate": 0.65,
            "editorial_score": 0.75
        }
    }
    
    extractor = TextFeatureExtractor()
    print("Feature extractor initialized")
