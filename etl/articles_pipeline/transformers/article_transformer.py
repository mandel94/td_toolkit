"""Article data transformation module."""
import sys
import os
from typing import List, Optional, Dict, Any
from logging import logger

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")))

from map_ga4_categories import map_ga4_categories
from etl.articles_pipeline.models.article import RawArticleData, ArticleMetadata, ProcessedArticle
class ArticleTransformer:
    """Transform raw article data into database-ready format."""
    
    def __init__(self):
        logger.info("Initialized ArticleTransformer")
    
    def transform_batch(
        self,
        raw_articles: List[RawArticleData]
    ) -> List[ProcessedArticle]:
        """Transform a batch of raw articles (no metadata scraping).
        
        Args:
            raw_articles: List of raw article data from GA4
            
        Returns:
            List of ProcessedArticle objects ready for database insertion
        """
        logger.info(f"Transforming {len(raw_articles)} articles")
        
        processed_articles = []
        
        for i, raw_article in enumerate(raw_articles):
            try:
                # Transform single article without metadata
                processed_article = self._transform_single(raw_article)
                processed_articles.append(processed_article)
                
            except Exception as e:
                logger.error(f"Failed to transform article {i}: {str(e)}")
                continue
        
        logger.success(f"Successfully transformed {len(processed_articles)} articles")
        return processed_articles
    
    def _transform_single(
        self,
        raw_article: RawArticleData
    ) -> ProcessedArticle:
        """Transform a single article (no metadata)."""
        
        # Map category using existing function
        category = self._map_category(raw_article.page_path)
        
        # Create processed article with minimal data
        processed_article = ProcessedArticle(
            title=None,  # Not populated for now
            author=None,  # Always None
            category=category,
            screen_page_views=raw_article.screen_page_views,
            sessions=raw_article.sessions,
            engaged_sessions=raw_article.engaged_sessions,
            engagement_rate=raw_article.engagement_rate,
            average_session_duration=raw_article.average_session_duration,
            publication_date=None,  # Not populated for now
            page_path=raw_article.page_path,  # Main field we care about
            url=None  # Not populated for now
        )
        
        return processed_article
    
    def _map_category(self, page_path: str) -> Optional[str]:
        """Map page path to article category."""
        try:
            # Use existing category mapping function
            category = map_ga4_categories(page_path)
            
            # Handle "Si farà" special case
            if "si-fara" in page_path:
                category = "Si farà"
            
            # Merge "Recensioni / In Sala" with "Recensioni"
            if category in ["Recensioni / In Sala", "Recensioni"]:
                category = "Recensioni"
            
            return category
            
        except Exception as e:
            logger.warning(f"Failed to map category for {page_path}: {str(e)}")
            return "Uncategorized"
    
    # URL building removed for simplicity
    
    def validate_processed_article(self, article: ProcessedArticle) -> bool:
        """Validate a processed article before database insertion."""
        try:
            # Check required fields
            if not article.page_path:
                logger.error("Missing page_path")
                return False
            
            if not article.url:
                logger.error("Missing URL")
                return False
            
            # Validate numeric fields
            if article.screen_page_views < 0:
                logger.error("Invalid screen_page_views")
                return False
            
            if article.sessions < 0:
                logger.error("Invalid sessions")
                return False
            
            if article.engaged_sessions < 0:
                logger.error("Invalid engaged_sessions")
                return False
            
            # Validate engagement rate (0-1)
            if not (0 <= float(article.engagement_rate) <= 1):
                logger.error("Invalid engagement_rate")
                return False
            
            # Validate duration
            if float(article.average_session_duration) < 0:
                logger.error("Invalid average_session_duration")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return False