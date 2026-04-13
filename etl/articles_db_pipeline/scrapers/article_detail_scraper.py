"""Article detail page scraper for TaxiDrivers.it."""
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger

from .base_scraper import ScraperBase
from .selectors import TaxiDriversSelectors


class ArticleDetailScraper(ScraperBase):
    """Scraper for individual TaxiDrivers.it article pages to extract detailed content."""
    
    def __init__(
        self,
        base_url: str = "https://www.taxidrivers.it",
        **kwargs
    ):
        """Initialize article detail scraper.
        
        Args:
            base_url: Base URL (default: https://www.taxidrivers.it)
            **kwargs: Additional arguments passed to ScraperBase
        """
        super().__init__(base_url=base_url, **kwargs)
        self.selectors = TaxiDriversSelectors()
    
    def scrape_article_detail(self, url: str) -> Optional[Dict[str, Optional[str]]]:
        """Scrape a single article detail page.
        
        Args:
            url: Article URL (can be full URL or page path)
            
        Returns:
            Dictionary containing article details:
            - page_path: Relative page path
            - title: Article title
            - subtitle: Article subtitle/excerpt
            - author: Article author
            - publication_date: Publication date (ISO format)
            - body_html: Full HTML body content
            - body_text: Plain text body content
        """
        # Normalize URL
        if not url.startswith('http'):
            full_url = f"{self.base_url}{url}" if url.startswith('/') else f"{self.base_url}/{url}"
            page_path = url if url.startswith('/') else f"/{url}"
        else:
            full_url = url
            page_path = url.replace(self.base_url, '')
            if not page_path.startswith('/'):
                page_path = f"/{page_path}"
        
        self.progress.current_url = full_url
        self._notify_progress()
        
        logger.debug(f"Scraping article detail: {full_url}")
        
        soup = self.fetch_page(full_url)
        if not soup:
            logger.error(f"Failed to fetch article page: {full_url}")
            return None
        
        try:
            article_data = self._extract_article_details(soup, page_path)
            return article_data
        except Exception as e:
            logger.error(f"Failed to extract article details from {full_url}: {str(e)}")
            self._notify_error(e, full_url)
            return None
    
    def _extract_article_details(self, soup: BeautifulSoup, page_path: str) -> Dict[str, Optional[str]]:
        """Extract all article details from parsed HTML.
        
        Args:
            soup: BeautifulSoup parsed HTML
            page_path: Relative page path
            
        Returns:
            Dictionary with article details
        """
        # Extract title
        title = None
        title_tag = soup.find('h1', class_=lambda x: x and self.selectors.DETAIL_TITLE_CLASS in x)
        if title_tag:
            title = title_tag.text.strip()
        
        # Extract subtitle/excerpt
        subtitle = None
        subtitle_tag = soup.find('span', class_=self.selectors.DETAIL_SUBTITLE_CLASS)
        if subtitle_tag:
            subtitle = subtitle_tag.text.strip()
        
        # Extract author
        author = None
        author_tag = soup.find('a', rel=self.selectors.DETAIL_AUTHOR_REL)
        if author_tag:
            author = author_tag.text.strip()
        
        # Extract publication date
        publication_date = None
        date_tag = soup.find('time', class_=self.selectors.DETAIL_DATE_CLASS)
        if date_tag:
            # Try to get datetime attribute first
            if date_tag.has_attr(self.selectors.DETAIL_DATE_ATTR):
                datetime_str = date_tag[self.selectors.DETAIL_DATE_ATTR]
                try:
                    # Parse ISO format datetime
                    dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                    publication_date = dt.date().isoformat()
                except Exception as e:
                    logger.warning(f"Failed to parse datetime attribute: {datetime_str}, error: {str(e)}")
            
            # Fallback to text content
            if not publication_date:
                date_text = date_tag.text.strip()
                try:
                    dt = datetime.fromisoformat(date_text)
                    publication_date = dt.date().isoformat()
                except Exception:
                    logger.warning(f"Failed to parse date text: {date_text}")
        
        # Extract body content (full HTML and text)
        body_html = None
        body_text = None
        
        # Try to find the main content container
        content_container = soup.find('div', class_=lambda x: x and self.selectors.DETAIL_BODY_CLASS in x)
        if not content_container:
            # Fallback to article tag
            content_container = soup.find('article')
        
        if content_container:
            # Get HTML content
            body_html = str(content_container)
            # Get plain text (stripped of HTML tags)
            body_text = content_container.get_text(separator='\n', strip=True)
        
        return {
            'page_path': page_path,
            'title': title,
            'subtitle': subtitle,
            'author': author,
            'publication_date': publication_date,
            'body_html': body_html,
            'body_text': body_text,
            'scraped_at': datetime.now().isoformat()
        }
    
    def scrape_multiple_articles(
        self,
        article_urls: List[str]
    ) -> List[Dict[str, Optional[str]]]:
        """Scrape multiple article detail pages with batching and progress monitoring.
        
        Args:
            article_urls: List of article URLs or page paths
            
        Returns:
            List of article detail dictionaries
        """
        logger.info(f"Starting batch scraping of {len(article_urls)} articles")
        
        results = self.process_in_batches(
            items=article_urls,
            process_func=self.scrape_article_detail,
            description="Scraping article details"
        )
        
        return results
    
    def scrape(self, article_urls: List[str]) -> List[Dict[str, Optional[str]]]:
        """Main scraping method for multiple articles.
        
        Args:
            article_urls: List of article URLs to scrape
            
        Returns:
            List of article detail dictionaries
        """
        return self.scrape_multiple_articles(article_urls)
