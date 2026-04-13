"""Archive page scraper for TaxiDrivers.it."""
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from loguru import logger

from .base_scraper import ScraperBase
from .selectors import TaxiDriversSelectors


class ArchiveScraper(ScraperBase):
    """Scraper for TaxiDrivers.it archive page to extract article listings."""
    
    def __init__(
        self,
        base_url: str = "https://www.taxidrivers.it",
        **kwargs
    ):
        """Initialize archive scraper.
        
        Args:
            base_url: Base URL (default: https://www.taxidrivers.it)
            **kwargs: Additional arguments passed to ScraperBase
        """
        super().__init__(base_url=base_url, **kwargs)
        self.selectors = TaxiDriversSelectors()
    
    def scrape_archive_page(self, url: str = "/archivio") -> List[Dict[str, Optional[str]]]:
        """Scrape the archive page for article listings.
        
        Args:
            url: Archive page URL path (default: /archivio)
            
        Returns:
            List of dictionaries containing article metadata:
            - title: Article title
            - category: Article category
            - url: Full article URL
            - page_path: Relative path (without domain)
            - published_text: Publication date text (Italian format)
        """
        if url != "/archivio":
            raise ValueError("Mass scraping disabled: only /archivio is allowed")

        full_url = f"{self.base_url}{url}" if url.startswith('/') else url
        
        logger.info(f"Scraping archive page: {full_url}")
        
        soup = self.fetch_page(full_url)
        if not soup:
            logger.error(f"Failed to fetch archive page: {full_url}")
            return []
        
        articles = []
        
        # Find all article containers
        article_containers = soup.find_all(class_=self.selectors.ARCHIVE_ARTICLE_CONTAINER)
        
        logger.info(f"Found {len(article_containers)} article containers")
        
        for container in article_containers:
            try:
                article_data = self._extract_article_from_container(container)
                if article_data and article_data.get('url'):
                    articles.append(article_data)
            except Exception as e:
                logger.warning(f"Failed to extract article from container: {str(e)}")
                continue
        
        logger.success(f"Extracted {len(articles)} articles from archive page")
        
        return articles

    def scrape_archive_all(self, max_pages: Optional[int] = None) -> List[Dict[str, Optional[str]]]:
        """Disabled: mass scraping is not allowed by design."""
        raise RuntimeError("Mass scraping disabled by design. Use scrape_archive_page('/archivio') only.")
    
    def _extract_article_from_container(self, container) -> Optional[Dict[str, Optional[str]]]:
        """Extract article data from a single container element.
        
        Args:
            container: BeautifulSoup element containing article data
            
        Returns:
            Dictionary with article metadata or None if extraction fails
        """
        # Extract title: first h2 inside div with class mvp-blog-story-out
        title = None
        story_out_div = container.find('div', class_=lambda x: x and 'mvp-blog-story-out' in x)
        if story_out_div:
            h2_tag = story_out_div.find('h2')
            if h2_tag:
                title = h2_tag.text.strip()
        
        # Extract category: first span inside div with class mvp-cat-date-wrap
        category = None
        cat_date_wrap = container.find('div', class_=lambda x: x and 'mvp-cat-date-wrap' in x)
        if cat_date_wrap:
            span_tag = cat_date_wrap.find('span', class_=self.selectors.ARCHIVE_CATEGORY_CLASS)
            if span_tag:
                category = span_tag.text.strip()
        
        # Extract published text
        published_text = None
        if cat_date_wrap:
            date_span = cat_date_wrap.find('span', class_=self.selectors.ARCHIVE_DATE_CLASS)
            if date_span:
                published_text = date_span.text.strip()
        
        # Extract URL: href from link in mvp-blog-story-wrap
        url = None
        link_tag = container.find('a', href=True)
        if link_tag:
            url = link_tag['href']
        
        # Create page_path (relative URL without domain)
        page_path = None
        if url:
            if url.startswith('http'):
                # Remove domain to get page path
                page_path = url.replace(self.base_url, '')
                if not page_path.startswith('/'):
                    page_path = '/' + page_path
            else:
                page_path = url if url.startswith('/') else '/' + url
                url = f"{self.base_url}{page_path}"
        
        # Validate minimum required fields
        if not url:
            return None
        
        return {
            'title': title,
            'category': category,
            'url': url,
            'page_path': page_path,
            'published_text': published_text,
            'scraped_at': datetime.now().isoformat()
        }
    
    def scrape(self, url: str = "/archivio") -> List[Dict[str, Optional[str]]]:
        """Main scraping method.
        
        Args:
            url: Archive page URL path
            
        Returns:
            List of article metadata dictionaries
        """
        return self.scrape_archive_page(url)
