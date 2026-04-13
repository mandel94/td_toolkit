"""CSS selectors configuration for TaxiDrivers.it website.

This centralized configuration allows easy maintenance when website structure changes.
"""
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class TaxiDriversSelectors:
    """CSS selectors for TaxiDrivers.it website scraping."""
    
    # Archive page selectors
    ARCHIVE_ARTICLE_CONTAINER: str = "mvp-blog-story-wrap"
    ARCHIVE_TITLE_TAG: str = "h2"
    ARCHIVE_TITLE_CONTAINER: str = "mvp-blog-story-out"
    ARCHIVE_CATEGORY_CLASS: str = "mvp-cd-cat"
    ARCHIVE_DATE_CLASS: str = "mvp-cd-date"
    ARCHIVE_LINK_TAG: str = "a"
    
    # Article detail page selectors
    DETAIL_TITLE_CLASS: str = "mvp-post-title"
    DETAIL_SUBTITLE_CLASS: str = "mvp-post-excerpt"
    DETAIL_AUTHOR_REL: str = "author"
    DETAIL_DATE_CLASS: str = "post-date"
    DETAIL_DATE_ATTR: str = "datetime"
    DETAIL_BODY_CLASS: str = "mvp-content-main"
    DETAIL_CONTENT_TAG: str = "article"
    
    @classmethod
    def get_archive_selectors(cls) -> Dict[str, str]:
        """Get all archive page selectors as dictionary."""
        return {
            'article_container': cls.ARCHIVE_ARTICLE_CONTAINER,
            'title_tag': cls.ARCHIVE_TITLE_TAG,
            'title_container': cls.ARCHIVE_TITLE_CONTAINER,
            'category_class': cls.ARCHIVE_CATEGORY_CLASS,
            'date_class': cls.ARCHIVE_DATE_CLASS,
            'link_tag': cls.ARCHIVE_LINK_TAG,
        }
    
    @classmethod
    def get_detail_selectors(cls) -> Dict[str, str]:
        """Get all article detail selectors as dictionary."""
        return {
            'title_class': cls.DETAIL_TITLE_CLASS,
            'subtitle_class': cls.DETAIL_SUBTITLE_CLASS,
            'author_rel': cls.DETAIL_AUTHOR_REL,
            'date_class': cls.DETAIL_DATE_CLASS,
            'date_attr': cls.DETAIL_DATE_ATTR,
            'body_class': cls.DETAIL_BODY_CLASS,
            'content_tag': cls.DETAIL_CONTENT_TAG,
        }
