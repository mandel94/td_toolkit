"""Data models for the ETL pipeline."""

from .article import (
    RawArticleData,
    ArticleMetadata,
    ProcessedArticle,
    ArticleBatch
)

__all__ = [
    "RawArticleData",
    "ArticleMetadata", 
    "ProcessedArticle",
    "ArticleBatch"
]