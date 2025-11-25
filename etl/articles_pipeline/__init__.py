"""Articles ETL Pipeline Package."""

from .pipeline import ArticlesETLPipeline
from .models.article import ProcessedArticle, RawArticleData, ArticleMetadata

__version__ = "1.0.0"
__author__ = "Taxi Drivers Analytics Team"

__all__ = [
    "ArticlesETLPipeline",
    "ProcessedArticle",
    "RawArticleData", 
    "ArticleMetadata"
]