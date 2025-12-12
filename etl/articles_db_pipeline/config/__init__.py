"""Configuration modules."""

from .database import (
    DB_CONFIG,
    DATABASE_URL,
    GA4_PROPERTY_ID,
    GA4_DIMENSIONS,
    GA4_METRICS,
    DOMAIN,
    MIN_PAGE_VIEWS_THRESHOLD,
    BATCH_SIZE
)

__all__ = [
    "DB_CONFIG",
    "DATABASE_URL",
    "GA4_PROPERTY_ID",
    "GA4_DIMENSIONS",
    "GA4_METRICS",
    "DOMAIN",
    "MIN_PAGE_VIEWS_THRESHOLD",
    "BATCH_SIZE"
]