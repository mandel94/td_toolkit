"""Database configuration for articles ETL pipeline."""
import os
from typing import Dict, Any

# Database connection configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'articles_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres123')
}

# SQLAlchemy connection string
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

# GA4 API configuration
GA4_PROPERTY_ID = '394327334'
GA4_DIMENSIONS = ['pagePath']
GA4_METRICS = [
    'screenPageViews',
    'sessions', 
    'engagedSessions',
    'engagementRate',
    'averageSessionDuration'
]

# ETL configuration
MIN_PAGE_VIEWS_THRESHOLD = 30

# Fixed date range - start from January 1, 2025
ETL_START_DATE = '2025-01-01'

# Batch processing
BATCH_SIZE = 100
MAX_RETRIES = 3