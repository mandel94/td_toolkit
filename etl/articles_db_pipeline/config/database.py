"""Database configuration for weekly articles ETL pipeline."""
import os

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

# ETL configuration
MIN_PAGE_VIEWS_THRESHOLD = 0  # Minimum page views per week to include article

# Fixed start date for 2025 data
ETL_START_DATE = '2025-01-01'