#!/usr/bin/env python
"""Test database connection."""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from sqlalchemy import create_engine, text
from etl.articles_db_pipeline.config.database import DATABASE_URL

try:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("✓ Database connection successful!")
except Exception as e:
    print(f"✗ Database connection failed: {str(e)}")
    print("\nPostgreSQL must be running to execute the full ETL test.")
    print("\nOptions to start PostgreSQL:")
    print("  1. Via Docker: docker run -d --name postgres_articles -e POSTGRES_PASSWORD=postgres123 -e POSTGRES_DB=articles_db -p 5432:5432 postgres:15")
    print("  2. Check Windows Services (services.msc) if PostgreSQL is installed")
