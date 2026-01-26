import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Adjust system path to import utility functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def count_total_articles():
    """
    Counts the total number of unique articles in the dim_articles table.
    """
    engine = get_db_engine()
    
    query = text("""
    SELECT COUNT(article_id) as total_articles
    FROM dim_articles;
    """)

    with engine.connect() as conn:
        result = conn.execute(query).fetchone()
        if result:
            total_articles = result[0]
            print(f"Total number of unique articles in the database: {total_articles}")
        else:
            print("Could not count articles.")

if __name__ == "__main__":
    count_total_articles()
