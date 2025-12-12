import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Adjust system path to import utility functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def save_top_articles_for_week_as_csv(week_number: int, limit: int = 100):
    """
    Retrieves the top articles for a given week and saves them to a CSV file.

    Args:
        week_number: The week of the year to analyze.
        limit: The number of top articles to retrieve.
    """
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f'top_{limit}_articles_week_{week_number}_tab_separated.csv')

    engine = get_db_engine()
    
    query = text("""
    SELECT 
        a.title,
        a.page_path,
        c.category_name,
        f.screen_page_views,
        f.engaged_sessions,
        f.sessions,
        f.engagement_rate,
        f.average_session_duration
    FROM fact_weekly_metrics f
    JOIN dim_articles a ON f.article_id = a.article_id
    JOIN dim_categories c ON f.category_id = c.category_id
    WHERE f.week_of_year = :week
    ORDER BY f.screen_page_views DESC
    LIMIT :limit;
    """)

    with engine.connect() as conn:
        result_df = pd.read_sql_query(query, conn, params={'week': week_number, 'limit': limit})

    if result_df.empty:
        print(f"No data found for week {week_number}.")
        return

    result_df.to_csv(output_path, index=False, sep='	')
    print(f"Successfully saved top {limit} articles for week {week_number} to '{output_path}'")


if __name__ == "__main__":
    save_top_articles_for_week_as_csv(week_number=47, limit=100)
