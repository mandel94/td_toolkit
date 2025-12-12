import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Adjust system path to import utility functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def run_query(query: str, engine, params=None) -> pd.DataFrame:
    """Helper to run a SQL query and return a DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params)

def test_overall_weekly_trends(engine):
    """Use Case 1: Analyze overall site-wide page view trends week-over-week."""
    print("\n--- Test Case 1: Overall Weekly Page View Trends ---")
    query = """
    SELECT year, week_of_year, SUM(screen_page_views) as total_views
    FROM fact_weekly_metrics
    GROUP BY year, week_of_year ORDER BY year, week_of_year;
    """
    df = run_query(query, engine)
    if df.empty:
        print("Result: FAILED - No data found for weekly trends.")
        return
    
    print("Result: PASSED - Data retrieved successfully.")
    df['pct_change'] = df['total_views'].pct_change() * 100
    significant_drops = df[df['pct_change'] < -50]
    if not significant_drops.empty:
        print("WARNING: Found significant week-over-week drops (>50%) in total page views:")
        print(significant_drops.to_string())
    else:
        print("Analysis: No significant week-over-week drops (>50%) detected.")

def test_data_integrity(engine):
    """Use Case 2: Run data integrity checks on the fact table."""
    print("\n--- Test Case 2: Core Data Integrity Checks ---")
    
    orphan_query = "SELECT COUNT(*) as c FROM fact_weekly_metrics WHERE article_id IS NULL OR category_id IS NULL OR author_id IS NULL;"
    if run_query(orphan_query, engine)['c'].iloc[0] > 0:
        print("Result: FAILED - Found orphaned records in fact_weekly_metrics.")
    else:
        print("Result: PASSED - No orphaned records found.")

    range_query = "SELECT COUNT(*) as c FROM fact_weekly_metrics WHERE engagement_rate < 0 OR engagement_rate > 1;"
    if run_query(range_query, engine)['c'].iloc[0] > 0:
        print("Result: FAILED - Found records with engagement_rate outside the [0, 1] range.")
    else:
        print("Result: PASSED - All engagement_rate values are within the expected [0, 1] range.")

def test_dimension_integrity(engine):
    """Use Case 3: Check for duplicates in dimension tables."""
    print("\n--- Test Case 3: Dimension Integrity (Categories) ---")
    query = "SELECT category_name, COUNT(*) as c FROM dim_categories GROUP BY category_name HAVING COUNT(*) > 1;"
    if not run_query(query, engine).empty:
        print("Result: FAILED - Found duplicate category names in dim_categories.")
    else:
        print("Result: PASSED - No duplicate category names found.")

def test_article_title_completeness(engine):
    """Use Case 4: Check for articles that have metrics but no valid title."""
    print("\n--- Test Case 4: Article Title Completeness ---")
    query = """
    SELECT COUNT(DISTINCT f.article_id) as c
    FROM fact_weekly_metrics f
    JOIN dim_articles a ON f.article_id = a.article_id
    WHERE a.title IS NULL OR a.title = 'N/A';
    """
    count = run_query(query, engine)['c'].iloc[0]
    if count > 0:
        print(f"Result: FAILED - Found {count} articles with metrics but no valid title.")
    else:
        print("Result: PASSED - All articles with metrics have a valid title.")

def test_metric_logical_consistency(engine):
    """Use Case 5: Check for logically impossible metric values."""
    print("\n--- Test Case 5: Metric Logical Consistency ---")
    query = "SELECT COUNT(*) as c FROM fact_weekly_metrics WHERE engaged_sessions > sessions;"
    count = run_query(query, engine)['c'].iloc[0]
    if count > 0:
        print(f"Result: FAILED - Found {count} records where engaged_sessions > sessions.")
    else:
        print("Result: PASSED - No records found where engaged_sessions > sessions.")

def test_uncategorized_impact(engine):
    """Use Case 6: Analyze the impact of the 'Uncategorized' category."""
    print("\n--- Test Case 6: 'Uncategorized' Impact Analysis ---")
    query = """
    WITH CategoryViews AS (
        SELECT
            c.category_name,
            SUM(f.screen_page_views) as total_views
        FROM fact_weekly_metrics f
        JOIN dim_categories c ON f.category_id = c.category_id
        GROUP BY c.category_name
    )
    SELECT
        (SELECT total_views FROM CategoryViews WHERE category_name = 'Uncategorized') * 100.0 / 
        (SELECT SUM(total_views) FROM CategoryViews) as percentage;
    """
    percentage = run_query(query, engine)['percentage'].iloc[0]
    if percentage is None:
        percentage = 0
        
    if percentage > 10:
        print(f"WARNING: 'Uncategorized' articles account for {percentage:.2f}% of total page views, which is high.")
    elif percentage > 0:
        print(f"Info: 'Uncategorized' articles account for {percentage:.2f}% of total page views.")
    else:
        print("Result: PASSED - No views from 'Uncategorized' articles.")

def test_author_assignment(engine):
    """Use Case 7: Verify that all articles are assigned to the default 'N/A' author."""
    print("\n--- Test Case 7: Author Assignment Verification ---")
    query = """
    SELECT COUNT(f.article_id) as c
    FROM fact_weekly_metrics f
    LEFT JOIN dim_authors a ON f.author_id = a.author_id
    WHERE a.author_name != 'N/A' OR a.author_name IS NULL;
    """
    count = run_query(query, engine)['c'].iloc[0]
    if count > 0:
        print(f"Result: FAILED - Found {count} records not assigned to the default 'N/A' author.")
    else:
        print("Info: Confirmed that all records are assigned to the default 'N/A' author as expected.")


if __name__ == "__main__":
    print("======== Starting Full Data Validation Suite ========")
    db_engine = get_db_engine()
    
    test_overall_weekly_trends(db_engine)
    test_data_integrity(db_engine)
    test_dimension_integrity(db_engine)
    test_article_title_completeness(db_engine)
    test_metric_logical_consistency(db_engine)
    test_uncategorized_impact(db_engine)
    test_author_assignment(db_engine)
    
    print("\n======== Validation Suite Finished ========")
