import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Adjust system path to import utility functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def run_query(query: str, engine) -> pd.DataFrame:
    """Helper to run a SQL query and return a DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql_query(text(query), conn)

def test_overall_weekly_trends(engine):
    """
    Use Case 1: Analyze overall site-wide page view trends week-over-week.
    This helps to spot major data gaps or anomalies.
    """
    print("\n--- Test Case 1: Overall Weekly Page View Trends ---")
    query = """
    SELECT 
        year, 
        week_of_year, 
        SUM(screen_page_views) as total_views
    FROM fact_weekly_metrics
    GROUP BY year, week_of_year
    ORDER BY year, week_of_year;
    """
    df = run_query(query, engine)
    if df.empty:
        print("Result: FAILED - No data found for weekly trends.")
        return
    
    print("Result: PASSED - Data retrieved successfully.")
    print("Sample Data:")
    print(df.head())
    
    # Spot check for major drops
    df['pct_change'] = df['total_views'].pct_change() * 100
    significant_drops = df[df['pct_change'] < -50]
    if not significant_drops.empty:
        print("\nWARNING: Found significant week-over-week drops in total page views:")
        print(significant_drops)
    else:
        print("\nAnalysis: No significant week-over-week drops (>50%) detected.")


def test_category_performance(engine):
    """
    Use Case 2: Analyze total page views per category for the entire period.
    This tests joins and aggregation logic.
    """
    print("\n--- Test Case 2: Category Performance Analysis ---")
    query = """
    SELECT
        c.category_name,
        SUM(f.screen_page_views) as total_views
    FROM fact_weekly_metrics f
    JOIN dim_categories c ON f.category_id = c.category_id
    GROUP BY c.category_name
    ORDER BY total_views DESC;
    """
    df = run_query(query, engine)
    if df.empty:
        print("Result: FAILED - Could not retrieve category performance data.")
        return
        
    print("Result: PASSED - Data retrieved successfully.")
    print("Top 5 Categories:")
    print(df.head())

def test_data_integrity(engine):
    """
    Use Case 3: Run data integrity checks on the fact table.
    - Check for orphaned records (NULL foreign keys).
    - Check for metrics outside their logical range.
    """
    print("\n--- Test Case 3: Data Integrity Checks ---")
    
    # 3a: Orphaned Records
    orphan_query = """
    SELECT COUNT(*) as orphan_count
    FROM fact_weekly_metrics
    WHERE article_id IS NULL OR category_id IS NULL OR author_id IS NULL;
    """
    orphan_count = run_query(orphan_query, engine)['orphan_count'].iloc[0]
    if orphan_count > 0:
        print(f"Result: FAILED - Found {orphan_count} orphaned records in fact_weekly_metrics.")
    else:
        print("Result: PASSED - No orphaned records found in the fact table.")

    # 3b: Metric Range Validation (Engagement Rate)
    range_query = """
    SELECT COUNT(*) as out_of_range_count
    FROM fact_weekly_metrics
    WHERE engagement_rate < 0 OR engagement_rate > 1;
    """
    out_of_range_count = run_query(range_query, engine)['out_of_range_count'].iloc[0]
    if out_of_range_count > 0:
        print(f"Result: FAILED - Found {out_of_range_count} records with engagement_rate outside the [0, 1] range.")
    else:
        print("Result: PASSED - All engagement_rate values are within the expected [0, 1] range.")

    # 3c: Negative Metrics
    negative_query = """
    SELECT COUNT(*) as negative_count
    FROM fact_weekly_metrics
    WHERE screen_page_views < 0 OR sessions < 0 OR engaged_sessions < 0;
    """
    negative_count = run_query(negative_query, engine)['negative_count'].iloc[0]
    if negative_count > 0:
        print(f"Result: FAILED - Found {negative_count} records with negative metric values.")
    else:
        print("Result: PASSED - No negative values found for key metrics.")

def test_dimension_integrity(engine):
    """
    Use Case 4: Check for duplicates in dimension tables.
    """
    print("\n--- Test Case 4: Dimension Integrity (Categories) ---")
    query = """
    SELECT category_name, COUNT(*) as count
    FROM dim_categories
    GROUP BY category_name
    HAVING COUNT(*) > 1;
    """
    df = run_query(query, engine)
    if not df.empty:
        print("Result: FAILED - Found duplicate category names in dim_categories.")
        print(df)
    else:
        print("Result: PASSED - No duplicate category names found.")


if __name__ == "__main__":
    print("Starting Database Test Suite...")
    db_engine = get_db_engine()
    
    test_overall_weekly_trends(db_engine)
    test_category_performance(db_engine)
    test_data_integrity(db_engine)
    test_dimension_integrity(db_engine)
    
    print("\nTest Suite Finished.")
