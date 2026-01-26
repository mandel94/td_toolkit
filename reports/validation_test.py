import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# Adjust system path to import utility functions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def validate_category_views_for_week(week_number: int):
    """
    Performs a data consistency test for a given week.
    It compares the total page views obtained by summing up category totals
    against the direct total sum of page views from the fact table.
    The two numbers must be equal.
    """
    print(f"--- Starting Data Validation Test for Week {week_number} ---")
    engine = get_db_engine()

    # Query 1: Get the sum of page views by first grouping by category
    query_grouped_by_category = text("""
    SELECT SUM(total_views) as grand_total
    FROM (
        SELECT
            c.category_name,
            SUM(f.screen_page_views) as total_views
        FROM fact_weekly_metrics f
        JOIN dim_categories c ON f.category_id = c.category_id
        WHERE f.week_of_year = :week
        GROUP BY c.category_name
    ) as grouped_sum;
    """)

    # Query 2: Get the direct total sum of page views from the fact table
    query_direct_total = text("""
    SELECT SUM(screen_page_views) as grand_total
    FROM fact_weekly_metrics
    WHERE week_of_year = :week;
    """)

    with engine.connect() as conn:
        # Execute first query
        result_grouped = conn.execute(query_grouped_by_category, {'week': week_number}).scalar_one_or_none()
        
        # Execute second query
        result_direct = conn.execute(query_direct_total, {'week': week_number}).scalar_one_or_none()

    print(f"Sum of views grouped by category: {result_grouped}")
    print(f"Direct sum of views from fact table: {result_direct}")

    if result_grouped is None or result_direct is None:
        print("\nResult: FAILED - Could not retrieve one or both values.")
        return

    if int(result_grouped) == int(result_direct):
        print("\nResult: PASSED - The two sums are equal. Data is consistent.")
    else:
        print(f"\nResult: FAILED - The sums do not match. Discrepancy of {abs(result_grouped - result_direct)} found.")

def validate_per_category_views(week_number: int):
    """
    Performs a granular consistency test, comparing category totals calculated in two different ways.
    This ensures the JOIN operation itself isn't causing data duplication.
    """
    print(f"\n--- Starting Granular Category Validation for Week {week_number} ---")
    engine = get_db_engine()

    # Method 1: Join first, then group by category name.
    query_join_first = text("""
    SELECT
        c.category_name,
        SUM(f.screen_page_views) as total_views
    FROM fact_weekly_metrics f
    JOIN dim_categories c ON f.category_id = c.category_id
    WHERE f.week_of_year = :week
    GROUP BY c.category_name;
    """)

    # Method 2: Group by category_id first, then join to get the name.
    query_group_first = text("""
    SELECT
        c.category_name,
        s.total_views
    FROM (
        SELECT
            category_id,
            SUM(screen_page_views) as total_views
        FROM fact_weekly_metrics
        WHERE week_of_year = :week
        GROUP BY category_id
    ) s
    JOIN dim_categories c ON s.category_id = c.category_id;
    """)

    with engine.connect() as conn:
        df_join_first = pd.read_sql_query(query_join_first, conn, params={'week': week_number})
        df_group_first = pd.read_sql_query(query_group_first, conn, params={'week': week_number})

    # Merge the two results for comparison
    comparison_df = pd.merge(
        df_join_first,
        df_group_first,
        on='category_name',
        suffixes=('_join_first', '_group_first'),
        how='outer'
    )

    # Find discrepancies
    comparison_df['discrepancy'] = (comparison_df['total_views_join_first'] != comparison_df['total_views_group_first'])
    discrepancies = comparison_df[comparison_df['discrepancy']]

    if discrepancies.empty:
        print("Result: PASSED - All category totals are consistent.")
    else:
        print("Result: FAILED - Found discrepancies in the following categories:")
        print(discrepancies)


if __name__ == "__main__":
    validate_category_views_for_week(week_number=47)
    validate_per_category_views(week_number=47)
