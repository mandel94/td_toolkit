# etl/ga4_to_articles_db.py

import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert

# Adjust system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from config import ARTICLES_DB_METRICS
from reports.map_ga4_categories import map_ga4_categories

def get_db_engine():
    """Creates and returns a SQLAlchemy engine."""
    return create_engine('postgresql://articles_user:secure_password_2025@localhost:5432/articles_db')

def extract_title_from_path(path):
    """Extracts a readable title from a URL path."""
    try:
        return path.split('/')[-1].replace('.html', '').replace('-', ' ').capitalize()
    except:
        return 'N/A'

class StarSchemaETL:
    """
    ETL process to populate a star schema from GA4 data.
    """
    def __init__(self, start_date_str, end_date_str):
        self.start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        self.end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        self.ga4_client = Ga4Client()
        self.engine = get_db_engine()
        self.property_id = "394327334"

        # Reflect tables from the database
        self.metadata = MetaData()
        self.dim_articles_table = Table('dim_articles', self.metadata, autoload_with=self.engine)
        self.dim_categories_table = Table('dim_categories', self.metadata, autoload_with=self.engine)
        self.dim_authors_table = Table('dim_authors', self.metadata, autoload_with=self.engine)
        self.dim_dates_table = Table('dim_dates', self.metadata, autoload_with=self.engine)
        self.fact_weekly_metrics_table = Table('fact_weekly_metrics', self.metadata, autoload_with=self.engine)


    def populate_dim_dates(self):
        """
        Populates the dim_dates table for the entire year of 2025.
        """
        print("Populating date dimension...")
        start_of_year = datetime(2025, 1, 1)
        end_of_year = datetime(2025, 12, 31)
        
        dates = []
        current_date = start_of_year
        while current_date <= end_of_year:
            dates.append({
                'date_id': int(current_date.strftime('%Y%m%d')),
                'full_date': current_date,
                'year': current_date.year,
                'quarter': (current_date.month - 1) // 3 + 1,
                'month': current_date.month,
                'week_of_year': current_date.isocalendar()[1],
                'day_of_month': current_date.day,
                'day_of_week': current_date.isoweekday(),
                'is_weekend': current_date.isoweekday() in [6, 7]
            })
            current_date += timedelta(days=1)
        
        df_dates = pd.DataFrame(dates)
        
        with self.engine.connect() as conn:
            # Use TRUNCATE for speed, assuming no FKs from other tables yet
            conn.execute(text("TRUNCATE TABLE dim_dates RESTART IDENTITY CASCADE;"))
            df_dates.to_sql('dim_dates', conn, if_exists='append', index=False)
            conn.commit()
        print("Date dimension populated.")

    def extract_weekly_data(self, week_start, week_end):
        """Extracts data from GA4 for a specific week."""
        print(f"Extracting data for week: {week_start} to {week_end}")
        return self.ga4_client.run_query(
            property_id=self.property_id,
            dimensions=["pagePath"],
            metrics=ARTICLES_DB_METRICS,
            start_date=week_start.strftime('%Y-%m-%d'),
            end_date=week_end.strftime('%Y-%m-%d'),
        )

    def transform_and_load(self, df, year, week_of_year):
        """Transforms weekly data and loads it into the star schema."""
        if df.empty:
            print("No data to transform.")
            return

        # 1. Rename GA4 metric columns to a consistent snake_case format.
        # The 'pagePath' column is left as-is for the PageAndScreenETL.
        df.rename(columns={
            'screenPageViews': 'screen_page_views',
            'engagedSessions': 'engaged_sessions',
            'sessions': 'sessions',
            'engagementRate': 'engagement_rate',
            'averageSessionDuration': 'average_session_duration'
        }, inplace=True)

        # 2. Run the page and screen ETL which performs filtering and cleaning.
        # This ETL expects the 'pagePath' column.
        etl = PageAndScreenETLFactory.get_etl("en", df=df)
        df_transformed = etl.run_etl()

        # 3. Now, rename 'pagePath' to 'page_path' in the transformed data.
        if 'pagePath' in df_transformed.columns:
            df_transformed.rename(columns={'pagePath': 'page_path'}, inplace=True)

        # 4. Populate Dimensions and get IDs
        self._update_dim_articles(df_transformed)
        self._update_dim_categories(df_transformed)
        
        df = self._add_dimension_ids(df_transformed)

        # 5. Prepare Fact Table Data
        df['year'] = year
        df['week_of_year'] = week_of_year
        
        fact_columns = [
            'article_id', 'author_id', 'category_id', 'year', 'week_of_year',
            'screen_page_views', 'engaged_sessions', 'sessions',
            'engagement_rate', 'average_session_duration'
        ]
        df_facts = df[fact_columns].copy()

        # 6. Load Fact Table
        self._load_facts(df_facts)

    def _update_dim_articles(self, df):
        """Upserts articles into the dim_articles table."""
        df_articles = df[['page_path']].drop_duplicates()
        df_articles['title'] = df_articles['page_path'].apply(extract_title_from_path)
        
        with self.engine.connect() as conn:
            stmt = insert(self.dim_articles_table).values(df_articles.to_dict('records'))
            stmt = stmt.on_conflict_do_nothing(index_elements=['page_path'])
            conn.execute(stmt)
            conn.commit()

    def _update_dim_categories(self, df):
        """Upserts categories into the dim_categories table."""
        df['category_name'] = df['page_path'].apply(map_ga4_categories)
        df_categories = df[['category_name']].drop_duplicates().dropna()

        with self.engine.connect() as conn:
            stmt = insert(self.dim_categories_table).values(df_categories.to_dict('records'))
            stmt = stmt.on_conflict_do_nothing(index_elements=['category_name'])
            conn.execute(stmt)
            conn.commit()

    def _add_dimension_ids(self, df):
        """Merges dimension IDs back into the main DataFrame."""
        with self.engine.connect() as conn:
            articles = pd.read_sql_table('dim_articles', conn, columns=['article_id', 'page_path'])
            categories = pd.read_sql_table('dim_categories', conn, columns=['category_id', 'category_name'])
            authors = pd.read_sql_table('dim_authors', conn, columns=['author_id', 'author_name'])

        df = pd.merge(df, articles, on='page_path', how='left')
        df = pd.merge(df, categories, on='category_name', how='left')
        
        # Assign default 'N/A' author
        na_author_id = authors[authors['author_name'] == 'N/A']['author_id'].iloc[0]
        df['author_id'] = na_author_id
        
        # Handle uncategorized
        uncategorized_id = categories[categories['category_name'] == 'Uncategorized']['category_id'].iloc[0]
        df['category_id'].fillna(uncategorized_id, inplace=True)

        return df

    def _load_facts(self, df_facts):
        """Upserts weekly metrics into the fact table."""
        with self.engine.connect() as conn:
            records = df_facts.to_dict('records')
            stmt = insert(self.fact_weekly_metrics_table).values(records)
            
            update_cols = {
                col: stmt.excluded[col] for col in df_facts.columns if col not in ['article_id', 'year', 'week_of_year']
            }
            
            stmt = stmt.on_conflict_do_update(
                index_elements=['article_id', 'year', 'week_of_year'],
                set_=update_cols
            )
            conn.execute(stmt)
            conn.commit()
        print(f"Successfully loaded {len(df_facts)} records into fact_weekly_metrics.")

    def run(self):
        """Orchestrates the full ETL process."""
        self.populate_dim_dates()
        
        # This always returns Mondays
        current_week_start = self.start_date - timedelta(days=self.start_date.weekday())
        
        while current_week_start <= self.end_date:
            current_week_end = current_week_start + timedelta(days=6)
            year, week_num, _ = current_week_start.isocalendar()

            df_weekly = self.extract_weekly_data(current_week_start, current_week_end)
            self.transform_and_load(df_weekly, year, week_num)
            
            current_week_start += timedelta(weeks=1)

if __name__ == "__main__":
    # For a full run from the beginning of the year to today
    start_date = "2025-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    # For testing the first week of 2025
    # start_date = "2025-01-01"
    # end_date = "2025-01-05"

    etl = StarSchemaETL(start_date, end_date)
    etl.run()
