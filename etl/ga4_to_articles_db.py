# etl/ga4_to_articles_db.py

import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# Adjust system path to include parent directories
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from ga4_api.ga4_api import Ga4Client
from etl.page_and_screen_etl import PageAndScreenETLFactory
from config import ARTICLES_DB_METRICS

class GA4ToArticlesDB:
    """
    ETL to extract data from GA4 and load it into the articles_db PostgreSQL database.
    """

    def __init__(self, property_id, reporting_date_str):
        """
        Initialize the ETL process.
        Args:
            property_id (str): GA4 property ID.
            reporting_date_str (str): Reporting date in 'YYYY-MM-DD' format.
        """
        self.property_id = property_id
        self.start_date = "2025-01-01"
        self.end_date = reporting_date_str
        self.ga4_client = Ga4Client()
        self.metrics = ARTICLES_DB_METRICS
        self.dimensions = ["pagePath"]

    def extract(self):
        """
        Extract data from Google Analytics 4.
        Returns:
            pd.DataFrame: DataFrame with GA4 data.
        """
        print(f"Extracting data from GA4 from {self.start_date} to {self.end_date}...")
        df = self.ga4_client.run_query(
            property_id=self.property_id,
            dimensions=self.dimensions,
            metrics=self.metrics,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        print(f"Successfully extracted {len(df)} records from GA4.")
        return df

    def transform(self, df):
        """
        Transform the GA4 data to match the articles_db schema.
        Args:
            df (pd.DataFrame): Raw data from GA4.
        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        print("Transforming data...")
        # Use the existing ETL logic for initial cleaning
        etl = PageAndScreenETLFactory.get_etl("en", df=df)
        df = etl.run_etl()

        # Rename columns to match the database schema
        column_mapping = {
            "pagePath": "page_path",
            "screenPageViews": "screen_page_views",
            "engagedSessions": "engaged_sessions",
            "sessions": "sessions",
            "engagementRate": "engagement_rate",
            "averageSessionDuration": "average_session_duration",
        }
        df.rename(columns=column_mapping, inplace=True)

        # Add placeholder columns for now
        df["title"] = "N/A"
        df["author"] = "N/A"
        df["publication_date"] = None
        df["categoria"] = None # Or apply category mapping if available

        # Ensure all required columns are present
        required_cols = [
            "page_path", "title", "author", "categoria", "screen_page_views",
            "engaged_sessions", "sessions", "engagement_rate", "average_session_duration",
            "publication_date"
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        return df[required_cols]

    def load(self, df):
        """
        Load the transformed data into the PostgreSQL database.
        (This is a placeholder for the actual database loading logic)
        """
        print("Loading data into articles_db...")
        # Here you would implement the logic to connect to PostgreSQL
        # and insert/update the data in the 'articles' table.
        # For now, we'll just print the DataFrame.
        print(df.head())
        print(f"Successfully prepared {len(df)} records for loading.")
        # Example of what the loading logic would look like:
        # from sqlalchemy import create_engine
        # engine = create_engine('postgresql://user:password@host:port/dbname')
        # df.to_sql('articles', engine, if_exists='append', index=False)

    def run(self):
        """
        Run the full ETL pipeline.
        """
        raw_data = self.extract()
        if not raw_data.empty:
            transformed_data = self.transform(raw_data)
            self.load(transformed_data)
        else:
            print("No data extracted from GA4. ETL process finished.")

if __name__ == "__main__":
    # Example usage:
    # The reporting date can be passed as a command-line argument
    # or set to a default (e.g., today).
    if len(sys.argv) > 1:
        reporting_date = sys.argv[1]
    else:
        reporting_date = datetime.now().strftime("%Y-%m-%d")

    etl_process = GA4ToArticlesDB(
        property_id="394327334",
        reporting_date_str=reporting_date
    )
    etl_process.run()
