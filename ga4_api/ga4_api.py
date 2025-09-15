from google_auth_oauthlib.flow import InstalledAppFlow
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.auth.transport.requests import Request
import pandas as pd
import pickle
import os

# List of ga4 metrics and dimensions
# https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema?hl=it#metrics

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
TOKEN_PICKLE = "token.pickle"

class Ga4Client:
    """
    Google Analytics 4 API client for authenticated data access.
    Handles OAuth2 authentication and provides a method to run GA4 queries.
    """
    def __init__(self, credentials_file=None, token_pickle=None, scopes=None):
        # Default: credentials file in the same directory as this script
        default_credentials_path = os.path.join(os.path.dirname(__file__), "client_secret_722854453271-t3dg269vqsvjjhbmpkh2a5etk0mmf6ve.apps.googleusercontent.com.json")
        self.credentials_file = credentials_file or default_credentials_path
        self.token_pickle = token_pickle or TOKEN_PICKLE
        self.scopes = scopes or SCOPES
        self.credentials = self._get_oauth_credentials()
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)

    def _get_oauth_credentials(self):
        creds = None
        if os.path.exists(self.token_pickle):
            with open(self.token_pickle, "rb") as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                creds = flow.run_local_server(port=0)
            with open(self.token_pickle, "wb") as token:
                pickle.dump(creds, token)
        return creds

    def run_query(self, property_id, dimensions=None, metrics=None, start_date=None, end_date=None):
        """
        Run a GA4 query and return the results as a pandas DataFrame.
        Args:
            property_id (str): GA4 property ID.
            dimensions (list, optional): List of dimension names. If None, no dimensions are used.
            metrics (list): List of metric names.
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str): End date in 'YYYY-MM-DD' format.
        Returns:
            pd.DataFrame: DataFrame with the query results.
        """
        from google.api_core.exceptions import DeadlineExceeded
        import time
        if metrics is None:
            raise ValueError("metrics must be provided")
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in dimensions] if dimensions else [],
            metrics=[Metric(name=m) for m in metrics],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)]
        )
        while True:
            try:
                response = self.client.run_report(request)
                rows = []
                for row in response.rows:
                    row_dict = {}
                    for d, v in zip(response.dimension_headers, row.dimension_values):
                        row_dict[d.name] = v.value
                    for m, v in zip(response.metric_headers, row.metric_values):
                        row_dict[m.name] = v.value
                    rows.append(row_dict)
                df = pd.DataFrame(rows)
                return df
            except DeadlineExceeded:
                print("GA4 API timeout occurred, retrying in 2 seconds...")
                time.sleep(2)
                continue
            except Exception as e:
                print(f"Error running GA4 query: {e}")
                time.sleep(2)
                return pd.DataFrame([])

# Example usage
if __name__ == "__main__":
    # Provide a specific path for the credentials file
    credentials_path = os.path.join(os.path.dirname(__file__), "client_secret_722854453271-t3dg269vqsvjjhbmpkh2a5etk0mmf6ve.apps.googleusercontent.com.json")
    ga4 = Ga4Client(credentials_file=credentials_path)
    df = ga4.run_query(
        property_id='394327334',
        dimensions=['pagePath'],
        metrics=['screenPageViews', 'activeUsers'],
        start_date='2024-06-01',
        end_date='2024-06-30'
    )
    print(df.head())
