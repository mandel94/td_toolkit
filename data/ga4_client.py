"""
GA4 Client - Facade Pattern
Abstracts complexity of Google Analytics 4 Data API
Following 2025 best practices for API integration
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pandas as pd
import pickle
import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
)
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


class GA4ClientFacade:
    """
    Facade for GA4 Data API
    Provides simplified interface for editorial analytics needs
    """
    
    def __init__(self, property_id: str, credentials_path: str, token_pickle_path: str = None):
        """
        Initialize GA4 client with OAuth credentials
        
        Args:
            property_id: GA4 property ID
            credentials_path: Path to OAuth client secret JSON
            token_pickle_path: Path to token.pickle file (default: same directory as credentials)
        """
        self.property_id = f"properties/{property_id}"
        self.credentials_path = credentials_path
        
        # Default token pickle path is in the same directory as credentials
        if token_pickle_path is None:
            credentials_dir = os.path.dirname(credentials_path)
            self.token_pickle_path = os.path.join(credentials_dir, "token.pickle")
        else:
            self.token_pickle_path = token_pickle_path
            
        self.credentials = self._get_oauth_credentials()
        self.client = BetaAnalyticsDataClient(credentials=self.credentials)
    
    def _get_oauth_credentials(self):
        """Get or refresh OAuth2 credentials using pickle file"""
        creds = None
        
        # Load existing credentials from pickle
        if os.path.exists(self.token_pickle_path):
            with open(self.token_pickle_path, "rb") as token:
                creds = pickle.load(token)
        
        # Refresh if expired or get new credentials
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path,
                    scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )
                creds = flow.run_local_server(port=0)
            
            # Save credentials for future use
            with open(self.token_pickle_path, "wb") as token:
                pickle.dump(creds, token)
        
        return creds
    
    def fetch_page_views_trend(
        self,
        start_date: datetime,
        end_date: datetime,
        metrics: Optional[List[str]] = None,
        dimensions: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch page views trend data
        
        Args:
            start_date: Start date for the analysis
            end_date: End date for the analysis
            metrics: List of metrics to fetch (default: screenPageViews)
            dimensions: List of dimensions (default: date)
            
        Returns:
            DataFrame with requested data
        """
        if metrics is None:
            metrics = ["screenPageViews"]
        if dimensions is None:
            dimensions = ["date"]
        
        request = RunReportRequest(
            property=self.property_id,
            date_ranges=[
                DateRange(
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d")
                )
            ],
            dimensions=[Dimension(name=dim) for dim in dimensions],
            metrics=[Metric(name=metric) for metric in metrics]
        )
        
        response = self.client.run_report(request)
        return self._parse_response(response)
    
    def fetch_comparison_period(
        self,
        start_date: datetime,
        end_date: datetime,
        comparison_type: str = "WoW"
    ) -> pd.DataFrame:
        """
        Fetch data for comparison period
        
        Args:
            start_date: Start date of current period
            end_date: End date of current period
            comparison_type: Type of comparison (WoW, MoM, YoY)
            
        Returns:
            DataFrame with comparison period data
        """
        days_diff = (end_date - start_date).days
        
        if comparison_type == "WoW":
            comp_start = start_date - timedelta(days=7)
            comp_end = end_date - timedelta(days=7)
        elif comparison_type == "MoM":
            comp_start = start_date - timedelta(days=30)
            comp_end = end_date - timedelta(days=30)
        elif comparison_type == "YoY":
            comp_start = start_date - timedelta(days=365)
            comp_end = end_date - timedelta(days=365)
        else:
            raise ValueError(f"Unknown comparison type: {comparison_type}")
        
        return self.fetch_page_views_trend(comp_start, comp_end)
    
    def _parse_response(self, response) -> pd.DataFrame:
        """
        Parse GA4 API response into DataFrame
        
        Args:
            response: GA4 API response object
            
        Returns:
            Parsed DataFrame
        """
        data = []
        
        for row in response.rows:
            row_data = {}
            
            # Parse dimensions
            for i, dimension_value in enumerate(row.dimension_values):
                dimension_name = response.dimension_headers[i].name
                row_data[dimension_name] = dimension_value.value
            
            # Parse metrics
            for i, metric_value in enumerate(row.metric_values):
                metric_name = response.metric_headers[i].name
                row_data[metric_name] = float(metric_value.value)
            
            data.append(row_data)
        
        df = pd.DataFrame(data)
        
        # Convert date column if present
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        
        return df
    
    def test_connection(self) -> bool:
        """
        Test GA4 connection
        
        Returns:
            True if connection successful
        """
        try:
            # Try to fetch last 7 days of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            df = self.fetch_page_views_trend(start_date, end_date)
            return not df.empty
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
