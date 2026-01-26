"""
Performance Analytics Module for Taxi Drivers Website

This module provides comprehensive website performance analysis using GA4 data,
including historical comparisons and professional reporting capabilities.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd

# Add parent directories to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from ga4_api.ga4_api import Ga4Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceMetricsExtractor:
    """
    Extracts key performance metrics from GA4 for website performance analysis.
    
    Follows data engineering best practices:
    - Separation of concerns
    - Error handling and validation
    - Comprehensive logging
    - Type hints for maintainability
    """
    
    def __init__(self, ga4_client: Ga4Client, property_id: str = "394327334"):
        """
        Initialize the metrics extractor.
        
        Args:
            ga4_client: Configured GA4 client instance
            property_id: GA4 property ID for Taxi Drivers website
        """
        self.ga4_client = ga4_client
        self.property_id = property_id
        
        # Define comprehensive metrics set for website performance analysis
        self.key_metrics = [
            'screenPageViews',           # Total page views
            'activeUsers',               # Active users
            'newUsers',                  # New users  
            'sessions',                  # Total sessions
            'bounceRate',               # Bounce rate
            'averageSessionDuration',    # Average session duration
            'engagementRate',           # Engagement rate
            'eventCount',               # Total events
            'conversions',              # Conversions
            'totalRevenue'              # Revenue (if applicable)
        ]
        
        # Additional dimensions for detailed analysis
        self.dimensions = [
            'date',
            'deviceCategory', 
            'channelGrouping',
            'country'
        ]
        
        logger.info(f"Initialized PerformanceMetricsExtractor for property {property_id}")
    
    def extract_period_metrics(
        self, 
        start_date: str, 
        end_date: str,
        include_dimensions: bool = True
    ) -> Dict[str, Any]:
        """
        Extract comprehensive metrics for a specific period.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            include_dimensions: Whether to include dimensional breakdowns
            
        Returns:
            Dictionary containing all extracted metrics and metadata
        """
        logger.info(f"Extracting metrics for period {start_date} to {end_date}")
        
        try:
            # Validate date format
            self._validate_dates(start_date, end_date)
            
            # Extract main metrics
            main_metrics = self._extract_main_metrics(start_date, end_date)
            
            # Extract dimensional data if requested
            dimensional_data = {}
            if include_dimensions:
                dimensional_data = self._extract_dimensional_data(start_date, end_date)
            
            # Calculate derived metrics
            derived_metrics = self._calculate_derived_metrics(main_metrics)
            
            # Compile complete dataset
            result = {
                'period': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'duration_days': self._calculate_days_between(start_date, end_date)
                },
                'extraction_timestamp': datetime.now().isoformat(),
                'main_metrics': main_metrics,
                'derived_metrics': derived_metrics,
                'dimensional_data': dimensional_data,
                'data_quality': self._assess_data_quality(main_metrics)
            }
            
            logger.info(f"Successfully extracted metrics for period {start_date} to {end_date}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting metrics for period {start_date} to {end_date}: {str(e)}")
            raise
    
    def _extract_main_metrics(self, start_date: str, end_date: str) -> Dict[str, float]:
        """Extract main website performance metrics."""
        try:
            # Query GA4 for main metrics
            df = self.ga4_client.run_query(
                property_id=self.property_id,
                dimensions=['date'],
                metrics=self.key_metrics,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"No data returned for period {start_date} to {end_date}")
                return {metric: 0.0 for metric in self.key_metrics}
            
            # Aggregate metrics across the period
            metrics_summary = {}
            for metric in self.key_metrics:
                if metric in df.columns:
                    if metric in ['bounceRate', 'engagementRate', 'averageSessionDuration']:
                        # For rate/average metrics, calculate weighted average
                        metrics_summary[metric] = self._calculate_weighted_average(df, metric)
                    else:
                        # For count metrics, sum across period
                        metrics_summary[metric] = float(df[metric].sum())
                else:
                    logger.warning(f"Metric {metric} not found in GA4 response")
                    metrics_summary[metric] = 0.0
            
            return metrics_summary
            
        except Exception as e:
            logger.error(f"Error extracting main metrics: {str(e)}")
            raise
    
    def _extract_dimensional_data(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Extract dimensional breakdown data."""
        dimensional_data = {}
        
        try:
            # Device breakdown
            device_df = self.ga4_client.run_query(
                property_id=self.property_id,
                dimensions=['deviceCategory'],
                metrics=['screenPageViews', 'activeUsers', 'sessions'],
                start_date=start_date,
                end_date=end_date
            )
            
            if not device_df.empty:
                dimensional_data['by_device'] = device_df.to_dict('records')
            
            # Channel breakdown
            channel_df = self.ga4_client.run_query(
                property_id=self.property_id,
                dimensions=['channelGrouping'],
                metrics=['screenPageViews', 'activeUsers', 'sessions'],
                start_date=start_date,
                end_date=end_date
            )
            
            if not channel_df.empty:
                dimensional_data['by_channel'] = channel_df.to_dict('records')
            
            # Geographic breakdown (top countries)
            geo_df = self.ga4_client.run_query(
                property_id=self.property_id,
                dimensions=['country'],
                metrics=['screenPageViews', 'activeUsers'],
                start_date=start_date,
                end_date=end_date
            )
            
            if not geo_df.empty:
                # Get top 10 countries by page views
                top_countries = geo_df.nlargest(10, 'screenPageViews')
                dimensional_data['by_geography'] = top_countries.to_dict('records')
            
            return dimensional_data
            
        except Exception as e:
            logger.error(f"Error extracting dimensional data: {str(e)}")
            return {}
    
    def _calculate_derived_metrics(self, main_metrics: Dict[str, float]) -> Dict[str, float]:
        """Calculate additional derived metrics from main metrics."""
        derived = {}
        
        try:
            # Pages per session
            if main_metrics['sessions'] > 0:
                derived['pages_per_session'] = main_metrics['screenPageViews'] / main_metrics['sessions']
            else:
                derived['pages_per_session'] = 0.0
            
            # New user rate
            if main_metrics['activeUsers'] > 0:
                derived['new_user_rate'] = main_metrics['newUsers'] / main_metrics['activeUsers']
            else:
                derived['new_user_rate'] = 0.0
            
            # Events per session
            if main_metrics['sessions'] > 0:
                derived['events_per_session'] = main_metrics['eventCount'] / main_metrics['sessions']
            else:
                derived['events_per_session'] = 0.0
            
            # Return user percentage
            returning_users = main_metrics['activeUsers'] - main_metrics['newUsers']
            if main_metrics['activeUsers'] > 0:
                derived['returning_user_rate'] = returning_users / main_metrics['activeUsers']
            else:
                derived['returning_user_rate'] = 0.0
                
            return derived
            
        except Exception as e:
            logger.error(f"Error calculating derived metrics: {str(e)}")
            return {}
    
    def _calculate_weighted_average(self, df: pd.DataFrame, metric: str) -> float:
        """Calculate weighted average for rate/percentage metrics."""
        try:
            if 'sessions' in df.columns and df['sessions'].sum() > 0:
                # Weight by sessions for most accurate average
                weights = df['sessions']
                values = df[metric]
                return float((values * weights).sum() / weights.sum())
            else:
                # Simple average if no session data
                return float(df[metric].mean())
        except:
            return 0.0
    
    def _validate_dates(self, start_date: str, end_date: str) -> None:
        """Validate date format and logical consistency."""
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if start_dt > end_dt:
                raise ValueError("Start date must be before end date")
            
            if start_dt < date(2025, 1, 1):
                raise ValueError("Start date cannot be before January 1, 2025")
                
            if end_dt > date.today():
                raise ValueError("End date cannot be in the future")
                
        except ValueError as e:
            logger.error(f"Date validation error: {str(e)}")
            raise
    
    def _calculate_days_between(self, start_date: str, end_date: str) -> int:
        """Calculate number of days between two dates."""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
        return (end_dt - start_dt).days + 1  # Include both start and end dates
    
    def _assess_data_quality(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Assess data quality and completeness."""
        quality_assessment = {
            'completeness_score': 0.0,
            'missing_metrics': [],
            'suspicious_values': [],
            'overall_quality': 'unknown'
        }
        
        try:
            total_metrics = len(self.key_metrics)
            complete_metrics = sum(1 for metric in self.key_metrics if metrics.get(metric, 0) > 0)
            
            quality_assessment['completeness_score'] = complete_metrics / total_metrics
            quality_assessment['missing_metrics'] = [
                metric for metric in self.key_metrics if metrics.get(metric, 0) == 0
            ]
            
            # Check for suspicious values
            if metrics.get('bounceRate', 0) > 1.0:
                quality_assessment['suspicious_values'].append('bounceRate > 100%')
            
            if metrics.get('engagementRate', 0) > 1.0:
                quality_assessment['suspicious_values'].append('engagementRate > 100%')
            
            # Overall quality assessment
            if quality_assessment['completeness_score'] >= 0.9:
                quality_assessment['overall_quality'] = 'excellent'
            elif quality_assessment['completeness_score'] >= 0.7:
                quality_assessment['overall_quality'] = 'good'
            elif quality_assessment['completeness_score'] >= 0.5:
                quality_assessment['overall_quality'] = 'fair'
            else:
                quality_assessment['overall_quality'] = 'poor'
            
            return quality_assessment
            
        except Exception as e:
            logger.error(f"Error assessing data quality: {str(e)}")
            return quality_assessment