"""
Content Score Validators
========================

This module provides validation and anomaly detection for content scoring.
Ensures data quality and identifies statistical anomalies.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

from .config import ContentScoringConfig, DEFAULT_CONFIG


logger = logging.getLogger(__name__)


class ContentScoreValidator:
    """
    Validate content scores and detect anomalies.
    
    This class provides comprehensive validation including:
    - Data quality checks
    - Statistical anomaly detection
    - Score consistency validation
    - Significance testing
    
    Example:
        >>> validator = ContentScoreValidator()
        >>> is_valid, issues = validator.validate(scored_df)
        >>> if not is_valid:
        ...     print("Issues found:", issues)
    """
    
    def __init__(self, config: Optional[ContentScoringConfig] = None):
        """
        Initialize the validator.
        
        Args:
            config: Configuration object. If None, uses default configuration.
        """
        self.config = config or DEFAULT_CONFIG
        logger.info("ContentScoreValidator initialized")
    
    def validate(
        self,
        df: pd.DataFrame,
        strict: bool = False
    ) -> Tuple[bool, List[Dict]]:
        """
        Perform comprehensive validation on scored data.
        
        Args:
            df: DataFrame with content scores
            strict: If True, raises exceptions on validation failures
            
        Returns:
            Tuple of (is_valid, list of issues)
            
        Raises:
            ValueError: If strict=True and validation fails
        """
        issues = []
        
        # Check 1: Required columns exist
        issues.extend(self._check_required_columns(df))
        
        # Check 2: Data types are correct
        issues.extend(self._check_data_types(df))
        
        # Check 3: Value ranges are valid
        issues.extend(self._check_value_ranges(df))
        
        # Check 4: Statistical anomalies
        issues.extend(self._check_statistical_anomalies(df))
        
        # Check 5: Score consistency
        issues.extend(self._check_score_consistency(df))
        
        # Check 6: Significance issues
        issues.extend(self._check_significance(df))
        
        is_valid = len(issues) == 0
        
        if not is_valid:
            logger.warning(f"Validation found {len(issues)} issues")
            for issue in issues:
                logger.warning(f"  - {issue['type']}: {issue['message']}")
            
            if strict:
                raise ValueError(
                    f"Validation failed with {len(issues)} issues. "
                    f"First issue: {issues[0]}"
                )
        else:
            logger.info("Validation passed successfully")
        
        return is_valid, issues
    
    def _check_required_columns(self, df: pd.DataFrame) -> List[Dict]:
        """Check that all required columns are present."""
        issues = []
        
        required_cols = [
            self.config.score_column_name,
            self.config.get_metric_name('views'),
            self.config.get_metric_name('engagement_rate'),
            self.config.get_metric_name('session_duration')
        ]
        
        for col in required_cols:
            if col not in df.columns:
                issues.append({
                    'type': 'MissingColumn',
                    'severity': 'ERROR',
                    'column': col,
                    'message': f"Required column '{col}' not found in DataFrame"
                })
        
        return issues
    
    def _check_data_types(self, df: pd.DataFrame) -> List[Dict]:
        """Check that columns have appropriate data types."""
        issues = []
        
        numeric_cols = [
            self.config.score_column_name,
            self.config.get_metric_name('views'),
            self.config.get_metric_name('engagement_rate'),
            self.config.get_metric_name('session_duration')
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append({
                        'type': 'InvalidDataType',
                        'severity': 'ERROR',
                        'column': col,
                        'expected': 'numeric',
                        'actual': str(df[col].dtype),
                        'message': f"Column '{col}' should be numeric but is {df[col].dtype}"
                    })
        
        return issues
    
    def _check_value_ranges(self, df: pd.DataFrame) -> List[Dict]:
        """Check that values are within expected ranges."""
        issues = []
        
        # Score should be in range [0, 100]
        score_col = self.config.score_column_name
        if score_col in df.columns:
            out_of_range = df[(df[score_col] < 0) | (df[score_col] > 100)]
            
            if len(out_of_range) > 0:
                issues.append({
                    'type': 'ValueOutOfRange',
                    'severity': 'ERROR',
                    'column': score_col,
                    'expected_range': (0, 100),
                    'count': len(out_of_range),
                    'examples': out_of_range[score_col].head().tolist(),
                    'message': f"{len(out_of_range)} scores outside range [0, 100]"
                })
        
        # Engagement rate should be [0, 1]
        engagement_col = self.config.get_metric_name('engagement_rate')
        if engagement_col in df.columns:
            out_of_range = df[(df[engagement_col] < 0) | (df[engagement_col] > 1)]
            
            if len(out_of_range) > 0:
                issues.append({
                    'type': 'ValueOutOfRange',
                    'severity': 'WARNING',
                    'column': engagement_col,
                    'expected_range': (0, 1),
                    'count': len(out_of_range),
                    'message': f"{len(out_of_range)} engagement rates outside [0, 1]"
                })
        
        # Views should be non-negative
        views_col = self.config.get_metric_name('views')
        if views_col in df.columns:
            negative_views = df[df[views_col] < 0]
            
            if len(negative_views) > 0:
                issues.append({
                    'type': 'NegativeValue',
                    'severity': 'ERROR',
                    'column': views_col,
                    'count': len(negative_views),
                    'message': f"{len(negative_views)} articles have negative view counts"
                })
        
        return issues
    
    def _check_statistical_anomalies(self, df: pd.DataFrame) -> List[Dict]:
        """Detect statistical anomalies in the data."""
        issues = []
        
        score_col = self.config.score_column_name
        if score_col not in df.columns:
            return issues
        
        # Check for extreme outliers using IQR method
        q1 = df[score_col].quantile(0.25)
        q3 = df[score_col].quantile(0.75)
        iqr = q3 - q1
        
        lower_bound = q1 - 3 * iqr
        upper_bound = q3 + 3 * iqr
        
        outliers = df[(df[score_col] < lower_bound) | (df[score_col] > upper_bound)]
        
        if len(outliers) > 0:
            issues.append({
                'type': 'StatisticalOutlier',
                'severity': 'INFO',
                'column': score_col,
                'count': len(outliers),
                'percentage': round(len(outliers) / len(df) * 100, 2),
                'bounds': (lower_bound, upper_bound),
                'message': f"{len(outliers)} ({len(outliers)/len(df)*100:.1f}%) scores are statistical outliers"
            })
        
        return issues
    
    def _check_score_consistency(self, df: pd.DataFrame) -> List[Dict]:
        """Check for logical inconsistencies in scores."""
        issues = []
        
        score_col = self.config.score_column_name
        views_col = self.config.get_metric_name('views')
        engagement_col = self.config.get_metric_name('engagement_rate')
        
        if not all(col in df.columns for col in [score_col, views_col, engagement_col]):
            return issues
        
        # Check: High score with very low views and engagement (percentile-based)
        views_threshold = df[views_col].quantile(self.config.low_traffic_percentile)
        engagement_threshold = df[engagement_col].quantile(0.20)
        score_threshold = df[score_col].quantile(self.config.top_performer_percentile) * 100
        
        suspicious = df[
            (df[score_col] > score_threshold) &
            (df[views_col] < views_threshold) &
            (df[engagement_col] < engagement_threshold)
        ]
        
        if len(suspicious) > 0:
            issues.append({
                'type': 'InconsistentScore',
                'severity': 'WARNING',
                'count': len(suspicious),
                'message': (
                    f"{len(suspicious)} articles have high scores (>p{self.config.top_performer_percentile:.0%}) "
                    f"but very low views (<p{self.config.low_traffic_percentile:.0%}) and engagement (<p20)"
                ),
                'examples': suspicious[['pagePath' if 'pagePath' in df.columns else score_col, 
                                       score_col, views_col, engagement_col]].head().to_dict('records')
            })
        
        return issues
    
    def _check_significance(self, df: pd.DataFrame) -> List[Dict]:
        """Check for statistically insignificant results."""
        issues = []
        
        views_col = self.config.get_metric_name('views')
        score_col = self.config.score_column_name
        
        if not all(col in df.columns for col in [views_col, score_col]):
            return issues
        
        # Articles with very few views but high scores may not be significant
        low_traffic_percentile = self.config.low_traffic_percentile
        top_score_percentile = self.config.top_performer_percentile
        
        views_threshold = df[views_col].quantile(low_traffic_percentile)
        score_threshold = df[score_col].quantile(top_score_percentile) * 100
        
        low_significance = df[
            (df[views_col] < views_threshold) &
            (df[score_col] > score_threshold)
        ]
        
        if len(low_significance) > 0:
            issues.append({
                'type': 'LowSignificance',
                'severity': 'INFO',
                'count': len(low_significance),
                'threshold': views_threshold,
                'message': (
                    f"{len(low_significance)} articles have high scores but are in the "
                    f"bottom {low_traffic_percentile:.0%} for views (may not be statistically significant)"
                )
            })
        
        return issues
    
    def get_data_quality_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate a comprehensive data quality report.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with quality metrics
        """
        report = {
            'total_rows': len(df),
            'columns': list(df.columns),
            'missing_values': {},
            'data_types': {},
            'summary_statistics': {}
        }
        
        # Missing values analysis
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                report['missing_values'][col] = {
                    'count': int(missing_count),
                    'percentage': round(missing_count / len(df) * 100, 2)
                }
        
        # Data types
        report['data_types'] = df.dtypes.astype(str).to_dict()
        
        # Summary statistics for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            stats = df[numeric_cols].describe().to_dict()
            report['summary_statistics'] = stats
        
        return report
    
    def flag_anomalies(
        self,
        df: pd.DataFrame,
        flag_column: str = 'anomaly_flag'
    ) -> pd.DataFrame:
        """
        Add anomaly flags to DataFrame.
        
        Args:
            df: Input DataFrame
            flag_column: Name of column to add with flags
            
        Returns:
            DataFrame with anomaly flags
        """
        df = df.copy()
        df[flag_column] = False
        
        score_col = self.config.score_column_name
        views_col = self.config.get_metric_name('views')
        
        if score_col in df.columns and views_col in df.columns:
            # Flag: High score with very low views (percentile-based)
            views_threshold = df[views_col].quantile(self.config.low_traffic_percentile)
            score_threshold = df[score_col].quantile(self.config.top_performer_percentile) * 100
            
            df.loc[
                (df[score_col] > score_threshold) & (df[views_col] < views_threshold),
                flag_column
            ] = True
            
            # Flag: Statistical outliers
            q1 = df[score_col].quantile(0.25)
            q3 = df[score_col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            df.loc[
                (df[score_col] < lower_bound) | (df[score_col] > upper_bound),
                flag_column
            ] = True
        
        flagged_count = df[flag_column].sum()
        logger.info(f"Flagged {flagged_count} anomalies out of {len(df)} records")
        
        return df
