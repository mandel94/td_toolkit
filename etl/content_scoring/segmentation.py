"""
Content Score Segmentation
===========================

This module provides functionality to segment and categorize articles
based on their content scores and engagement patterns.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Tuple
import logging

from .config import ContentScoringConfig, DEFAULT_CONFIG


logger = logging.getLogger(__name__)


class ContentScoreSegmentation:
    """
    Segment and categorize articles based on content scores.
    
    This class provides methods to automatically classify articles into
    meaningful segments that can inform editorial strategy:
    
    - Top Performer: High score across all metrics
    - Niche Value: High engagement but lower traffic
    - Underperforming: Needs optimization
    - Rising Star: Growing engagement trends
    - Standard: Average performance
    
    Example:
        >>> segmenter = ContentScoreSegmentation()
        >>> segmented_df = segmenter.segment(scored_df)
        >>> print(segmented_df['content_segment'].value_counts())
    """
    
    def __init__(self, config: Optional[ContentScoringConfig] = None):
        """
        Initialize the segmentation module.
        
        Args:
            config: Configuration object. If None, uses default configuration.
        """
        self.config = config or DEFAULT_CONFIG
        logger.info("ContentScoreSegmentation initialized")
    
    def segment(
        self,
        df: pd.DataFrame,
        score_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Apply segmentation to scored articles.
        
        Args:
            df: DataFrame with content scores
            score_column: Name of score column (uses config default if None)
            
        Returns:
            DataFrame with segment column added
            
        Raises:
            ValueError: If score column not found
        """
        df = df.copy()
        
        score_col = score_column or self.config.score_column_name
        
        if score_col not in df.columns:
            raise ValueError(
                f"Score column '{score_col}' not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )
        
        # Apply segmentation logic
        df[self.config.segment_column_name] = df.apply(
            lambda row: self._classify_article(row, score_col),
            axis=1
        )
        
        # Add segment rank (priority for editorial decisions)
        df['segment_priority'] = df[self.config.segment_column_name].map(
            self._get_segment_priority()
        )
        
        logger.info(f"Segmentation completed: {df[self.config.segment_column_name].value_counts().to_dict()}")
        
        return df
    
    def _classify_article(self, row: pd.Series, score_col: str) -> str:
        """
        Classify a single article into a segment using percentile-based thresholds.
        
        Args:
            row: DataFrame row
            score_col: Score column name
            
        Returns:
            Segment name
        """
        score = row[score_col]
        views = row.get(self.config.get_metric_name('views'), 0)
        engagement = row.get(self.config.get_metric_name('engagement_rate'), 0)
        
        # Calculate dynamic thresholds from dataframe if available
        # For single row classification, use config thresholds
        score_top_threshold = self.config.top_performer_percentile * 100
        score_bottom_threshold = self.config.underperforming_percentile * 100
        engagement_high_threshold = self.config.high_engagement_percentile
        
        # Top Performer: High score overall
        if score >= score_top_threshold:
            return "Top Performer"
        
        # Niche Value: High engagement regardless of traffic
        if engagement >= engagement_high_threshold:
            return "Niche Value"
        
        # Underperforming: Low score and engagement
        if score < score_bottom_threshold and engagement < 0.25:
            return "Underperforming"
        
        # Rising Star: Good score with decent engagement
        if score >= 60 and engagement >= 0.35:
            return "Rising Star"
        
        # Standard: Everything else
        return "Standard"
    
    def _get_segment_priority(self) -> Dict[str, int]:
        """
        Get priority ranking for segments (lower = higher priority).
        
        Returns:
            Dictionary mapping segment names to priority values
        """
        return {
            "Top Performer": 1,
            "Rising Star": 2,
            "Niche Value": 3,
            "Standard": 4,
            "Underperforming": 5
        }
    
    def get_segment_statistics(
        self,
        df: pd.DataFrame,
        segment_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Calculate statistics for each segment.
        
        Args:
            df: Segmented DataFrame
            segment_column: Name of segment column
            
        Returns:
            DataFrame with segment statistics
        """
        segment_col = segment_column or self.config.segment_column_name
        
        if segment_col not in df.columns:
            raise ValueError(f"Segment column '{segment_col}' not found")
        
        stats = df.groupby(segment_col).agg({
            self.config.get_metric_name('views'): ['count', 'mean', 'sum'],
            self.config.get_metric_name('engagement_rate'): 'mean',
            self.config.score_column_name: ['mean', 'min', 'max']
        }).round(2)
        
        stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
        stats = stats.reset_index()
        
        return stats
    
    def get_recommendations(
        self,
        df: pd.DataFrame,
        segment: str,
        n_articles: int = 5
    ) -> List[Dict]:
        """
        Get actionable recommendations for a specific segment.
        
        Args:
            df: Segmented DataFrame
            segment: Segment name
            n_articles: Number of example articles to include
            
        Returns:
            List of recommendation dictionaries
        """
        segment_col = self.config.segment_column_name
        
        if segment_col not in df.columns:
            raise ValueError(f"Segment column '{segment_col}' not found")
        
        segment_df = df[df[segment_col] == segment]
        
        if len(segment_df) == 0:
            return []
        
        recommendations = {
            "Top Performer": self._recommendations_top_performer,
            "Niche Value": self._recommendations_niche_value,
            "Underperforming": self._recommendations_underperforming,
            "Rising Star": self._recommendations_rising_star,
            "Standard": self._recommendations_standard
        }
        
        return recommendations.get(segment, lambda df, n: [])(segment_df, n_articles)
    
    def _recommendations_top_performer(
        self,
        df: pd.DataFrame,
        n: int
    ) -> List[Dict]:
        """Generate recommendations for Top Performer articles."""
        top_articles = df.nlargest(n, self.config.score_column_name)
        
        return [{
            'segment': 'Top Performer',
            'strategy': 'Amplify and Replicate',
            'actions': [
                'Promote these articles across all channels',
                'Analyze content patterns for future articles',
                'Consider creating series or follow-ups',
                'Use as templates for editorial guidelines'
            ],
            'examples': top_articles[[
                'title' if 'title' in df.columns else 'pagePath',
                self.config.score_column_name
            ]].to_dict('records') if n > 0 else []
        }]
    
    def _recommendations_niche_value(
        self,
        df: pd.DataFrame,
        n: int
    ) -> List[Dict]:
        """Generate recommendations for Niche Value articles."""
        top_engagement = df.nlargest(n, self.config.get_metric_name('engagement_rate'))
        
        return [{
            'segment': 'Niche Value',
            'strategy': 'Expand Reach',
            'actions': [
                'Increase promotional budget for these topics',
                'Optimize SEO for broader discoverability',
                'Consider social media advertising',
                'Build email campaigns around these topics'
            ],
            'examples': top_engagement[[
                'title' if 'title' in df.columns else 'pagePath',
                self.config.get_metric_name('engagement_rate')
            ]].to_dict('records') if n > 0 else []
        }]
    
    def _recommendations_underperforming(
        self,
        df: pd.DataFrame,
        n: int
    ) -> List[Dict]:
        """Generate recommendations for Underperforming articles."""
        worst_performers = df.nsmallest(n, self.config.score_column_name)
        
        return [{
            'segment': 'Underperforming',
            'strategy': 'Optimize or Redirect',
            'actions': [
                'Review and improve content quality',
                'Optimize page load speed and UX',
                'Add relevant internal links',
                'Consider content refresh or archive',
                'Check for misleading headlines or metadata'
            ],
            'examples': worst_performers[[
                'title' if 'title' in df.columns else 'pagePath',
                self.config.score_column_name,
                self.config.get_metric_name('engagement_rate')
            ]].to_dict('records') if n > 0 else []
        }]
    
    def _recommendations_rising_star(
        self,
        df: pd.DataFrame,
        n: int
    ) -> List[Dict]:
        """Generate recommendations for Rising Star articles."""
        top_balanced = df.nlargest(n, self.config.score_column_name)
        
        return [{
            'segment': 'Rising Star',
            'strategy': 'Nurture Growth',
            'actions': [
                'Increase editorial support for these topics',
                'Create content series to build momentum',
                'Engage community around these themes',
                'Monitor performance for promotion to Top Performer'
            ],
            'examples': top_balanced[[
                'title' if 'title' in df.columns else 'pagePath',
                self.config.score_column_name
            ]].to_dict('records') if n > 0 else []
        }]
    
    def _recommendations_standard(
        self,
        df: pd.DataFrame,
        n: int
    ) -> List[Dict]:
        """Generate recommendations for Standard articles."""
        return [{
            'segment': 'Standard',
            'strategy': 'Maintain and Monitor',
            'actions': [
                'Continue regular publishing cadence',
                'Look for opportunities to elevate performance',
                'Test different promotion strategies',
                'Monitor for emerging trends'
            ],
            'examples': []
        }]
    
    def create_segment_report(
        self,
        df: pd.DataFrame,
        output_path: Optional[str] = None
    ) -> Dict:
        """
        Create a comprehensive segmentation report.
        
        Args:
            df: Segmented DataFrame
            output_path: Optional path to save report
            
        Returns:
            Dictionary with complete report data
        """
        segment_col = self.config.segment_column_name
        
        if segment_col not in df.columns:
            raise ValueError(f"Segment column '{segment_col}' not found")
        
        report = {
            'summary': {
                'total_articles': len(df),
                'segments': df[segment_col].value_counts().to_dict(),
                'average_score': df[self.config.score_column_name].mean(),
                'median_score': df[self.config.score_column_name].median()
            },
            'statistics': self.get_segment_statistics(df).to_dict('records'),
            'recommendations': {}
        }
        
        # Add recommendations for each segment
        for segment in df[segment_col].unique():
            recs = self.get_recommendations(df, segment, n_articles=3)
            if recs:
                report['recommendations'][segment] = recs[0]
        
        # Save to file if requested
        if output_path:
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info(f"Segment report saved to {output_path}")
        
        return report
