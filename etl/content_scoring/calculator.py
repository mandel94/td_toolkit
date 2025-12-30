"""
Editorial Ranking Calculator
=============================

Rank-based scoring system for editorial content.
Produces stable rankings independent of absolute metric values.
"""

import pandas as pd
import numpy as np
from typing import Union, Dict, Optional
import logging
from pathlib import Path

from .config import ContentScoringConfig, DEFAULT_CONFIG
from .weighting_strategies import WeightingStrategyFactory, WeightingStrategy


logger = logging.getLogger(__name__)


class ContentScoreCalculator:
    """
    Editorial ranking calculator using percentile-based normalization.
    
    Produces stable rankings robust to outliers and batch variations.
    Features are orthogonal to avoid double counting.
    
    Features:
    - Reach: log(views) → measures audience size
    - Engagement: engagement_rate → measures content quality
    - Depth: session_duration → measures content value
    
    Example:
        calculator = ContentScoreCalculator()
        scored_df = calculator.calculate(df)
        top_articles = scored_df.nsmallest(10, 'editorial_rank')
    """
    
    def __init__(self, config: Optional[ContentScoringConfig] = None):
        """Initialize calculator with configuration.
        
        Design Pattern: Strategy Pattern
        The calculator delegates weighting logic to interchangeable
        strategy objects, allowing runtime selection of different
        weighting algorithms.
        """
        self.config = config or DEFAULT_CONFIG
        
        # Initialize weighting strategy (Strategy Pattern + Factory Pattern)
        self._initialize_strategy()
        
        logger.info("EditorialRankingCalculator initialized")
        logger.info(f"Using strategy: {self.strategy.get_name()}")
    
    def _initialize_strategy(self) -> None:
        """
        Initialize weighting strategy using Factory Pattern.
        
        Design Pattern: Factory Pattern
        Creates appropriate strategy instance based on config.strategy_name.
        For custom strategies, passes weights from config.
        """
        if self.config.strategy_name == 'custom':
            # Custom strategy with config weights
            self.strategy: WeightingStrategy = WeightingStrategyFactory.create(
                'custom',
                reach_weight=self.config.reach_weight,
                engagement_weight=self.config.engagement_weight,
                depth_weight=self.config.depth_weight,
                name=f"Custom ({self.config.reach_weight:.2f}/{self.config.engagement_weight:.2f}/{self.config.depth_weight:.2f})"
            )
        else:
            # Predefined strategy from factory
            self.strategy = WeightingStrategyFactory.create(
                self.config.strategy_name
            )
        
        logger.info(f"Weighting strategy initialized: {self.strategy.get_name()}")
        logger.info(f"Weights: {self.strategy.get_weights()}")
    
    def calculate(
        self,
        data: Union[pd.DataFrame, Dict, str, Path],
        **kwargs
    ) -> pd.DataFrame:
        """
        Calculate editorial scores and ranks.
        
        Args:
            data: Input data (DataFrame, dict, or file path)
            **kwargs: Config overrides
            
        Returns:
            DataFrame with editorial_score and editorial_rank
        """
        df = self._load_data(data)
        config = self._apply_config_overrides(kwargs)
        
        self._validate_columns(df)
        self._validate_domain(df, config)
        
        df = self._execute_ranking_pipeline(df, config)
        
        logger.info(f"Editorial ranking completed for {len(df)} articles")
        return df
    
    def _load_data(self, data: Union[pd.DataFrame, Dict, str, Path]) -> pd.DataFrame:
        """Load data from various input formats."""
        if isinstance(data, pd.DataFrame):
            return data.copy()
        elif isinstance(data, dict):
            return pd.DataFrame([data])
        elif isinstance(data, (str, Path)):
            path = Path(data)
            if not path.exists():
                raise FileNotFoundError(f"File not found: {path}")
            if path.suffix == '.csv':
                return pd.read_csv(path)
            elif path.suffix in ['.xlsx', '.xls']:
                return pd.read_excel(path)
            else:
                raise ValueError(f"Unsupported file format: {path.suffix}")
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")
    
    def _apply_config_overrides(self, kwargs: Dict) -> ContentScoringConfig:
        """Apply temporary configuration overrides."""
        if not kwargs:
            return self.config
        config_dict = self.config.to_dict()
        config_dict.update(kwargs)
        return ContentScoringConfig.from_dict(config_dict)
    
    def _validate_columns(self, df: pd.DataFrame) -> None:
        """Validate required columns exist."""
        required = [
            self.config.get_metric_name('views'),
            self.config.get_metric_name('engagement_rate'),
            self.config.get_metric_name('session_duration')
        ]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
    
    def _validate_domain(self, df: pd.DataFrame, config: ContentScoringConfig) -> None:
        """Validate metric values are within expected domains."""
        views_col = config.get_metric_name('views')
        engagement_col = config.get_metric_name('engagement_rate')
        duration_col = config.get_metric_name('session_duration')
        
        if views_col in df.columns:
            invalid = df[df[views_col] < config.views_min]
            if len(invalid) > 0:
                logger.warning(f"{len(invalid)} articles have views < {config.views_min}")
        
        if engagement_col in df.columns:
            invalid = df[(df[engagement_col] < config.engagement_min) | 
                        (df[engagement_col] > config.engagement_max)]
            if len(invalid) > 0:
                logger.warning(
                    f"{len(invalid)} articles have engagement outside "
                    f"[{config.engagement_min}, {config.engagement_max}]"
                )
        
        if duration_col in df.columns:
            invalid = df[df[duration_col] < config.duration_min]
            if len(invalid) > 0:
                logger.warning(f"{len(invalid)} articles have duration < {config.duration_min}")
    
    def _execute_ranking_pipeline(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Execute ranking pipeline."""
        logger.info("Starting editorial ranking pipeline")
        
        df = self._engineer_features(df, config)
        df = self._handle_outliers(df, config)
        df = self._rank_features(df, config)
        df = self._calculate_editorial_score(df, config)
        df = self._assign_ranks(df, config)
        
        logger.info("Ranking pipeline completed")
        return df
    
    def _engineer_features(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Engineer ranking features from raw metrics."""
        df = df.copy()
        
        views_col = config.get_metric_name('views')
        engagement_col = config.get_metric_name('engagement_rate')
        duration_col = config.get_metric_name('session_duration')
        
        # Feature 1: Reach (log-scaled views)
        if config.log_transform_views:
            df['feature_reach'] = np.log1p(df[views_col].fillna(0))
        else:
            df['feature_reach'] = df[views_col].fillna(0)
        
        # Feature 2: Engagement (direct)
        df['feature_engagement'] = df[engagement_col]
        
        # Feature 3: Depth (session duration)
        df['feature_depth'] = df[duration_col]
        
        logger.info("Features engineered: reach, engagement, depth")
        return df
    
    def _handle_outliers(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Apply winsorization to handle outliers."""
        if not config.winsorize_enabled:
            return df
        
        df = df.copy()
        features = ['feature_reach', 'feature_engagement', 'feature_depth']
        
        for feature in features:
            if feature not in df.columns:
                continue
            
            valid_mask = df[feature].notna()
            if valid_mask.sum() == 0:
                continue
            
            lower = df.loc[valid_mask, feature].quantile(config.lower_percentile)
            upper = df.loc[valid_mask, feature].quantile(config.upper_percentile)
            
            df.loc[valid_mask, feature] = df.loc[valid_mask, feature].clip(lower, upper)
        
        logger.info(
            f"Winsorization applied: "
            f"p{config.lower_percentile:.0%} - p{config.upper_percentile:.0%}"
        )
        return df
    
    def _rank_features(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Convert features to percentile ranks [0, 1]."""
        df = df.copy()
        features = ['feature_reach', 'feature_engagement', 'feature_depth']
        
        for feature in features:
            if feature not in df.columns:
                continue
            
            rank_col = f'{feature}_rank'
            
            # Handle missing: assign neutral rank (0.5)
            missing_mask = df[feature].isna()
            
            if missing_mask.all():
                df[rank_col] = config.missing_rank_percentile
                continue
            
            # Rank valid values using percentile ranking
            valid_data = df.loc[~missing_mask, feature]
            
            if len(valid_data) == 1:
                df.loc[~missing_mask, rank_col] = 0.5
            else:
                # Use rank with average method for ties, then normalize to [0, 1]
                ranks = valid_data.rank(method='average', pct=True)
                df.loc[~missing_mask, rank_col] = ranks
            
            # Assign neutral rank to missing
            df.loc[missing_mask, rank_col] = config.missing_rank_percentile
        
        logger.info("Features converted to percentile ranks")
        return df
    
    def _calculate_editorial_score(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Calculate weighted editorial score using Strategy Pattern.
        
        Design Pattern: Strategy Pattern
        The actual weighting calculation is delegated to self.strategy,
        which can be any WeightingStrategy implementation.
        This allows flexible, runtime-swappable weighting algorithms.
        """
        df = df.copy()
        
        # Apply weighting strategy to each row
        def apply_strategy(row):
            feature_ranks = {
                'reach': row['feature_reach_rank'],
                'engagement': row['feature_engagement_rank'],
                'depth': row['feature_depth_rank']
            }
            return self.strategy.apply_weights(feature_ranks)
        
        # Calculate weighted score [0, 1] using strategy
        df[config.score_column_name] = df.apply(apply_strategy, axis=1) * 100
        
        weights = self.strategy.get_weights()
        logger.info(
            f"Editorial score calculated using {self.strategy.get_name()} strategy: "
            f"reach={weights['reach']:.0%}, "
            f"engagement={weights['engagement']:.0%}, "
            f"depth={weights['depth']:.0%}"
        )
        
        # Cleanup intermediate columns if not requested
        if not config.include_feature_ranks:
            rank_cols = [col for col in df.columns if col.endswith('_rank')]
            feature_cols = [col for col in df.columns if col.startswith('feature_')]
            df = df.drop(columns=rank_cols + feature_cols, errors='ignore')
        
        return df
    
    def _assign_ranks(
        self,
        df: pd.DataFrame,
        config: ContentScoringConfig
    ) -> pd.DataFrame:
        """Assign integer ranks (1 = best)."""
        df = df.copy()
        
        # Rank by editorial_score (higher is better)
        df[config.rank_column_name] = df[config.score_column_name].rank(
            method='min',
            ascending=False
        ).astype(int)
        
        logger.info(
            f"Ranks assigned: "
            f"best={df[config.rank_column_name].min()}, "
            f"worst={df[config.rank_column_name].max()}"
        )
        
        return df
    
    def calculate_batch(
        self,
        data_list: list,
        **kwargs
    ) -> list:
        """Calculate scores for multiple datasets."""
        results = []
        for i, data in enumerate(data_list):
            logger.info(f"Processing batch {i+1}/{len(data_list)}")
            result = self.calculate(data, **kwargs)
            results.append(result)
        return results

