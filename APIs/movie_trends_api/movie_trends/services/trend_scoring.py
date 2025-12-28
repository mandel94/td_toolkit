"""Trend scoring engine with explainable metrics calculation.

This module implements the exact trend formula from the ROADMAP:

trend_score = 100 * (w1 * norm_pop_growth + w2 * norm_vote_velocity)
              * recency_factor * stability_factor

All components are designed to be:
- Explainable
- Comparable across movies
- Stable (no random spikes)
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence

import numpy as np

from movie_trends.config import get_settings
from movie_trends.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class MovieMetrics:
    """Raw metrics for a movie in a time period."""

    movie_key: int
    avg_popularity: float
    avg_vote_count: float
    avg_vote_average: float
    popularity_stddev: float | None = None


@dataclass
class TrendComponents:
    """Decomposed trend score components."""

    popularity_growth: float | None
    vote_velocity: float | None
    norm_popularity_growth: float
    norm_vote_velocity: float
    recency_factor: float
    stability_factor: float
    volatility: float | None
    trend_score: float
    trend_classification: str


def calculate_relative_growth(current: float, previous: float) -> float:
    """
    Calculate relative growth avoiding division by zero.
    
    Formula: (current - previous) / max(previous, 1)
    
    Args:
        current: Current value
        previous: Previous value
        
    Returns:
        Relative growth rate
    """
    return (current - previous) / max(previous, 1.0)


def calculate_recency_factor(release_date: date | None, current_date: date, lambda_days: int) -> float:
    """
    Calculate recency boost factor using exponential decay.
    
    Formula: exp(-days_since_release / λ)
    
    Args:
        release_date: Movie release date
        current_date: Current date
        lambda_days: Decay parameter (typically 60-90 days)
        
    Returns:
        Recency factor (0-1)
    """
    if release_date is None or release_date > current_date:
        return 1.0
    
    days_since_release = (current_date - release_date).days
    
    # Exponential decay
    factor = math.exp(-days_since_release / lambda_days)
    
    return factor


def calculate_stability_factor(volatility: float | None) -> float:
    """
    Calculate stability factor penalizing high volatility.
    
    Formula: 1 / (1 + volatility)
    
    Args:
        volatility: Standard deviation of popularity
        
    Returns:
        Stability factor (0-1)
    """
    if volatility is None or volatility == 0:
        return 1.0
    
    return 1.0 / (1.0 + volatility)


def normalize_percentile(
    value: float,
    values: Sequence[float],
    p10: float | None = None,
    p90: float | None = None,
) -> float:
    """
    Normalize value using percentile-based scaling.
    
    Formula: (value - p10) / (p90 - p10), clipped to [0, 1]
    
    This is robust to outliers compared to min-max scaling.
    
    Args:
        value: Value to normalize
        values: Population of values
        p10: 10th percentile (computed if None)
        p90: 90th percentile (computed if None)
        
    Returns:
        Normalized value in [0, 1]
    """
    if len(values) == 0:
        return 0.0
    
    arr = np.array(values)
    
    if p10 is None:
        p10 = float(np.percentile(arr, 10))
    if p90 is None:
        p90 = float(np.percentile(arr, 90))
    
    if p90 - p10 == 0:
        return 0.5
    
    normalized = (value - p10) / (p90 - p10)
    
    # Clip to [0, 1]
    return max(0.0, min(1.0, normalized))


def classify_trend(
    trend_score: float,
    previous_score: float | None = None,
) -> str:
    """
    Classify trend based on score and momentum.
    
    Rules:
    - EMERGING: score > 75 and delta > 10
    - PEAKING: score > 75 and abs(delta) < 3
    - DECLINING: score < 40 and delta < 0
    - STABLE: everything else
    
    Args:
        trend_score: Current trend score
        previous_score: Previous period's score
        
    Returns:
        Classification: EMERGING, PEAKING, STABLE, or DECLINING
    """
    if previous_score is None:
        delta = 0.0
    else:
        delta = trend_score - previous_score
    
    if trend_score > 75 and delta > 10:
        return "EMERGING"
    elif trend_score > 75 and abs(delta) < 3:
        return "PEAKING"
    elif trend_score < 40 and delta < 0:
        return "DECLINING"
    else:
        return "STABLE"


class TrendScoringEngine:
    """Engine for calculating trend scores with full explainability."""

    def __init__(self):
        """Initialize with configuration."""
        settings = get_settings()
        self.recency_lambda = settings.recency_lambda_days
        self.popularity_weight = settings.popularity_weight
        self.vote_velocity_weight = settings.vote_velocity_weight
        self.formula_version = settings.trend_formula_version

    def calculate_trend_components(
        self,
        current_metrics: MovieMetrics,
        previous_metrics: MovieMetrics | None,
        release_date: date | None,
        current_date: date,
        all_current_metrics: Sequence[MovieMetrics],
        all_previous_metrics: Sequence[MovieMetrics] | None = None,
    ) -> TrendComponents:
        """
        Calculate all trend components for a movie.
        
        Args:
            current_metrics: Current period metrics
            previous_metrics: Previous period metrics
            release_date: Movie release date
            current_date: Current date for recency calculation
            all_current_metrics: All movies' current metrics (for normalization)
            all_previous_metrics: All movies' previous metrics (for growth calculation)
            
        Returns:
            Complete trend components
        """
        # 1. Calculate popularity growth
        if previous_metrics is not None:
            popularity_growth = calculate_relative_growth(
                current_metrics.avg_popularity,
                previous_metrics.avg_popularity,
            )
            vote_velocity = calculate_relative_growth(
                current_metrics.avg_vote_count,
                previous_metrics.avg_vote_count,
            )
        else:
            popularity_growth = None
            vote_velocity = None
        
        # 2. Calculate recency factor
        recency_factor = calculate_recency_factor(
            release_date,
            current_date,
            self.recency_lambda,
        )
        
        # 3. Calculate stability factor
        stability_factor = calculate_stability_factor(current_metrics.popularity_stddev)
        
        # 4. Normalize growth metrics across population
        growth_values = []
        velocity_values = []
        
        for i, curr in enumerate(all_current_metrics):
            # Match with previous if available
            if all_previous_metrics and i < len(all_previous_metrics):
                prev = all_previous_metrics[i]
                growth_values.append(
                    calculate_relative_growth(curr.avg_popularity, prev.avg_popularity)
                )
                velocity_values.append(
                    calculate_relative_growth(curr.avg_vote_count, prev.avg_vote_count)
                )
        
        # Normalize
        if popularity_growth is not None and growth_values:
            norm_pop_growth = normalize_percentile(popularity_growth, growth_values)
        else:
            norm_pop_growth = 0.0
        
        if vote_velocity is not None and velocity_values:
            norm_vote_velocity = normalize_percentile(vote_velocity, velocity_values)
        else:
            norm_vote_velocity = 0.0
        
        # 5. Calculate final trend score
        base_score = (
            self.popularity_weight * norm_pop_growth +
            self.vote_velocity_weight * norm_vote_velocity
        )
        
        trend_score = 100 * base_score * recency_factor * stability_factor
        
        # 6. Classify trend
        trend_classification = classify_trend(trend_score)
        
        logger.debug(
            "trend_calculated",
            movie_key=current_metrics.movie_key,
            trend_score=round(trend_score, 2),
            classification=trend_classification,
            components={
                "norm_pop_growth": round(norm_pop_growth, 3),
                "norm_vote_velocity": round(norm_vote_velocity, 3),
                "recency": round(recency_factor, 3),
                "stability": round(stability_factor, 3),
            },
        )
        
        return TrendComponents(
            popularity_growth=popularity_growth,
            vote_velocity=vote_velocity,
            norm_popularity_growth=norm_pop_growth,
            norm_vote_velocity=norm_vote_velocity,
            recency_factor=recency_factor,
            stability_factor=stability_factor,
            volatility=current_metrics.popularity_stddev,
            trend_score=trend_score,
            trend_classification=trend_classification,
        )

    def calculate_batch_trends(
        self,
        current_period_metrics: Sequence[MovieMetrics],
        previous_period_metrics: Sequence[MovieMetrics] | None,
        movie_release_dates: dict[int, date | None],
        current_date: date,
    ) -> dict[int, TrendComponents]:
        """
        Calculate trends for a batch of movies.
        
        Args:
            current_period_metrics: Current period metrics for all movies
            previous_period_metrics: Previous period metrics for all movies
            movie_release_dates: Map of movie_key to release_date
            current_date: Current date
            
        Returns:
            Map of movie_key to TrendComponents
        """
        results = {}
        
        # Create lookup for previous metrics
        previous_lookup = {}
        if previous_period_metrics:
            previous_lookup = {m.movie_key: m for m in previous_period_metrics}
        
        for current_metric in current_period_metrics:
            movie_key = current_metric.movie_key
            previous_metric = previous_lookup.get(movie_key)
            release_date = movie_release_dates.get(movie_key)
            
            components = self.calculate_trend_components(
                current_metrics=current_metric,
                previous_metrics=previous_metric,
                release_date=release_date,
                current_date=current_date,
                all_current_metrics=current_period_metrics,
                all_previous_metrics=previous_period_metrics,
            )
            
            results[movie_key] = components
        
        logger.info(
            "batch_trends_calculated",
            total_movies=len(results),
            emerging=sum(1 for c in results.values() if c.trend_classification == "EMERGING"),
            peaking=sum(1 for c in results.values() if c.trend_classification == "PEAKING"),
            stable=sum(1 for c in results.values() if c.trend_classification == "STABLE"),
            declining=sum(1 for c in results.values() if c.trend_classification == "DECLINING"),
        )
        
        return results
