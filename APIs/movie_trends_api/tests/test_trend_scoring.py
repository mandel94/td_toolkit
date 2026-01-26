"""Tests for trend scoring engine."""

from datetime import date, timedelta

import pytest

from movie_trends.services.trend_scoring import (
    MovieMetrics,
    TrendScoringEngine,
    calculate_recency_factor,
    calculate_relative_growth,
    calculate_stability_factor,
    classify_trend,
    normalize_percentile,
)


class TestTrendScoringFunctions:
    """Test individual scoring functions."""

    def test_calculate_relative_growth(self):
        """Test relative growth calculation."""
        assert calculate_relative_growth(150, 100) == pytest.approx(0.5)
        assert calculate_relative_growth(100, 200) == pytest.approx(-0.5)
        assert calculate_relative_growth(100, 0) == pytest.approx(100.0)
        assert calculate_relative_growth(0, 100) == pytest.approx(-1.0)

    def test_calculate_recency_factor(self):
        """Test recency factor calculation."""
        current_date = date(2025, 1, 1)
        
        # Recent release
        recent_release = date(2024, 12, 1)
        factor = calculate_recency_factor(recent_release, current_date, 75)
        assert 0.6 < factor < 0.8
        
        # Old release
        old_release = date(2023, 1, 1)
        factor = calculate_recency_factor(old_release, current_date, 75)
        assert 0 < factor < 0.01
        
        # Future release
        future_release = date(2025, 6, 1)
        factor = calculate_recency_factor(future_release, current_date, 75)
        assert factor == 1.0
        
        # No release date
        factor = calculate_recency_factor(None, current_date, 75)
        assert factor == 1.0

    def test_calculate_stability_factor(self):
        """Test stability factor calculation."""
        assert calculate_stability_factor(0) == 1.0
        assert calculate_stability_factor(None) == 1.0
        assert calculate_stability_factor(1.0) == pytest.approx(0.5)
        assert calculate_stability_factor(10.0) == pytest.approx(0.0909, rel=0.01)

    def test_normalize_percentile(self):
        """Test percentile normalization."""
        values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        
        # Middle value
        assert 0.4 < normalize_percentile(50, values) < 0.6
        
        # Low value
        assert normalize_percentile(15, values) < 0.2
        
        # High value
        assert normalize_percentile(95, values) > 0.8
        
        # Edge cases
        assert normalize_percentile(-10, values) == 0.0
        assert normalize_percentile(200, values) == 1.0

    def test_classify_trend(self):
        """Test trend classification."""
        # EMERGING
        assert classify_trend(80, 65) == "EMERGING"
        
        # PEAKING
        assert classify_trend(80, 79) == "PEAKING"
        
        # DECLINING
        assert classify_trend(35, 50) == "DECLINING"
        
        # STABLE
        assert classify_trend(60, 55) == "STABLE"
        assert classify_trend(50, None) == "STABLE"


class TestTrendScoringEngine:
    """Test trend scoring engine."""

    def test_calculate_trend_components(self):
        """Test complete trend calculation."""
        engine = TrendScoringEngine()
        
        current_metrics = MovieMetrics(
            movie_key=1,
            avg_popularity=150.0,
            avg_vote_count=1000,
            avg_vote_average=7.5,
            popularity_stddev=10.0,
        )
        
        previous_metrics = MovieMetrics(
            movie_key=1,
            avg_popularity=100.0,
            avg_vote_count=800,
            avg_vote_average=7.3,
            popularity_stddev=15.0,
        )
        
        all_current = [current_metrics] + [
            MovieMetrics(
                movie_key=i,
                avg_popularity=100 + i * 10,
                avg_vote_count=500 + i * 100,
                avg_vote_average=7.0,
            )
            for i in range(2, 10)
        ]
        
        all_previous = [previous_metrics] + [
            MovieMetrics(
                movie_key=i,
                avg_popularity=90 + i * 10,
                avg_vote_count=450 + i * 100,
                avg_vote_average=7.0,
            )
            for i in range(2, 10)
        ]
        
        release_date = date(2024, 11, 1)
        current_date = date(2025, 1, 1)
        
        components = engine.calculate_trend_components(
            current_metrics=current_metrics,
            previous_metrics=previous_metrics,
            release_date=release_date,
            current_date=current_date,
            all_current_metrics=all_current,
            all_previous_metrics=all_previous,
        )
        
        # Assertions
        assert components.popularity_growth == pytest.approx(0.5)
        assert components.vote_velocity == pytest.approx(0.25)
        assert 0 <= components.norm_popularity_growth <= 1
        assert 0 <= components.norm_vote_velocity <= 1
        assert 0 < components.recency_factor < 1
        assert 0 < components.stability_factor <= 1
        assert 0 <= components.trend_score <= 100
        assert components.trend_classification in ["EMERGING", "PEAKING", "STABLE", "DECLINING"]

    def test_calculate_batch_trends(self):
        """Test batch trend calculation."""
        engine = TrendScoringEngine()
        
        current_metrics = [
            MovieMetrics(
                movie_key=i,
                avg_popularity=100 + i * 20,
                avg_vote_count=500 + i * 100,
                avg_vote_average=7.0 + i * 0.1,
                popularity_stddev=5.0,
            )
            for i in range(1, 6)
        ]
        
        previous_metrics = [
            MovieMetrics(
                movie_key=i,
                avg_popularity=80 + i * 15,
                avg_vote_count=400 + i * 80,
                avg_vote_average=6.8 + i * 0.1,
                popularity_stddev=8.0,
            )
            for i in range(1, 6)
        ]
        
        release_dates = {
            i: date(2024, 11, 1) for i in range(1, 6)
        }
        
        results = engine.calculate_batch_trends(
            current_period_metrics=current_metrics,
            previous_period_metrics=previous_metrics,
            movie_release_dates=release_dates,
            current_date=date(2025, 1, 1),
        )
        
        assert len(results) == 5
        
        for movie_key, components in results.items():
            assert 1 <= movie_key <= 5
            assert 0 <= components.trend_score <= 100
            assert components.trend_classification in ["EMERGING", "PEAKING", "STABLE", "DECLINING"]
