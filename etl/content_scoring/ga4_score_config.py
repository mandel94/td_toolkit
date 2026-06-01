"""
GA4 Editorial Score Configuration
===================================

Configuration for the GA4-standard editorial score (v2).

This score is based on the methodology defined in:
  docs/score_editoriale_articolo_ga4.md

Formula:
  Score = (Portata × 0.15) + (Qualità di lettura × 0.40)
        + (Completamento × 0.25) + (Recirculation × 0.20)

Each component is normalized to 0–100 before being combined.

Required GA4 inputs per article:
  - views (screenPageViews)
  - average_engagement_time (averageSessionDuration as proxy)
  - word_count (from CMS / article text — NOT from GA4)
  - scroll_90_rate  (scroll 90% events / views — from GA4 Explore or Events)
  - recirculation_rate (sessions with 2+ pageviews / total sessions — from GA4 Explore)
    OR engagement_rate as simplified proxy if recirculation_rate is unavailable.
"""

from dataclasses import dataclass


@dataclass
class Ga4ScoringConfig:
    """
    Configuration for the GA4-standard editorial score calculator.

    Column names
    ------------
    views_col : str
        Column with page views per article.
    engagement_time_col : str
        Column with average engagement time in seconds (averageSessionDuration proxy).
    word_count_col : str
        Column with article word count (computed from article_text if not present).
    scroll_90_rate_col : str
        Column with the fraction of sessions that scrolled to 90% of the page
        (scroll_90_events / views). If absent, component defaults to neutral (45 pts).
    recirculation_rate_col : str
        Column with recirculation rate: sessions with 2+ pageviews / total sessions
        on this landing page. If absent, engagement_rate_col is used as proxy.
    engagement_rate_col : str
        Column with GA4 engagement rate (engagedSessions / sessions × 100 or fraction).
        Used as fallback for recirculation when recirculation_rate_col is missing.

    Weights (must sum to 1.0)
    -------------------------
    portata_weight : float     — audience reach, default 0.15
    reading_quality_weight : float — time-on-page vs expected reading, default 0.40
    completion_weight : float  — scroll depth completion, default 0.25
    recirculation_weight : float — multi-page sessions ratio, default 0.20

    Other
    -----
    reading_speed_wpm : int
        Words per minute used to estimate expected reading time. Default 200.
    score_column_name : str
        Output column for the 0–100 composite score.
    rank_column_name : str
        Output column for the integer rank (1 = best).
    include_component_scores : bool
        If True, adds intermediate component columns to output.
    """

    # --- Input column names ---
    views_col: str = "screenPageViews"
    engagement_time_col: str = "averageSessionDuration"
    word_count_col: str = "word_count"
    scroll_90_rate_col: str = "scroll_90_rate"
    recirculation_rate_col: str = "recirculation_rate"
    engagement_rate_col: str = "engagementRate"

    # --- Component weights ---
    portata_weight: float = 0.15
    reading_quality_weight: float = 0.40
    completion_weight: float = 0.25
    recirculation_weight: float = 0.20

    # --- Reading speed ---
    reading_speed_wpm: int = 200

    # --- Output settings ---
    score_column_name: str = "editorial_score"
    rank_column_name: str = "editorial_rank"
    include_component_scores: bool = True

    def __post_init__(self) -> None:
        total = (
            self.portata_weight
            + self.reading_quality_weight
            + self.completion_weight
            + self.recirculation_weight
        )
        if not (0.99 <= total <= 1.01):
            raise ValueError(
                f"Component weights must sum to 1.0. Current sum: {total:.4f}. "
                f"Adjust portata_weight ({self.portata_weight}), "
                f"reading_quality_weight ({self.reading_quality_weight}), "
                f"completion_weight ({self.completion_weight}), or "
                f"recirculation_weight ({self.recirculation_weight})."
            )


DEFAULT_GA4_CONFIG = Ga4ScoringConfig()
