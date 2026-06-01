"""
GA4 Editorial Score Calculator (v2)
=====================================

Implements the GA4-standard editorial score defined in:
  docs/score_editoriale_articolo_ga4.md

Formula
-------
  Score = (Portata × 0.15) + (Qualità di lettura × 0.40)
        + (Completamento × 0.25) + (Recirculation × 0.20)

Each component is normalised to 0–100 before weighting.

Components
----------
1. Portata (Reach) — 15%
   min(views / avg_views_period, 1) × 100
   Capped at 100; avg_views is computed from the batch when not supplied.

2. Qualità di lettura (Reading Quality) — 40%
   min(avg_engagement_time / expected_reading_time_sec, 1) × 100
   expected_reading_time_sec = (word_count / reading_speed_wpm) × 60
   If word_count is missing or zero, the component defaults to 50 (neutral).

3. Completamento (Completion) — 25%
   scroll_90_rate is the share of pageviews that triggered the GA4 Enhanced
   Measurement scroll event at 90%.
   Converted to a score via stepped scale (5/20/45/75/100).
   If scroll_90_rate is missing, component defaults to 45 (mid-band neutral).

4. Recirculation — 20%
   Share of sessions landing on the article that contained 2+ pageviews.
   Converted to a score via stepped scale (5/25/50/75/100).
   If recirculation_rate is missing, the GA4 engagement rate is used as proxy
   (documented fallback from the spec).
   If both are missing, component defaults to 25 (below-average neutral).
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .ga4_score_config import Ga4ScoringConfig, DEFAULT_GA4_CONFIG

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stepped-scale helpers
# ---------------------------------------------------------------------------

def _completion_score(scroll_90_rate: float) -> float:
    """
    Convert scroll-90% rate (0–1 fraction or 0–100 percent) to a 0–100 score.

    Scale defined in docs/score_editoriale_articolo_ga4.md:
        ≥ 0.70  → 100
        0.50–0.69 → 75
        0.30–0.49 → 45
        0.15–0.29 → 20
        < 0.15  → 5
    """
    # Accept both 0–1 and 0–100 representations
    r = scroll_90_rate / 100.0 if scroll_90_rate > 1.0 else scroll_90_rate
    if r >= 0.70:
        return 100.0
    elif r >= 0.50:
        return 75.0
    elif r >= 0.30:
        return 45.0
    elif r >= 0.15:
        return 20.0
    else:
        return 5.0


def _recirculation_score(recirculation_rate: float) -> float:
    """
    Convert recirculation rate (0–1 fraction or 0–100 percent) to a 0–100 score.

    Scale defined in docs/score_editoriale_articolo_ga4.md:
        ≥ 0.45  → 100
        0.30–0.44 → 75
        0.20–0.29 → 50
        0.10–0.19 → 25
        < 0.10  → 5
    """
    r = recirculation_rate / 100.0 if recirculation_rate > 1.0 else recirculation_rate
    if r >= 0.45:
        return 100.0
    elif r >= 0.30:
        return 75.0
    elif r >= 0.20:
        return 50.0
    elif r >= 0.10:
        return 25.0
    else:
        return 5.0


# ---------------------------------------------------------------------------
# Main calculator
# ---------------------------------------------------------------------------

class Ga4EditorialScoreCalculator:
    """
    Calculate the GA4-standard editorial score (v2) for a batch of articles.

    This scorer is complementary to the rank-based ContentScoreCalculator (v1).
    The two can coexist; the active version is selected via
    WeeklyTransformer(scoring_version='v2').

    Parameters
    ----------
    config : Ga4ScoringConfig, optional
        Scoring configuration. Uses DEFAULT_GA4_CONFIG if not provided.

    Example
    -------
        from etl.content_scoring import Ga4EditorialScoreCalculator, Ga4ScoringConfig

        calc = Ga4EditorialScoreCalculator()
        scored_df = calc.calculate(df)
    """

    def __init__(self, config: Optional[Ga4ScoringConfig] = None) -> None:
        self.config = config or DEFAULT_GA4_CONFIG
        logger.info("Ga4EditorialScoreCalculator initialised (scoring v2)")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate editorial scores and ranks for all articles in df.

        Missing optional inputs are handled gracefully with neutral defaults
        (documented per component above).

        Parameters
        ----------
        df : pd.DataFrame
            Must contain at minimum:
            - config.views_col  (screenPageViews)
            - config.engagement_time_col  (averageSessionDuration)
            Optional but recommended:
            - config.word_count_col  (word_count)
            - config.scroll_90_rate_col  (scroll_90_rate)
            - config.recirculation_rate_col  (recirculation_rate)
              OR config.engagement_rate_col  (engagementRate) as fallback

        Returns
        -------
        pd.DataFrame
            Input df with added columns:
            - editorial_score  (0–100, float)
            - editorial_rank   (integer, 1 = best)
            When include_component_scores is True, also adds:
            - score_portata, score_reading_quality, score_completion,
              score_recirculation
        """
        cfg = self.config
        df = df.copy()

        self._ensure_required_columns(df)
        df = self._ensure_word_count(df)

        avg_views = self._compute_avg_views(df)
        logger.info(f"Batch avg views: {avg_views:.1f}")

        df = self._compute_components(df, avg_views)
        df = self._compute_final_score(df)
        df = self._assign_ranks(df)

        if not cfg.include_component_scores:
            df = df.drop(
                columns=[
                    "score_portata",
                    "score_reading_quality",
                    "score_completion",
                    "score_recirculation",
                ],
                errors="ignore",
            )

        logger.info(
            f"GA4 editorial score (v2) computed for {len(df)} articles. "
            f"Mean score: {df[cfg.score_column_name].mean():.1f}"
        )
        return df

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_required_columns(self, df: pd.DataFrame) -> None:
        cfg = self.config
        required = [cfg.views_col, cfg.engagement_time_col]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(
                f"Ga4EditorialScoreCalculator requires columns: {missing}. "
                f"Available columns: {list(df.columns)}"
            )

    def _ensure_word_count(self, df: pd.DataFrame) -> pd.DataFrame:
        """Derive word_count from article_text when the column is absent."""
        cfg = self.config
        if cfg.word_count_col not in df.columns:
            if "article_text" in df.columns:
                logger.info(
                    f"'{cfg.word_count_col}' not found; computing from 'article_text'."
                )
                df[cfg.word_count_col] = df["article_text"].apply(
                    lambda t: len(str(t).split()) if pd.notna(t) and t else np.nan
                )
            else:
                logger.warning(
                    f"'{cfg.word_count_col}' and 'article_text' both absent. "
                    "Reading quality will use neutral default (50)."
                )
                df[cfg.word_count_col] = np.nan
        return df

    def _compute_avg_views(self, df: pd.DataFrame) -> float:
        """Average views across the batch (used to normalise Portata)."""
        cfg = self.config
        views = pd.to_numeric(df[cfg.views_col], errors="coerce").dropna()
        return float(views.mean()) if len(views) > 0 else 1.0

    def _compute_components(self, df: pd.DataFrame, avg_views: float) -> pd.DataFrame:
        cfg = self.config

        df["score_portata"] = df.apply(
            lambda row: self._portata(row, avg_views), axis=1
        )
        df["score_reading_quality"] = df.apply(
            lambda row: self._reading_quality(row), axis=1
        )
        df["score_completion"] = df.apply(
            lambda row: self._completion(row), axis=1
        )
        df["score_recirculation"] = df.apply(
            lambda row: self._recirculation(row), axis=1
        )
        return df

    def _portata(self, row: pd.Series, avg_views: float) -> float:
        """Portata (Reach) component, 0–100, capped at 100."""
        views = pd.to_numeric(row.get(self.config.views_col, np.nan), errors="coerce")
        if pd.isna(views) or avg_views <= 0:
            return 50.0  # neutral default
        return min((views / avg_views) * 100.0, 100.0)

    def _reading_quality(self, row: pd.Series) -> float:
        """Qualità di lettura component, 0–100, capped at 100."""
        cfg = self.config
        eng_time = pd.to_numeric(
            row.get(cfg.engagement_time_col, np.nan), errors="coerce"
        )
        word_count = pd.to_numeric(
            row.get(cfg.word_count_col, np.nan), errors="coerce"
        )
        if pd.isna(eng_time):
            return 50.0
        if pd.isna(word_count) or word_count <= 0:
            # Cannot compute expected reading time without word count
            return 50.0
        expected_sec = (word_count / cfg.reading_speed_wpm) * 60.0
        if expected_sec <= 0:
            return 50.0
        return min((eng_time / expected_sec) * 100.0, 100.0)

    def _completion(self, row: pd.Series) -> float:
        """Completamento component from scroll-90% rate, 0–100."""
        cfg = self.config
        if cfg.scroll_90_rate_col in row.index:
            rate = pd.to_numeric(row[cfg.scroll_90_rate_col], errors="coerce")
            if not pd.isna(rate):
                return _completion_score(rate)
        # Default: mid-band neutral (30–49% bracket → 45 pts)
        return 45.0

    def _recirculation(self, row: pd.Series) -> float:
        """Recirculation component, 0–100."""
        cfg = self.config

        # Primary: explicit recirculation_rate column
        if cfg.recirculation_rate_col in row.index:
            rate = pd.to_numeric(row[cfg.recirculation_rate_col], errors="coerce")
            if not pd.isna(rate):
                return _recirculation_score(rate)

        # Fallback: engagement rate as proxy (documented in spec)
        if cfg.engagement_rate_col in row.index:
            eng_rate = pd.to_numeric(row[cfg.engagement_rate_col], errors="coerce")
            if not pd.isna(eng_rate):
                logger.debug(
                    "recirculation_rate missing; using engagement_rate as proxy."
                )
                return _recirculation_score(eng_rate)

        # No data available — below-average neutral
        return 25.0

    def _compute_final_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Weighted sum of four components → editorial_score (0–100)."""
        cfg = self.config
        df[cfg.score_column_name] = (
            df["score_portata"] * cfg.portata_weight
            + df["score_reading_quality"] * cfg.reading_quality_weight
            + df["score_completion"] * cfg.completion_weight
            + df["score_recirculation"] * cfg.recirculation_weight
        ).round(2)
        return df

    def _assign_ranks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Integer ranks (1 = best score)."""
        cfg = self.config
        df[cfg.rank_column_name] = (
            df[cfg.score_column_name]
            .rank(method="min", ascending=False)
            .astype(int)
        )
        return df

    # ------------------------------------------------------------------
    # Scoring-band helper (convenience)
    # ------------------------------------------------------------------

    @staticmethod
    def score_band(score: float) -> str:
        """
        Return the evaluation band for a given 0–100 score.

        Bands:
          80–100 → Eccellente
          60–79  → Buono
          40–59  → Nella media
          20–39  → Sotto la media
          0–19   → Critico
        """
        if score >= 80:
            return "Eccellente"
        elif score >= 60:
            return "Buono"
        elif score >= 40:
            return "Nella media"
        elif score >= 20:
            return "Sotto la media"
        else:
            return "Critico"
