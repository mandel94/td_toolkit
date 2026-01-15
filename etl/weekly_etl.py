from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Callable, List, Optional, Protocol
import pandas as pd
from datetime import datetime

from etl.page_and_screen_etl import PageAndScreenETLFactory
from etl.content_scoring import (
    ContentScoreCalculator,
    ContentScoreSegmentation,
    ContentScoreValidator,
    ContentScoringConfig,
)
from scrape_content.ArticleScraper import ArticleScraper


class Extractor(Protocol):
    def extract(self) -> pd.DataFrame: ...


class Transformer(Protocol):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...


class Loader(Protocol):
    def load(self, df: pd.DataFrame) -> None: ...


@dataclass
class GA4Extractor:
    ga4_client: any
    property_id: str
    dimensions: List[str]
    metrics: List[str]
    start_date: str
    end_date: str

    def extract(self) -> pd.DataFrame:
        df = self.ga4_client.run_query(
            property_id=self.property_id,
            dimensions=self.dimensions,
            metrics=self.metrics,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        etl = PageAndScreenETLFactory.get_etl("en", df=df)
        df = etl.run_etl()
        return df


@dataclass
class LocalDFExtractor:
    df: pd.DataFrame

    def extract(self) -> pd.DataFrame:
        return self.df.copy()


@dataclass
class WeeklyTransformer:
    domain: str
    map_categories_func: Callable[[str], str]
    si_fara_func: Callable[[str], bool]
    min_views: int = 10
    scoring_strategy_name: str = "balanced"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["Categoria"] = df["pagePath"].apply(self.map_categories_func)
        df.loc[df["pagePath"].apply(self.si_fara_func), "Categoria"] = "Si farà"
        df.loc[
            (df["Categoria"] == "Recensioni / In Sala") | (df["Categoria"] == "Recensioni"),
            "Categoria",
        ] = "Recensioni"

        # Convert metrics to numeric
        for col in ["screenPageViews", "engagementRate", "averageSessionDuration"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Filter by minimum views
        if "screenPageViews" in df.columns:
            df = df[df["screenPageViews"] > self.min_views]

        # Enrich with scraped metadata (including stubbed text)
        scraper = ArticleScraper(
            domain=self.domain,
            features=["publication_date", "author", "title", "text"],
            stub=True,
        )
        scraped_df = scraper.scrape(df[["pagePath"]])
        # publication_date is YYYY-MM-DD string or None; convert to ISO string consistently
        def _pub_to_iso(s: Optional[str]) -> Optional[str]:
            if not s:
                return None
            try:
                return datetime.strptime(s, "%Y-%m-%d").isoformat()
            except Exception:
                return None
        scraped_df["Publication Date"] = scraped_df["publication_date"].apply(_pub_to_iso)
        scraped_df.rename(columns={
            "author": "Author",
            "title": "Title",
            "text": "article_text",
        }, inplace=True)
        scraped_df = scraped_df[["pagePath", "Publication Date", "Author", "Title", "article_text"]]
        df = df.merge(scraped_df, on="pagePath", how="left")

        # Content scoring
        try:
            scoring_config = ContentScoringConfig(strategy_name=self.scoring_strategy_name)
            calculator = ContentScoreCalculator(scoring_config)
            segmenter = ContentScoreSegmentation(scoring_config)
            validator = ContentScoreValidator(scoring_config)

            df = calculator.calculate(df)
            df = segmenter.segment(df)
            is_valid, issues = validator.validate(df)
            if not is_valid:
                # Flags are added even if validation finds issues
                df = validator.flag_anomalies(df)

            ordered_cols = [
                "editorial_rank",
                "editorial_score",
                "Title",
                "pagePath",
                "Publication Date",
                "Author",
                "article_text",
                "Categoria",
                "content_segment",
                "screenPageViews",
                "engagementRate",
                "averageSessionDuration",
                "feature_reach_rank",
                "feature_engagement_rank",
                "feature_depth_rank",
                "anomaly_flag",
            ]
            extra_cols = [c for c in df.columns if c not in ordered_cols]
            df = df[[c for c in ordered_cols if c in df.columns] + extra_cols]
        except Exception:
            # If scoring fails, continue without reordering
            pass

        return df


@dataclass
class ExcelLoader:
    output_path: str

    def load(self, df: pd.DataFrame) -> None:
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_excel(self.output_path, index=False)


@dataclass
class CSVSaver:
    output_path: str

    def load(self, df: pd.DataFrame) -> None:
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)


@dataclass
class ETLPipeline:
    extractor: Extractor
    transformer: Transformer
    loaders: List[Loader]

    def run(self) -> pd.DataFrame:
        df = self.extractor.extract()
        df = self.transformer.transform(df)
        for loader in self.loaders:
            loader.load(df)
        return df
