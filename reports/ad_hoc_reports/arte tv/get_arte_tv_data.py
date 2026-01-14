import argparse
import os
import sys
from datetime import date, timedelta
import pandas as pd

# Ensure project root is on sys.path for local package imports
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ga4_api.ga4_api import Ga4Client

try:
    from google.analytics.data_v1beta.types import (
        FilterExpression,
        FilterExpressionList,
        Filter,
        StringFilter,
    )
except Exception:  # Fallback type hints not required at runtime
    FilterExpression = None  # type: ignore
    FilterExpressionList = None  # type: ignore
    Filter = None  # type: ignore
    StringFilter = None  # type: ignore


DEFAULT_PROPERTY_ID = "394327334"


class ArteTvExtractor:
    def __init__(self, ga4_client: Ga4Client, property_id: str):
        self.client = ga4_client
        self.property_id = property_id

    @staticmethod
    def _build_filter_expression():
        # Build GA4 filter: include pagePath CONTAINS "arte-tv"
        # and exclude EXACT "/arte-tv" and BEGINS_WITH "/tag/arte-tv"
        # If GA4 SDK types are unavailable at import, return None (will filter client-side)
        if FilterExpression is None:
            return None
        include_expr = FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=StringFilter(match_type=StringFilter.MatchType.CONTAINS, value="arte-tv"),
            )
        )
        exclude_exact = FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=StringFilter(match_type=StringFilter.MatchType.EXACT, value="/arte-tv"),
            )
        )
        exclude_tag = FilterExpression(
            filter=Filter(
                field_name="pagePath",
                string_filter=StringFilter(match_type=StringFilter.MatchType.BEGINS_WITH, value="/tag/arte-tv"),
            )
        )
        exclude_or = FilterExpression(or_group=FilterExpressionList(expressions=[exclude_exact, exclude_tag]))
        # AND include with NOT(exclude_or)
        return FilterExpression(
            and_group=FilterExpressionList(
                expressions=[include_expr, FilterExpression(not_expression=exclude_or)]
            )
        )

    def fetch(self, start_date: str, end_date: str) -> pd.DataFrame:
        dimensions = ["pagePath"]
        # sessions for engagement rate computation; dropped later
        metrics = ["screenPageViews", "activeUsers", "engagedSessions", "sessions"]
        dim_filter = self._build_filter_expression()
        df = self.client.run_query(
            property_id=self.property_id,
            dimensions=dimensions,
            metrics=metrics,
            start_date=start_date,
            end_date=end_date,
            dimension_filter=dim_filter,
        )
        # If server-side filter not available, filter client-side
        if df.empty:
            return df
        if dim_filter is None:
            mask = (
                df["pagePath"].str.contains("arte-tv", na=False)
                & (df["pagePath"] != "/arte-tv")
                & (~df["pagePath"].str.startswith("/tag/arte-tv", na=False))
            )
            df = df.loc[mask].copy()
        return df


class ArteTvTransformer:
    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=[
                "path",
                "views",
                "activeUsers",
                "engagedSessions",
                "engagement_rate",
            ])
        # Coerce numeric
        for col in ["screenPageViews", "activeUsers", "engagedSessions", "sessions"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # Compute engagement rate = engagedSessions / sessions
        if "engagedSessions" in df.columns and "sessions" in df.columns:
            df["engagement_rate"] = (df["engagedSessions"] / df["sessions"]).fillna(0.0)
        else:
            df["engagement_rate"] = pd.NA
        # Select/rename columns
        out = df[["pagePath", "screenPageViews", "activeUsers", "engagedSessions", "engagement_rate"]].copy()
        out = out.rename(
            columns={
                "pagePath": "path",
                "screenPageViews": "views",
            }
        )
        return out


class CsvLoader:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def save(self, df: pd.DataFrame) -> str:
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        df.to_csv(self.output_path, index=False)
        return self.output_path


def default_dates() -> tuple[str, str]:
    # Default to last 30 full days ending yesterday
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=29)
    return start.isoformat(), end.isoformat()


def build_output_path(base_dir: str, start_date: str, end_date: str) -> str:
    base_dir = base_dir or os.path.join(
        os.path.dirname(__file__),
        "output",
    )
    os.makedirs(base_dir, exist_ok=True)
    filename = f"arte_tv_ga4_{start_date}_{end_date}.csv"
    return os.path.join(base_dir, filename)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract GA4 data for arte-tv pages and save to CSV")
    parser.add_argument("--start-date", dest="start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", dest="end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--property-id", dest="property_id", type=str, default=DEFAULT_PROPERTY_ID, help="GA4 property ID")
    parser.add_argument("--debug", action="store_true", help="Enable diagnostics to observe filtering and sample matches")
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        type=str,
        default=os.path.join(os.path.dirname(__file__), "output"),
        help="Directory to write CSV output",
    )
    return parser.parse_args()


def run_etl(start_date: str, end_date: str, property_id: str, output_dir: str, debug: bool = False) -> str:
    ga4_client = Ga4Client()
    extractor = ArteTvExtractor(ga4_client, property_id)
    if debug:
        print(f"[DEBUG] Property: {property_id}  Range: {start_date} → {end_date}")
    raw_df = extractor.fetch(start_date, end_date)
    if debug:
        print(f"[DEBUG] Rows returned with server-side filter: {len(raw_df)}")
        # Run a limited unfiltered pull to observe potential matches and filtering correctness
        try:
            sample_df = ga4_client.run_query(
                property_id=property_id,
                dimensions=["pagePath"],
                metrics=["screenPageViews", "activeUsers", "engagedSessions", "sessions"],
                start_date=start_date,
                end_date=end_date,
                dimension_filter=None,
                limit=5000,
            )
            if not sample_df.empty:
                mask_contains = sample_df["pagePath"].str.contains("arte-tv", na=False)
                mask_exact = sample_df["pagePath"].eq("/arte-tv")
                mask_tag = sample_df["pagePath"].str.startswith("/tag/arte-tv", na=False)
                matched = sample_df.loc[mask_contains]
                excluded = sample_df.loc[mask_exact | mask_tag]
                print(f"[DEBUG] Sample without filter (limit=5000): {len(sample_df)} rows")
                print(f"[DEBUG] Sample matches 'arte-tv' contains: {len(matched)} rows")
                print(f"[DEBUG] Sample excluded exact '/arte-tv' or '/tag/arte-tv*': {len(excluded)} rows")
                if not matched.empty:
                    print("[DEBUG] Example matching paths:")
                    for p in matched["pagePath"].head(10).tolist():
                        print(f"  - {p}")
                if not excluded.empty:
                    print("[DEBUG] Example excluded paths:")
                    for p in excluded["pagePath"].head(10).tolist():
                        print(f"  - {p}")
            else:
                print("[DEBUG] Unfiltered sample returned no rows (limit=5000).")
        except Exception as ex:
            print(f"[DEBUG] Sampling diagnostics failed: {ex}")
    transformed = ArteTvTransformer.transform(raw_df)
    output_path = build_output_path(output_dir, start_date, end_date)
    loader = CsvLoader(output_path)
    saved_path = loader.save(transformed)
    return saved_path


if __name__ == "__main__":
    args = parse_args()
    s, e = (args.start_date, args.end_date)
    if not s or not e:
        s, e = default_dates()
    out_path = run_etl(s, e, args.property_id, args.output_dir, debug=args.debug)
    print(f"Saved: {out_path}")
