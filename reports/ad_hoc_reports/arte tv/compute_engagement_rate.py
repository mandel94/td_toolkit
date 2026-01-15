import os
import sys
import pandas as pd
from datetime import date, timedelta

# Ensure project root is on sys.path for local package imports
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from ga4_api.ga4_api import Ga4Client


DEFAULT_PROPERTY_ID = "394327334"

# Articles to analyze
ARTICLES = [
    {
        "title": "Intervista a Olive Pere: Il valore artistico della critica nel panorama del cinema contemporaneo",
        "url": "/495884/interviews/intervista-a-olive-pere-il-valore-artistico-della-critica-nel-panorama-del-cinema-contemporaneo.html"
    },
    {
        "title": "Nel cuore notturno di Barcellona: Il viaggio emotivo di Yo, la busco",
        "url": "/494635/arte-tv/nel-cuore-notturno-di-barcellona-il-viaggio-emotivo-di-yo-la-busco.html"
    },
    {
        "title": "Mataharis (2007), di Icíar Bollaín: Analisi",
        "url": "/504652/arte-tv/mataharis-film-2007-iciar-bollain-analisi.html"
    },
    {
        "title": "Bodas de sangre: Un amore danzante scolpito nell'ombra",
        "url": "/493531/arte-tv/bodas-de-sangre-un-amore-danzante-scolpito-nellombra.html"
    },
    {
        "title": "La comunidad: Intrigo all'ultimo piano, un film ancora attuale",
        "url": "/498541/arte-tv/la-comunidad-intrigo-allultimo-piano-un-film-ancora-attuale.html"
    }
]


def fetch_engagement_metrics(start_date: str, end_date: str, property_id: str = DEFAULT_PROPERTY_ID) -> pd.DataFrame:
    """Fetch engagement metrics for specific articles"""
    ga4_client = Ga4Client()
    
    # Extract URLs from articles
    urls = [article["url"] for article in ARTICLES]
    
    # Fetch data
    dimensions = ["pagePath"]
    metrics = ["screenPageViews", "activeUsers", "engagedSessions", "sessions"]
    
    df = ga4_client.run_query(
        property_id=property_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )
    
    # Filter client-side for our specific URLs
    if not df.empty:
        mask = df["pagePath"].isin(urls)
        df = df.loc[mask].copy()
    
    return df


def fetch_google_cpc_duration(start_date: str, end_date: str, property_id: str = DEFAULT_PROPERTY_ID) -> pd.DataFrame:
    """Fetch average session duration for Google/CPC traffic"""
    ga4_client = Ga4Client()
    
    # Extract URLs from articles
    urls = [article["url"] for article in ARTICLES]
    
    # Fetch data with session source/medium dimension and session duration
    dimensions = ["pagePath", "sessionSource", "sessionMedium"]
    metrics = ["sessions", "averageSessionDuration"]
    
    df = ga4_client.run_query(
        property_id=property_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )
    
    if df.empty:
        return pd.DataFrame(columns=["pagePath", "google_cpc_avg_duration"])
    
    # Filter for our URLs and Google/CPC traffic
    mask_urls = df["pagePath"].isin(urls)
    mask_google = df["sessionSource"].str.lower().eq("google")
    mask_cpc = df["sessionMedium"].str.lower().eq("cpc")
    
    df_filtered = df.loc[mask_urls & mask_google & mask_cpc].copy()
    
    if df_filtered.empty:
        # Return empty data for URLs with no Google/CPC traffic
        return pd.DataFrame({"pagePath": urls, "google_cpc_avg_duration": [None] * len(urls)})
    
    # Convert to numeric
    df_filtered["sessions"] = pd.to_numeric(df_filtered["sessions"], errors="coerce")
    df_filtered["averageSessionDuration"] = pd.to_numeric(df_filtered["averageSessionDuration"], errors="coerce")
    
    # Calculate weighted average duration by page
    df_filtered["total_duration"] = df_filtered["averageSessionDuration"] * df_filtered["sessions"]
    
    grouped = df_filtered.groupby("pagePath").agg({
        "total_duration": "sum",
        "sessions": "sum"
    }).reset_index()
    
    grouped["google_cpc_avg_duration"] = grouped["total_duration"] / grouped["sessions"]
    
    return grouped[["pagePath", "google_cpc_avg_duration"]]


def compute_engagement_rate(df: pd.DataFrame, duration_df: pd.DataFrame = None) -> pd.DataFrame:
    """Compute engagement rate = engagedSessions / sessions and merge with duration data"""
    if df.empty:
        return pd.DataFrame(columns=[
            "pagePath",
            "screenPageViews",
            "activeUsers",
            "engagedSessions",
            "sessions",
            "engagement_rate",
            "google_cpc_avg_duration"
        ])
    
    # Coerce numeric
    for col in ["screenPageViews", "activeUsers", "engagedSessions", "sessions"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Compute engagement rate
    if "engagedSessions" in df.columns and "sessions" in df.columns:
        df["engagement_rate"] = (df["engagedSessions"] / df["sessions"]).fillna(0.0)
    else:
        df["engagement_rate"] = pd.NA
    
    # Merge with duration data if provided
    if duration_df is not None and not duration_df.empty:
        df = pd.merge(df, duration_df, on="pagePath", how="left")
    else:
        df["google_cpc_avg_duration"] = pd.NA
    
    return df


def merge_with_titles(df: pd.DataFrame) -> pd.DataFrame:
    """Merge with article titles"""
    # Create mapping dataframe
    titles_df = pd.DataFrame(ARTICLES)
    titles_df = titles_df.rename(columns={"url": "pagePath", "title": "Titolo"})
    
    # Merge
    result = pd.merge(titles_df, df, on="pagePath", how="left")
    
    # Reorder columns
    cols = ["Titolo", "pagePath", "screenPageViews", "activeUsers", "engagedSessions", "sessions", "engagement_rate", "google_cpc_avg_duration"]
    result = result[cols]
    
    # Rename columns for better readability
    result = result.rename(columns={
        "pagePath": "URL",
        "screenPageViews": "Visualizzazioni",
        "activeUsers": "Utenti attivi",
        "engagedSessions": "Sessioni coinvolte",
        "sessions": "Sessioni totali",
        "engagement_rate": "Tasso di coinvolgimento",
        "google_cpc_avg_duration": "Durata media sessioni Google/CPC (s)"
    })
    
    return result


def main():
    # From January 1, 2025 to yesterday (to capture recent articles)
    start_date = "2025-01-01"
    end = date.today() - timedelta(days=1)
    end_date = end.isoformat()
    
    print(f"Fetching engagement metrics from {start_date} to {end_date}...")
    
    # Fetch data
    df = fetch_engagement_metrics(start_date, end_date)
    
    if df.empty:
        print("No data found for the specified articles.")
        return
    
    print(f"Found data for {len(df)} articles.")
    
    # Fetch Google/CPC duration data
    print("Fetching Google/CPC session duration...")
    duration_df = fetch_google_cpc_duration(start_date, end_date)
    
    # Compute engagement rate and merge with duration
    df = compute_engagement_rate(df, duration_df)
    
    # Merge with titles
    result = merge_with_titles(df)
    
    # Display results
    print("\n" + "="*100)
    print("ENGAGEMENT RATE ANALYSIS")
    print("="*100)
    print(f"\nPeriod: {start_date} to {end_date}")
    print(f"\nFormula: Engagement Rate = Engaged Sessions / Total Sessions")
    print("\n" + result.to_string(index=False))
    
    # Save to CSV
    output_path = os.path.join(os.path.dirname(__file__), "engagement_rate_analysis.csv")
    result.to_csv(output_path, index=False, sep=";")
    print(f"\n\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
