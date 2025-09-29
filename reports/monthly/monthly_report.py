# Change system path to include two levels from parent directory
import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# And one level
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import (
    MONTHLY_OUTPUT_DIR,
    MONTHLY_PARAMETERS_MONTH,
    MONTHLY_REPORT_METRICS
)

from etl.page_and_screen_etl import PageAndScreenETLFactory
from ga4_api.ga4_api import Ga4Client
from map_ga4_categories import map_ga4_categories
from bs4 import BeautifulSoup
from datetime import datetime
import concurrent.futures
from scrape_content.scrape_archive import scrape_archive
from scrape_content.scrape_articles import scrape_article




months_data_range = {
    "January": ("2025-01-01", "2025-01-31"),
    "February": ("2025-02-01", "2025-02-28"),
    "March": ("2025-03-01", "2025-03-31"),
    "April": ("2025-04-01", "2025-04-30"),
    "May": ("2025-05-01", "2025-05-31"),
    "June": ("2025-06-01", "2025-06-30"),
    "July": ("2025-07-01", "2025-07-31"),
    "August": ("2025-08-01", "2025-08-31"),
    "September": ("2025-09-01", "2025-09-30"),
    "October": ("2025-10-01", "2025-10-31"),
    "November": ("2025-11-01", "2025-11-30"),
    "December": ("2025-12-01", "2025-12-31"),
}

# Define a function that returns True if the pagepath countains "si-fara" substring


def contains_si_fara(path: str) -> bool:
    return "si-fara" in path



def get_ga4_data_api(
    ga4_client, property_id, dimensions, metrics, start_date, end_date
):
    """
    Load GA4 data directly from the Google Analytics API using a Ga4Client instance.
    """
    df = ga4_client.run_query(
        property_id=property_id,
        dimensions=dimensions,
        metrics=metrics,
        start_date=start_date,
        end_date=end_date,
    )
    etl = PageAndScreenETLFactory.get_etl("en", df=df)
    df = etl.run_etl()
    return df


def get_ga4_data(source="local", **kwargs):
    """
    Unified interface to load GA4 data from either local CSV or API.
    Args:
        source (str): 'local' or 'api'.
        kwargs: arguments for the chosen source.
            For 'local': input_filename
            For 'api': ga4_client, property_id, dimensions, metrics, start_date, end_date
    Returns:
        pd.DataFrame
    """
    if source == "api":
        return get_ga4_data_api(
            kwargs["ga4_client"],
            kwargs["property_id"],
            kwargs["dimensions"],
            kwargs["metrics"],
            kwargs["start_date"],
            kwargs["end_date"],
        )
    else:
        raise ValueError(f"Unknown GA4 data source: {source}")


def get_article_metadata(url, delay=4):
    """
    Scrape the article page and return a tuple: (publication_date, author_name, article_title).
    Publication date is a datetime object or None.
    Author is a string or None.
    Title is a string or None.
    Retries on timeout until successful.
    delay: seconds to wait after unsuccessful requests (default 4)
    """
    import time
    import requests
    from requests.exceptions import Timeout

    while True:
        try:
            time.sleep(4)
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            # Publication date
            time_tag = soup.find("time", attrs={"datetime": True})
            pub_date = None
            if time_tag and time_tag.has_attr("datetime"):
                try:
                    pub_date = datetime.fromisoformat(time_tag["datetime"][:19])
                except Exception:
                    pub_date = None
            elif time_tag:
                try:
                    pub_date = datetime.fromisoformat(time_tag.text.strip()[:19])
                except Exception:
                    pub_date = None
            # Author
            author_tag = soup.find("a", rel="author")
            author = author_tag.text.strip() if author_tag else None
            # Title
            title_tag = soup.find(
                "h1", class_="mvp-post-title left entry-title", itemprop="headline"
            )
            title = title_tag.text.strip() if title_tag else None
            return pub_date, author, title
        except Timeout:
            print(f"Timeout occurred for {url}, retrying...")
            time.sleep(delay)
            continue
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            time.sleep(delay)
            return None, None, None


def scrape_article_metadata(
    paths, domain, metadata_collector=get_article_metadata, max_workers=8
):
    """
    Given a list of page paths, scrape publication date, author, and title for each article in parallel.
    Returns three lists: publication dates, authors, titles.
    """

    def scrape_one(path):
        url = domain + path if path.startswith("/") else domain + "/" + path
        pub_date, author, title = metadata_collector(url)
        print("OK")
        return pub_date, author, title

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(scrape_one, paths))
    if results:
        pub_dates, authors, titles = zip(*results)
    else:
        pub_dates, authors, titles = [], [], []
    return list(pub_dates), list(authors), list(titles)

def remove_invalid_chars(sheet_name):
    """
    Remove invalid characters for Excel sheet names: : \ / ? * [ ]
    Truncate to 31 chars (Excel limit).
    """
    invalid_chars = [":", "\\", "/", "?", "*", "[", "]"]
    for ch in invalid_chars:
        sheet_name = sheet_name.replace(ch, "_")
    return sheet_name[:31]


def run_monthly_report(
    data_args=None,
    domain="https://taxidrivers.it",
    max_workers=8,
    excel_output_path=None,
    map_categories_func=map_ga4_categories,
    si_fara_func=contains_si_fara,
):
    """
    Run the full monthly report pipeline.
    Args:
        data_args: dict of arguments for get_ga4_data
        domain: site domain for scraping
        max_workers: parallel scraping workers
        csv_output_path: where to save the CSV (optional)
        excel_output_path: where to save the Excel (optional)
        map_categories_func: function to map categories
        si_fara_func: function to flag si_fara articles
        output_dir: directory to ensure exists (optional)
        gemini_api_key: API key for Gemini (optional)
        use_gemini: whether to generate Gemini summary
        use_template: whether to generate template summary
        sort_by_metric: metric to use for sorting top articles
    Returns:
        The processed DataFrame
    If use_gemini is True and gemini_api_key is provided, also generate the Gemini summary and return it.
    """
    if data_args is None:
        data_args = {}
    df = get_ga4_data(**data_args)
    print("Number of articles that have generated views:", df.shape[0])
    print("Number of recent articles:", df.shape[0])
    df["Categoria"] = df["pagePath"].apply(map_categories_func)
    df.loc[df["pagePath"].apply(si_fara_func), "Categoria"] = "Si farà"
    df.loc[
        (df["Categoria"] == "Recensioni / In Sala") | (df["Categoria"] == "Recensioni"),
        "Categoria",
    ] = "Recensioni"
    # Convert metrics to numeric
    for metric_col in MONTHLY_REPORT_METRICS:
        if metric_col in df.columns:
            df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
    # Scrape metadata for each article
    print("Scraping article metadata...")
    # Scrape recent archive
    recent_archive = scrape_archive("reports/monthly/archive_page.html")
    print(f"Recent archive articles found: {recent_archive.shape[0]}")
    # Merge on title
    df = df.merge(
        recent_archive,
        on="pagePath",
        how="left",
    )
    # Keep only df with "published" not null (i.e. articles found in the archive)
    df = df[df["published"].notnull()].copy()
    df = df[~df["published"].str.contains("2 mesi ago", na=False)]
    print(f"Articles after merging with recent archive: {df.shape[0]}")
    paths = df["pagePath"].tolist()
    # Divide the df in 10 chunks. Scrape each chunk sequentially, waiting 10 minutes between each chunk
    chunk_size = max(1, len(paths) // 1) # 1 is for testing
    all_pub_dates = []
    all_authors = []
    all_titles = []
    for i in range(0, len(paths), chunk_size):
        chunk_paths = paths[i : i + chunk_size]
        print(f"Scraping chunk {i // chunk_size + 1} with {len(chunk_paths)} articles...")
        pub_dates, authors, titles = scrape_article_metadata(
            chunk_paths, domain, max_workers=max_workers
        )
        all_pub_dates.extend(pub_dates)
        all_authors.extend(authors)
        all_titles.extend(titles)
        if i + chunk_size < len(paths):
            print(f"Chunk {i // chunk_size + 1} done.")
            print("Waiting 10 minutes before next chunk...")
            import time

            time.sleep(2)
    # Convert publication dates to JSON serializable format
    df["Publication Date"] = [
        d.isoformat() if isinstance(d, datetime) else None for d in all_pub_dates
    ]
    df["Author"] = all_authors
    df["Title"] = all_titles
    # Save all results as a json
    try:
        if excel_output_path:
            df.to_excel(excel_output_path, index=False, engine="openpyxl")
            print(f"Top articles saved to {excel_output_path}")
    except Exception as e:
        print(f"Fallback to current directory: {e}")
        df.to_excel(".", index=False, engine="openpyxl")
    return df


# Example usage (uncomment to run):
# df = get_ga4_data()
# print(df.head())

if __name__ == "__main__":
    month = "August"
    ga4_client = Ga4Client()
    monthly_output = run_monthly_report(
        data_args={
            "source": "api",
            "ga4_client": ga4_client,
            "property_id": "394327334",
            "dimensions": ["pagePath"],
            "metrics": ["activeUsers", "screenPageViews", 'engagementRate', 'bounceRate', 'averageSessionDuration'],
            "start_date": months_data_range[MONTHLY_PARAMETERS_MONTH][0],
            "end_date": months_data_range[MONTHLY_PARAMETERS_MONTH][1],
        },
        domain="https://taxidrivers.it",
        max_workers=8,
        excel_output_path=os.path.join(
            MONTHLY_OUTPUT_DIR, f"top_articles_{MONTHLY_PARAMETERS_MONTH}.xlsx"
        ),
    )
