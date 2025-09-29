# Change system path to include two levels from parent directory
import json
import os
import sys
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import concurrent.futures
from requests.exceptions import Timeout

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
# And one level
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from map_ga4_categories import map_ga4_categories
from scrape_content.ArticleScraper import ArticleScraper

from config import (
    WEEKLY_OUTPUT_DIR,
    WEEKLY_REPORT_OUTPUT_FILENAME,
    WEEKLY_REPORT_METRICS,
    WEEKLY_REPORT_DATA_RANGE,
)
from etl.page_and_screen_etl import PageAndScreenETLFactory
# from gemini import WeeklyTopOfTheTops
from reports.weekly.weekly_top_template import (
    weekly_top_template_from_df,
)


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
    paths, domain, metadata_collector=None, max_workers=8
):
    """
    Given a list of page paths, scrape publication date, author, and title for each article in parallel.
    Returns three lists: publication dates, authors, titles.
    Uses ArticleScraper if metadata_collector is None.
    """
    if metadata_collector is None:
        scraper = ArticleScraper(domain=domain, features=["publication_date", "author", "title"])
        def metadata_collector(url):
            # url is domain + path
            path = url.replace(domain, "")
            result = scraper.scrape_article(path)
            pub_date = None
            if result.get("publication_date"):
                try:
                    pub_date = datetime.strptime(result["publication_date"], "%Y-%m-%d")
                except Exception:
                    pub_date = None
            return pub_date, result.get("author"), result.get("title")

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





def run_weekly_report(
    data_args=None,
    domain="https://taxidrivers.it",
    n=10,
    max_workers=8,
    csv_output_path=None,
    excel_output_path=None,
    map_categories_func=map_ga4_categories,
    si_fara_func=contains_si_fara,
    output_dir=None,
    gemini_api_key=None,
    use_gemini=False,
    use_template=False,
    sort_by_metric="Utenti attivi",
):
    """
    Run the full weekly report pipeline.
    Args:
        data_args: dict of arguments for get_ga4_data
        domain: site domain for scraping
        n: top N articles per category
        max_workers: parallel scraping workers
        filter_by_date: whether to filter by date
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
    df["Categoria"] = df["pagePath"].apply(map_categories_func)
    # Map si farà articles directly in Categoria
    df.loc[df["pagePath"].apply(si_fara_func), "Categoria"] = "Si farà"
    # Merge "Recensioni / In Sala" and "Recensioni"
    df.loc[
        (df["Categoria"] == "Recensioni / In Sala") | (df["Categoria"] == "Recensioni"),
        "Categoria",
    ] = "Recensioni"
    # Convert metrics to numeric
    for metric_col in WEEKLY_REPORT_METRICS:
        if metric_col in df.columns:
            df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce").fillna(0)
    # Keep only articles with more than 30 page views
    df = df[df["screenPageViews"] > 30] if "screenPageViews" in df.columns else df
    # Print len(df), dtypes
    print(f"Results dataFrame length: {len(df)}")
    # Scrape metadata for each article
    print("Scraping article metadata...")
    paths = df["pagePath"].tolist()
    pub_dates, authors, titles = scrape_article_metadata(
        paths, domain, max_workers=max_workers
    )
    # Convert publication dates to JSON serializable format
    df["Publication Date"] = [
        d.isoformat() if isinstance(d, datetime) else None for d in pub_dates
    ]
    #
    df["Author"] = authors
    df["Title"] = titles
    df.to_csv(csv_output_path, index=False) if csv_output_path else None
    if excel_output_path:
        df.to_excel(excel_output_path, index=False)
        print(f"Top articles saved to {excel_output_path}")
    gemini_summary = None
    template_summary = None
    if use_gemini and gemini_api_key:
        from gemini import WeeklyTopOfTheTops

        weekly_gemini = WeeklyTopOfTheTops(api_key=gemini_api_key)
        gemini_summary = weekly_gemini.generate(df, model="gemini-2.5-pro")
        print(gemini_summary)
    if use_template and excel_output_path:
        template_summary = weekly_top_template_from_df(
            excel_output_path, n=3, metric=sort_by_metric
        )
        print(template_summary)
    return df


# Example usage (uncomment to run):
# df = get_ga4_data()
# print(df.head())

if __name__ == "__main__":
    # Append one to three folder above
    sys.path.append(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    )

    from ga4_api.ga4_api import Ga4Client

    ga4_client = Ga4Client()
    weekly_output = run_weekly_report(
        data_args={
            "source": "api",
            "ga4_client": ga4_client,
            "property_id": "394327334",
            "dimensions": ["pagePath"],
            "metrics": WEEKLY_REPORT_METRICS,
            "start_date": WEEKLY_REPORT_DATA_RANGE[0],
            "end_date": WEEKLY_REPORT_DATA_RANGE[1],
        },
        domain="https://taxidrivers.it",
        n=10,
        max_workers=8,
        csv_output_path=os.path.join(WEEKLY_OUTPUT_DIR, WEEKLY_REPORT_OUTPUT_FILENAME),
        excel_output_path=os.path.join(
            WEEKLY_OUTPUT_DIR,
            f"top_articles_by_category_{WEEKLY_REPORT_DATA_RANGE[0].replace('-', '')}_{WEEKLY_REPORT_DATA_RANGE[1].replace('-', '')}.xlsx",
        ),
        output_dir=WEEKLY_OUTPUT_DIR,
        gemini_api_key=os.getenv("GEMINI_API_KEY"),
        use_gemini=False,
        use_template=False,
        sort_by_metric="screenPageViews",
    )
