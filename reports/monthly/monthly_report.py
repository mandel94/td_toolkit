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

MOJIBAKE_MARKERS = ("Ã", "â€™", "â€œ", "â€", "Â")


def _fix_mojibake_text(value):
    if not isinstance(value, str) or not value:
        return value
    text = value.strip()
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text

    candidates = [text]
    for source_encoding in ("latin-1", "cp1252"):
        try:
            repaired = text.encode(source_encoding).decode("utf-8")
            if repaired:
                candidates.append(repaired.strip())
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue

    def _mojibake_score(s):
        return sum(s.count(marker) for marker in MOJIBAKE_MARKERS)

    best = min(candidates, key=_mojibake_score)
    return best


def _normalize_text_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    cols = columns
    if cols is None:
        cols = df.select_dtypes(include=["object"]).columns.tolist()

    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(_fix_mojibake_text)
    return df


def _decode_response_text(response):
    if not response.encoding or response.encoding.lower() in {"iso-8859-1", "latin-1"}:
        candidate = response.apparent_encoding or "utf-8"
    else:
        candidate = response.encoding

    for encoding in (candidate, "utf-8", "cp1252", "latin-1"):
        if not encoding:
            continue
        try:
            return response.content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return response.content.decode("utf-8", errors="replace")

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


def get_article_metadata(
    url,
    request_delay=10,
    retry_delay=10,
    max_retries=5,
    timeout_seconds=30,
):
    """
    Scrape the article page and return a tuple: (publication_date, author_name, article_title).
    Publication date is a datetime object or None.
    Author is a string or None.
    Title is a string or None.
    Retries on timeout until successful.
    request_delay: seconds to wait before each request
    retry_delay: base seconds to wait after timeout; multiplied by retry attempt
    max_retries: maximum timeout retries before giving up
    timeout_seconds: HTTP timeout for each request
    """
    import time
    import requests
    from requests.exceptions import Timeout

    attempt = 0
    while attempt <= max_retries:
        attempt += 1
        try:
            time.sleep(request_delay)
            response = requests.get(url, timeout=timeout_seconds)
            response.raise_for_status()
            soup = BeautifulSoup(_decode_response_text(response), "html.parser")
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
            author = _fix_mojibake_text(author_tag.text) if author_tag else None
            # Title
            title_tag = soup.find(
                "h1", class_="mvp-post-title left entry-title", itemprop="headline"
            )
            title = _fix_mojibake_text(title_tag.text) if title_tag else None
            return pub_date, author, title
        except Timeout:
            if attempt > max_retries:
                print(f"Timeout occurred for {url}, max retries reached.")
                return None, None, None
            backoff = retry_delay * attempt
            print(
                f"Timeout occurred for {url}, retrying in {backoff}s "
                f"({attempt}/{max_retries})..."
            )
            time.sleep(backoff)
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            time.sleep(retry_delay)
            return None, None, None
    return None, None, None


def scrape_article_metadata(
    paths,
    domain,
    metadata_collector=get_article_metadata,
    max_workers=8,
    request_delay=10,
    retry_delay=10,
    max_retries=5,
    progress_log_every=20,
):
    """
    Given a list of page paths, scrape publication date, author, and title for each article in parallel.
    Returns three lists: publication dates, authors, titles.
    """

    import time

    def scrape_one(path):
        url = domain + path if path.startswith("/") else domain + "/" + path
        pub_date, author, title = metadata_collector(
            url,
            request_delay=request_delay,
            retry_delay=retry_delay,
            max_retries=max_retries,
        )
        return pub_date, author, title

    total = len(paths)
    if total == 0:
        return [], [], []

    ordered_results = [None] * total
    completed = 0
    success_count = 0
    start_ts = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(scrape_one, path): idx for idx, path in enumerate(paths)
        }

        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            completed += 1
            path = paths[idx]
            try:
                result = future.result()
            except Exception as e:
                print(f"Error in worker for path index {idx}: {e}")
                result = (None, None, None)

            ordered_results[idx] = result
            if any(result):
                success_count += 1

            pub_date, author, title = result
            pub_date_str = pub_date.isoformat() if isinstance(pub_date, datetime) else "None"
            title_str = str(title).replace("\n", " ").strip() if title else "None"
            if len(title_str) > 80:
                title_str = title_str[:80] + "..."
            author_str = str(author) if author else "None"
            print(
                f"[{completed}/{total}] Scraped {path} | "
                f"title={title_str} | author={author_str} | pub_date={pub_date_str}"
            )

            if (
                completed == 1
                or completed % progress_log_every == 0
                or completed == total
            ):
                elapsed = time.time() - start_ts
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = total - completed
                eta = remaining / rate if rate > 0 else 0
                pct = (completed / total) * 100
                print(
                    f"Scraping progress: {completed}/{total} ({pct:.1f}%) | "
                    f"success={success_count} fail={completed - success_count} | "
                    f"elapsed={elapsed:.0f}s eta={eta:.0f}s"
                )

    failed_indices = [idx for idx, result in enumerate(ordered_results) if not any(result)]
    if failed_indices:
        print(
            f"Final retry pass started for {len(failed_indices)} failed URLs..."
        )
        for retry_pos, idx in enumerate(failed_indices, 1):
            path = paths[idx]
            try:
                result = scrape_one(path)
            except Exception as e:
                print(f"Final retry error for {path}: {e}")
                result = (None, None, None)

            ordered_results[idx] = result
            pub_date, author, title = result
            pub_date_str = pub_date.isoformat() if isinstance(pub_date, datetime) else "None"
            title_str = str(title).replace("\n", " ").strip() if title else "None"
            if len(title_str) > 80:
                title_str = title_str[:80] + "..."
            author_str = str(author) if author else "None"
            print(
                f"[Retry {retry_pos}/{len(failed_indices)}] Scraped {path} | "
                f"title={title_str} | author={author_str} | pub_date={pub_date_str}"
            )

        unresolved = sum(1 for idx in failed_indices if not any(ordered_results[idx]))
        print(
            f"Final retry pass completed: recovered={len(failed_indices) - unresolved} "
            f"still_failed={unresolved}"
        )

    pub_dates, authors, titles = zip(*ordered_results)
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
    request_delay=10,
    retry_delay=10,
    max_retries=5,
    chunk_pause_seconds=30,
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
        request_delay: delay before each request in seconds
        retry_delay: base delay after timeout in seconds
        max_retries: retries on timeout before giving up
        chunk_pause_seconds: pause between chunks in seconds
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
            chunk_paths,
            domain,
            max_workers=max_workers,
            request_delay=request_delay,
            retry_delay=retry_delay,
            max_retries=max_retries,
            progress_log_every=20,
        )
        all_pub_dates.extend(pub_dates)
        all_authors.extend(authors)
        all_titles.extend(titles)
        if i + chunk_size < len(paths):
            print(f"Chunk {i // chunk_size + 1} done.")
            print(f"Waiting {chunk_pause_seconds} seconds before next chunk...")
            import time

            time.sleep(chunk_pause_seconds)
    # Convert publication dates to JSON serializable format
    df["Publication Date"] = [
        d.isoformat() if isinstance(d, datetime) else None for d in all_pub_dates
    ]
    df["Author"] = all_authors
    df["Title"] = all_titles
    df = _normalize_text_columns(df)
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
        request_delay=10,
        retry_delay=10,
        max_retries=5,
        chunk_pause_seconds=30,
        excel_output_path=os.path.join(
            MONTHLY_OUTPUT_DIR, f"top_articles_{MONTHLY_PARAMETERS_MONTH}.xlsx"
        ),
    )
