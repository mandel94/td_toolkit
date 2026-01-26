import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time

from reports.map_ga4_categories import map_ga4_categories



class ArticleScraper:
    """
    Extensible scraper for enriching a DataFrame of articles with additional scraped features.
    Usage:
        scraper = ArticleScraper(domain="https://taxidrivers.it", features=["title", "author", "publication_date", "category"])
        enriched_df = scraper.scrape(df)
    """

    def __init__(self, domain, features=None, delay=4, max_workers=8):
        self.domain = domain
        self.features = features or [
            "title",
            "author",
            "publication_date",
            "category",
        ]
        self.delay = delay
        self.max_workers = max_workers

    def scrape_article(self, path):
        url = self.domain + path if path.startswith("/") else self.domain + "/" + path
        try:
            time.sleep(self.delay)
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            result = {"pagePath": path}
            if "title" in self.features:
                title_tag = soup.find(
                    "h1",
                    class_="mvp-post-title left entry-title",
                    itemprop="headline",
                )
                result["title"] = title_tag.text.strip() if title_tag else None
            if "author" in self.features:
                author_tag = soup.find("a", rel="author")
                result["author"] = author_tag.text.strip() if author_tag else None
            if "publication_date" in self.features:
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
                result["publication_date"] = (
                    pub_date.strftime("%Y-%m-%d") if pub_date else None
                )
            # Add extensibility for new features here
            return result
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {"pagePath": path, **{f: None for f in self.features}}

    def scrape(self, df):
        paths = df["pagePath"].tolist()
        results = []
        for idx, path in enumerate(paths):
            print(f"Scraping {idx+1} / {len(paths)}: {path}")
            result = self.scrape_article(path)
            results.append(result)
        scraped_df = pd.DataFrame(results)
        # Merge scraped features into original df
        merged = df.merge(scraped_df, on="pagePath", how="left")
        return merged