import requests
from bs4 import BeautifulSoup
from datetime import datetime

def get_article_metadata(url):
    """
    Scrape the article page and return a tuple: (publication_date, author_name, article_title).
    Publication date is a datetime object or None.
    Author is a string or None.
    Title is a string or None (capitalized).
    """
    try:
        response = requests.get(url, timeout=10)
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
        title_tag = soup.find("h1", class_="mvp-post-title left entry-title", itemprop="headline")
        title = title_tag.text.strip() if title_tag else None
        if title:
            title = title.title()
        return pub_date, author, title
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None, None, None
