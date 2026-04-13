import requests
from bs4 import BeautifulSoup
from datetime import datetime


MOJIBAKE_MARKERS = ("Ã", "â€™", "â€œ", "â€", "Â")


def _fix_mojibake_text(value):
    """Attempt to repair common UTF-8/Latin-1 mojibake artifacts."""
    if not isinstance(value, str) or not value:
        return value

    text = value.strip()
    if not any(marker in text for marker in MOJIBAKE_MARKERS):
        return text

    try:
        repaired = text.encode("latin-1").decode("utf-8")
        return repaired.strip() if repaired else text
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _get_response_text(response):
    """Decode HTTP response with safer encoding strategy for WP pages."""
    content = response.content

    # Prefer apparent encoding when requests defaults to latin-1 without certainty.
    if not response.encoding or response.encoding.lower() in {"iso-8859-1", "latin-1"}:
        candidate = response.apparent_encoding or "utf-8"
    else:
        candidate = response.encoding

    for encoding in (candidate, "utf-8", "cp1252", "latin-1"):
        if not encoding:
            continue
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue

    return content.decode("utf-8", errors="replace")

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
        soup = BeautifulSoup(_get_response_text(response), "html.parser")
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
        title_tag = soup.find("h1", class_="mvp-post-title left entry-title", itemprop="headline")
        title = _fix_mojibake_text(title_tag.text) if title_tag else None
        if title:
            title = title.title()
        return pub_date, author, title
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None, None, None
