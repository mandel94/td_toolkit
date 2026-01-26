import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Union, IO
from urllib.parse import urlparse
import pandas as pd

def get_articles_from_xml(file_path: Optional[str] = None, file_like: Optional[IO] = None, as_dataframe: bool = False) -> Union[List[Dict[str, Optional[str]]], pd.DataFrame]:
    """
    Parses an XML file or file-like object and extracts articles' information.

    :param file_path: Path to the XML file.
    :param file_like: File-like object (e.g., FastAPI UploadFile).
    :param as_dataframe: Return a pandas DataFrame if True, else a list of dicts.
    :returns: Parsed articles as a DataFrame or list of dictionaries.
    """
    try:
        if file_like:
            tree = ET.parse(file_like)
        elif file_path:
            tree = ET.parse(file_path)
        else:
            raise ValueError("Either file_path or file_like must be provided.")
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return pd.DataFrame() if as_dataframe else []
    except Exception as e:
        print(f"General error while parsing XML: {e}")
        return pd.DataFrame() if as_dataframe else []

    channel = root.find('channel')
    if not channel:
        print("Error: No <channel> element found in the XML.")
        return pd.DataFrame() if as_dataframe else []

    articles = channel.findall("item")
    namespaces = {
        'content': "http://purl.org/rss/1.0/modules/content/",
        'wp': 'http://wordpress.org/export/1.2/'
    }

    def _get_domain(link: str) -> str:
        parsed_url = urlparse(link)
        return parsed_url.path.lstrip('/')

    def _get_text(element: Optional[ET.Element]) -> Optional[str]:
        return element.text if element is not None else None

    def _get_yoast_meta(article: ET.Element, key: str) -> Optional[str]:
        try:
            meta = article.find(f'wp:postmeta/wp:meta_key[. = "{key}"]/../wp:meta_value', namespaces)
            return _get_text(meta)
        except Exception as e:
            print(f"Error extracting Yoast {key}: {e}")
            return None

    def _clean_html(path: str) -> str:
        if path.endswith('.html'):
            return path
        match = re.search(r'.*\.html', path)
        return match.group(0) if match else ""

    def _clean_article(article_data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        if article_data.get("pagepath"):
            article_data["pagepath"] = _clean_html(article_data["pagepath"])
        return article_data

    def parse_article(article: ET.Element) -> Dict[str, Optional[str]]:
        data = {
            'title': _get_text(article.find('title')),
            'link': _get_text(article.find('link')),
            'pubdate': _get_text(article.find('pubDate')),
            'category': _get_text(article.find('category')),
            'publication_tags': ", ".join([t.text for t in article.findall('.//category[@domain="post_tag"]') if t.text]),
            'content': _get_text(article.find('content:encoded', namespaces)),
            'post_id': _get_text(article.find('wp:post_id', namespaces))
        }

        yoast_keys = [
            "post_views_count", "_yoast_wpseo_focuskw", "_yoast_wpseo_metadesc",
            "_yoast_wpseo_linkdex", "_yoast_wpseo_content_score", "_yoast_wpseo_keywordsynonyms",
            "_yoast_wpseo_estimated-reading-time-minutes"
        ]

        for key in yoast_keys:
            data[key] = _get_yoast_meta(article, key)

        if data.get("yoast_keyword_synonyms"):
            data["yoast_keyword_synonyms"] = data["yoast_keyword_synonyms"].replace('[', '').replace(']', '').replace('"', '')

        if data['link']:
            data["pagepath"] = _get_domain(data['link'])

        return _clean_article(data)

    articles_data = [parse_article(article) for article in articles]

    return pd.DataFrame(articles_data) if as_dataframe else articles_data
