import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
from models.Article import Article
from sqlalchemy.orm import sessionmaker

def get_articles_from_xml(file_path: str) -> List[Dict[str, Optional[str]]]:
    """
    Parses an XML file and extracts articles' information, returning a list of dictionaries.

    :param file_path: Path to the XML file that contains the article data.
    :returns: A list of dictionaries containing article information.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    channel = root[0]
    articles = channel.findall("item")
    
    namespaces: Dict[str, str] = {
        'content': "http://purl.org/rss/1.0/modules/content/",
        'wp': 'http://wordpress.org/export/1.2/'
    }
    taxidrivers_domain: str = 'https://www.taxidrivers.it'

    def _get_domain(link: str) -> str:
        """
        Extracts the domain path from a full URL.

        :param link: Full URL from which to extract the domain path.
        :returns: Domain path extracted from the URL.
        """
        return link.split(taxidrivers_domain)[1]

    def _get_text_from_element(element: Optional[ET.Element]) -> Optional[str]:
        """
        Helper function to safely extract text from an XML element.

        :param element: The XML element from which to extract text.
        :returns: The text content of the element or None if not found.
        """
        return element.text if element is not None else None

    def _get_yoast_meta_data(article: ET.Element, key: str, namespaces: Dict[str, str]) -> Optional[str]:
        """
        Extracts Yoast SEO meta information for a given key from the article.

        :param article: The article element to extract meta information from.
        :param key: The Yoast meta key to search for.
        :param namespaces: The namespaces used for querying XML elements.
        :returns: The meta value associated with the given key or None if not found.
        """
        meta = article.find(f'wp:postmeta/wp:meta_key[. = "{key}"]/../wp:meta_value', namespaces)
        return _get_text_from_element(meta)
    
    def _clean_html(html_path: str) -> str:
        if html_path.endswith('html'):
            return html_path
        else:
            path_match = re.search(r'.*\.html', html_path)
            if path_match:
                return path_match.group(0)
            return ""
    
    def _clean_article_data(article_data: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        article_data["pagepath"] = _clean_html(article_data["pagepath"] or "")
        return article_data

    def parse_article(article: ET.Element) -> Dict[str, Optional[str]]:
        """
        Extracts relevant information from a single article in the XML file.

        :param article: The XML element representing the article.
        :returns: A dictionary containing the extracted article information.
        """
        title: Optional[str] = _get_text_from_element(article.find('title'))
        link: Optional[str] = _get_text_from_element(article.find('link'))
        pubdate: Optional[str] = _get_text_from_element(article.find('pubDate'))
        category: Optional[str] = _get_text_from_element(article.find('category'))
        publication_tags: str = ", ".join([tag.text for tag in article.findall('.//category[@domain="post_tag"]') if tag.text])
        content: Optional[str] = _get_text_from_element(article.find('content:encoded', namespaces))
        post_id: Optional[str] = _get_text_from_element(article.find('wp:post_id', namespaces))
        post_views: Optional[str] = _get_yoast_meta_data(article, "post_views_count", namespaces)
        yoast_focus_keyword: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_focuskw", namespaces)
        yoast_metadesc: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_metadesc", namespaces)
        yoast_seo_score: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_linkdex", namespaces)
        yoast_content_readability_score: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_content_score", namespaces)
        yoast_keyword_synonyms: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_keywordsynonyms", namespaces)
        yoast_estimated_reading_time: Optional[str] = _get_yoast_meta_data(article, "_yoast_wpseo_estimated-reading-time-minutes", namespaces)

        if yoast_keyword_synonyms:
            yoast_keyword_synonyms = yoast_keyword_synonyms.replace('[', '').replace(']', '').replace('"', '')

        article_data: Dict[str, Optional[str]] = {
            'title': title,
            'link': link,
            'pagepath': _get_domain(link) if link else None,
            'pubdate': pubdate,
            'category': category,
            'publication_tags': publication_tags,
            'content': content,
            'post_id': post_id,
            'post_views': post_views,
            'yoast_focus_keyword': yoast_focus_keyword,
            'yoast_metadesc': yoast_metadesc,
            'yoast_seo_score': yoast_seo_score,
            'yoast_content_readability_score': yoast_content_readability_score,
            'yoast_keyword_synonyms': yoast_keyword_synonyms,
            'yoast_estimated_reading_time': yoast_estimated_reading_time
        }

        return _clean_article_data(article_data)

    return [parse_article(article) for article in articles]
