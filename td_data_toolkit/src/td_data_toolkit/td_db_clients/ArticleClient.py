from typing import Dict, List, Any
import requests
from ..utils.xml_utils import get_articles_from_xml
import urllib.parse


class TDArticleClient:
    def __init__(self, base_url="http://localhost:8000"):
        """
        Initialize the TDArticleClient with a base URL.

        :param base_url: The base URL of the API.
        :type base_url: str
        """
        self.base_url = base_url

    def upsert_articles(self, articles: List[dict]):
        """
        Upsert a list of articles.
        - If an article with the given pagepath exists, update it.
        - Otherwise, create a new article.

        :param articles: List of articles to be processed.
        :type articles: List[dict]
        :return: The HTTP response from the API.
        :rtype: requests.models.Response

        **Example**::

            articles = [
                {"pagepath": "/about", "title": "About Us", "content": "Content of about page"},
                {"pagepath": "/contact", "title": "Contact Us", "content": "Content of contact page"}
            ]
            client = TDArticleClient(base_url="http://localhost:8000")
            response = client.upsert_articles(articles)
            print(response.status_code)  # Output: 200 (if successful)
        """
        response = requests.put(f"{self.base_url}/articles/batch", json=articles)
        return response

    def get_articles_by_pagepaths(self, pagepaths: List[str]):
        """
        Retrieve a list of articles by their pagepaths.

        :param pagepaths: List of pagepaths to search for.
        :type pagepaths: List[str]
        :return: The HTTP response from the API.
        :rtype: requests.models.Response

        **Example**::

            pagepaths = ["/about", "/contact"]
            client = TDArticleClient(base_url="http://localhost:8000")
            response = client.get_articles_by_pagepaths(pagepaths)
            print(response.json())  # Output: List of articles matching the pagepaths
        """
        response = requests.post(f"{self.base_url}/articles/search/batch", json={"pagepaths": pagepaths})
        return response
    
    def get_by_pagepath(self, pagepath: str):
        """
        Add documentation
        """
        encoded_pagepath = urllib.parse.quote(pagepath)
        response = requests.get(f"{self.base_url}/articles/search{encoded_pagepath}")
        return response


class WpArticleClient(TDArticleClient):
    """TDArticleClient to perform CRUD operations on WordPress data."""

    def add_wordpress_articles_from_file(self, file_path: str) -> Dict[str, Any]:
        """
        Add WordPress articles from a local file path.

        :param file_path: The path pointing to the local XML file exported from WordPress.
        :type file_path: str
        :return: A dictionary containing the operation status, message, and metadata.
        :rtype: Dict[str, Any]

        **Example**::

            file_path = "wp_articles.xml"
            client = WpArticleClient(base_url="http://localhost:8000")
            result = client.add_wordpress_articles_from_file(file_path)
            print(result["status"])  # Output: 'success'
            print(result["articles_imported"])  # Output: Number of articles imported

        **Notes**:
        - The method assumes that the XML file contains a valid structure and that the function `get_articles_from_xml(file_path)` can parse it into a list of articles.
        """
        wp_articles: List[dict] = get_articles_from_xml(file_path)
        wp_articles = [article for article in wp_articles if article.get("title")]  # Only keep articles with a title

        if not wp_articles:
            return {
                "status": "error",
                "message": "No valid articles found in the XML file.",
                "file_path": file_path,
                "articles_imported": 0
            }
        print("Upserting articles")
        self.upsert_articles(wp_articles)

        return {
            "status": "success",
            "message": "Articles successfully imported and updated.",
            "file_path": file_path,
            "articles_imported": len(wp_articles)
        }
