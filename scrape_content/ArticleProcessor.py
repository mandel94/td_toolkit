import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from typing import Callable, List, Optional, Any, Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from scrape_content.ArticleProcessor import ContentExtractor, StructureExtractor

from reports.map_ga4_categories import map_ga4_categories


class StructureExtractor:
    """
    Extracts structural properties from article HTML (e.g., layout, section count, etc.).
    Extensible via subclassing or strategy injection.
    """
    def __init__(self, extract_strategy: Optional[Callable[[str], Dict[str, Any]]] = None):
        self.extract_strategy = extract_strategy or self._default_extract_strategy

    def _default_extract_strategy(self, html: str) -> Dict[str, Any]:
        # Placeholder: return empty dict for now
        return {}

    def extract(self, html: str) -> Dict[str, Any]:
        return self.extract_strategy(html)


class ContentExtractor:
    """
    Extracts content metadata from article HTML (title, author, publication date, etc.).
    Extensible via subclassing or strategy injection.
    """
    def __init__(self, extract_strategy: Optional[Callable[[str, Optional[List[str]], Optional[str]], Dict[str, Any]]] = None):
        self.extract_strategy = extract_strategy or self._default_extract_strategy

    def _default_extract_strategy(self, html: str, features: Optional[List[str]] = None, path: Optional[str] = None) -> Dict[str, Any]:
        features = features or ["title", "author", "publication_date", "category"]
        soup = BeautifulSoup(html, "html.parser")
        result: Dict[str, Any] = {"pagePath": path}
        if "title" in features:
            title_tag = soup.find("h1", class_="mvp-post-title left entry-title", itemprop="headline")
            result["title"] = title_tag.text.strip() if title_tag else None
        if "author" in features:
            author_tag = soup.find("a", rel="author")
            result["author"] = author_tag.text.strip() if author_tag else None
        if "publication_date" in features:
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
            result["publication_date"] = pub_date.strftime("%Y-%m-%d") if pub_date else None
        if "category" in features:
            result["category"] = map_ga4_categories(path) if path else None
        return result

    def extract(self, html: str, features: Optional[List[str]] = None, path: Optional[str] = None) -> Dict[str, Any]:
        return self.extract_strategy(html, features=features, path=path)

class ArticleProcessor:
    """
    Processes HTML code to extract article data using ContentExtractor and StructureExtractor.
    Returns a dict with 'content_metadata' and 'structure_metadata' fields.
    Usage:
        processor = ArticleProcessor(content_extractor=..., structure_extractor=...)
        result = processor.process(html, features=[...], path=...)
    """
    def __init__(
        self,
        content_extractor: Optional[ContentExtractor] = None,
        structure_extractor: Optional[StructureExtractor] = None,
    ):
        self.content_extractor = content_extractor or ContentExtractor()
        self.structure_extractor = structure_extractor or StructureExtractor()

    def process(
        self,
        html: str,
        features: Optional[List[str]] = None,
        path: Optional[str] = None,
        as_json: bool = False,
    ) -> Union[Dict[str, Any], str]:
        content_metadata = self.content_extractor.extract(html, features=features, path=path)
        structure_metadata = self.structure_extractor.extract(html)
        result = {
            "content_metadata": content_metadata,
            "structure_metadata": structure_metadata,
        }
        if as_json:
            import json
            return json.dumps(result, ensure_ascii=False)
        return result

    def process_many(
        self,
        html_list: List[str],
        features: Optional[List[str]] = None,
        paths: Optional[List[str]] = None,
        as_dataframe: bool = False,
    ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        results: List[Dict[str, Any]] = []
        for i, html in enumerate(html_list):
            path = paths[i] if paths else None
            results.append(self.process(html, features=features, path=path))
        if as_dataframe:
            return pd.DataFrame(results)
        return results


if __name__ == "__main__":
    # Example HTML (replace with real HTML for real test)
    example_html = '''
    <html>
        <body>
            <h1 class="mvp-post-title left entry-title" itemprop="headline">Test Article Title</h1>
            <a rel="author">Jane Doe</a>
            <time datetime="2025-09-30T12:00:00">2025-09-30</time>
        </body>
    </html>
    '''
    processor = ArticleProcessor()
    # Single article test
    data = processor.process(example_html, path="/test-article")
    print("Single article as dict:", data)
    # As JSON
    data_json = processor.process(example_html, path="/test-article", as_json=True)
    print("Single article as JSON:", data_json)
    # Multiple articles test
    html_list = [example_html, example_html]
    paths = ["/test-article-1", "/test-article-2"]
    df = processor.process_many(html_list, paths=paths, as_dataframe=True)
    print("Multiple articles as DataFrame:")
    print(df)