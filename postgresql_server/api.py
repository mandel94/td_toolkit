from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from session_utils import get_session
from services.article_crud_user import ArticleUser
from api_utils import response_json_wrapper
from fastapi import Query
from pydantic import BaseModel
from typing import Any, List, Dict
from models.Article import Article
import urllib.parse

class BatchSearchRequestModel(BaseModel):
    pagepaths: list[str]


app = FastAPI()
article_user = ArticleUser()



@app.get("/")
def home() -> str:
    """
    Home route to verify the API is working.
    """
    return "Benvenuti nel database di TaxiDrivers"


@app.post("/articles/")
def create_article(article_props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new article.

    :param article_props: A dictionary containing article properties to be saved in the database.
    :type article_props: Dict[str, Any]


    :return: The created article object.
    :rtype: Dict[str, Any]
    """
    with get_session() as db:
        article = article_user.create(db=db, article_props=article_props)
        return response_json_wrapper(article)


@app.post("/articles/search/batch", response_model=False)
def get_articles(playload: BatchSearchRequestModel) -> list[Article]:
    """Retrieve articles from a list of pagepaths.

    :param payload: A list of pagepaths.
    :type pagepaths: List[str]


    :return: A list of articles.
    :rtype: List[Article]
    :raises HTTPException: If no article is found.
    """
    with get_session() as db:
        articles = article_user.get_by_pagepaths(db, playload.pagepaths)
        if not articles:
            raise HTTPException(
                status_code=404, detail="No articles found for the provided pagepaths."
            )
        {"query": articles}
        return articles
    
@app.get("/articles/{pagepath}", response_model=False)
def get_article_by_pagepath(pagepath: str) -> Article:
    """Retrieve a single article based on its pagepath.

    :param pagepath: The pagepath to search for.
    :type pagepath: str

    :return: The article corresponding to the pagepath.
    :rtype: Article
    :raises HTTPException: If no article is found.
    """
    # Assuming article_user.get_by_pagepath returns a single article or None
    decoded_pagepath = urllib.parse.unquote(pagepath)
    with get_session() as db:
        article = article_user.get_by_pagepath(db, decoded_pagepath)
        if not article:
            raise HTTPException(
                status_code=404,
                detail=f"No article found for the pagepath '{pagepath}'."
            )
        return article




@app.get("/articles/{article_id}")
def get_article(article_id: int) -> Dict[str, Any]:
    """
    Retrieve an article by its ID.

    :param article_id: The ID of the article to retrieve.
    :type article_id: int

    :return: The article object.
    :rtype: Dict[str, Any]
    :raises HTTPException: If the article is not found.
    """
    with get_session() as db:
        article = article_user.get_by_id(db=db, article_id=article_id)
        if not article:
            raise HTTPException(
                status_code=404, detail=f"Article with id: {article_id} not found"
            )
        return response_json_wrapper(article)


@app.put("/articles/{article_id}")
def update_article(article_id: int, article_props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update an existing article by its ID.

    :param article_id: The ID of the article to update.
    :type article_id: int
    :param article_props: A dictionary containing the article properties to update.
    :type article_props: Dict[str, Any]


    :return: The updated article object.
    :rtype: Dict[str, Any]
    :raises HTTPException: If the article is not found.
    """
    with get_session() as db:
        article = article_user.update(
            db=db, article_id=article_id, article_props=article_props
        )
        if not article:
            raise HTTPException(status_code=404, detail="Article not found")
        return response_json_wrapper(article)


@app.put("/articles/{pagepath}")
def update_article_by_pagepath(pagepath: str, article_props: Dict[str, Any], db: Session = Depends(get_session)) -> Dict[str, Any]:
    """
    Update an existing article by its pagepath.

    :param pagepath: The pagepath of the article to update.
    :type pagepath: str
    :param article_props: A dictionary containing the article properties to update.
    :type article_props: Dict[str, Any]
    :param db: The database session dependency.
    :type db: Session

    :return: The updated article object.
    :rtype: Dict[str, Any]
    :raises HTTPException: If the article is not found.
    """
    article = article_user.update_by_pagepath(db, pagepath, article_props)
    if not article:
        raise HTTPException(
            status_code=404, detail=f"Article with pagepath: {pagepath} not found"
        )
    return response_json_wrapper(article)


@app.put("/articles/batch")
def upsert_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Upsert a list of articles.
    - If an article with the given pagepath exists, update it.
    - Otherwise, create a new article.

    :param articles: List of articles to be processed.
    :type articles: List[Dict[str, Any]]

    :return: A list of processed articles (created or updated).
    :rtype: List[Dict[str, Any]]
    """
    with get_session() as db:
        processed_articles = []

        print("Received articles:", articles)

        pagepaths = [
            article["pagepath"] for article in articles if "pagepath" in article
        ]
        existing_articles = {
            article.pagepath: article
            for article in article_user.get_by_pagepaths(db, pagepaths)
        }

        for article_props in articles:
            pagepath = article_props.get("pagepath")
            if not pagepath:
                continue  # Skip if no pagepath provided

            if pagepath in existing_articles:
                updated_article = article_user.update_by_pagepath(
                    db, pagepath, article_props
                )
                processed_articles.append(updated_article)
            else:
                new_article = article_user.create(db, article_props)
                processed_articles.append(new_article)

        return response_json_wrapper(processed_articles)


@app.delete("/articles/{article_id}")
def delete_article(article_id: int) -> Dict[str, Any]:
    """
    Delete an article by its ID.

    :param article_id: The ID of the article to delete.
    :type article_id: int

    :return: A success message if the article is deleted.
    :rtype: Dict[str, Any]
    :raises HTTPException: If the article is not found.
    """
    with get_session() as db:
        success = article_user.delete(db=db, article_id=article_id)
        if not success:
            raise HTTPException(status_code=404, detail="Article not found")
        return response_json_wrapper({"detail": "Article deleted successfully"})