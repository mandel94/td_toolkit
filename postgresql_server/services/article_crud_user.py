from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from orm_utils import get_props
from models.Article import Article

# Retrieve the properties of the Article model
article_table_props = get_props(Article)

class ArticleUser:
    """ 
    Implements CRUD operations for managing `Article` data in the database. 

    Provides methods to create, retrieve, update, and delete articles.
    """

    def create(self, db: Session, article_props: Dict[str, Any]) -> Article:
        """ 
        Create a new article with specified properties.
        """
        valid_props = {prop: article_props[prop] for prop in article_props if prop in article_table_props}
        db_article = Article(**valid_props)
        db.add(db_article)
        db.commit()
        db.refresh(db_article)
        return db_article

    def get_by_id(self, db: Session, article_id: int) -> Optional[Article]:
        """ 
        Retrieve an article by its ID.
        """
        stmt = select(Article).where(Article.id == article_id)
        db_article = db.scalar(stmt)
        return db_article

    def get_by_pagepath(self, db: Session, pagepath: str) -> Article:
        """ 
        Retrieve an article by its page path.
        """
        stmt = select(Article).where(Article.pagepath == pagepath)
        db_article = db.scalar(stmt)
        return db_article

    def get_by_pagepaths(self, db: Session, pagepaths: List[str]) -> List[Article]:
        """
        Retrieve multiple articles by their page paths.

        :param db: The database session.
        :type db: Session
        :param pagepaths: A list of page paths to search for.
        :type pagepaths: List[str]
        :return: A list of found articles.
        :rtype: List[Article]
        """
        stmt = select(Article).where(Article.pagepath.in_(pagepaths))
        db_articles = db.scalars(stmt).all()
        return db_articles

    def get_by_properties(self, db: Session, filters: Dict[str, Any], skip: int = 0, limit: int = 10) -> List[Article]:
        """
        Retrieve articles matching specified properties and return as a list of Article objects.
        """
        stmt = select(Article)
        
        for key, value in filters.items():
            if hasattr(Article, key):
                stmt = stmt.where(getattr(Article, key) == value)

        stmt = stmt.offset(skip).limit(limit)
        db_articles = db.scalars(stmt).all()
        return db_articles

    def get_all(self, db: Session, skip: int = 0, limit: int = 10) -> List[Article]:
        """ 
        Retrieve all articles with pagination.
        """
        stmt = select(Article).offset(skip).limit(limit)
        db_article_list = db.scalars(stmt).all()
        return db_article_list

    def update(self, db: Session, article_id: int, article_props: Dict[str, Any]) -> Optional[Article]:
        """ 
        Update an existing article by its ID.
        """
        db_article = self.get_by_id(db, article_id)
        if db_article:
            for key, value in article_props.items():
                if key in article_table_props:
                    setattr(db_article, key, value)
            db.commit()
            db.refresh(db_article)
        return db_article

    def update_by_pagepath(self, db: Session, pagepath: str, article_props: Dict[str, Any]) -> Optional[Article]:
        """ 
        Update an existing article by its page path.
        """
        db_article = self.get_by_pagepath(db, pagepath)
        if db_article:
            for key, value in article_props.items():
                if key in article_table_props:
                    setattr(db_article, key, value)
            db.commit()
            db.refresh(db_article)
        return db_article

    def delete(self, db: Session, article_id: int) -> bool:
        """ 
        Delete an article by its ID.
        """
        db_article = self.get_by_id(db, article_id)
        if not db_article:
            return False
        db.delete(db_article)
        db.commit()
        return True
