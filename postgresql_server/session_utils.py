from contextlib import contextmanager
from sqlalchemy.orm import Session
from engine import engine
from typing import Generator

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Create a new SQLAlchemy session and manage its lifecycle.

    :yield: A SQLAlchemy session object.
    :rtype: Generator[Session, None, None]

    :raises Exception: If an error occurs during the database transaction, 
        the session is rolled back and the exception is re-raised.
    """
    session = Session(bind=engine)
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()
