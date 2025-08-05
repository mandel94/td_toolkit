import logging
import functools
from models.SqlQueryLog import SqlQueryLog
from sqlalchemy.orm import Session
import time
import json
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from datetime import datetime
from engine import engine

logging.basicConfig(filename='errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def suppress_exception():
    """
    Decorator to catch and log exceptions while allowing execution to continue.

    :return: Decorator function
    :rtype: function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.error("Error in function '%s': %s", func.__name__, str(e), exc_info=True)
                return None  # Function execution continues without raising an error
        return wrapper
    return decorator

# SQL Query Execution Logging
logger = logging.getLogger("td_db.sqltime")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("sqltime.log")
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """
    Event listener for SQLAlchemy before executing a cursor operation.

    :param conn: Database connection
    :type conn: sqlalchemy.engine.base.Connection
    :param cursor: Database cursor
    :type cursor: Any
    :param statement: SQL statement
    :type statement: str
    :param parameters: Query parameters
    :type parameters: tuple or dict
    :param context: Execution context
    :type context: Any
    :param executemany: Flag for batch execution
    :type executemany: bool
    """
    conn.info.setdefault("query_start_time", []).append(time.time())
    logger.debug("Start Query: %s", statement)

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """
    Event listener for SQLAlchemy after executing a cursor operation.

    :param conn: Database connection
    :type conn: sqlalchemy.engine.base.Connection
    :param cursor: Database cursor
    :type cursor: Any
    :param statement: SQL statement
    :type statement: str
    :param parameters: Query parameters
    :type parameters: tuple or dict
    :param context: Execution context
    :type context: Any
    :param executemany: Flag for batch execution
    :type executemany: bool
    """
    total = time.time() - conn.info["query_start_time"].pop(-1)
    logger.debug("Query Complete!")
    logger.debug("Total Time: %f", total)

def log_query(func):
    """
    Decorator to log SQL queries and their execution times.

    :param func: Function to be wrapped
    :type func: function
    :return: Wrapped function
    :rtype: function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = Session(bind=engine)
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        query = str(func.__name__)  # Placeholder for query
        parameters_json = json.dumps(kwargs, default=str)
        
        with session.begin():
            new_log = SqlQueryLog(
                query=query,
                parameters=parameters_json,
                execution_time=execution_time
            )
            session.add(new_log)
        session.commit()
        return result
    
    return wrapper
