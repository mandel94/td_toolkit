
from sqlalchemy import Column, Integer, TIMESTAMP, Float, func, String
from models.base import MyBase
from sqlalchemy.dialects.postgresql import JSON
from engine import engine



class SqlQueryLog(MyBase):
    __tablename__ = 'sql_query_logs'

    id = Column(Integer, primary_key=True)
    query = Column(String, nullable=False)
    parameters = Column(JSON, nullable=True)  # JSON column to store query parameters
    execution_time = Column(Float, nullable=False)  # Execution time of the query
    timestamp = Column(TIMESTAMP, default=func.now())  # Default to current timestamp


MyBase.metadata.create_all(engine)