from sqlalchemy.inspection import inspect
from typing import List, Any, Union
from sqlalchemy.orm import MappedClassProtocol 
 


def get_props(orm_model: MappedClassProtocol) -> List[str]:
    """
    Retrieve the property names (columns) of an SQLAlchemy ORM class.

    :param orm_class: The SQLAlchemy ORM class to inspect.
    :type orm_class: Any

    :return: A list of property names (column names) of the ORM class.
    :rtype: List[str]
    """
    inspector = inspect(orm_model)
    return [prop.key for prop in inspector.mapper.column_attrs]


def from_model_to_dict(orm_model: MappedClassProtocol) -> dict:
    return {
        column.name: getattr(orm_model, column.name)
        for column in orm_model.__table__.columns
    }





