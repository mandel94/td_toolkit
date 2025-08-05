from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class MyBase(Base):
    __abstract__ = True

    def to_dict(self):
        # https://stackoverflow.com/questions/73146024/sqlalchemy-method-to-get-orm-object-as-dict
        return {field.name: getattr(self, field.name) for field in self.__table__.c}
