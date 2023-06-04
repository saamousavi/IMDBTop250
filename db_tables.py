from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Movie(Base):
    __tablename__ = "movie"


class Person(Base):
    __tablename__ = "person"


class Cast(Base):
    __tablename__ = "cast"


class Crew(Base):
    __tablename__ = "crew"


class Genre(Base):
    __tablename__ = "genre"
