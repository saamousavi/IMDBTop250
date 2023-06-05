from sqlalchemy.orm import DeclarativeBase
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class Movie(Base):
    __tablename__ = "movie"

    id: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    title: Mapped[str] = mapped_column(String(128), nullable=True)
    year: Mapped[int] = mapped_column(Integer)
    runtime: Mapped[int] = mapped_column(Integer)
    parental_guide: Mapped[str] = mapped_column(String(8))
    budget: Mapped[int] = mapped_column(Integer)
    gross_us_canada: Mapped[int] = mapped_column(Integer)
    gross_worldwide: Mapped[int] = mapped_column(Integer)


class Person(Base):
    __tablename__ = "person"

    id: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(String(32), nullable=False)


class Cast(Base):
    __tablename__ = "cast"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    movie_id: Mapped[str] = mapped_column(ForeignKey('Movie.id'), nullable=False)
    person_id: Mapped[str] = mapped_column(ForeignKey('Person.id'), nullable=False)


class Crew(Base):
    __tablename__ = "crew"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    movie_id: Mapped[str] = mapped_column(ForeignKey('Movie.id'), nullable=False)
    person_id: Mapped[str] = mapped_column(ForeignKey('Person.id'), nullable=False)
    role: Mapped[str] = mapped_column(String(8), nullable=False)


class Genre(Base):
    __tablename__ = "genre"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    movie_id: Mapped[str] = mapped_column(ForeignKey('Movie.id'), nullable=False)
    genre: Mapped[str] = mapped_column(String(16), nullable=False)
