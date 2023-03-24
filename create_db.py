from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

"""Создание базе данных в PostgreSQL"""

DATABASE_URI = 'postgresql://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(DATABASE_URI.format(
    username='postgres',
    password='admin',
    host='localhost',
    port=5432,
    database='star_db'
))

if not database_exists(engine.url):
    create_database(engine.url)