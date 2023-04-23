from collections.abc import Iterable
import os
from dotenv import load_dotenv

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_batch

import uuid
from dataclasses import dataclass, field, fields, astuple
import datetime


load_dotenv()


@dataclass
class FilmWork:
    title: str
    description: str
    file_path: str
    type: str
    creation_date: datetime.datetime
    created: datetime.datetime
    modified: datetime.datetime
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __init__(self, uuid4, title, description, creation_date, file_path, rating, type, created, modified):
        self.id = uuid4
        self.title = title
        self.description = description
        self.creation_date = creation_date
        self.file_path = file_path
        self.rating = rating
        self.type = type
        self.created = created
        self.modified = modified


@dataclass
class Genre:
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __init__(self, uuid4, name, description, created, modified):
        self.id = uuid4
        self.name = name
        self.description = description
        self.created = created
        self.modified = modified


@dataclass
class Person:
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __init__(self, uuid4, full_name, created, modified):
        self.id = uuid4
        self.full_name = full_name
        self.created = created
        self.modified = modified


@dataclass
class GenreFilmWork:
    created: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __init__(self, uuid4, film_work_id, genre_id, created):
        self.id = uuid4
        self.film_work_id = film_work_id
        self.genre_id = genre_id
        self.created = created


@dataclass
class PersonFilmWork:
    role: str
    created: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)

    def __init__(self, uuid4, film_work_id, person_id, role, created):
        self.id = uuid4
        self.film_work_id = film_work_id
        self.person_id = person_id
        self.role = role
        self.created = created


def get_sqlite_config(connection: sqlite3.Connection):
    curs = connection.cursor()
    curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
    data = curs.fetchall()
    tables_name = [name[0] for name in data]
    return tables_name


def load_from_sqlite(connection: sqlite3.Connection, table_name: list[str], max_range: int):
    curs = connection.cursor()
    curs.execute("SELECT * FROM {0};".format(table_name))
    for amount_rows in range(0, max_range, 1000):
        table_sql_data = yield curs.fetchmany(1000)


def sqlite_to_dataclass(rows: list[tuple], table_name):
    tables_data = []
    exec('tables_data.append([{0}(*dataclass) for dataclass in rows])'.format(
        table_name.title().replace('_', '')))
    return tables_data[0]


def save_to_pg(conn: _connection, tables: Iterable[dataclass], table_name: str):
    with conn.cursor() as cursor:
        iter_table = (astuple(table_data) for table_data in tables)
        insert_keys = tuple([field.name for field in fields(tables[0])])
        sql_row = """INSERT INTO content.{0} {1} VALUES ({2}%s) ON CONFLICT DO NOTHING""".format(table_name, insert_keys, str('%s, '*(len(insert_keys)-1)))
        sql_row = sql_row.replace("'", "")
        execute_batch(cursor, sql_row, iter_table)


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'), 'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST', '127.0.0.1'), 'port': os.environ.get('DB_PORT', 5432)}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        max_range = int(os.environ.get('MAX_RANGE', 10000000))
        sqlite_config = get_sqlite_config(sqlite_conn)
        for table_name in sqlite_config:
            from_sqlite_data = load_from_sqlite(sqlite_conn, table_name, max_range)
            for rows in from_sqlite_data:
                if not rows:
                    break
                save_to_pg(pg_conn, sqlite_to_dataclass(rows, table_name), table_name)
