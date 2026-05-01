import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(1, 10, dsn=os.environ["DATABASE_URL"])
    return _pool


def get_conn():
    return _get_pool().getconn()


def put_conn(conn):
    _get_pool().putconn(conn)


class db_cursor:
    """Context manager that borrows a connection from the pool and returns it."""
    def __init__(self, dict_cursor: bool = True):
        self.dict_cursor = dict_cursor
        self.conn = None
        self.cur = None

    def __enter__(self):
        self.conn = get_conn()
        factory = RealDictCursor if self.dict_cursor else None
        self.cur = self.conn.cursor(cursor_factory=factory)
        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        put_conn(self.conn)
