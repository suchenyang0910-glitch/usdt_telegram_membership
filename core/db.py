# core/db.py
import mysql.connector
from mysql.connector import pooling
from mysql.connector.errors import OperationalError, InterfaceError
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

_pool: pooling.MySQLConnectionPool | None = None


def _get_pool() -> pooling.MySQLConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    _pool = pooling.MySQLConnectionPool(
        pool_name="main_pool",
        pool_size=10,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        autocommit=True,
        pool_reset_session=True,
        connection_timeout=10,
    )
    return _pool

def get_conn():
    global _pool
    last_err: Exception | None = None
    for _ in range(3):
        try:
            conn = _get_pool().get_connection()
            try:
                conn.ping(reconnect=True, attempts=3, delay=1)
            except Exception:
                conn.ping(reconnect=True, attempts=3, delay=1)
            return conn
        except (OperationalError, InterfaceError) as e:
            last_err = e
            _pool = None
            continue
    raise last_err or OperationalError("MySQL Connection not available.")
