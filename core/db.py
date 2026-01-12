# core/db.py
import mysql.connector
from mysql.connector import pooling
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
    )
    return _pool

def get_conn():
    return _get_pool().get_connection()
