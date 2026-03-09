"""
db_config.py – Nawiązywanie połączeń i sprawdzanie statusu (ping)
dla PostgreSQL, MySQL, MongoDB i Redis.
"""

import psycopg2
import mysql.connector
from pymongo import MongoClient
import redis

# ─── Domyślne parametry połączeń ──────────────────────────────────────────────

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "hospital_db",
    "user": "admin",
    "password": "password",
}

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "hospital_db",
    "user": "root",
    "password": "password",
}

MONGO_URI = "mongodb://admin:password@localhost:27017"
MONGO_DB = "ztdb"

REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 0,
}


# ─── Funkcje połączeń ─────────────────────────────────────────────────────────

def get_pg_connection():
    """Zwraca nowe połączenie do PostgreSQL."""
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    return conn


def get_mysql_connection():
    """Zwraca nowe połączenie do MySQL."""
    conn = mysql.connector.connect(**MYSQL_CONFIG, autocommit=True)
    return conn


def get_mongo_client():
    """Zwraca klienta MongoClient i obiekt bazy danych."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[MONGO_DB]
    return client, db


def get_redis_client():
    """Zwraca klienta Redis."""
    return redis.Redis(**REDIS_CONFIG, decode_responses=True, socket_timeout=3)


# ─── Ping – sprawdzanie dostępności baz ───────────────────────────────────────

def ping_postgresql() -> bool:
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return True
    except Exception:
        return False


def ping_mysql() -> bool:
    try:
        conn = get_mysql_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return True
    except Exception:
        return False


def ping_mongodb() -> bool:
    try:
        client, _ = get_mongo_client()
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


def ping_redis() -> bool:
    try:
        r = get_redis_client()
        return r.ping()
    except Exception:
        return False


def ping_all() -> dict[str, bool]:
    """Zwraca słownik {nazwa_bazy: True/False}."""
    return {
        "PostgreSQL": ping_postgresql(),
        "MySQL": ping_mysql(),
        "MongoDB": ping_mongodb(),
        "Redis": ping_redis(),
    }
