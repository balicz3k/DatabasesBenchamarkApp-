"""
core/database.py – Zarządzanie połączeniami do 4 systemów bazodanowych.
Wzorzec: Abstract Factory + Connection Manager.
"""

from abc import ABC, abstractmethod
from enum import Enum

import struct as _struct

import psycopg2
import mysql.connector
import mysql.connector.connection as _mc_conn_module
import mysql.connector.abstracts as _mc_abs_module

# platform.architecture() hangs on some Windows systems (spawns a subprocess).
# mysql.connector calls it via get_platform() inside _add_default_conn_attrs().
# Patch both module references before any connection is made.
def _safe_get_platform():
    _arch = "x86_64" if _struct.calcsize("P") * 8 == 64 else "i386"
    return {"arch": _arch, "version": "Windows-10.0"}

_mc_conn_module.get_platform = _safe_get_platform
_mc_abs_module.get_platform = _safe_get_platform

from pymongo import MongoClient
import redis


class DatabaseType(Enum):
    POSTGRES = "PostgreSQL"
    MYSQL = "MySQL"
    MONGODB = "MongoDB"
    REDIS = "Redis"


class DatabaseConfig:
    POSTGRES = {
        "host": "localhost",
        "port": 5432,
        "database": "hospital_db",
        "user": "admin",
        "password": "password",
    }
    MYSQL = {
        "host": "localhost",
        "port": 3306,
        "database": "hospital_db",
        "user": "root",
        "password": "password",
    }
    MONGO_URI = "mongodb://admin:password@localhost:27017"
    MONGO_DB = "hospital_db"
    REDIS = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
    }


class DatabaseConnection(ABC):
    """Abstrakcyjna klasa bazowa dla połączeń z bazami danych."""

    def __init__(self):
        self.connection = None

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def ping(self) -> bool:
        pass

    def get_connection(self):
        if self.connection is None:
            self.connect()
        return self.connection

    def reconnect(self):
        self.close()
        self.connect()
        return self.connection


class PostgresConnection(DatabaseConnection):
    def connect(self):
        self.connection = psycopg2.connect(**DatabaseConfig.POSTGRES)
        self.connection.autocommit = True

    def close(self):
        if self.connection and not self.connection.closed:
            self.connection.close()
        self.connection = None

    def ping(self) -> bool:
        try:
            conn = psycopg2.connect(**DatabaseConfig.POSTGRES)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            conn.close()
            return True
        except Exception:
            return False


class MysqlConnection(DatabaseConnection):
    def connect(self):
        self.connection = mysql.connector.connect(
            **DatabaseConfig.MYSQL, autocommit=True, use_pure=True
        )

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
        self.connection = None

    def get_connection(self):
        if self.connection is None or not self.connection.is_connected():
            self.connect()
        return self.connection

    def ping(self) -> bool:
        try:
            conn = mysql.connector.connect(**DatabaseConfig.MYSQL, autocommit=True, use_pure=True)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            conn.close()
            return True
        except Exception:
            return False


class MongoConnection(DatabaseConnection):
    def __init__(self):
        super().__init__()
        self.db = None

    def connect(self):
        self.connection = MongoClient(
            DatabaseConfig.MONGO_URI,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=3600000,
            connectTimeoutMS=30000,
        )
        self.db = self.connection[DatabaseConfig.MONGO_DB]

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None
        self.db = None

    def ping(self) -> bool:
        try:
            client = MongoClient(
                DatabaseConfig.MONGO_URI, serverSelectionTimeoutMS=3000
            )
            client.admin.command("ping")
            client.close()
            return True
        except Exception:
            return False

    def get_db(self):
        if self.db is None:
            self.connect()
        return self.db


class RedisConnection(DatabaseConnection):
    def connect(self):
        self.connection = redis.Redis(
            **DatabaseConfig.REDIS,
            decode_responses=True,
            socket_timeout=600,
            socket_connect_timeout=30,
        )

    def close(self):
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None

    def ping(self) -> bool:
        try:
            r = redis.Redis(
                **DatabaseConfig.REDIS, decode_responses=True, socket_timeout=3
            )
            result = r.ping()
            r.close()
            return result
        except Exception:
            return False


class ConnectionManager:
    """Centralne zarządzanie połączeniami do wszystkich baz danych."""

    _classes = {
        DatabaseType.POSTGRES: PostgresConnection,
        DatabaseType.MYSQL: MysqlConnection,
        DatabaseType.MONGODB: MongoConnection,
        DatabaseType.REDIS: RedisConnection,
    }

    def __init__(self):
        self._instances: dict[DatabaseType, DatabaseConnection] = {}

    def get_connector(self, db_type: DatabaseType) -> DatabaseConnection:
        if db_type not in self._instances:
            self._instances[db_type] = self._classes[db_type]()
        return self._instances[db_type]

    def ping_all(self) -> dict[str, bool]:
        results = {}
        for db_enum in self._classes:
            connector = self._classes[db_enum]()
            results[db_enum.value] = connector.ping()
        return results

    def close_all(self):
        for inst in self._instances.values():
            try:
                inst.close()
            except Exception:
                pass
        self._instances.clear()
