import psycopg2
import mysql.connector
import redis
from pymongo import MongoClient
from db_config import Databases, DB_CONFIG


class ConnectionManager:
    def __init__(self, db_names: list[Databases]):
        self.connections = {db_name: self._create_connection(db_name) for db_name in db_names}

    def get_connection(self, db_name: Databases):
        return self.connections[db_name]

    def ping(self, db_name: Databases):
        if db_name == Databases.POSTGRES:
            return self.get_connection(db_name).cursor().execute("SELECT 1")
        elif db_name == Databases.MYSQL:
            return self.get_connection(db_name).cursor().execute("SELECT 1")
        elif db_name == Databases.MONGODB:
            return self.get_connection(db_name).admin.command("ping")
        elif db_name == Databases.REDIS:
            return self.get_connection(db_name).ping()
        else:
            raise ValueError(f"Unknown database: {db_name}")

    def _create_connection(self, db_name: Databases):
        if db_name == Databases.POSTGRES:   
            return psycopg2.connect(**DB_CONFIG[Databases.POSTGRES])
        elif db_name == Databases.MYSQL:
            return mysql.connector.connect(**DB_CONFIG[Databases.MYSQL])
        elif db_name == Databases.MONGODB:
            return MongoClient(**DB_CONFIG[Databases.MONGODB])
        elif db_name == Databases.REDIS:
            return redis.Redis(**DB_CONFIG[Databases.REDIS])
        else:
            raise ValueError(f"Unknown database: {db_name}")