import psycopg2
import mysql.connector
from pymongo import MongoClient
import redis
from enum import Enum

class Databases(Enum):
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    MONGODB = "MONGODB"
    REDIS = "REDIS"

DB_CONFIG = {
    Databases.POSTGRES: {
        "host": "localhost",
        "port": 5432,
        "database": "hospital_db",
        "user": "admin",
        "password": "password",
    },
    Databases.MYSQL: {
        "host": "localhost",
        "port": 3306,
        "database": "hospital_db",
        "user": "root",
        "password": "password",
    },
    Databases.MONGODB: {
        "host": "localhost",
        "port": 27017,
        "database": "hospital_db",
        "user": "admin",
        "password": "password",
    },
    Databases.REDIS: {
        "host": "localhost",
        "port": 6379,
        "db": 0,
    },
}
