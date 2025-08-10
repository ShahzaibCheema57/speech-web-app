import psycopg2
import logging
import os
from abc import ABC, abstractmethod
from dotenv import load_dotenv

# Configure Logging
logging.basicConfig(
    filename="db.log",  # Log file
    level=logging.INFO,  # Logging level
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Load environment variables from .env file
load_dotenv()

# Fetch values from environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# class DatabaseConnector(ABC):
#     """Abstract Base Class for database connection"""

#     @staticmethod
#     @abstractmethod
#     def get_connection():
#         """Abstract static method to return a database connection"""
#         pass

# class PostgresConnector(DatabaseConnector):
#     """Concrete class to connect to PostgreSQL"""

#     @staticmethod
#     def get_connection():
#         """Establishes and returns a PostgreSQL database connection"""
#         try:
#             logging.info(f"🔗 Attempting to connect to database {DB_NAME} at {DB_HOST}:{DB_PORT} as {DB_USER}...")

#             conn = psycopg2.connect(
#                 dbname=DB_NAME,
#                 user=DB_USER,
#                 password=DB_PASSWORD,
#                 host=DB_HOST,
#                 port=DB_PORT
#             )

#             logging.info("✅ Database connection successful!")
#             return conn

#         except Exception as e:
#             logging.error(f"❌ Database connection failed: {e}", exc_info=True)
#             return None


# ✅ Call method directly using class name (no instance needed)
# connection = PostgresConnector.get_connection()
import psycopg2
from psycopg2 import pool
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class PostgresConnector:
    _connection_pool = None

    @classmethod
    def initialize(cls, minconn=1, maxconn=15):
        """Initialize the PostgreSQL connection pool."""
        if cls._connection_pool is None:
            try:
                cls._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    minconn, maxconn,
                   dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
                )
                logging.info(f"✅ PostgreSQL connection pool initialized with minconn={minconn}, maxconn={maxconn}.")
            except Exception as e:
                logging.error(f"❌ Failed to initialize connection pool: {e}", exc_info=True)

    @classmethod
    def get_connection(cls):
        """Retrieve a connection from the pool."""
        if cls._connection_pool is None:
            logging.error("❌ Connection pool is not initialized.")
            raise Exception("Database connection pool is not initialized.")
        
        try:
            conn = cls._connection_pool.getconn()
            logging.info("🔄 Connection retrieved from pool.")
            return conn
        except Exception as e:
            logging.error(f"❌ Failed to get connection from pool: {e}", exc_info=True)
            raise

    @classmethod
    def release_connection(cls, connection):
        """Release the connection back to the pool."""
        if cls._connection_pool and connection:
            try:
                cls._connection_pool.putconn(connection)
                logging.info("🔁 Connection released back to pool.")
            except Exception as e:
                logging.error(f"❌ Failed to release connection: {e}", exc_info=True)

    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool."""
        if cls._connection_pool:
            try:
                cls._connection_pool.closeall()
                logging.info("🚫 All connections in the pool have been closed.")
            except Exception as e:
                logging.error(f"❌ Failed to close all connections: {e}", exc_info=True)
PostgresConnector.initialize()
