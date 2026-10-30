import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

RAW_DB = os.getenv("RAW_DB", "food_orders_raw_db")
ANALYTICS_DB = os.getenv("ANALYTICS_DB", "food_orders_analytics_db")

SOURCE_CSV = BASE_DIR / os.getenv("SOURCE_CSV", "food_orders.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))

SOURCE_COLUMNS = [
    "Order ID",
    "Customer ID",
    "Restaurant ID",
    "Order Date and Time",
    "Delivery Date and Time",
    "Order Value",
    "Delivery Fee",
    "Payment Method",
    "Discounts and Offers",
    "Commission Fee",
    "Payment Processing Fee",
    "Refunds/Chargebacks",
    "Weather Condition",
    "Order Channel"
]


def create_connection_url(database=None):
    return URL.create(
        "mysql+pymysql",
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=database,
        query={"charset": "utf8mb4"}
    )


def server_engine():
    return create_engine(
        create_connection_url(),
        pool_pre_ping=True,
        future=True
    )


def database_engine(database):
    return create_engine(
        create_connection_url(database),
        pool_pre_ping=True,
        future=True
    )


def create_databases():
    engine = server_engine()
    try:
        with engine.begin() as connection:
            connection.execute(text(
                f"""
                CREATE DATABASE IF NOT EXISTS `{RAW_DB}`
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci
                """
            ))
            connection.execute(text(
                f"""
                CREATE DATABASE IF NOT EXISTS `{ANALYTICS_DB}`
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci
                """
            ))
        print("Databases created successfully.")
    finally:
        engine.dispose()

