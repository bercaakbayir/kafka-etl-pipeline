from kafka import KafkaConsumer
import json
import psycopg2
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv()

POSTGRE_HOST = os.getenv('POSTGRE_HOST')
POSTGRE_USER = os.getenv('POSTGRE_USER')
POSTGRE_PASSWORD = os.getenv('POSTGRE_PASSWORD')
POSTGRE_DBNAME = os.getenv('POSTGRE_DBNAME')

DATA_FEATURE_DATE = os.getenv('DATA_FEATURE_DATE')
DATA_FEATURE_TEMP = os.getenv('DATA_FEATURE_TEMP')
DATA_FEATURE_HUMID = os.getenv('DATA_FEATURE_HUMID')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_consumer():
    logger.info("Creating Kafka consumer...")
    return KafkaConsumer(
        'sensor_data',
        bootstrap_servers=['kafka:29092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='sensor_data_group',
        enable_auto_commit=True,
        auto_commit_interval_ms=2000
    )


def store_data():
    max_retries = 5
    retry_delay = 5

    consumer = None
    conn = None
    cur = None
    retry_count = 0

    try:
        consumer = create_consumer()
        conn = psycopg2.connect(
            dbname=POSTGRE_DBNAME,
            user=POSTGRE_USER,
            password=POSTGRE_PASSWORD,
            host=POSTGRE_HOST
        )
        conn.autocommit = False  # Explicit transaction management
        cur = conn.cursor()
        logger.info("Successfully connected to PostgreSQL database")

        # Create table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                date_time TIMESTAMP,
                temp FLOAT,
                humidity FLOAT
            )
        """)
        conn.commit()

        for message in consumer:
            try:
                if message is None:
                    logger.error("Received None message")
                    continue

                data = message.value
                if data is None:
                    logger.error("Received invalid message with None value")
                    continue

                cur.execute(
                    "INSERT INTO sensor_data (date_time, temp, humidity) VALUES (%s, %s, %s)",
                    (data[DATA_FEATURE_DATE], data[DATA_FEATURE_TEMP], data[DATA_FEATURE_HUMID])
                )
                conn.commit()
                logger.info(f"Stored sensor data: {data}")
                retry_count = 0  # Reset retry counter on successful processing
            except psycopg2.Error as e:
                logger.error(f"Database error while processing message: {e}")
                conn.rollback()
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error("Max retries reached, restarting consumer")
                    break
                time.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue

    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        time.sleep(2)  # Wait before retrying connection
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        time.sleep(2)  # Wait before retrying


if __name__ == "__main__":
    logger.info("Consumer starting up...")
    time.sleep(30)  # Wait for Kafka and Postgres to be ready
    store_data()