import psycopg2
import time
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

POSTGRE_HOST = os.getenv('POSTGRE_HOST')
POSTGRE_USER = os.getenv('POSTGRE_USER')
POSTGRE_PASSWORD = os.getenv('POSTGRE_PASSWORD')
POSTGRE_DBNAME = os.getenv('POSTGRE_DBNAME')

TEMPERATURE_INTERVAL = int(os.getenv('TEMPERATURE_INTERVAL', 10))
HUMIDITY_INTERVAL = int(os.getenv('HUMIDITY_INTERVAL', 20))

DATA_FEATURE_DATE = os.getenv('DATA_FEATURE_DATE')
DATA_FEATURE_TEMP = os.getenv('DATA_FEATURE_TEMP')
DATA_FEATURE_HUMID = os.getenv('DATA_FEATURE_HUMID')


def process_data():
    try:
        conn = psycopg2.connect(
            dbname=POSTGRE_DBNAME,
            user=POSTGRE_USER,
            password=POSTGRE_PASSWORD,
            host=POSTGRE_HOST
        )
        cur = conn.cursor()
        logger.info("Successfully connected to PostgreSQL database")

        cur.execute("DROP TABLE IF EXISTS temperature_summary")
        cur.execute("DROP TABLE IF EXISTS humidity_summary")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS temperature_summary (
                interval_start TIMESTAMP,
                interval_end TIMESTAMP,
                avg_temperature FLOAT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS humidity_summary (
                interval_start TIMESTAMP,
                interval_end TIMESTAMP,
                avg_humidity FLOAT
            )
        """)
        conn.commit()
        logger.info("Successfully created summary tables")

        while True:
            try:
                cur.execute("SELECT COUNT(*) FROM sensor_data")
                count = cur.fetchone()[0]
                logger.info(f"Found {count} records in sensor_data table")

                cur.execute(f"""
                    INSERT INTO temperature_summary (interval_start, interval_end, avg_temperature)
                    SELECT 
                        date_trunc('hour', {DATA_FEATURE_DATE}) + 
                            INTERVAL '{TEMPERATURE_INTERVAL} min' * (EXTRACT(MINUTE FROM {DATA_FEATURE_DATE})::integer / {TEMPERATURE_INTERVAL}) AS interval_start,
                        date_trunc('hour', {DATA_FEATURE_DATE}) + 
                            INTERVAL '{TEMPERATURE_INTERVAL} min' * (EXTRACT(MINUTE FROM {DATA_FEATURE_DATE})::integer / {TEMPERATURE_INTERVAL} + 1) AS interval_end,
                        AVG({DATA_FEATURE_TEMP}) as avg_temperature
                    FROM sensor_data
                    WHERE NOT EXISTS (
                        SELECT 1 FROM temperature_summary ts
                        WHERE ts.interval_start = date_trunc('hour', sensor_data.{DATA_FEATURE_DATE}) + 
                            INTERVAL '{TEMPERATURE_INTERVAL} min' * (EXTRACT(MINUTE FROM sensor_data.{DATA_FEATURE_DATE})::integer / {TEMPERATURE_INTERVAL})
                    )
                    GROUP BY interval_start, interval_end
                """)

                cur.execute(f"""
                    INSERT INTO humidity_summary (interval_start, interval_end, avg_humidity)
                    SELECT 
                        date_trunc('hour', {DATA_FEATURE_DATE}) + 
                            INTERVAL '{HUMIDITY_INTERVAL} min' * (EXTRACT(MINUTE FROM {DATA_FEATURE_DATE})::integer / {HUMIDITY_INTERVAL}) AS interval_start,
                        date_trunc('hour', {DATA_FEATURE_DATE}) + 
                            INTERVAL '{HUMIDITY_INTERVAL} min' * (EXTRACT(MINUTE FROM {DATA_FEATURE_DATE})::integer / {HUMIDITY_INTERVAL} + 1) AS interval_end,
                        AVG({DATA_FEATURE_HUMID}) as avg_humidity
                    FROM sensor_data
                    WHERE NOT EXISTS (
                        SELECT 1 FROM humidity_summary hs
                        WHERE hs.interval_start = date_trunc('hour', sensor_data.{DATA_FEATURE_DATE}) + 
                            INTERVAL '{HUMIDITY_INTERVAL} min' * (EXTRACT(MINUTE FROM sensor_data.{DATA_FEATURE_DATE})::integer / {HUMIDITY_INTERVAL})
                    )
                    GROUP BY interval_start, interval_end
                """)

                conn.commit()
                logger.info("Successfully processed new sensor data")
                time.sleep(5)  # Check for new data every 5 seconds

            except psycopg2.Error as e:
                logger.error(f"Database error during processing: {e}")
                break

    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        time.sleep(5)  # Wait before retrying connection


if __name__ == "__main__":
    logger.info("ETL processor starting up...")
    time.sleep(60)  # Wait for other services to be ready
    process_data()