import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PARQUET_URL = str(os.getenv('PARQUET_URL'))

KAFKA_HOST = str(os.getenv('KAFKA_HOST'))
KAFKA_PORT = str(os.getenv('KAFKA_PORT'))

DATA_FEATURE_DATE = os.getenv('DATA_FEATURE_DATE')
DATA_FEATURE_TEMP = os.getenv('DATA_FEATURE_TEMP')
DATA_FEATURE_HUMID = os.getenv('DATA_FEATURE_HUMID')


def create_producer():
    logger.info("Creating Kafka producer...")
    retries = 30
    while retries > 0:
        try:
            kafka_server_name = f"{KAFKA_HOST}:{KAFKA_PORT}"
            producer = KafkaProducer(
                bootstrap_servers=[kafka_server_name],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            logger.warning(f"Failed to connect to Kafka, retrying... ({retries} attempts left)")
            retries -= 1
            time.sleep(1)
    raise Exception("Could not connect to Kafka after multiple attempts")


def read_and_send_data():
    producer = create_producer()
    df = pd.read_parquet(PARQUET_URL)
    df = df.sort_values(DATA_FEATURE_DATE)

    # Use absolute path for state directory
    state_dir = '/app/state'
    state_file = os.path.join(state_dir, 'last_processed.txt')
    last_processed_time = None

    # Check if state directory and file exist
    if os.path.exists(state_dir) and os.path.exists(state_file):
        logger.info(f"Found state file: {state_file}")
        with open(state_file, 'r') as f:
            last_processed_time = f.read().strip()
            logger.info(f"Resuming from timestamp: {last_processed_time}")
    else:
        logger.info("No state file found, starting from beginning")
        if not os.path.exists(state_dir):
            os.makedirs(state_dir)
            logger.info(f"Created state directory at: {state_dir}")
        logger.info("Starting data processing from beginning")

    try:
        for _, row in df.iterrows():
            current_time = str(row[DATA_FEATURE_DATE])

            # Modified comparison logic to resume after the last processed time
            if last_processed_time and current_time <= last_processed_time:
                # logger.debug(f"Skipping record: {current_time}")
                continue

            # Start processing from the record after the last processed time
            data = {
                'date_time': current_time,
                'temp': row[DATA_FEATURE_TEMP],
                'humidity': row[DATA_FEATURE_HUMID]
            }

            try:
                producer.send('sensor_data', value=data)
                logger.info(f"Sent data for timestamp: {current_time}")

                # Update state file after successful send
                with open(state_file, 'w') as f:
                    f.write(current_time)

            except Exception as e:
                logger.error(f"Failed to send data: {e}")
                break

            time.sleep(1)

    finally:
        producer.flush()
        if 'current_time' in locals():
            with open(state_file, 'w') as f:
                f.write(current_time)
            logger.info(f"Saved last processed timestamp: {current_time}")
        producer.close()


if __name__ == "__main__":
    time.sleep(10)  # Wait for Kafka to be ready
    read_and_send_data()