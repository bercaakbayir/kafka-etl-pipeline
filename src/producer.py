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
KAFKA_HOST=str(os.getenv('KAFKA_HOST'))
KAFKA_PORT=str(os.getenv('KAFKA_PORT'))

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
    
    for _, row in df.iterrows():
        data = {
        'date_time': str(row[DATA_FEATURE_DATE]),
        'temp': row[DATA_FEATURE_TEMP],
        'humidity': row[DATA_FEATURE_HUMID]
        }

        try:
            producer.send('sensor_data', value=data)
            logger.info("Successfully sent the data to Kafka")
        except Exception as e:
            logger.error(f"Failed to send the data to Kafka: {e}")
        
        time.sleep(1)  # Send one record per second
        
    producer.close()

if __name__ == "__main__":
    time.sleep(10)  # Wait for Kafka to be ready
    read_and_send_data()