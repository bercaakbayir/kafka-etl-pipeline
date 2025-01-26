import pytest
import os

@pytest.fixture(autouse=True)
def env_setup():
    os.environ['PARQUET_URL'] = 'test_data.parquet'
    os.environ['KAFKA_HOST'] = 'localhost'
    os.environ['KAFKA_PORT'] = '9092'
    os.environ['DATA_FEATURE_DATE'] = 'date_time'
    os.environ['DATA_FEATURE_TEMP'] = 'temp'
    os.environ['DATA_FEATURE_HUMID'] = 'humidity'