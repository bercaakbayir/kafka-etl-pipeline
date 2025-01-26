import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.consumer import create_consumer, store_data

@pytest.fixture(autouse=True)
def setup_environment():
    """Setup environment variables before each test"""
    with patch.dict('os.environ', {
        'POSTGRE_HOST': 'localhost',
        'POSTGRE_USER': 'test_user',
        'POSTGRE_PASSWORD': 'test_password',
        'POSTGRE_DBNAME': 'test_db',
        'DATA_FEATURE_DATE': 'date_time',
        'DATA_FEATURE_TEMP': 'temp',
        'DATA_FEATURE_HUMID': 'humidity'
    }):
        yield

# Creates a mock consumer that simulates receiving sensor data
@pytest.fixture
def mock_kafka_consumer():
    consumer = MagicMock()
    message = MagicMock()
    message.value = {
        'date_time': '2024-01-01 12:00:00',
        'temp': 20.5,
        'humidity': 45.0
    }
    consumer.__iter__.return_value = iter([message])
    
    with patch('src.consumer.KafkaConsumer', return_value=consumer):
        yield consumer

# Provides mock database connection and cursor
@pytest.fixture
def mock_db_connection():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    
    with patch('psycopg2.connect', return_value=conn):
        yield conn, cur

# Verifies successful consumer creation
def test_create_consumer_success(mock_kafka_consumer):
    consumer = create_consumer()
    assert consumer is not None
    assert isinstance(consumer, MagicMock)

# Tests error handling during consumer creation
def test_create_consumer_failure():
    with patch('src.consumer.KafkaConsumer', side_effect=Exception('Connection failed')):
        with pytest.raises(Exception):
            create_consumer()

# Validates successful data storage flow
def test_store_data_success(mock_kafka_consumer, mock_db_connection):
    conn, cur = mock_db_connection
    
    with patch('time.sleep'):
        store_data()
        
        # Verify table creation
        cur.execute.assert_any_call(
            """\n            CREATE TABLE IF NOT EXISTS sensor_data (\n                date_time TIMESTAMP,\n                temp FLOAT,\n                humidity FLOAT\n            )\n        """)
        
        # Verify data insertion
        cur.execute.assert_any_call(
            "INSERT INTO sensor_data (date_time, temp, humidity) VALUES (%s, %s, %s)",
            ('2024-01-01 12:00:00', 20.5, 45.0)
        )
        
        # Verify commits
        assert conn.commit.called

# Tests database connection error handling
def test_store_data_db_connection_error(mock_kafka_consumer):
    with patch('psycopg2.connect', side_effect=psycopg2.Error('Connection failed')), \
         patch('time.sleep'):
        store_data()
        # Function should handle the error gracefully

# Verifies proper handling of data insertion errors
def test_store_data_insertion_error(mock_kafka_consumer, mock_db_connection):
    conn, cur = mock_db_connection
    cur.execute.side_effect = [None, psycopg2.Error('Insert failed')]
    
    with patch('time.sleep'):
        store_data()
        
        # Verify rollback was called
        assert conn.rollback.called

