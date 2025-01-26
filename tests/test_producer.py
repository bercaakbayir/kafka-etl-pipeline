import pytest
from unittest.mock import Mock, patch, mock_open
import pandas as pd
import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.producer import create_producer, read_and_send_data

@pytest.fixture(autouse=True)
def setup_environment():
    """Setup environment variables before each test"""
    with patch.dict('os.environ', {
        'PARQUET_URL': 'test_data.parquet',
        'KAFKA_HOST': 'kafka',
        'KAFKA_PORT': '29092',
        'DATA_FEATURE_DATE': 'date_time',
        'DATA_FEATURE_TEMP': 'temp',
        'DATA_FEATURE_HUMID': 'humidity'
    }):
        yield

# Creates a mock Kafka producer for testing
@pytest.fixture
def mock_kafka_producer():
    producer = Mock()
    producer.send = Mock()
    producer.flush = Mock()
    producer.close = Mock()
    
    with patch('src.producer.KafkaProducer', return_value=producer):
        yield producer

# Provides sample test data
@pytest.fixture
def mock_parquet_data():
    return pd.DataFrame({
        'date_time': ['2024-01-01', '2024-01-02'],
        'temp': [20.5, 21.0],
        'humidity': [45.0, 46.0]
    })

# Verifies successful producer creation
def test_create_producer_success(mock_kafka_producer):
    with patch('time.sleep'):
        producer = create_producer()
        assert producer is not None
        assert isinstance(producer, Mock)


# Tests error handling during producer creation
def test_create_producer_failure():
    with patch('src.producer.KafkaProducer', side_effect=Exception('Connection failed')), \
         patch('time.sleep'):
        with pytest.raises(Exception):
            create_producer()

# Validates parquet file reading and data sending
def test_read_parquet_file(mock_parquet_data, mock_kafka_producer):
    with patch('pandas.read_parquet', return_value=mock_parquet_data), \
         patch('os.path.exists', return_value=False), \
         patch('os.makedirs'), \
         patch('builtins.open', mock_open()), \
         patch('time.sleep'):
            read_and_send_data()
            mock_kafka_producer.send.assert_called()

# Checks state file handling and resumption logic
def test_state_management(mock_kafka_producer, mock_parquet_data):
    with patch('pandas.read_parquet', return_value=mock_parquet_data), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data='2024-01-01')), \
         patch('time.sleep'):
            read_and_send_data()
            mock_kafka_producer.send.assert_called()

# Ensures proper error handling when sending fails
def test_kafka_send_failure(mock_kafka_producer):
    # Setup the mock to raise an exception
    mock_kafka_producer.send.side_effect = Exception('Send failed')
    test_data = pd.DataFrame({
        'date_time': ['2024-01-01'],
        'temp': [20.5],
        'humidity': [45.0]
    })
    
    with patch('pandas.read_parquet', return_value=test_data), \
         patch('os.path.exists', return_value=False), \
         patch('os.makedirs'), \
         patch('builtins.open', mock_open()), \
         patch('time.sleep'):
            # The function should complete without raising an exception
            # as it handles the error internally
            read_and_send_data()
            # Verify that send was called and failed
            mock_kafka_producer.send.assert_called_once()
            # Verify that close was called during cleanup
            mock_kafka_producer.close.assert_called_once()