import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
import sys
import os

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl_processor import process_data

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
        'DATA_FEATURE_HUMID': 'humidity',
        'TEMPERATURE_INTERVAL': '10',
        'HUMIDITY_INTERVAL': '20'
    }):
        yield

@pytest.fixture
def mock_db_connection():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    
    # Mock the count query result
    cur.fetchone.return_value = [3]  # Simulate 3 records in sensor_data
    
    with patch('psycopg2.connect', return_value=conn):
        yield conn, cur

# Verifies that the summary tables (temperature and humidity) are properly created and dropped
def test_process_data_table_creation(mock_db_connection):
    conn, cur = mock_db_connection
    
    with patch('time.sleep', side_effect=Exception('Stop loop')):
        try:
            process_data()
        except Exception as e:
            if str(e) != 'Stop loop':
                raise e
    
    # Verify tables were dropped and recreated
    cur.execute.assert_any_call('DROP TABLE IF EXISTS temperature_summary')
    cur.execute.assert_any_call('DROP TABLE IF EXISTS humidity_summary')
    
    # Verify temperature summary table creation
    temperature_table_creation = any(
        'CREATE TABLE IF NOT EXISTS temperature_summary' in str(call)
        for call in cur.execute.call_args_list
    )
    assert temperature_table_creation
    
    # Verify humidity summary table creation
    humidity_table_creation = any(
        'CREATE TABLE IF NOT EXISTS humidity_summary' in str(call)
        for call in cur.execute.call_args_list
    )
    assert humidity_table_creation

# Ensures correct data aggregation for both temperature (10-minute intervals) and humidity (20-minute intervals)
def test_process_data_aggregation(mock_db_connection):
    conn, cur = mock_db_connection
    
    with patch('time.sleep', side_effect=Exception('Stop loop')):
        try:
            process_data()
        except Exception as e:
            if str(e) != 'Stop loop':
                raise e
    
    # Verify data count check
    cur.execute.assert_any_call('SELECT COUNT(*) FROM sensor_data')
    
    # Verify temperature aggregation query
    temperature_aggregation = any(
        'INSERT INTO temperature_summary' in str(call) and
        'INTERVAL \'10 min\'' in str(call)
        for call in cur.execute.call_args_list
    )
    assert temperature_aggregation
    
    # Verify humidity aggregation query
    humidity_aggregation = any(
        'INSERT INTO humidity_summary' in str(call) and
        'INTERVAL \'20 min\'' in str(call)
        for call in cur.execute.call_args_list
    )
    assert humidity_aggregation

# Validates proper error handling when database operations fail
def test_process_data_database_error(mock_db_connection):
    conn, cur = mock_db_connection
    cur.execute.side_effect = psycopg2.Error('Database error')
    
    with patch('time.sleep'):
        process_data()
        # Function should handle the error gracefully

# Confirms that database commits occur after successful operations
def test_process_data_commit_on_success(mock_db_connection):
    conn, cur = mock_db_connection
    
    with patch('time.sleep', side_effect=Exception('Stop loop')):
        try:
            process_data()
        except Exception as e:
            if str(e) != 'Stop loop':
                raise e
    
    # Verify commits were called
    assert conn.commit.called