# Sensor Data Processing System

A distributed system for processing sensor data using Apache Kafka and PostgreSQL.

## Architecture

- **Producer**: Reads sensor data from parquet file and sends to Kafka
- **Consumer**: Receives data from Kafka and stores in PostgreSQL
- **ETL Processor**: Processes raw data into temperature and humidity summaries with time intervals, inserts the data into the database
- **PostgreSQL**: Stores both raw and processed data
- **Kafka**: Message broker for data streaming

## Prerequisites

- Docker
- Python 

## Setup

1. Clone the repository:

```bash
   git clone <repository-url>
```
2. Build and start the services

```bash
  docker-compose up --build
```

3. Stop the services

```bash
  docker-compose down
```

## Environment Variables
Define these variables in your .env file:

TEMPERATURE_INTERVAL: Interval for temperature summaries (minutes).

HUMIDITY_INTERVAL: Interval for humidity summaries (minutes).

POSTGRE_*: PostgreSQL connection settings.

KAFKA_*: Kafka connection settings.

DATA_FEATURE_*: Data column names in the Parquet file.


## Monitoring
To check service logs:

```bash
  docker-compose logs -f [service_name]
```

## Current Services
- producer : This service is able to read the data from parquet and send the message to Kafka.
- consumer : Consumer reads the data from Kafka and store it into the PostgreSQL, in main table sensor_data.
- etl : ETL processor service creates queries to summarize the temperature and humidity data and insert into the summary tables batch by batch with determined time intervals.
- postgres : Stores the data in every step. 
- kafka : Handles the sensor data messages.
- zookeeper : Provides orchestration of services

## Data Flow
- Producer reads the Parquet file.
- Data is sent to the Kafka topic sensor_data.
- Consumer stores raw data in PostgreSQL.
- ETL processor creates summary tables.
- Results are available in the PostgreSQL database.


## Database Schema
- sensor_data: Raw sensor readings.
- temperature_summary: Aggregated temperature data.
- humidity_summary: Aggregated humidity data.

## Run Unit Tests

- Run All Unit Tests:
```bash
  pytest tests -v
```

- Run Unit Test for Specific Service:
```bash
  pytest tests/test_<service_name>.py -v
```

