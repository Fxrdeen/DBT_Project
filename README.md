# Stock Market Trend Analyzer

A real-time and batch processing pipeline for stock market data analysis using Apache Kafka, Apache Spark, and PostgreSQL.

## Overview

This project implements a complete data pipeline for analyzing stock market data (specifically AAPL stock):

1. **Data Ingestion**: Historical stock data is loaded into Kafka topics and PostgreSQL
2. **Batch Processing**: Complete dataset analysis using Spark SQL and pandas
3. **Stream Processing**: Real-time analysis of stock data using Spark Structured Streaming
4. **Performance Analysis**: Comparison of batch vs. streaming processing approaches

## Prerequisites

- macOS with Homebrew installed
- Python 3.6+
- PostgreSQL 14 (installed via Homebrew)
- Apache Kafka 3.x (installed via Homebrew)
- Apache Spark 3.x

## Installation

### 1. Install Required Software

```bash
# Install PostgreSQL
brew install postgresql@14

# Install Kafka (will also install Zookeeper as a dependency)
brew install kafka

# Install Python dependencies
pip install pyspark pandas numpy psycopg2 matplotlib seaborn
```

### 2. Start Required Services

```bash
# Start PostgreSQL
brew services start postgresql@14

# Start Zookeeper
brew services start zookeeper

# Start Kafka
brew services start kafka
```

### 3. Create Database and Schema

```bash
# Create PostgreSQL database and user
psql postgres -c "CREATE USER stockuser WITH PASSWORD 'stockpassword';"
psql postgres -c "CREATE DATABASE stockdb OWNER stockuser;"

# Load schema
psql -U stockuser -d stockdb -f create_schema.sql
```

### 4. Download PostgreSQL JDBC Driver

```bash
# Download PostgreSQL JDBC driver if not already present
curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o postgresql-42.6.0.jar
```

### 5. Create Kafka Topics

```bash
# Create required Kafka topics
/opt/homebrew/bin/kafka-topics --create --topic aapl-raw-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/homebrew/bin/kafka-topics --create --topic aapl-technical-indicators --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
/opt/homebrew/bin/kafka-topics --create --topic aapl-trading-signals --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Project Components

- **data_producer.py**: Reads AAPL_historical.csv and publishes data to Kafka and PostgreSQL
- **batch_processor.py**: Spark batch processing job for historical analysis
- **spark_streaming.py**: Spark Structured Streaming job for real-time analysis
- **technical_indicators.py**: Library for calculating technical indicators (SMA, EMA, MACD, RSI, etc.)
- **performance_analyzer.py**: Compares performance between batch and streaming approaches
- **run_pipeline.sh**: Main script to orchestrate the entire pipeline

## Running the Pipeline

### Method 1: Using the Pipeline Script

The easiest way to run the entire pipeline is using the provided script:

```bash
# Make the script executable
chmod +x run_pipeline.sh

# Run the pipeline
./run_pipeline.sh
```

This will:

1. Check for the AAPL_historical.csv file
2. Start the data producer to load data
3. Run the batch processing job
4. Start the streaming job
5. Run for 5 minutes, then stop the streaming job
6. Generate performance comparison charts

### Method 2: Running Components Individually

If you prefer to run each component manually:

#### 1. Data Producer

```bash
python data_producer.py
```

#### 2. Batch Processor

```bash
python batch_processor.py
```

#### 3. Spark Streaming

```bash
spark-submit --jars postgresql-42.6.0.jar --driver-class-path postgresql-42.6.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming.py
```

#### 4. Performance Analysis

```bash
python performance_analyzer.py
```

## Analyzing Results

After running the pipeline:

1. Performance metrics will be available in the PostgreSQL database:

```bash
psql -U stockuser -d stockdb -c "SELECT processing_type, metric_name, metric_value FROM performance_metrics;"
```

2. Technical indicators and trading signals can be queried:

```bash
psql -U stockuser -d stockdb -c "SELECT * FROM technical_indicators LIMIT 10;"
psql -U stockuser -d stockdb -c "SELECT * FROM trading_signals LIMIT 10;"
```

3. Performance charts will be generated in the `performance_plots` directory

## Troubleshooting

### Kafka Connection Issues

If you encounter Kafka connection problems:

```bash
# Check if Kafka is running
brew services info kafka

# Check if topics exist
/opt/homebrew/bin/kafka-topics --list --bootstrap-server localhost:9092
```

### PostgreSQL Issues

If you have PostgreSQL connection problems:

```bash
# Check if PostgreSQL is running
brew services info postgresql@14

# Check connection
psql -U stockuser -d stockdb -c "SELECT 1;"
```

### Spark Submission Issues

For Spark errors:

- Ensure the PostgreSQL JDBC driver exists (postgresql-42.6.0.jar)
- Check that all Python dependencies are installed
- Verify Kafka topics are created correctly

## Shutdown

To shut down all services:

```bash
# Stop Kafka
brew services stop kafka

# Stop Zookeeper
brew services stop zookeeper

# Stop PostgreSQL
brew services stop postgresql@14
```

## Author

[Your Name]

## License

[Specify License]
