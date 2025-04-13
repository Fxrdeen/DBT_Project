#!/bin/bash

echo "Stock Market Trend Analyzer - Complete Pipeline"
echo "----------------------------------------------"

# Check if Kafka is running
# if ! nc -z localhost 9092 >/dev/null 2>&1; then
#     echo "Starting Kafka and Zookeeper..."
#     cd $KAFKA_HOME
#     nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zookeeper.log 2>&1 &
#     sleep 5
#     nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
#     sleep 10
# else
#     echo "Kafka is already running."
# fi

# # Create Kafka topics if they don't exist
# echo "Creating Kafka topics..."
# $KAFKA_HOME/bin/kafka-topics.sh --create --topic aapl-raw-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
# $KAFKA_HOME/bin/kafka-topics.sh --create --topic aapl-technical-indicators --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
# $KAFKA_HOME/bin/kafka-topics.sh --create --topic aapl-trading-signals --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists

# Download PostgreSQL JDBC driver if not present
if [ ! -f postgresql-42.6.0.jar ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    curl -L https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -o postgresql-42.6.0.jar
fi

# Check if data file exists
if [ ! -f AAPL_historical.csv ]; then
    echo "ERROR: AAPL_historical.csv data file not found!"
    echo "Please place the CSV file in the current directory and try again."
    exit 1
fi

# Run the data ingestion script
echo "Starting data ingestion process..."
python data_producer.py &
DATA_PRODUCER_PID=$!

# Wait for data to be loaded into PostgreSQL
echo "Waiting for data to be loaded into PostgreSQL..."
sleep 10

# Run the batch processing
echo "Running batch processing..."
python batch_processor.py &
BATCH_PROCESSOR_PID=$!

# Wait for batch processing to complete
echo "Waiting for batch processing to complete..."
sleep 10

# Run the Spark streaming
echo "Starting Spark Streaming..."
spark-submit \
    --jars postgresql-42.6.0.jar \
    --driver-class-path postgresql-42.6.0.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    spark_streaming.py & 
SPARK_PID=$!

# Give Spark time to process data
echo "Letting Spark process data (will run for 5 minutes)..."
sleep 300

# Stop Spark streaming gracefully
echo "Stopping Spark Streaming gracefully..."
PID=$(ps -ef | grep spark_streaming.py | grep -v grep | awk '{print $2}')
if [ ! -z "$PID" ]; then
    kill -TERM $PID
    # Wait longer for graceful shutdown
    sleep 20
    # Force kill if still running
    if ps -p $PID > /dev/null; then
        echo "Forcing Spark process to stop..."
        kill -9 $PID
    fi
else
    echo "No Spark streaming process found to stop"
fi

# Run performance comparison
echo "Running performance comparison..."
python performance_analyzer.py

echo "Pipeline execution completed!"
echo "Check 'performance_plots' directory for analysis results."