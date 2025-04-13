import pandas as pd
import json
import time
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import execute_values
import csv
from datetime import datetime
import signal
import sys

# Global flag to indicate shutdown
shutdown_requested = False

# Signal handler
def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested. Cleaning up...")
    shutdown_requested = True
    # Allow some time for cleanup before exiting
    time.sleep(1)
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGINT, signal_handler)

# PostgreSQL connection parameters
pg_params = {
    'dbname': 'stockdb',
    'user': 'stockuser',
    'password': 'stockpassword',
    'host': 'localhost'
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Function to load data into PostgreSQL
def load_data_to_postgres(data_file):
    print("Loading data into PostgreSQL...")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_params)
    cur = conn.cursor()
    
    # Create a temporary table for efficient bulk loading
    cur.execute("""
    CREATE TEMP TABLE temp_stock_data (
        date DATE,
        ticker VARCHAR(10),
        open NUMERIC(16,6),
        high NUMERIC(16,6),
        low NUMERIC(16,6),
        close NUMERIC(16,6),
        adj_close NUMERIC(16,6),
        volume BIGINT
    )
    """)
    
    # Read CSV and insert into temp table
    with open(data_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        # Prepare data for bulk insert
        data_to_insert = []
        for row in reader:
            date_str, open_val, high, low, close, adj_close, volume = row
            data_to_insert.append((
                date_str,
                'AAPL',
                float(open_val),
                float(high),
                float(low),
                float(close),
                float(adj_close),
                int(volume)
            ))
        
        # Bulk insert
        execute_values(
            cur,
            "INSERT INTO temp_stock_data VALUES %s",
            data_to_insert
        )
    
    # Insert from temp table to actual table with conflict handling
    cur.execute("""
    INSERT INTO raw_stock_data (date, ticker, open, high, low, close, adj_close, volume)
    SELECT date, ticker, open, high, low, close, adj_close, volume
    FROM temp_stock_data
    ON CONFLICT (date, ticker) DO NOTHING
    """)
    
    # Commit and close
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Loaded {len(data_to_insert)} records into PostgreSQL")

# Function to stream data to Kafka
def stream_data_to_kafka(data_file):
    print("Starting to stream data to Kafka...")
    
    # Load CSV file
    df = pd.read_csv(data_file)
    
    # Sort by date if not already sorted
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date')
    
    # Simulate real-time streaming
    record_count = 0
    start_time = time.time()
    
    while not shutdown_requested:
        for index, row in df.iterrows():
            # Create data record
            stock_data = {
                'date': row['Date'].strftime('%Y-%m-%d'),
                'ticker': 'AAPL',
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'adj_close': float(row['Adj Close']),
                'volume': int(row['Volume'])
            }
            
            # Send to raw data topic
            producer.send('aapl-raw-data', value=stock_data)
            record_count += 1
            
            # Sleep to simulate passage of time
            time.sleep(0.1)  # Adjust as needed
            
            # Print progress every 100 records
            if record_count % 100 == 0:
                elapsed = time.time() - start_time
                print(f"Published {record_count} records ({record_count/elapsed:.2f} records/second)")
            
            # Check for shutdown after each batch
            if shutdown_requested:
                break
        
        # Ensure all messages are sent
        producer.flush()
        print(f"Completed streaming {record_count} records to Kafka")

if __name__ == "__main__":
    data_file = 'AAPL_historical.csv'  # Update with your file path
    
    # First load the data to PostgreSQL for batch processing
    load_data_to_postgres(data_file)
    
    # Then stream it to Kafka
    stream_data_to_kafka(data_file)