from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import time
import pandas as pd
import numpy as np
from technical_indicators import TechnicalIndicators
import psycopg2
from datetime import datetime
import traceback
import signal
import sys

# PostgreSQL connection parameters
pg_params = {
    'dbname': 'stockdb',
    'user': 'stockuser',
    'password': 'stockpassword',
    'host': 'localhost'
}

# Create Spark session
spark = SparkSession.builder \
    .appName("StockMarketAnalyzer") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", "postgresql-42.6.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()
# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Define schema for incoming data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("adj_close", FloatType(), True),
    StructField("volume", LongType(), True)
])

# Global flag for shutdown
shutdown_requested = False

def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested, finishing current batch...")
    shutdown_requested = True

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Function to process each micro-batch
def process_batch(df, epoch_id):
    global shutdown_requested
    # Start timing at the beginning of each batch
    batch_start_time = time.time()
    
    try:
        if df.count() > 0:
            # Convert to pandas for easier calculations
            pandas_df = df.toPandas()
            pandas_df['date'] = pd.to_datetime(pandas_df['date'])
            pandas_df = pandas_df.sort_values('date')
            
            # Calculate technical indicators
            close_prices = pandas_df['close']
            
            # Calculate indicators
            sma_5 = TechnicalIndicators.calculate_sma(close_prices, 5)
            sma_10 = TechnicalIndicators.calculate_sma(close_prices, 10)
            sma_20 = TechnicalIndicators.calculate_sma(close_prices, 20)
            ema_12 = TechnicalIndicators.calculate_ema(close_prices, 12)
            ema_26 = TechnicalIndicators.calculate_ema(close_prices, 26)
            macd, macd_signal, macd_hist = TechnicalIndicators.calculate_macd(close_prices)
            rsi_14 = TechnicalIndicators.calculate_rsi(close_prices)
            bollinger_upper, bollinger_middle, bollinger_lower = TechnicalIndicators.calculate_bollinger_bands(close_prices)
            
            # Combine all indicators
            indicators_df = pd.DataFrame({
                'date': pandas_df['date'],
                'ticker': pandas_df['ticker'],
                'close': close_prices,
                'sma_5': sma_5,
                'sma_10': sma_10,
                'sma_20': sma_20,
                'ema_12': ema_12,
                'ema_26': ema_26,
                'macd': macd,
                'macd_signal': macd_signal,
                'macd_histogram': macd_hist,
                'rsi_14': rsi_14,
                'bollinger_upper': bollinger_upper,
                'bollinger_middle': bollinger_middle,
                'bollinger_lower': bollinger_lower
            })
            
            # Drop rows with NaN (due to insufficient data for calculation)
            indicators_df = indicators_df.dropna()
            
            if not indicators_df.empty:
                # Connect to PostgreSQL
                conn = psycopg2.connect(**pg_params)
                cur = conn.cursor()
                
                # Save technical indicators to PostgreSQL
                for _, row in indicators_df.iterrows():
                    cur.execute("""
                    INSERT INTO technical_indicators 
                        (date, ticker, processing_type, sma_5, sma_10, sma_20, ema_12, ema_26, 
                         macd, macd_signal, macd_histogram, rsi_14, 
                         bollinger_upper, bollinger_middle, bollinger_lower)
                    VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, ticker, processing_type) DO UPDATE SET
                        sma_5 = EXCLUDED.sma_5,
                        sma_10 = EXCLUDED.sma_10,
                        sma_20 = EXCLUDED.sma_20,
                        ema_12 = EXCLUDED.ema_12,
                        ema_26 = EXCLUDED.ema_26,
                        macd = EXCLUDED.macd,
                        macd_signal = EXCLUDED.macd_signal,
                        macd_histogram = EXCLUDED.macd_histogram,
                        rsi_14 = EXCLUDED.rsi_14,
                        bollinger_upper = EXCLUDED.bollinger_upper,
                        bollinger_middle = EXCLUDED.bollinger_middle,
                        bollinger_lower = EXCLUDED.bollinger_lower,
                        created_at = CURRENT_TIMESTAMP
                    """, (
                        row['date'].strftime('%Y-%m-%d'),
                        row['ticker'],
                        'STREAMING',
                        float(row['sma_5']) if not pd.isna(row['sma_5']) else None,
                        float(row['sma_10']) if not pd.isna(row['sma_10']) else None,
                        float(row['sma_20']) if not pd.isna(row['sma_20']) else None,
                        float(row['ema_12']) if not pd.isna(row['ema_12']) else None,
                        float(row['ema_26']) if not pd.isna(row['ema_26']) else None,
                        float(row['macd']) if not pd.isna(row['macd']) else None,
                        float(row['macd_signal']) if not pd.isna(row['macd_signal']) else None,
                        float(row['macd_histogram']) if not pd.isna(row['macd_histogram']) else None,
                        float(row['rsi_14']) if not pd.isna(row['rsi_14']) else None,
                        float(row['bollinger_upper']) if not pd.isna(row['bollinger_upper']) else None,
                        float(row['bollinger_middle']) if not pd.isna(row['bollinger_middle']) else None,
                        float(row['bollinger_lower']) if not pd.isna(row['bollinger_lower']) else None
                    ))
                
                # Generate trading signals
                for _, row in indicators_df.iterrows():
                    if not pd.isna(row['sma_5']) and not pd.isna(row['sma_20']) and \
                       not pd.isna(row['rsi_14']) and not pd.isna(row['macd']) and not pd.isna(row['macd_signal']):
                        
                        signals = TechnicalIndicators.generate_signals(
                            row['close'],
                            row['sma_5'],
                            row['sma_20'],
                            row['rsi_14'],
                            row['macd'],
                            row['macd_signal']
                        )
                        
                        # Save signals to PostgreSQL
                        for signal in signals:
                            cur.execute("""
                            INSERT INTO trading_signals 
                                (date, ticker, processing_type, signal_type, signal_strength, signal_source, description)
                            VALUES 
                                (%s, %s, %s, %s, %s, %s, %s)
                            """, (
                                row['date'].strftime('%Y-%m-%d'),
                                row['ticker'],
                                'STREAMING',
                                signal['signal_type'],
                                signal['signal_strength'],
                                signal['signal_source'],
                                signal['description']
                            ))
                
                # Commit changes
                conn.commit()
                
                # Calculate and save performance metrics
                end_time = time.time()
                processing_time = end_time - batch_start_time
                
                cur.execute("""
                INSERT INTO performance_metrics 
                    (processing_type, metric_name, metric_value, window_size, details)
                VALUES 
                    (%s, %s, %s, %s, %s)
                """, (
                    'STREAMING',
                    'PROCESSING_TIME',
                    processing_time,
                    f"{df.count()} records",
                    json.dumps({
                        'batch_id': epoch_id,
                        'record_count': df.count(),
                        'processing_time_ms': processing_time * 1000
                    })
                ))
                
                conn.commit()
                cur.close()
                conn.close()
                
                # Only attempt Kafka writes if not shutting down
                if not shutdown_requested:
                    try:
                        # Convert indicators to DataFrames and write to Kafka
                        for _, row in indicators_df.iterrows():
                            indicator_data = {
                                'date': row['date'].strftime('%Y-%m-%d'),
                                'ticker': row['ticker'],
                                'sma_5': None if pd.isna(row['sma_5']) else float(row['sma_5']),
                                'sma_10': None if pd.isna(row['sma_10']) else float(row['sma_10']),
                                'sma_20': None if pd.isna(row['sma_20']) else float(row['sma_20']),
                                'ema_12': None if pd.isna(row['ema_12']) else float(row['ema_12']),
                                'ema_26': None if pd.isna(row['ema_26']) else float(row['ema_26']),
                                'macd': None if pd.isna(row['macd']) else float(row['macd']),
                                'macd_signal': None if pd.isna(row['macd_signal']) else float(row['macd_signal']),
                                'macd_histogram': None if pd.isna(row['macd_histogram']) else float(row['macd_histogram']),
                                'rsi_14': None if pd.isna(row['rsi_14']) else float(row['rsi_14']),
                                'bollinger_upper': None if pd.isna(row['bollinger_upper']) else float(row['bollinger_upper']),
                                'bollinger_middle': None if pd.isna(row['bollinger_middle']) else float(row['bollinger_middle']),
                                'bollinger_lower': None if pd.isna(row['bollinger_lower']) else float(row['bollinger_lower'])
                            }
                            
                            # Create a single-row DataFrame for the indicator data
                            indicator_df = spark.createDataFrame([indicator_data])
                            
                            # Write to Kafka
                            indicator_df.selectExpr("to_json(struct(*)) AS value") \
                                .write \
                                .format("kafka") \
                                .option("kafka.bootstrap.servers", "localhost:9092") \
                                .option("topic", "aapl-technical-indicators") \
                                .save()
                            
                            # Generate and send signals
                            if not pd.isna(row['sma_5']) and not pd.isna(row['sma_20']) and \
                               not pd.isna(row['rsi_14']) and not pd.isna(row['macd']) and not pd.isna(row['macd_signal']):
                                
                                signals = TechnicalIndicators.generate_signals(
                                    row['close'],
                                    row['sma_5'],
                                    row['sma_20'],
                                    row['rsi_14'],
                                    row['macd'],
                                    row['macd_signal']
                                )
                                
                                # Send signals to Kafka
                                for signal in signals:
                                    signal_data = {
                                        'date': row['date'].strftime('%Y-%m-%d'),
                                        'ticker': row['ticker'],
                                        'signal_type': signal['signal_type'],
                                        'signal_strength': signal['signal_strength'],
                                        'signal_source': signal['signal_source'],
                                        'description': signal['description']
                                    }
                                    
                                    # Create a single-row DataFrame for the signal
                                    signal_df = spark.createDataFrame([signal_data])
                                    
                                    # Write to Kafka
                                    signal_df.selectExpr("to_json(struct(*)) AS value") \
                                        .write \
                                        .format("kafka") \
                                        .option("kafka.bootstrap.servers", "localhost:9092") \
                                        .option("topic", "aapl-trading-signals") \
                                        .save()
                
                        print(f"Processed batch {epoch_id} with {df.count()} records")
                    except Exception as kafka_error:
                        print(f"Error sending data to Kafka: {str(kafka_error)}")
                        traceback.print_exc()
    except Exception as e:
        print(f"Error in process_batch: {str(e)}")
        traceback.print_exc()

# Track batch start time
batch_start_time = 0

# Custom function to capture batch start time
def track_batch_start(df, epoch_id):
    global batch_start_time
    batch_start_time = time.time()
    return df

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.29.226:9092") \
    .option("subscribe", "aapl-raw-data") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Create a watermarked stream with a 1-day watermark - convert to timestamp instead of date
watermarked_stream = parsed_stream \
    .withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd")) \
    .withWatermark("date", "1 day")

# Define a 20-day tumbling window for aggregation
windowed_stream = watermarked_stream \
    .withColumn("window", window(col("date"), "20 days")) \
    .groupBy("window", "ticker") \
    .agg(
        collect_list("date").alias("dates"),
        collect_list("open").alias("opens"),
        collect_list("high").alias("highs"),
        collect_list("low").alias("lows"),
        collect_list("close").alias("closes"),
        collect_list("adj_close").alias("adj_closes"),
        collect_list("volume").alias("volumes")
    )
    # Flatten the aggregated data back to individual records
flattened_stream = windowed_stream \
    .withColumn("date_close_pairs", arrays_zip("dates", "opens", "highs", "lows", "closes", "adj_closes", "volumes")) \
    .withColumn("date_close_pair", explode("date_close_pairs")) \
    .select(
        "window",
        "ticker",
        col("date_close_pair.dates").alias("date"),
        col("date_close_pair.opens").alias("open"),
        col("date_close_pair.highs").alias("high"),
        col("date_close_pair.lows").alias("low"),
        col("date_close_pair.closes").alias("close"),
        col("date_close_pair.adj_closes").alias("adj_close"),
        col("date_close_pair.volumes").alias("volume")
    )

# Process each micro-batch with our custom function
query = flattened_stream \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: track_batch_start(df, epoch_id)) \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoint") \
    .start()

# Wait for the query to terminate
query.awaitTermination()