from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
import numpy as np
import json
import time
from technical_indicators import TechnicalIndicators
from datetime import datetime

# PostgreSQL connection parameters
pg_params = {
    'dbname': 'stockdb',
    'user': 'stockuser',
    'password': 'stockpassword',
    'host': 'localhost'
}

def batch_process():
    print("Starting batch processing...")
    start_time = time.time()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("StockMarketBatchAnalyzer") \
        .config("spark.jars", "postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "postgresql-42.6.0.jar") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read data from PostgreSQL
    jdbc_url = f"jdbc:postgresql://localhost:5432/stockdb"
    
    raw_data = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "raw_stock_data") \
        .option("user", "stockuser") \
        .option("password", "stockpassword") \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Register as temp view for SQL
    raw_data.createOrReplaceTempView("stock_data")
    
    # Process with Spark SQL
    batch_df = spark.sql("""
        SELECT 
            date,
            ticker,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        FROM stock_data
        WHERE ticker = 'AAPL'
        ORDER BY date
    """)
    
    # Convert to pandas for indicator calculations
    pandas_df = batch_df.toPandas()
    pandas_df['date'] = pd.to_datetime(pandas_df['date'])
    
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
                'BATCH',
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
                        'BATCH',
                        signal['signal_type'],
                        signal['signal_strength'],
                        signal['signal_source'],
                        signal['description']
                    ))
        
        # Calculate and save performance metrics
        end_time = time.time()
        processing_time = end_time - start_time
        
        cur.execute("""
        INSERT INTO performance_metrics 
            (processing_type, metric_name, metric_value, window_size, details)
        VALUES 
            (%s, %s, %s, %s, %s)
        """, (
            'BATCH',
            'PROCESSING_TIME',
            processing_time,
            f"FULL DATASET",
            json.dumps({
                'record_count': len(indicators_df),
                'processing_time_sec': processing_time
            })
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Batch processing completed in {processing_time:.2f} seconds")
        print(f"Processed {len(indicators_df)} records")

if __name__ == "__main__":
    batch_process()