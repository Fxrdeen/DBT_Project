import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json

# PostgreSQL connection parameters
pg_params = {
    'dbname': 'stockdb',
    'user': 'stockuser',
    'password': 'stockpassword',
    'host': 'localhost'
}

def compare_performance():
    print("Comparing streaming vs batch performance...")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(**pg_params)
    
    # Query performance metrics
    perf_df = pd.read_sql("""
        SELECT 
            execution_date,
            processing_type,
            metric_name,
            metric_value,
            window_size,
            details
        FROM 
            performance_metrics
        WHERE 
            metric_name = 'PROCESSING_TIME'
        ORDER BY 
            execution_date
    """, conn)
    
    # Filter out unreasonable processing times (e.g., > 1 hour)
    perf_df = perf_df[perf_df['metric_value'] < 3600]
    
    # Query indicator comparison
    indicator_df = pd.read_sql("""
        SELECT 
            stream.date,
            stream.sma_20 as stream_sma20,
            batch.sma_20 as batch_sma20,
            ABS(stream.sma_20 - batch.sma_20) as sma_diff,
            stream.rsi_14 as stream_rsi,
            batch.rsi_14 as batch_rsi,
            ABS(stream.rsi_14 - batch.rsi_14) as rsi_diff,
            stream.macd as stream_macd,
            batch.macd as batch_macd,
            ABS(stream.macd - batch.macd) as macd_diff
        FROM 
            technical_indicators stream
        JOIN 
            technical_indicators batch 
            ON stream.date = batch.date AND stream.ticker = batch.ticker
        WHERE 
            stream.processing_type = 'STREAMING' AND
            batch.processing_type = 'BATCH'
        ORDER BY 
            stream.date
    """, conn)
    
    # Query signal comparison
    signal_df = pd.read_sql("""
        SELECT 
            date,
            COUNT(CASE WHEN processing_type = 'STREAMING' AND signal_type = 'BUY' THEN 1 END) as stream_buy,
            COUNT(CASE WHEN processing_type = 'BATCH' AND signal_type = 'BUY' THEN 1 END) as batch_buy,
            COUNT(CASE WHEN processing_type = 'STREAMING' AND signal_type = 'SELL' THEN 1 END) as stream_sell,
            COUNT(CASE WHEN processing_type = 'BATCH' AND signal_type = 'SELL' THEN 1 END) as batch_sell,
            COUNT(CASE WHEN processing_type = 'STREAMING' AND signal_type = 'HOLD' THEN 1 END) as stream_hold,
            COUNT(CASE WHEN processing_type = 'BATCH' AND signal_type = 'HOLD' THEN 1 END) as batch_hold
        FROM 
            trading_signals
        GROUP BY 
            date
        ORDER BY 
            date
    """, conn)
    
    conn.close()
    
    # Performance Analysis
    if not perf_df.empty:
        # Separate streaming and batch metrics
        streaming_perf = perf_df[perf_df['processing_type'] == 'STREAMING']
        batch_perf = perf_df[perf_df['processing_type'] == 'BATCH']
        
        # Calculate statistics
        if not streaming_perf.empty:
            streaming_avg = streaming_perf['metric_value'].mean()
            streaming_total = streaming_perf['metric_value'].sum()
            streaming_count = len(streaming_perf)
            print(f"Streaming Performance: {streaming_avg:.4f} sec avg, {streaming_total:.2f} sec total, {streaming_count} batches")
        
        if not batch_perf.empty:
            batch_total = batch_perf['metric_value'].sum()
            print(f"Batch Performance: {batch_total:.2f} sec total")
        
        # Create directory for plots
        import os
        os.makedirs("performance_plots", exist_ok=True)
        
        # Plot streaming vs batch performance
        plt.figure(figsize=(12, 6))
        sns.barplot(x='processing_type', y='metric_value', data=perf_df)
        plt.title('Processing Time Comparison')
        plt.ylabel('Time (seconds)')
        plt.savefig('performance_plots/processing_time.png')
        
    # Indicator Accuracy Analysis
    if not indicator_df.empty:
        # Plot SMA differences
        plt.figure(figsize=(12, 6))
        plt.plot(indicator_df['date'], indicator_df['sma_diff'], label='SMA-20 Difference')
        plt.plot(indicator_df['date'], indicator_df['rsi_diff'], label='RSI-14 Difference')
        plt.plot(indicator_df['date'], indicator_df['macd_diff'], label='MACD Difference')
        plt.title('Indicator Differences: Streaming vs Batch')
        plt.xlabel('Date')
        plt.ylabel('Absolute Difference')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('performance_plots/indicator_diff.png')
    
    # Signal Agreement Analysis
    if not signal_df.empty:
        # Calculate agreement percentages
        signal_df['buy_agreement'] = (signal_df['stream_buy'] == signal_df['batch_buy']).astype(int)
        signal_df['sell_agreement'] = (signal_df['stream_sell'] == signal_df['batch_sell']).astype(int)
        signal_df['hold_agreement'] = (signal_df['stream_hold'] == signal_df['batch_hold']).astype(int)
        
        buy_agreement_pct = signal_df['buy_agreement'].mean() * 100
        sell_agreement_pct = signal_df['sell_agreement'].mean() * 100
        hold_agreement_pct = signal_df['hold_agreement'].mean() * 100
        
        print(f"Signal Agreement - Buy: {buy_agreement_pct:.2f}%, Sell: {sell_agreement_pct:.2f}%, Hold: {hold_agreement_pct:.2f}%")
        
        # Plot signal counts
        plt.figure(figsize=(12, 6))
        plt.plot(signal_df['date'], signal_df['stream_buy'], 'g-', label='Stream Buy')
        plt.plot(signal_df['date'], signal_df['batch_buy'], 'g--', label='Batch Buy')
        plt.plot(signal_df['date'], signal_df['stream_sell'], 'r-', label='Stream Sell')
        plt.plot(signal_df['date'], signal_df['batch_sell'], 'r--', label='Batch Sell')
        plt.title('Trading Signals: Streaming vs Batch')
        plt.xlabel('Date')
        plt.ylabel('Signal Count')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('performance_plots/signal_comparison.png')
    
    # Save results to JSON
    results = {
        'streaming_performance': {
            'avg_time': streaming_avg if 'streaming_avg' in locals() else None,
            'total_time': streaming_total if 'streaming_total' in locals() else None,
            'batch_count': streaming_count if 'streaming_count' in locals() else 0
        },
        'batch_performance': {
            'total_time': batch_total if 'batch_total' in locals() else None
        },
        'indicator_accuracy': {
            'avg_sma_diff': indicator_df['sma_diff'].mean() if not indicator_df.empty else None,
            'avg_rsi_diff': indicator_df['rsi_diff'].mean() if not indicator_df.empty else None,
            'avg_macd_diff': indicator_df['macd_diff'].mean() if not indicator_df.empty else None
        },
        'signal_agreement': {
            'buy_agreement': buy_agreement_pct if 'buy_agreement_pct' in locals() else None,
            'sell_agreement': sell_agreement_pct if 'sell_agreement_pct' in locals() else None,
            'hold_agreement': hold_agreement_pct if 'hold_agreement_pct' in locals() else None
        }
    }
    
    with open('performance_plots/results.json', 'w') as f:
        json.dump(results, f, indent=4)
    
    print("Performance comparison completed. Results saved to 'performance_plots' directory.")

if __name__ == "__main__":
    compare_performance()