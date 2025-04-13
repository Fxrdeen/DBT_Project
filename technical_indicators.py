import numpy as np
import pandas as pd

class TechnicalIndicators:
    @staticmethod
    def calculate_sma(data, window):
        """Calculate Simple Moving Average"""
        return data.rolling(window=window).mean()
    
    @staticmethod
    def calculate_ema(data, span):
        """Calculate Exponential Moving Average"""
        return data.ewm(span=span, adjust=False).mean()
    
    @staticmethod
    def calculate_macd(data, fast_period=12, slow_period=26, signal_period=9):
        """Calculate MACD"""
        ema_fast = TechnicalIndicators.calculate_ema(data, fast_period)
        ema_slow = TechnicalIndicators.calculate_ema(data, slow_period)
        macd_line = ema_fast - ema_slow
        signal_line = TechnicalIndicators.calculate_ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    @staticmethod
    def calculate_rsi(data, window=14):
        """Calculate Relative Strength Index"""
        delta = data.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        
        ema_up = up.ewm(com=window-1, adjust=False).mean()
        ema_down = down.ewm(com=window-1, adjust=False).mean()
        
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_bollinger_bands(data, window=20, num_std=2):
        """Calculate Bollinger Bands"""
        middle_band = TechnicalIndicators.calculate_sma(data, window)
        std_dev = data.rolling(window=window).std()
        upper_band = middle_band + (std_dev * num_std)
        lower_band = middle_band - (std_dev * num_std)
        return upper_band, middle_band, lower_band
    
    @staticmethod
    def generate_signals(close, sma_short, sma_long, rsi, macd, macd_signal):
        """Generate trading signals based on indicators"""
        signals = []
        
        # Golden Cross / Death Cross (SMA)
        if sma_short > sma_long:
            signals.append({
                'signal_type': 'BUY',
                'signal_strength': 80.0,
                'signal_source': 'SMA_CROSS',
                'description': 'Golden Cross: Short-term SMA crossed above long-term SMA'
            })
        elif sma_short < sma_long:
            signals.append({
                'signal_type': 'SELL',
                'signal_strength': 80.0,
                'signal_source': 'SMA_CROSS',
                'description': 'Death Cross: Short-term SMA crossed below long-term SMA'
            })
        
        # RSI signals
        if rsi < 30:
            signals.append({
                'signal_type': 'BUY',
                'signal_strength': 70.0 + (30 - rsi),  # Stronger signal the lower RSI goes
                'signal_source': 'RSI',
                'description': f'Oversold condition: RSI = {rsi:.2f}'
            })
        elif rsi > 70:
            signals.append({
                'signal_type': 'SELL',
                'signal_strength': 70.0 + (rsi - 70),  # Stronger signal the higher RSI goes
                'signal_source': 'RSI',
                'description': f'Overbought condition: RSI = {rsi:.2f}'
            })
        
        # MACD signals
        if macd > macd_signal:
            signals.append({
                'signal_type': 'BUY',
                'signal_strength': 75.0,
                'signal_source': 'MACD',
                'description': 'MACD line crossed above signal line'
            })
        elif macd < macd_signal:
            signals.append({
                'signal_type': 'SELL',
                'signal_strength': 75.0,
                'signal_source': 'MACD',
                'description': 'MACD line crossed below signal line'
            })
        
        # If no signals, generate HOLD
        if not signals:
            signals.append({
                'signal_type': 'HOLD',
                'signal_strength': 50.0,
                'signal_source': 'COMBINED',
                'description': 'No clear buy or sell signals'
            })
        
        return signals