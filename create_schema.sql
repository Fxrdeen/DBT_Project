-- Connect to the database
\c stockdb

-- Raw Stock Data Table
CREATE TABLE raw_stock_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    open NUMERIC(16,6) NOT NULL,
    high NUMERIC(16,6) NOT NULL,
    low NUMERIC(16,6) NOT NULL,
    close NUMERIC(16,6) NOT NULL,
    adj_close NUMERIC(16,6) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, ticker)
);

CREATE INDEX idx_raw_stock_data_date ON raw_stock_data(date);
CREATE INDEX idx_raw_stock_data_ticker ON raw_stock_data(ticker);

-- Technical Indicators Table
CREATE TABLE technical_indicators (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    processing_type VARCHAR(20) NOT NULL, -- STREAMING or BATCH
    sma_5 NUMERIC(16,6),
    sma_10 NUMERIC(16,6),
    sma_20 NUMERIC(16,6),
    ema_12 NUMERIC(16,6),
    ema_26 NUMERIC(16,6),
    macd NUMERIC(16,6),
    macd_signal NUMERIC(16,6),
    macd_histogram NUMERIC(16,6),
    rsi_14 NUMERIC(16,6),
    bollinger_upper NUMERIC(16,6),
    bollinger_middle NUMERIC(16,6),
    bollinger_lower NUMERIC(16,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (date, ticker, processing_type)
);

CREATE INDEX idx_technical_indicators_date ON technical_indicators(date);
CREATE INDEX idx_technical_indicators_ticker ON technical_indicators(ticker);
CREATE INDEX idx_technical_indicators_type ON technical_indicators(processing_type);

-- Trading Signals Table
CREATE TABLE trading_signals (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    processing_type VARCHAR(20) NOT NULL, -- STREAMING or BATCH
    signal_type VARCHAR(50) NOT NULL,  -- BUY, SELL, HOLD
    signal_strength NUMERIC(5,2),      -- 0-100 scale
    signal_source VARCHAR(50) NOT NULL, -- RSI, MACD, PATTERN, etc.
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trading_signals_date ON trading_signals(date);
CREATE INDEX idx_trading_signals_ticker ON trading_signals(ticker);
CREATE INDEX idx_trading_signals_type ON trading_signals(signal_type);

-- Performance Metrics Table
CREATE TABLE performance_metrics (
    id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_type VARCHAR(20) NOT NULL, -- STREAMING or BATCH
    metric_name VARCHAR(50) NOT NULL,     -- PROCESSING_TIME, ACCURACY, etc.
    metric_value NUMERIC(16,6) NOT NULL,
    window_size VARCHAR(20),              -- e.g., "10 DAYS"
    details JSONB                         -- Additional details
);

CREATE INDEX idx_performance_metrics_type ON performance_metrics(processing_type);
CREATE INDEX idx_performance_metrics_name ON performance_metrics(metric_name);