-- BTC Spot Data - PostgreSQL Schema (Normalized)
-- Lookup tables for exchanges and symbols to avoid redundant strings

-- =============================================================================
-- LOOKUP TABLES
-- =============================================================================

-- Exchange lookup (1=Binance, 2=Bybit, etc.)
CREATE TABLE IF NOT EXISTS exchanges (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert known exchanges
INSERT INTO exchanges (name) VALUES
    ('Binance'),
    ('Bybit'),
    ('OKX'),
    ('Coinbase'),
    ('Crypto.com'),
    ('Hyperliquid'),
    ('Upbit'),
    ('Bitget'),
    ('MEXC')
ON CONFLICT (name) DO NOTHING;

-- Symbol lookup (1=BTC/USDT, 2=BTC/USD, etc.)
CREATE TABLE IF NOT EXISTS symbols (
    id SERIAL PRIMARY KEY,
    name VARCHAR(20) NOT NULL UNIQUE,
    base_currency VARCHAR(10) NOT NULL,
    quote_currency VARCHAR(10) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert known symbols
INSERT INTO symbols (name, base_currency, quote_currency) VALUES
    ('BTC/USDT', 'BTC', 'USDT'),
    ('BTC/USDC', 'BTC', 'USDC'),
    ('BTC/USD', 'BTC', 'USD'),
    ('BTC/KRW', 'BTC', 'KRW')
ON CONFLICT (name) DO NOTHING;

-- =============================================================================
-- DATA TABLES (using integer foreign keys)
-- =============================================================================

-- Exchange OHLCV data (uses exchange_id and symbol_id instead of strings)
CREATE TABLE IF NOT EXISTS ohlcv_daily (
    id SERIAL PRIMARY KEY,
    exchange_id INTEGER NOT NULL REFERENCES exchanges(id),
    symbol_id INTEGER NOT NULL REFERENCES symbols(id),
    timestamp TIMESTAMPTZ NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(30, 8) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(exchange_id, symbol_id, timestamp)
);

-- Aggregated USD candles (volume-weighted across all exchanges)
CREATE TABLE IF NOT EXISTS aggregated_usd_daily (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL UNIQUE,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume_usd DECIMAL(30, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Daily exchange weights (market share by volume)
CREATE TABLE IF NOT EXISTS exchange_weights (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    exchange_id INTEGER NOT NULL REFERENCES exchanges(id),
    symbol_id INTEGER NOT NULL REFERENCES symbols(id),
    notional_usd DECIMAL(30, 2),
    weight DECIMAL(10, 6),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, exchange_id, symbol_id)
);

-- Premium metrics (Coinbase and Kimchi)
CREATE TABLE IF NOT EXISTS premiums_daily (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    coinbase_premium_pct DECIMAL(10, 4),
    kimchi_premium_pct DECIMAL(10, 4),
    usdkrw_rate DECIMAL(15, 4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_ohlcv_exchange_symbol ON ohlcv_daily(exchange_id, symbol_id);
CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp ON ohlcv_daily(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weights_date ON exchange_weights(date DESC);
CREATE INDEX IF NOT EXISTS idx_premiums_date ON premiums_daily(date DESC);

-- =============================================================================
-- VIEWS (for easy querying with names instead of IDs)
-- =============================================================================

CREATE OR REPLACE VIEW v_ohlcv_daily AS
SELECT 
    o.id,
    e.name AS exchange,
    s.name AS symbol,
    o.timestamp,
    o.open, o.high, o.low, o.close, o.volume,
    o.created_at
FROM ohlcv_daily o
JOIN exchanges e ON o.exchange_id = e.id
JOIN symbols s ON o.symbol_id = s.id;

CREATE OR REPLACE VIEW v_exchange_weights AS
SELECT 
    w.id,
    w.date,
    e.name AS exchange,
    s.name AS symbol,
    CONCAT(e.name, ' ', s.name) AS exchange_pair,
    w.notional_usd,
    w.weight,
    w.created_at
FROM exchange_weights w
JOIN exchanges e ON w.exchange_id = e.id
JOIN symbols s ON w.symbol_id = s.id;
