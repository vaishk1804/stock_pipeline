-- Create the stocks database and user

CREATE USER stocks_user WITH PASSWORD 'stocks_pass';
CREATE DATABASE stocks OWNER stocks_user;

\connect stocks

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE stocks TO stocks_user;
GRANT ALL ON SCHEMA public TO stocks_user;

-- Three schemas: raw (landing zone), staging(dbt views), marts (dbt tables)
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION stocks_user;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION stocks_user;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION stocks_user;

GRANT ALL ON SCHEMA raw TO stocks_user;
GRANT ALL ON SCHEMA staging TO stocks_user;
GRANT ALL ON SCHEMA marts TO stocks_user;

-- Raw OHLCV table: one row per ticker per trading day
-- UNIQUE constraint is what makes the upsert idempotent (safe to re-run)

CREATE TABLE IF NOT EXISTS raw.daily_prices (
  id SERIAL PRIMARY KEY,
  ticker VARCHAR(10) NOT NULL,
  trade_date DATE NOT NULL,
  open_price NUMERIC(12,4),
  high_price NUMERIC(12,4),
  low_price NUMERIC (12,4),
  close_price NUMERIC(12,4),
  volume BIGINT,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (ticker, trade_date)
);

-- Intraday prices: written by Kafka consumer in real time
CREATE TABLE IF NOT EXISTS raw.intraday_prices (
  id SERIAL PRIMARY KEY,
  ticker VARCHAR(10) NOT NULL,
  price NUMERIC(12,4) NOT NULL,
  open NUMERIC(12,4),
  high NUMERIC(12,4),
  low NUMERIC(12,4),
  volume BIGINT,
  previous_close NUMERIC(12,4),
  change_amt NUMERIC(12,4),
  change_pct NUMERIC (12,4),
  produced_at TIMESTAMPTZ NOT NULL,
  consumed_At TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (ticker,produced_at)
);

-- Ticker reference table: seeded below
CREATE TABLE IF NOT EXISTS raw.tickers (
  ticker VARCHAR(10) PRIMARY KEY,
  company_name VARCHAR(255),
  sector VARCHAR(100),
  industry VARCHAR(100),
  added_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed ticker metadata
INSERT INTO raw.tickers (ticker,company_name,sector,industry) VALUES
    ('AAPL',  'Apple Inc.',               'Technology',         'Consumer Electronics'),
    ('MSFT',  'Microsoft Corporation',     'Technology',         'Software—Infrastructure'),
    ('GOOGL', 'Alphabet Inc.',             'Technology',         'Internet Content & Information'),
    ('AMZN',  'Amazon.com Inc.',           'Consumer Cyclical',  'Internet Retail'),
    ('META',  'Meta Platforms Inc.',       'Technology',         'Internet Content & Information'),
    ('NVDA',  'NVIDIA Corporation',        'Technology',         'Semiconductors'),
    ('TSLA',  'Tesla Inc.',                'Consumer Cyclical',  'Auto Manufacturers'),
    ('JPM',   'JPMorgan Chase & Co.',      'Financial Services', 'Banks—Diversified'),
    ('JNJ',   'Johnson & Johnson',         'Healthcare',         'Drug Manufacturers'),
    ('V',     'Visa Inc.',                 'Financial Services', 'Credit Services')
ON CONFLICT (ticker) DO NOTHING;

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON raw.daily_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_daily_prices_trade_data ON raw.daily_prices(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker_date ON raw.daily_prices(ticker, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_intraday_ticker ON raw.intraday_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_intraday_produced ON raw.intraday_prices(produced_at DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_ticker_ts ON raw.intraday_prices(ticker, produced_at DESC);

GRANT ALL ON ALL TABLES IN SCHEMA raw TO stocks_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw TO stocks_user;

ALTER TABLE raw.daily_prices    OWNER TO stocks_user;
ALTER TABLE raw.intraday_prices OWNER TO stocks_user;
ALTER TABLE raw.tickers         OWNER TO stocks_user;