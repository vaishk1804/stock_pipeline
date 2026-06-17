"""
scripts/ingest_all_tickers.py

Fetches daily OHLCV data for all 10 tickers from Alpha Vantage and upserts into raw.daily_prices.

Rate Limit: Alpha Vantage free tier = 25 calls/day, 5 calls/min.
Sleep 15 seconds between each ticker to stay safe
"""

import json
import os
import time
import psycopg2
import psycopg2.extras
import requests

from dotenv import load_dotenv

load_dotenv()

# Loading .env file so we don't hardcode and credentials
# -- Config --
API_KEY=os.getenv("ALPHA_VANTAGE_API_KEY")
API_URL="https://www.alphavantage.co/query"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "JNJ", "V"]

DB_CONN={
  "host":os.getenv("STOCK_DB_HOST"),
  "port": int(os.getenv("STOCK_DB_PORT",5433)),
  "dbname": os.getenv("STOCK_DB_NAME","stocks"),
  "user": os.getenv("STOCK_DB_USER"),
  "password": os.getenv("STOCK_DB_PASSWORD"),
}

UPSERT_SQL="""
INSERT INTO raw.daily_prices 
(ticker,trade_date,open_price,high_price,low_price,close_price,volume)
VALUES
(%(ticker)s,%(trade_date)s,%(open_price)s,%(high_price)s,%(low_price)s,%(close_price)s,%(volume)s)
ON CONFLICT (ticker,trade_date)
DO UPDATE SET
  open_price=EXCLUDED.open_price,
  high_price=EXCLUDED.high_price,
  low_price=EXCLUDED.low_price,
  close_price=EXCLUDED.close_price,
  volume = EXCLUDED.volume,
  ingested_At = NOW();
"""

def fetch_ticker(ticker: str) -> list[dict]:
  """Fetch 100 days of OHLCV for one ticker. Returns list of row dicts"""
  url=(
    f"{API_URL}"
    f"?function=TIME_SERIES_DAILY"
    f"&symbol={ticker}"
    f"&apikey={API_KEY}"
    f"&outputsize=compact"
  )
  response = requests.get(url,timeout=15)
  response.raise_for_status()
  data=response.json()

  # Catch API-level errors (rate_limit, bad_key, invalid ticker)
  if "Error Message" in data:
    raise ValueError(f"API error for {ticker}: {data['Error Message']}")
  if "Note" in data:
    raise ValueError(f"Rate limit hit for {ticker}: {data['Note']}")
  
  time_series = data.get("Time Series (Daily)", {})
  if not time_series:
    raise ValueError(f"No data returned for {ticker}")
  
  rows = []
  for date_str, values in time_series.items():
    rows.append({
      "ticker": ticker,
      "trade_date": date_str,
      "open_price": float(values["1. open"]),
      "high_price": float(values["2. high"]),
      "low_price": float(values["3. low"]),
      "close_price": float(values["4. close"]),
      "volume": int(values["5. volume"]),
    })
  return rows
  
def upsert_rows(conn, rows: list[dict]):
  """Write a batch of rows to raw.daily_prices"""
  with conn:
    with conn.cursor() as cur:
      psycopg2.extras.execute_batch(cur,UPSERT_SQL,rows,page_size=100)

def main():
  print(">>> Connecting to PostgreSQL..")
  conn = psycopg2.connect(**DB_CONN)
  print(">>> Connected.\n")

  results=[]   # track success/failure per ticker

  for i,ticker in enumerate(TICKERS):
    print(f"[{i+1}/{len(TICKERS)}] Fetching {ticker}...")

    try:
      rows=fetch_ticker(ticker)
      upsert_rows(conn,rows)
      print(f"{len(rows)} rows upserted for {ticker}")
      results.append((ticker,len(rows),"OK"))

    except Exception as e:
      print(f"Failed for {ticker}:{e}")
      results.append((ticker,0,str(e)))

    # Rate limit: 5 calls/min on free tier -> wait 15s between calls
    # Skip sleep after the last ticker
    if i<len(TICKERS)-1:
      print(f"Waiting 15s(rate limit)...")
      time.sleep(15)

  conn.close()

  # Summary
  print("\n" + "="*50)
  print("INGESTION SUMMARY")
  print("="*50)
  total_rows = 0
  for ticker, row_count, status in results:
      print(f"  {ticker:<6} {status:<5} {row_count:>5} rows")
      total_rows += row_count
  print(f"\n  Total rows upserted: {total_rows}")
  print("="*50)

if __name__ =="__main__":
  main()




