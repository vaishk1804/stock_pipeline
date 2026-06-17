"""
scripts/ingest_all_tickers.py

Fetches daily OHLCV data for all 10 tickers from Alpha Vantage and upserts into raw.daily_prices.

Rate Limit: Alpha Vantage free tier = 25 calls/day, 5 calls/min.
Two ways to use:
  1. Run directly:  python scripts/ingest_all_tickers.py
  2. Import into Airflow DAG: from ingest_all_tickers import fetch_ticker
"""

import json
import os
import time
import psycopg2
import psycopg2.extras
import requests

from dotenv import load_dotenv
from typing import List, Dict
load_dotenv()

# Loading .env file so we don't hardcode and credentials
# -- Config --
API_KEY=os.getenv("ALPHA_VANTAGE_API_KEY")
API_URL="https://www.alphavantage.co/query"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "JNJ", "V"]

DB_CONN={
  "host":os.getenv("STOCK_DB_HOST","127.0.0.1"),
  "port": int(os.getenv("STOCK_DB_PORT",5433)),
  "dbname": os.getenv("STOCK_DB_NAME","stocks"),
  "user": os.getenv("STOCK_DB_USER","airflow"),
  "password": os.getenv("STOCK_DB_PASSWORD","airflow"),
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

def _fetch_from_api(ticker: str) -> List[Dict]:
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
  
def fetch_ticker(ticker:str):
  """
    Fetch and upsert one ticker.
    Called by Airflow — one task per ticker.
    Also callable standalone for testing.
    """
  rows = _fetch_from_api(ticker)
  conn = psycopg2.connect(**DB_CONN)
  try:
    with conn:
      with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur,UPSERT_SQL,rows,page_size=100)
    print(f"Upserted {len(rows)} rows for {ticker}")
  finally:
    conn.close()

  # Rate limit: 5 calls/min on free tier
  time.sleep(15)

def main():
  print(">>> Connecting to PostgreSQL..")

  results=[]   # track success/failure per ticker

  for i,ticker in enumerate(TICKERS):
    print(f"[{i+1}/{len(TICKERS)}] Fetching {ticker}...")

    try:
      rows=fetch_ticker(ticker)
      results.append((ticker,"OK"))

    except Exception as e:
      print(f"Failed for {ticker}:{e}")
      results.append((ticker,str(e)))

  # Summary
  print("\n" + "="*50)
  print("INGESTION SUMMARY")
  print("="*50)
  total_rows = 0
  for ticker,status in results:
      print(f"  {ticker:<6} {status}")
  print("="*50)

if __name__ =="__main__":
  main()




