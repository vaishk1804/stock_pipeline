"""
scripts/ingest_one_ticker.py

1. Hits Alpha Vantage API for AAPL and prints the raw respose
2. Parses response into rows
3. Upserts rows into raw.daily_prices

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

TICKER = "AAPL"
API_KEY= os.getenv("ALPHA_VANTAGE_API_KEY")
API_URL="https://www.alphavantage.co/query"

DB_CONN = {
  "host": "127.0.0.1",
  "port": int(os.getenv("STOCK_DB_PORT",5432)),
  "dbname": os.getenv("STOCK_DB_NAME","stocks"),
  "user": os.getenv("STOCK_DB_USER", "stocks_user"),
  "password": os.getenv("STOCK_DB_PASSWORD", "stocks_pass"),
}

# Step 1: Hit the API

print(f"\n>>> Fetching data for {TICKER} from Alpha Vantage...")

url=(
  f"{API_URL}"
  f"?function=TIME_SERIES_DAILY"
  f"&symbol={TICKER}"
  f"&apikey={API_KEY}"
  f"&outputsize=compact"  # outputs last 100 trading days
)

response = requests.get(url, timeout=15)
response.raise_for_status()
data = response.json()

# Printing the raw API response so we can check exactly what the API is returning
print(f"\n>>> Raw API response ( first 3 dates only ):")

time_series = data.get("Time Series (Daily)",{})
for i, (date,values) in enumerate(time_series.items()):
  if i>=3:
    break
  print(f"   {date} : {json.dumps(values, indent=4)}")

print(f" \n >>> Total trading days returned: { len(time_series)}")

# Parsing into rows

rows=[]
for date_str,values in time_series.items():
  rows.append({
        "ticker":       TICKER,
        "trade_date":   date_str,
        "open_price":   float(values["1. open"]),
        "high_price":   float(values["2. high"]),
        "low_price":    float(values["3. low"]),
        "close_price":  float(values["4. close"]),
        "volume":       int(values["5. volume"]),
    })
  
print(f">> Parsed {len(rows)} rows")
print(f"\n>>> Sample row: {rows[0]}")

print(f"\n>> Connecting to PostgreSQL")

conn= psycopg2.connect(**DB_CONN)
print(">>>Connected.")

# ON CONFLICT DO UPDATE
# - if (ticker,trade_date) doesn't exist -> INSERT new row
# - if it already exists -> UPDATE prices (safe to re-run)

UPSERT_SQL="""
INSERT INTO raw.daily_prices (ticker, trade_date, open_price,high_price, low_price, close_price,volume)
VALUES
(%(ticker)s, %(trade_date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s)
ON CONFLICT (ticker, trade_date)
DO UPDATE SET
  open_price = EXCLUDED.open_price,
  high_price = EXCLUDED.high_price,
  low_price = EXCLUDED.low_price,
  close_price = EXCLUDED.close_price,
  volume = EXCLUDED.volume,
  ingested_at = NOW();
"""

with conn:
  with conn.cursor() as curr:
    psycopg2.extras.execute_batch(curr, UPSERT_SQL, rows, page_size=100)

print(f">>> Upserted {len(rows)} rows for {TICKER}")

# Verification

print("\n Verifying database content")

with conn.cursor() as cur:
  cur.execute("""
SELECT trade_date, close_price, volume 
              FROM raw.daily_prices
              where ticker = %s
              ORDER BY trade_date DESC
              LIMIT 5
              """, (TICKER,))
  result=cur.fetchall()

print(f">> Last 5 rows in raw.daily_prices for {TICKER}:")
print(f"  {'Date':<15} {'Close':>10} {'Volume':>15}")
print(f"  {'-'*15} {'-'*10} {'-'*15}")
for row in result:
    print(f"  {str(row[0]):<15} {float(row[1]):>10.2f} {row[2]:>15,}")

conn.close()
print("\nDone")

