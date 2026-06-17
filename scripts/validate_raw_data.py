"""
scripts/validate_raw_data.py
----------------------------
Runs data quality checks against raw.daily_prices.
Prints a pass/fail report.

This is the same logic that will run as a tasks in the Airflow DAG
before dbt is allowed to execute - bad data should never reach the mart layer.
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

EXPECTED_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN","META","NVDA","TSLA","JPM","JNJ","V"]

DB_CONN={
  "host": os.getenv("STOCK_DB_HOST", "127.0.0.1"),
  "port": int(os.getenv("STOCK_DB_PORT",5433)),
  "dbname": os.getenv("STOCK_DB_NAME"),
  "user": os.getenv("STOCK_DB_USER"),
  "password":os.getenv("STOCK_DB_PASSWORD"),
}

def run_check(cur,name:str,sql:str,expect_zero:bool=True) -> bool:
  """
  Run a SQL check.
  expect_zero=True -> passes if result is 0 (e.g. no NULLS found)
  expect_zero=False -> passes if result is >0 (e.g. rows exist)
  """

  cur.execute(sql)
  result = cur.fetchone()[0]

  if expect_zero:
    passed = (result == 0)
  else:
    passed = (result > 0)

  status = "PASS" if passed else "FAIL"
  print(f" [{status}] {name}: {result}")
  return passed

def main():
  conn=psycopg2.connect(**DB_CONN)
  cur = conn.cursor()

  print("\n"+"-"*55)
  print("DATA QUALITY REPORT - raw.daily_prices")
  print("-"*55)

  checks=[]

  # -- Check 1: Table is not empty ----------
  passed = run_check(
    cur,
    "Total row count > 0",
    "SELECT COUNT(*)FROM raw.daily_prices",
    expect_zero=False
  )
  checks.append(passed)

  # ── Check 2: Exact expected tickers present ───────────────
  cur.execute("SELECT DISTINCT ticker FROM raw.daily_prices ORDER BY ticker")
  actual_tickers = {row[0] for row in cur.fetchall()}
  expected_set   = set(EXPECTED_TICKERS)
  missing= expected_set - actual_tickers
  unexpected     = actual_tickers - expected_set
  passed = (missing == set() and unexpected == set())
  status = "PASS" if passed else "FAIL"
  print(f"  [{status}] Exact ticker list match", end="")
  if missing:
    print(f" | missing: {missing}", end="")
  if unexpected:
    print(f" | unexpected: {unexpected}", end="")
  print()
  checks.append(passed)

  # --- Check 3: No NULL prices in any price column -------------------
  for col in ["open_price","high_price","low_price","close_price"]:
    checks.append(run_check(
    cur,
    f"No NULL {col}",
    f"SELECT COUNT(*) FROM raw.daily_prices WHERE {col} IS NULL"
  ))

  # ---- Check 4: No negative or zero prices -------
  checks.append(run_check(
    cur,
    "No zero or negative prices",
    "SELECT COUNT(*) FROM raw.daily_prices WHERE close_price<=0"
  ))

  # --- Check 6: Close within High/Low range -----------
  checks.append(run_check(
    cur,
    "Close price within High/Low range",
    """SELECT COUNT(*) FROM raw.daily_prices
    WHERE close_price>high_price OR close_price<low_price"""
  ))

  # --- Check 7: No duplicates -------------
  checks.append(run_check(
    cur,
    "No duplicate (ticker,date) rows",
    """SELECT COUNT(*) FROM (
    SELECT ticker, trade_date, COUNT(*)
    FROM raw.daily_prices
    GROUP BY ticker, trade_date
    HAVING COUNT(*)>1) dupes"""
  ))

  # ---- Check 8: Each ticker has at least 90 rows ----------------
  cur.execute("""
SELECT ticker, COUNT(*) as rows
              FROM raw.daily_prices
              GROUP BY ticker
              HAVING COUNT(*) < 90
              """)
  thin_tickers=cur.fetchall()
  passed = (len(thin_tickers)==0)
  status = "PASS" if passed else "FAIL"
  print(f" [{status}] All tickers have >=90 rows" + (f": thin tickers = {thin_tickers}" if not passed else ""))
  checks.append(passed)

  # ── Check 9: High always >= Low ───────────────────────────
  checks.append(run_check(
    cur,
    "High price always >= Low price",
    "SELECT COUNT(*) FROM raw.daily_prices WHERE high_price < low_price"
))
  
  # --- Summary --------------
  total = len(checks)
  passed = sum(checks)
  failed=total-passed

  print("="*55)
  print(f" {passed}/{total} checks passed",end="")
  if failed == 0:
    print(" All clear - safe to run dbt.")
  else:
    print(f"\n {failed} check(s) Failed - fix before running dbt")
  print("="*55+"\n")

  cur.close()
  conn.close()


  # Exit with non-zero code if any check failed
  # # (Airflow reads exit code - that is how the DAG knows to stop)
  if failed > 0:
    exit(1)

if __name__=="__main__":
  main() 