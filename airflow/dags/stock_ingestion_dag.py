"""
airflow/dags/stock_ingestion_dag.py
---------------------------------
Daily ELT DAG: fetch OHLCV from Alpha Vantage -> upsert into raw.daily_prices -> validate data quality

Schedule: 6:00 AM UTC Mon-Fri
Each ticker is a seperate task so failures are isolated per ticker.
"""

import os
import time
import logging
import sys

import requests 
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Adding scripts/ to path so we can import directly
sys.path.insert(0,"/opt/airflow/scripts")

from ingest_all_tickers import fetch_ticker, TICKERS, DB_CONN
from validate_raw_data import main as run_validation

logger = logging.getLogger(__name__)

# ---- Wrapper functions -----
# Airflow tasks functions need **context as parameter
# The scripts dont have that so we wrap them

def check_api_health(**context):
  API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY","")
  API_BASE="https://www.alphavantage.co/query"
  url=f"{API_BASE}?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={API_KEY}&outputsize=compact"
  resp = requests.get(url,timeout=15)
  resp.raise_for_status()
  data = resp.json()
  if "Error Message" in data:
    raise ValueError(f"API error: {data['Error Message']}")
  if "Note" in data:
    raise ValueError(f"Rate limit: {data['Note']}")
  logger.info("API health check passed.")

def fetch_ticker_task(ticker: str, **context):
  fetch_ticker(ticker)

def validate_task(**context):
  run_validation()

# ---- DAG definition ---------------

default_args = {
  "owner": "vaishnavi",
  "depends_on_past": False,
  "email_on_failure": False,
  "retries": 2,
  "retry_delay": timedelta(minutes=5),
}

with DAG(
  dag_id="stock_daily_ingestion",
  default_args=default_args,
  description="Daily ELT: Alpha Vantage -> PostgreSQL raw layer",
  schedule_interval="0 6 * * 1-5",
  start_date=days_ago(1),
  catchup=False,
  max_active_runs=1,
  tags=["stocks","elt"],
) as dag:
  
  api_health = PythonOperator(
    task_id="check_api_health",
    python_callable=check_api_health,
  )

  fetch_tasks=[]
  for ticker in TICKERS:
    task = PythonOperator(
      task_id=f"fetch_{ticker.lower()}",
      python_callable=fetch_ticker_task,
      op_kwargs={"ticker":ticker},
    )
    fetch_tasks.append(task)

  validate = PythonOperator(
      task_id="validate_raw_data",
      python_callable=validate_task,
    )

  pipeline_complete = EmptyOperator(task_id="pipeline_complete")

  api_health >> fetch_tasks[0]
  for i in range(len(fetch_tasks)-1):
      fetch_tasks[i]>>fetch_tasks[i+1]
  fetch_tasks[-1]>>validate>>pipeline_complete


