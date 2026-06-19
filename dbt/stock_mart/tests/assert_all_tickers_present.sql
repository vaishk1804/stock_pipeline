/*
Custom test: assert_all_tickers_present
---------------------------------
Confirms all 10 expected tickers exist in the fact table.
Catches a scenario the built-in tests can't: if one ticker's
Airflow fetch task silently failed (e.g. API hiccup) and the DAG somehow continued, this test catches the gap at the dbt layer as a second line of defense.
*/

WITH expected AS (
  SELECT unnest(ARRAY[
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA','JPM','JNJ','V'
  ]) AS ticker
),

actual AS (
  SELECT DISTINCT ticker FROM {{ ref('fct_daily_returns')}}
)

SELECT
    e.ticker AS missing_ticker
FROM expected e
LEFT JOIN actual a ON e.ticker = a.ticker
WHERE a.ticker IS NULL