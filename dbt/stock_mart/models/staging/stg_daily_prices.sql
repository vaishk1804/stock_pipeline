{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
stg_daiy_prices
---------------
Cleans and types raw OHLCV data from the Airflow ingest layer.

Changes from raw:
- Rename price columns: open_price -> open, etc.
- Adds intraday_range_pct: (high-low) / open *100
- Adds is_recent: TRUE if trade_date is within last 30 days
- Filters out NULL and zero close prices
*/

WITH source AS (
  SELECT * FROM {{ source('raw','daily_prices')}}
),

cleaned AS (
  SELECT
  ticker,
  trade_date,
  CAST(open_price AS NUMERIC(12,4)) AS open,
  CAST(high_price AS NUMERIC(12,4)) as high,
  CAST(low_price AS NUMERIC(12,4)) as low,
  CAST(close_price AS NUMERIC(12,4)) AS close,
  volume,

  ROUND(
    ((CAST(high_price AS NUMERIC)-CAST(low_price as NUMERIC))/ NULLIF(CAST(open_price AS NUMERIC),0)) * 100,
    4
  ) as intraday_range_pct,

  CASE 
      WHEN trade_date >= CURRENT_DATE - INTERVAL '30 days'
      THEN TRUE
      ELSE FALSE
  END AS is_recent,

  ingested_At

  FROM source
  WHERE close_price IS NOT NULL
  AND close_price > 0
)

SELECT * FROM cleaned