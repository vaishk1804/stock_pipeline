{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
fct_daily_returns
--------------------
Core fact table. One row per ticker per trading day.

Window functions used:
  LAG(close) over (PARTITION BY ticker ORDER BY trade_date)
    - previous trading day's close for this ticker
    - used to compute daily_return_pct
  
  FIRST_VALUE(close) OVER (PARTITION BY ticker ORDER BY trade_date)
    - first close ever recorded for this ticker
    - used to compute cumulative_return_pct
*/

WITH price AS (
  SELECT * from {{ ref('stg_daily_prices')}}
),

tickers AS (
  SELECT * FROM {{ref('stg_tickers')}}
),

with_prev_close AS (
  SELECT 
    p.ticker,
    p.trade_date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    p.intraday_range_pct,
    p.is_recent,

    -- Previous trading day's close for the particular ticker only
    LAG(p.close) OVER (
      PARTITION BY p.ticker
      ORDER BY p.trade_date
    ) as prev_close,

    -- First close ever recorded for this ticker
    FIRST_VALUE(p.close) OVER (
      PARTITION BY p.ticker
      ORDER BY p.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_close,

    t.company_name,
    t.sector,
    t.industry

    FROM price p
    LEFT JOIN tickers t USING (ticker)
),

final as (
  SELECT
    ticker,
    trade_date,
    company_name,
    sector,
    industry,
    open,
    high,
    low,
    close,
    volume,
    intraday_range_pct,
    is_recent,
    prev_close,

    -- Daily return: NULL for first row per ticker (no previous day)
    CASE 
      when prev_close IS NOT NULL AND prev_close != 0
      THEN ROUND(((close - prev_close)/prev_close)*100,4)
      ELSE NULL
    END AS daily_return_pct,

    -- Cumulative return from first data point for the particular ticker
    CASE 
      WHEN first_close IS NOT NULL AND first_close != 0
      THEN ROUND (((close-first_close)/first_close)*100,4)
      ELSE NULL
    END AS cumulative_return_pct
  
  FROM with_prev_close
)

SELECT * from final
ORDER BY ticker, trade_date

