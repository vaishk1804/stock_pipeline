{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
mart_moving_averages
---------------------
7,30, and 90-day simple moving averages of close price per ticker per day.
Also includes relative volume and golden/death cross signals.

Golden/Death cross:
  When 7-day MA crosses above 30-day MA -> bullish signal (golden cross)
  When 7-day MA crosses below 30-day MA -> bearish signal (death cross)
*/

WITH base as (
  SELECT * from {{ref('fct_daily_returns')}}
),

with_moving_avgs AS (
  SELECT
    ticker,
    trade_date,
    company_name,
    sector,
    close,
    daily_return_pct,
    cumulative_return_pct,
    is_recent,
    volume,

    -- 7-day simple moving average
    ROUND(
      AVG(close) OVER
(
  PARTITION BY ticker
  ORDER BY trade_date
  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
),4
    ) AS ma_7d,


    -- 30-day simple moving average
    ROUND(
      AVG(close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
      ), 4
    ) AS ma_30d,

    -- 90-day simple moving average
    ROUND(
      AVG(close) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
      ), 4
    ) AS ma_90d,

    -- 20-day average volume
    ROUND(
      AVG(volume) OVER (
        PARTITION BY ticker
        ORDER BY trade_date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
      ), 0
    ) AS volume_avg_20d

    FROM base
),

with_cross_signals AS (
  SELECT
  *,

  -- Previous day's MAs for cross detection
  LAG(ma_7d) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_ma_7d,
  LAG(ma_30d) OVER (PARTITION BY ticker ORDER BY trade_date) AS prev_ma_30d,

  -- Relative volume: today vs 20-day average
  CASE
    WHEN volume_avg_20d > 0
    THEN ROUND(volume::NUMERIC / volume_avg_20d,4)
    ELSE NULL
  END AS relative_volume

  FROM with_moving_avgs
),

final as (
  SELECT
      ticker,
      trade_date,
      company_name,
      sector,
      close,
      daily_return_pct,
      cumulative_return_pct,
      is_recent,
      volume,
      volume_avg_20d,
      relative_volume,
      ma_7d,
      ma_30d,
      ma_90d,

      -- Golden/Death cross signal
      CASE
          WHEN prev_ma_7d < prev_ma_30d AND ma_7d >=ma_30d THEN 'golden_cross'
          WHEN prev_ma_7d > prev_ma_30d AND ma_7d<=ma_30d THEN 'death_cross'
          ELSE NULL
      END AS ma_cross_signal

    FROM with_cross_signals
)

SELECT * FROM final
ORDER BY ticker, trade_date