{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
mart_top_movers
------------------
Daily leaderboard: top 3 gainers and top 3 losers by daily_return_pct, per trading day.
Includes 5-day rolling return for momentum context.
*/

WITH returns AS (
  SELECT * from {{ref('fct_daily_returns')}}
  WHERE daily_return_pct IS NOT NULL   --- exclude first-row NULLs per ticker
),

with_rolling AS (
  SELECT
      ticker,
      trade_date,
      company_name,
      sector,
      close,
      daily_return_pct,

      -- 5-day return: close today vs close 5 trading days ago
      ROUND(
          ((close-LAG(close,5) OVER (PARTITION BY ticker ORDER BY trade_date))/
          NULLIF(LAG(close,5) OVER (PARTITION BY ticker ORDER BY trade_date),0))*100,4
      ) as return_5d_pct,

      -- Rank within each date: 1 = best performer that day
      DENSE_RANK() OVER (
        PARTITION BY trade_date
        ORDER BY daily_return_pct DESC
      ) AS gain_rank,

      -- Rank within each date: 1 = worst performer that day
      DENSE_RANK() OVER (
        PARTITION BY trade_date
        ORDER BY daily_return_pct ASC
      ) as loss_rank

      FROM returns
),

with_mover_type AS (
  SELECT
      *,
      CASE
        WHEN gain_rank<=3 AND loss_rank > 3 THEN 'top_gainer'
        WHEN loss_rank <= 3 AND gain_rank > 3 then 'top_loser'
        WHEN gain_rank <= 3 AND loss_rank<=3 THEN 'both'   -- edge case: near-zero return on a quiet day
        ELSE 'neither'
      END AS mover_type

    FROM with_rolling
)

SELECT 
    ticker,
    trade_date,
    company_name,
    sector,
    close,
    daily_return_pct,
    return_5d_pct,
    gain_rank,
    loss_rank,
    mover_type 
FROM with_mover_type
WHERE mover_type != 'neither'
ORDER BY trade_date DESC, daily_return_pct DESC