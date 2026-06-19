/*
Custom test: assert_no_negative_prices
-------------------------------
dbt custom tests pass when the query returns ZERO rows.
Any row returned is treated as a failure.

This checks the staging layer specifically - if close price is ever negative or zero after staging's own filter, something is wrong with the filter logic itself not just the raw data.
*/

SELECT 
    ticker,
    trade_date,
    close
FROM {{ref('stg_daily_prices')}}
WHERE close <= 0