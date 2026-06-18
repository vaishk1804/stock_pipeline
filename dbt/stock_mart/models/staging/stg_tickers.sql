{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
stg_tickers
--------------
Passes through ticker reference data with consistent naming.
*/

WITH source as (
  SELECT * FROM {{ source('raw','tickers')}}
)

SELECT
    ticker,
    company_name,
    sector,
    industry,
    added_at
FROM source