-- OHLC consistency check: high should always be >= low
-- Any rows returned by this query are failures

select
    ticker,
    date,
    high_price,
    low_price

from {{ ref('stg_prices') }}

where high_price < low_price