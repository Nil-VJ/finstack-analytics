-- Every daily bar must satisfy low <= open <= high and low <= close <= high.
-- The 1e-6 tolerance absorbs float rounding from Yahoo's price adjustment.
-- Any rows returned are failures.

with prices as (

    select * from {{ ref('stg_prices') }}

)

select
    ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price

from prices

where open_price < low_price - 1e-6
   or open_price > high_price + 1e-6
   or close_price < low_price - 1e-6
   or close_price > high_price + 1e-6
