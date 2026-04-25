{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'date'],
        on_schema_change='append_new_columns'
    )
}}

with prices as (

    select * from {{ ref('int_prices_moving_averages') }}

)

select
    ticker,
    date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    prev_close_price,
    daily_return_pct,
    sma_20,
    sma_50

from prices

{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}