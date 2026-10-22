{{ config(materialized='table') }}

with prices as (

    select * from {{ ref('int_prices_moving_averages') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['ticker', 'date']) }} as price_id,
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

)

select * from final
