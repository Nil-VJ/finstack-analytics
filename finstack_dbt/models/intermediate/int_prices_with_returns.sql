with prices as (

    select * from {{ ref('stg_prices') }}

),

with_prev_close as (

    select
        *,
        lag(close_price) over (partition by ticker order by date) as prev_close_price

    from prices

),

final as (

    select
        *,
        {{ pct_change('close_price', 'prev_close_price', precision=4) }} as daily_return_pct

    from with_prev_close

)

select * from final
