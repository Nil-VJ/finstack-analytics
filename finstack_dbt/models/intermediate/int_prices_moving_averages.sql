with prices_with_returns as (

    select * from {{ ref('int_prices_with_returns') }}

),

moving_averages as (

    select
        *,
        round(avg(close_price) over (
            partition by ticker order by date
            rows between 19 preceding and current row
        ), 2) as sma_20,

        round(avg(close_price) over (
            partition by ticker order by date
            rows between 49 preceding and current row
        ), 2) as sma_50

    from prices_with_returns

)

select * from moving_averages