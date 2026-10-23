with prices_with_returns as (

    select * from {{ ref('int_prices_with_returns') }}

),

windows as (

    select
        *,
        row_number() over (partition by ticker order by date) as history_days,
        avg(close_price) over (
            partition by ticker order by date
            rows between 19 preceding and current row
        ) as avg_close_20,
        avg(close_price) over (
            partition by ticker order by date
            rows between 49 preceding and current row
        ) as avg_close_50

    from prices_with_returns

),

-- A moving average over fewer rows than its window is misleading, so it stays null
-- until the ticker has a full window of history.
final as (

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
        case when history_days >= 20 then round(avg_close_20, 2) end as sma_20,
        case when history_days >= 50 then round(avg_close_50, 2) end as sma_50

    from windows

)

select * from final
