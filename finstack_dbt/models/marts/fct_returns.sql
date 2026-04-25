with daily as (

    select * from {{ ref('fct_prices_daily') }}

),

monthly_returns as (

    select
        ticker,
        date_trunc(date, MONTH) as month,
        count(*) as trading_days,
        round(avg(daily_return_pct), 4) as avg_daily_return_pct,
        round(min(close_price), 2) as month_low,
        round(max(close_price), 2) as month_high,
        round(sum(volume), 0) as total_volume

    from daily
    where daily_return_pct is not null
    group by ticker, date_trunc(date, MONTH)

)

select * from monthly_returns