with daily as (

    select * from {{ ref('fct_prices_daily') }}

),

monthly as (

    select
        ticker,
        date_trunc(date, month) as month,
        count(*) as trading_days,
        min(low_price) as month_low,
        max(high_price) as month_high,
        sum(volume) as total_volume,
        array_agg(close_price order by date desc limit 1)[offset(0)] as month_close

    from daily
    group by ticker, month

),

with_prev_month as (

    select
        *,
        lag(month_close) over (partition by ticker order by month) as prev_month_close

    from monthly

),

final as (

    select
        ticker,
        month,
        trading_days,
        month_low,
        month_high,
        total_volume,
        {{ pct_change('month_close', 'prev_month_close') }} as monthly_return_pct

    from with_prev_month

)

select * from final
