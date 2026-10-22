with financials as (

    select * from {{ ref('stg_financials') }}

),

with_prev_quarter as (

    select
        *,
        lag(quarter_date) over by_ticker as prev_quarter_date,
        lag(total_revenue) over by_ticker as prev_quarter_revenue

    from financials
    window by_ticker as (partition by ticker order by quarter_date)

),

final as (

    select
        ticker,
        quarter_date,
        total_revenue,
        gross_profit,
        operating_income,
        net_income,
        ebitda,
        -- Only a true quarter-over-quarter change: the previous row must be the previous
        -- quarter (80-100 days earlier), not an older quarter after a gap in the data.
        case
            when date_diff(quarter_date, prev_quarter_date, day) between 80 and 100
            then {{ pct_change('total_revenue', 'prev_quarter_revenue') }}
        end as revenue_growth_pct,
        round(safe_divide(gross_profit, total_revenue) * 100, 2) as gross_margin_pct,
        round(safe_divide(net_income, total_revenue) * 100, 2) as net_margin_pct

    from with_prev_quarter

)

select * from final
