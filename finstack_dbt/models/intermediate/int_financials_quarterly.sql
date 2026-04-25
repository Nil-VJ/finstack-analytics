with financials as (

    select * from {{ ref('stg_financials') }}

),

with_growth as (

    select
        *,
        lag(total_revenue) over (
            partition by ticker order by quarter_date
        ) as prev_quarter_revenue,

        case
            when lag(total_revenue) over (
                partition by ticker order by quarter_date
            ) is not null and lag(total_revenue) over (
                partition by ticker order by quarter_date
            ) != 0
            then round(
                (total_revenue - lag(total_revenue) over (
                    partition by ticker order by quarter_date
                )) / lag(total_revenue) over (
                    partition by ticker order by quarter_date
                ) * 100,
                2
            )
        end as revenue_growth_pct,

        case
            when total_revenue is not null and total_revenue != 0
            then round(
                cast(gross_profit as float64) / cast(total_revenue as float64) * 100,
                2
            )
        end as gross_margin_pct,

        case
            when total_revenue is not null and total_revenue != 0
            then round(
                cast(net_income as float64) / cast(total_revenue as float64) * 100,
                2
            )
        end as net_margin_pct

    from financials

)

select * from with_growth