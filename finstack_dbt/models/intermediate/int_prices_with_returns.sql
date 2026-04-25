with prices as (

    select * from {{ ref('stg_prices') }}

),

returns as (

    select
        *,
        lag(close_price) over (
            partition by ticker order by date
        ) as prev_close_price,

        case
            when lag(close_price) over (
                partition by ticker order by date
            ) is not null
            then round(
                (close_price - lag(close_price) over (
                    partition by ticker order by date
                )) / lag(close_price) over (
                    partition by ticker order by date
                ) * 100,
                4
            )
        end as daily_return_pct

    from prices

)

select * from returns