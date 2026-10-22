with source as (

    select * from {{ source('finstack_raw', 'raw_financials') }}

),

renamed as (

    select
        ticker,
        cast(date as date) as quarter_date,
        cast(total_revenue as int64) as total_revenue,
        cast(gross_profit as int64) as gross_profit,
        cast(operating_income as int64) as operating_income,
        cast(net_income as int64) as net_income,
        cast(ebitda as int64) as ebitda,
        _loaded_at

    from source

),

-- raw_financials is append-only: each load re-sends the quarters Yahoo still exposes.
-- Keep the most recently loaded version of each quarter.
final as (

    select * from renamed
    qualify row_number() over (
        partition by ticker, quarter_date
        order by _loaded_at desc
    ) = 1

)

select * from final
