with company_info as (

    select * from {{ ref('stg_company_info') }}

),

final as (

    select
        ticker,
        short_name,
        long_name,
        sector,
        industry,
        country,
        market_cap,
        enterprise_value,
        recommendation_key

    from company_info

)

select * from final