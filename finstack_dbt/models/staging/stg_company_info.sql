with source as (

    select * from {{ source('finstack_raw', 'raw_company_info') }}

),

renamed as (

    select
        ticker,
        short_name,
        long_name,
        sector,
        industry,
        country,
        cast(market_cap as int64) as market_cap,
        cast(enterprise_value as int64) as enterprise_value,
        recommendation_key,
        _loaded_at

    from source

)

select * from renamed