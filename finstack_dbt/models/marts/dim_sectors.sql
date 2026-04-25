with companies as (

    select * from {{ ref('dim_companies') }}

),

sector_summary as (

    select
        sector,
        count(*) as company_count,
        sum(market_cap) as total_market_cap,
        round(avg(market_cap), 0) as avg_market_cap

    from companies
    where sector is not null
    group by sector

)

select * from sector_summary