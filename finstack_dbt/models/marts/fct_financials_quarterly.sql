-- Coverage caveat: Yahoo does not return every company for every quarter. The oldest and
-- newest quarters are often incomplete, so sector-level sums can dip without a real change.
with financials as (

    select * from {{ ref('int_financials_quarterly') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['ticker', 'quarter_date']) }} as financials_id,
        ticker,
        quarter_date,
        total_revenue,
        gross_profit,
        operating_income,
        net_income,
        ebitda,
        revenue_growth_pct,
        gross_margin_pct,
        net_margin_pct

    from financials

)

select * from final
