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
