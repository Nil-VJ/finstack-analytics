{% snapshot snp_company_info %}

{{
    config(
        schema='snapshots',
        unique_key='ticker',
        strategy='check',
        check_cols=['sector', 'industry', 'market_cap', 'recommendation_key']
    )
}}

select * from {{ source('finstack_raw', 'raw_company_info') }}

{% endsnapshot %}