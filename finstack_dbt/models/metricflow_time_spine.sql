{{
    config(
        materialized='table'
    )
}}

with days as (
    select
        cast(date as date) as date_day
    from
        unnest(
            generate_date_array(
                '2020-01-01',
                current_date(),
                interval 1 day
            )
        ) as date
)

select * from days