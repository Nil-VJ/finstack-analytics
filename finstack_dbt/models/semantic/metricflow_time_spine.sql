{{ config(materialized='table') }}

with days as (

    select date_day
    from unnest(generate_date_array('2020-01-01', current_date(), interval 1 day)) as date_day

)

select * from days
