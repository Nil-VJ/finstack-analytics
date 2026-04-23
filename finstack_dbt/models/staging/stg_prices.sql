with source as (

    select * from {{ source('finstack_raw', 'raw_prices') }}

),

renamed as (

    select
        ticker,
        cast(date as date) as date,
        cast(open as float64) as open_price,
        cast(high as float64) as high_price,
        cast(low as float64) as low_price,
        cast(close as float64) as close_price,
        cast(volume as int64) as volume,
        _loaded_at

    from source
    where close is not null

)

select * from renamed