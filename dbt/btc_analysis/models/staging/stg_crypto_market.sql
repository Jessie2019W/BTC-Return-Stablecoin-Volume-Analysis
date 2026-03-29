with source as (
    select * from {{ source('staging', 'crypto_market') }}
)

select
    cast(date as date) as market_date,
    cast(price as float64) as price,
    cast(total_volume as float64) as total_volume,
    cast(market_cap as float64) as market_cap,
    lower(coin_id) as coin_id
from source
where date is not null
order by market_date, coin_id