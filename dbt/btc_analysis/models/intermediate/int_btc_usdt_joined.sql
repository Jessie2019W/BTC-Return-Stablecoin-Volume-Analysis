with btc_daily as (
    select
        market_date,
        price as btc_price,
        total_volume as btc_volume,
        -- Calculate daily return: (current - previous) / previous
        (price - lag(price) over (order by market_date)) / nullif(lag(price) over (order by market_date), 0) as btc_daily_return
    from {{ ref('stg_crypto_market') }}
    where coin_id = 'bitcoin'
),

usdt_daily as (
    select
        market_date,
        total_volume as usdt_volume,
        market_cap as usdt_market_cap
    from {{ ref('stg_crypto_market') }}
    where coin_id = 'tether'
)

select
    b.market_date,
    b.btc_price,
    b.btc_volume,
    -- Use absolute value of return to represent volatility/movement strength
    abs(b.btc_daily_return) as btc_volatility,
    u.usdt_volume,
    u.usdt_market_cap
from btc_daily b
inner join usdt_daily u on b.market_date = u.market_date
order by market_date