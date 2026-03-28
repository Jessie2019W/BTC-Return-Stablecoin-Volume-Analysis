select
    *,
    case 
        when btc_volatility >= 0.05 then 'High (5%+)'
        when btc_volatility >= 0.02 then 'Medium (2-5%)'
        else 'Low (<2%)'
    end as volatility_category
from {{ ref('int_btc_usdt_joined') }}