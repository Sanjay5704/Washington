select
    ticker,
    price_date,
    adj_close
from {{ source('silver', 'stock_prices') }}
where adj_close > 0
