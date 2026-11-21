-- An open-market purchase must have positive shares, price and value.
select transaction_key
from {{ ref('fact_insider_transaction') }}
where is_open_market_purchase
  and (transaction_value is null or transaction_value <= 0)
