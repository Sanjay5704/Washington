select
    md5(accession_no || '-' || line_no::text) as transaction_key,
    accession_no,
    line_no,
    nullif(trim(security_title), '') as security_title,
    transaction_date,
    upper(trim(transaction_code)) as transaction_code,
    upper(trim(acquired_disposed)) as acquired_disposed,
    shares,
    price_per_share,
    shares * price_per_share as transaction_value,
    shares_owned_after,
    upper(trim(direct_indirect)) as direct_indirect,
    equity_swap,
    loaded_at
from {{ source('silver', 'form4_nonderiv_transactions') }}
