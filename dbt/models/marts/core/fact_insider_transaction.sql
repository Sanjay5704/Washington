{{
    config(
        materialized='incremental',
        unique_key='transaction_key',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['issuer_cik']},
            {'columns': ['transaction_date']},
            {'columns': ['is_open_market_purchase']},
        ],
    )
}}

-- Grain: one non-derivative transaction line of one Form 4 filing.
-- Incremental: each run only processes silver rows loaded since the last run.

with filings as (
    select * from {{ ref('stg_form4_filings') }}
),

-- A joint filing can list several owners (e.g. a fund, its GP and its manager).
-- We attribute the trade to ONE insider (lowest CIK) so a joint filing never counts
-- as several "distinct insiders" in cluster detection.
primary_owner as (
    select
        accession_no,
        min(owner_cik) as insider_cik,
        count(*) as n_reporting_owners
    from {{ ref('stg_form4_owners') }}
    group by accession_no
),

txns as (
    select * from {{ ref('stg_form4_transactions') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    t.transaction_key,
    t.accession_no,
    t.line_no,
    f.issuer_cik,
    o.insider_cik,
    o.n_reporting_owners,
    f.filing_date,
    t.transaction_date,
    -- A trade can't happen after it was reported, and typos like year 0202 exist.
    (t.transaction_date between '2000-01-01' and f.filing_date) as is_valid_transaction_date,
    case
        when t.transaction_date between '2000-01-01' and f.filing_date then t.transaction_date
    end as transaction_date_key,
    t.transaction_code,
    t.acquired_disposed,
    t.security_title,
    t.shares,
    t.price_per_share,
    t.transaction_value,
    t.shares_owned_after,
    t.direct_indirect,
    -- Code P = open market or private purchase. Must be an acquisition with a real price.
    (
        t.transaction_code = 'P'
        and t.acquired_disposed = 'A'
        and t.shares > 0
        and t.price_per_share > 0
    ) as is_open_market_purchase,
    f.source,
    t.loaded_at
from txns as t
join filings as f using (accession_no)
left join primary_owner as o using (accession_no)
