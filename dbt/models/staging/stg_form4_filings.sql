with filings as (
    select *
    from {{ source('silver', 'form4_filings') }}
    where document_type = '4'
),

-- Clean each distinct hand-typed ticker once (~10k values) instead of once per filing
-- (~900k rows); doing it per row made dim_company take minutes.
raw_tickers as (
    select distinct issuer_ticker as raw_ticker
    from filings
    where issuer_ticker is not null
),

tickers as (
    -- DISTINCT happens first, in its own CTE; otherwise Postgres evaluates the cleaning
    -- expression for every row as part of the DISTINCT key.
    select raw_ticker, {{ clean_ticker('raw_ticker') }} as clean_ticker
    from raw_tickers
)

select
    f.accession_no,
    f.filing_date,
    f.period_of_report,
    f.issuer_cik,
    nullif(trim(f.issuer_name), '') as issuer_name,
    f.issuer_ticker as issuer_ticker_raw,
    t.clean_ticker as issuer_ticker,
    f.aff10b5one,
    f.source,
    f.loaded_at
from filings as f
left join tickers as t on t.raw_ticker = f.issuer_ticker
