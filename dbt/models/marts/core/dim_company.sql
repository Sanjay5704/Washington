-- One row per issuer (company), keyed by SEC CIK.
-- Names and tickers change over time; we keep the most recently filed values.
with ranked as (
    select
        issuer_cik,
        issuer_name,
        issuer_ticker,
        filing_date,
        row_number() over (
            partition by issuer_cik order by filing_date desc, accession_no desc
        ) as rn
    from {{ ref('stg_form4_filings') }}
),

latest_ticker as (
    -- latest filing that actually had a ticker
    select distinct on (issuer_cik) issuer_cik, issuer_ticker
    from {{ ref('stg_form4_filings') }}
    where issuer_ticker is not null
    order by issuer_cik, filing_date desc, accession_no desc
),

stats as (
    select
        issuer_cik,
        min(filing_date) as first_filing_date,
        max(filing_date) as last_filing_date,
        count(*) as n_filings
    from {{ ref('stg_form4_filings') }}
    group by issuer_cik
)

select
    r.issuer_cik,
    r.issuer_name,
    t.issuer_ticker as ticker,
    s.first_filing_date,
    s.last_filing_date,
    s.n_filings
from ranked as r
left join latest_ticker as t using (issuer_cik)
join stats as s using (issuer_cik)
where r.rn = 1
