select
    accession_no,
    filing_date,
    period_of_report,
    issuer_cik,
    nullif(trim(issuer_name), '') as issuer_name,
    nullif(upper(trim(issuer_ticker)), '') as issuer_ticker,
    aff10b5one,
    source,
    loaded_at
from {{ source('silver', 'form4_filings') }}
where document_type = '4'
