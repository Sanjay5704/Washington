select
    accession_no,
    owner_cik,
    nullif(trim(owner_name), '') as owner_name,
    coalesce(is_director, false) as is_director,
    coalesce(is_officer, false) as is_officer,
    coalesce(is_ten_percent_owner, false) as is_ten_percent_owner,
    coalesce(is_other, false) as is_other,
    nullif(trim(officer_title), '') as officer_title
from {{ source('silver', 'form4_reporting_owners') }}
