-- One row per insider (reporting owner), keyed by SEC CIK. Latest name / role wins.
with owner_filings as (
    select
        o.*,
        f.filing_date,
        row_number() over (
            partition by o.owner_cik order by f.filing_date desc, o.accession_no desc
        ) as rn
    from {{ ref('stg_form4_owners') }} as o
    join {{ ref('stg_form4_filings') }} as f using (accession_no)
)

select
    owner_cik as insider_cik,
    owner_name as insider_name,
    is_director,
    is_officer,
    is_ten_percent_owner,
    is_other,
    officer_title,
    case
        when is_officer and officer_title is not null then officer_title
        when is_director then 'Director'
        when is_ten_percent_owner then '10% Owner'
        when is_officer then 'Officer'
        else 'Other'
    end as role_label
from owner_filings
where rn = 1
