-- Every ticker that reaches the gold layer must look like one exchange symbol
-- (see macros/clean_ticker.sql). Returns offending rows; zero rows = pass.
select ticker
from {{ ref('dim_company') }}
where ticker is not null
  and ticker !~ '^[A-Z][A-Z0-9]{0,5}([.\-][A-Z0-9]{1,2})?$'
