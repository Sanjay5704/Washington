-- start <= first qualifying day <= end, and the signal can't come before the trades.
select cluster_id
from {{ ref('insider_clusters') }}
where cluster_start > first_qualified_date
   or first_qualified_date > cluster_end
   or signal_date < first_qualified_date
