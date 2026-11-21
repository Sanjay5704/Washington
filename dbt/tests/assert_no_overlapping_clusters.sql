-- Two clusters of the same company must never overlap in time (they would be one cluster).
select a.cluster_id, b.cluster_id as overlapping_cluster_id
from {{ ref('insider_clusters') }} as a
join {{ ref('insider_clusters') }} as b
  on a.issuer_cik = b.issuer_cik
 and a.cluster_id < b.cluster_id
 and a.cluster_start <= b.cluster_end
 and b.cluster_start <= a.cluster_end
