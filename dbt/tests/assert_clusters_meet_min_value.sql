-- Every cluster's total purchase value must reach the configured threshold.
select cluster_id, total_value_usd
from {{ ref('insider_clusters') }}
where total_value_usd < {{ var('cluster_min_value_usd') }}
