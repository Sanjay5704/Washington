-- Every cluster must have at least the configured number of distinct insiders.
select cluster_id, n_insiders
from {{ ref('insider_clusters') }}
where n_insiders < {{ var('cluster_min_insiders') }}
