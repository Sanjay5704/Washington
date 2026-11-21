{{ config(severity='warn') }}
-- Flags likely bad price data (e.g. a missed split): a 30-day move above +1000% or below -95%.
select cluster_id, ticker, return_30d
from {{ ref('cluster_returns') }}
where return_30d > 10 or return_30d < -0.95
