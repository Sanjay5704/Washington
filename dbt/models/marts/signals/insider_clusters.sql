{{ config(materialized='table') }}

{#-
  CLUSTER BUY DETECTION
  A "cluster buy" = at least N distinct insiders of the same company making open-market
  purchases (code P) within a rolling W-day window, with total value >= V dollars.
  N, W, V come from dbt vars (see dbt_project.yml).
-#}
{%- set min_insiders = var('cluster_min_insiders') | int -%}
{%- set window_days = var('cluster_window_days') | int -%}
{%- set min_value = var('cluster_min_value_usd') -%}

with

-- STEP 1: open-market purchases, rolled up to one row per company / insider / day.
-- (An insider often splits one buy into many lines at different prices.)
purchases as (
    select
        issuer_cik,
        insider_cik,
        transaction_date,
        sum(transaction_value) as value_usd,
        max(filing_date) as filing_date
    from {{ ref('fact_insider_transaction') }}
    where is_open_market_purchase
      and is_valid_transaction_date
      and insider_cik is not null
    group by issuer_cik, insider_cik, transaction_date
),

-- STEP 2: a rolling window per company. For every purchase day D we look back over
-- [D - (W-1) days, D], i.e. W calendar days including D. RANGE (not ROWS) makes the
-- window time-based, so it works no matter how many rows fall inside it.
-- Postgres can't do COUNT(DISTINCT) as a window function, so we collect the insider
-- CIKs into an array and count the distinct ones in the next step.
rolling as (
    select
        issuer_cik,
        transaction_date as anchor_date,
        array_agg(insider_cik) over w as window_insiders,
        sum(value_usd) over w as window_value_usd,
        min(transaction_date) over w as window_start,
        -- latest filing date inside the window = the day the public could know about it
        max(filing_date) over w as window_last_filing_date
    from purchases
    window w as (
        partition by issuer_cik
        order by transaction_date
        range between interval '{{ window_days - 1 }} days' preceding and current row
    )
),

-- STEP 3: keep the windows that qualify as a cluster (one row per company per anchor day;
-- several insiders buying on the same day produce identical windows, hence DISTINCT ON).
qualifying_windows as (
    select distinct on (issuer_cik, anchor_date)
        issuer_cik,
        anchor_date,
        window_start,
        window_value_usd,
        window_last_filing_date
    from rolling
    where (select count(distinct i) from unnest(window_insiders) as i) >= {{ min_insiders }}
      and window_value_usd >= {{ min_value }}
    order by issuer_cik, anchor_date
),

-- STEP 4: overlapping qualifying windows describe the SAME cluster. Classic
-- "gaps and islands": a window starts a new cluster only if it begins after the
-- previous qualifying window ended. A running SUM of those flags numbers the clusters.
flagged as (
    select
        *,
        case
            when window_start <= lag(anchor_date) over (
                partition by issuer_cik order by anchor_date
            ) then 0
            else 1
        end as starts_new_cluster
    from qualifying_windows
),

numbered as (
    select
        *,
        sum(starts_new_cluster) over (
            partition by issuer_cik order by anchor_date rows unbounded preceding
        ) as cluster_seq
    from flagged
),

-- STEP 5: one row per cluster. signal_date = when the FIRST qualifying window became
-- public (its last filing date). Returns are measured from there, avoiding look-ahead bias.
clusters as (
    select
        issuer_cik,
        cluster_seq,
        min(window_start) as cluster_start,
        max(anchor_date) as cluster_end,
        min(anchor_date) as first_qualified_date,
        (array_agg(window_last_filing_date order by anchor_date))[1] as signal_date
    from numbered
    group by issuer_cik, cluster_seq
),

-- STEP 6: totals over every purchase inside the cluster's date span.
cluster_totals as (
    select
        c.issuer_cik,
        c.cluster_start,
        c.cluster_end,
        c.first_qualified_date,
        c.signal_date,
        count(distinct p.insider_cik) as n_insiders,
        count(*) as n_insider_days,
        sum(p.value_usd) as total_value_usd,
        string_agg(distinct coalesce(i.insider_name, p.insider_cik), '; ') as insider_names
    from clusters as c
    join purchases as p
        on p.issuer_cik = c.issuer_cik
        and p.transaction_date between c.cluster_start and c.cluster_end
    left join {{ ref('dim_insider') }} as i on i.insider_cik = p.insider_cik
    group by c.issuer_cik, c.cluster_start, c.cluster_end, c.first_qualified_date, c.signal_date
)

select
    md5(t.issuer_cik || '|' || t.cluster_start::text) as cluster_id,
    t.issuer_cik,
    co.issuer_name,
    co.ticker,
    t.cluster_start,
    t.cluster_end,
    t.first_qualified_date,
    t.signal_date,
    t.n_insiders,
    t.n_insider_days,
    round(t.total_value_usd, 2) as total_value_usd,
    t.insider_names
from cluster_totals as t
left join {{ ref('dim_company') }} as co on co.issuer_cik = t.issuer_cik
