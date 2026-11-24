{{ config(materialized='table') }}

-- Forward returns after each cluster, versus the S&P 500 (SPY).
--   entry  = first trading day AFTER the signal date (filings often land after the close)
--   exit_N = first trading day on/after entry + N calendar days
-- Only clusters with prices on both dates get a value; others stay NULL.
-- Each lateral reads the view directly (not via a shared CTE): Postgres materialises a CTE
-- that is used more than once, which would hide the (ticker, price_date) index.

with clusters as (
    select cluster_id, ticker, signal_date
    from {{ ref('insider_clusters') }}
    where ticker is not null
),

entry as (
    select c.*, p.price_date as entry_date, p.adj_close as entry_price
    from clusters as c
    join lateral (
        select price_date, adj_close
        from {{ ref('stg_stock_prices') }} as prices
        where prices.ticker = c.ticker
          and price_date > c.signal_date
          and price_date <= c.signal_date + 7
        order by price_date
        limit 1
    ) as p on true
),

{% for days in [30, 90] %}
exit_{{ days }} as (
    select
        e.cluster_id,
        x.price_date as exit_date,
        x.adj_close / e.entry_price - 1 as stock_return,
        spy_x.adj_close / spy_e.adj_close - 1 as spy_return
    from entry as e
    join lateral (
        select price_date, adj_close from {{ ref('stg_stock_prices') }} as prices
        where prices.ticker = e.ticker
          and price_date >= e.entry_date + {{ days }}
          and price_date <= e.entry_date + {{ days + 7 }}
        order by price_date limit 1
    ) as x on true
    join lateral (
        select adj_close from {{ ref('stg_stock_prices') }} as prices
        where prices.ticker = 'SPY' and price_date >= e.entry_date
        order by price_date limit 1
    ) as spy_e on true
    join lateral (
        select adj_close from {{ ref('stg_stock_prices') }} as prices
        where prices.ticker = 'SPY' and price_date >= x.price_date
        order by price_date limit 1
    ) as spy_x on true
),
{% endfor %}

final as (
    select
        e.cluster_id,
        e.ticker,
        e.signal_date,
        e.entry_date,
        e.entry_price,
        r30.exit_date as exit_date_30d,
        round(r30.stock_return, 4) as return_30d,
        round(r30.spy_return, 4) as spy_return_30d,
        round(r30.stock_return - r30.spy_return, 4) as excess_return_30d,
        r90.exit_date as exit_date_90d,
        round(r90.stock_return, 4) as return_90d,
        round(r90.spy_return, 4) as spy_return_90d,
        round(r90.stock_return - r90.spy_return, 4) as excess_return_90d
    from entry as e
    left join exit_30 as r30 using (cluster_id)
    left join exit_90 as r90 using (cluster_id)
)

select * from final
