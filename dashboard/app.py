"""Washington dashboard: recent cluster buys, company drill-down, returns summary."""

from __future__ import annotations

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st

from washington.config import load_settings

st.set_page_config(page_title="Washington - Insider Cluster Buys", layout="wide")


@st.cache_data(ttl=600)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg2.connect(load_settings().pg_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def num(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


st.title("Washington: insider cluster buys")
st.caption(
    "Three or more insiders buying their own company's stock on the open market within "
    "10 days. Source: SEC EDGAR Form 4. This is a data-engineering portfolio project, "
    "**not investment advice**."
)

kpis = query(
    """
    SELECT (SELECT count(*) FROM silver.form4_filings) AS filings,
           (SELECT count(*) FROM gold.fact_insider_transaction) AS transactions,
           (SELECT count(*) FROM gold.fact_insider_transaction WHERE is_open_market_purchase)
               AS purchases,
           (SELECT count(*) FROM gold.insider_clusters) AS clusters,
           (SELECT max(filing_date) FROM silver.form4_filings) AS latest_filing
    """
).iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Form 4 filings", f"{kpis.filings:,}")
c2.metric("Transactions", f"{kpis.transactions:,}")
c3.metric("Open-market buys", f"{kpis.purchases:,}")
c4.metric("Clusters found", f"{kpis.clusters:,}")
c5.metric("Latest filing", str(kpis.latest_filing))

tab_recent, tab_company, tab_returns, tab_health = st.tabs(
    ["Recent clusters", "Company drill-down", "Returns vs SPY", "Pipeline health"]
)

clusters = num(
    query(
        """
        SELECT c.cluster_id, c.ticker, c.issuer_name, c.issuer_cik, c.cluster_start,
               c.cluster_end, c.signal_date, c.n_insiders, c.total_value_usd, c.insider_names,
               r.excess_return_30d, r.excess_return_90d
        FROM gold.insider_clusters c
        LEFT JOIN gold.cluster_returns r USING (cluster_id)
        ORDER BY c.signal_date DESC
        """
    ),
    "total_value_usd",
    "excess_return_30d",
    "excess_return_90d",
)

with tab_recent:
    left, right = st.columns([1, 3])
    min_value = left.select_slider(
        "Minimum cluster value (USD)",
        options=[250_000, 500_000, 1_000_000, 5_000_000, 10_000_000],
        value=250_000,
    )
    min_insiders = left.slider("Minimum insiders", 3, 10, 3)
    shown = clusters[
        (clusters.total_value_usd >= min_value) & (clusters.n_insiders >= min_insiders)
    ]
    left.metric("Matching clusters", len(shown))
    right.dataframe(
        shown.drop(columns=["cluster_id", "issuer_cik"]).head(300),
        hide_index=True,
        use_container_width=True,
        column_config={
            "total_value_usd": st.column_config.NumberColumn("Total value", format="$%,.0f"),
            "excess_return_30d": st.column_config.NumberColumn("Excess 30d", format="%.1%%"),
            "excess_return_90d": st.column_config.NumberColumn("Excess 90d", format="%.1%%"),
        },
    )
    by_month = (
        clusters.assign(month=pd.to_datetime(clusters.signal_date).dt.to_period("M").astype(str))
        .groupby("month")
        .size()
        .reset_index(name="clusters")
    )
    st.altair_chart(
        alt.Chart(by_month, title="Clusters per month (by signal date)")
        .mark_bar()
        .encode(x=alt.X("month:O", title=None), y="clusters:Q", tooltip=["month", "clusters"]),
        use_container_width=True,
    )

with tab_company:
    companies = (
        clusters.assign(
            label=clusters.ticker.fillna("n/a") + " - " + clusters.issuer_name.fillna("")
        )
        .drop_duplicates("issuer_cik")[["label", "issuer_cik"]]
        .sort_values("label")
    )
    if companies.empty:
        st.info("No clusters yet.")
    else:
        label = st.selectbox("Company (with at least one cluster)", companies.label.tolist())
        cik = companies.set_index("label").loc[label, "issuer_cik"]
        st.subheader(label)
        st.dataframe(
            clusters[clusters.issuer_cik == cik].drop(columns=["cluster_id", "issuer_cik"]),
            hide_index=True,
            use_container_width=True,
        )
        buys = num(
            query(
                """
                SELECT f.transaction_date, coalesce(i.insider_name, f.insider_cik) AS insider,
                       i.role_label AS role, f.shares, f.price_per_share,
                       f.transaction_value, f.accession_no
                FROM gold.fact_insider_transaction f
                LEFT JOIN gold.dim_insider i USING (insider_cik)
                WHERE f.issuer_cik = %s AND f.is_open_market_purchase
                ORDER BY f.transaction_date DESC
                """,
                (cik,),
            ),
            "shares",
            "price_per_share",
            "transaction_value",
        )
        st.altair_chart(
            alt.Chart(buys, title="Open-market purchases by insider")
            .mark_circle(opacity=0.8)
            .encode(
                x=alt.X("transaction_date:T", title=None),
                y=alt.Y("transaction_value:Q", title="Value (USD)", scale=alt.Scale(type="log")),
                color=alt.Color("insider:N", legend=alt.Legend(orient="bottom")),
                size=alt.Size("transaction_value:Q", legend=None),
                tooltip=["transaction_date:T", "insider", "role", "shares", "price_per_share"],
            ),
            use_container_width=True,
        )
        st.dataframe(buys, hide_index=True, use_container_width=True)

with tab_returns:
    returns = num(
        query("SELECT * FROM gold.cluster_returns"),
        "return_30d",
        "spy_return_30d",
        "excess_return_30d",
        "return_90d",
        "spy_return_90d",
        "excess_return_90d",
    )
    rows = []
    for h in (30, 90):
        r = returns.dropna(subset=[f"return_{h}d"])
        if len(r):
            rows.append(
                {
                    "horizon": f"{h} days",
                    "clusters measured": len(r),
                    "avg return": r[f"return_{h}d"].mean(),
                    "avg SPY return": r[f"spy_return_{h}d"].mean(),
                    "avg excess": r[f"excess_return_{h}d"].mean(),
                    "median excess": r[f"excess_return_{h}d"].median(),
                    "beat SPY": (r[f"excess_return_{h}d"] > 0).mean(),
                }
            )
    summary = pd.DataFrame(rows)
    if summary.empty:
        st.info("No returns yet: run the refresh_prices task.")
    else:
        pct = "{:.2%}".format
        st.dataframe(
            summary.style.format(
                {c: pct for c in summary.columns if c not in ("horizon", "clusters measured")}
            ),
            hide_index=True,
            use_container_width=True,
        )
        hist = returns.dropna(subset=["excess_return_90d"]).assign(
            excess=lambda d: d.excess_return_90d.clip(-1, 2)
        )
        st.altair_chart(
            alt.Chart(hist, title="90-day excess return vs SPY (clipped to -100%..+200%)")
            .mark_bar()
            .encode(
                x=alt.X(
                    "excess:Q",
                    bin=alt.Bin(maxbins=60),
                    title="Excess return",
                    axis=alt.Axis(format="%"),
                ),
                y=alt.Y("count():Q", title="Clusters"),
            ),
            use_container_width=True,
        )
        st.caption(
            "Entry = first close after the signal date; exits = first close on/after entry "
            "+30 / +90 calendar days. Survivorship bias: delisted tickers have no Yahoo prices."
        )

with tab_health:
    audit = query(
        """
        SELECT run_date, step, metric, value, recorded_at
        FROM ops.ingest_audit
        WHERE step NOT LIKE 'spark_backfill%%'
        ORDER BY recorded_at DESC
        LIMIT 300
        """
    )
    st.dataframe(audit, hide_index=True, use_container_width=True)
