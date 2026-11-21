-- Silver layer: parsed, typed, de-duplicated Form 4 data.
-- Safe to run many times (IF NOT EXISTS everywhere).

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS ops;

-- One row per Form 4 filing (accession number = SEC's unique filing id).
CREATE TABLE IF NOT EXISTS silver.form4_filings (
    accession_no      text PRIMARY KEY,
    filing_date       date NOT NULL,
    document_type     text NOT NULL,
    period_of_report  date,
    issuer_cik        text NOT NULL,
    issuer_name       text,
    issuer_ticker     text,
    aff10b5one        boolean,
    source            text NOT NULL CHECK (source IN ('edgar_daily', 'sec_dataset')),
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_form4_filings_issuer ON silver.form4_filings (issuer_cik);
CREATE INDEX IF NOT EXISTS ix_form4_filings_date ON silver.form4_filings (filing_date);

-- Reporting owners (insiders). Joint filings have several owners per accession.
CREATE TABLE IF NOT EXISTS silver.form4_reporting_owners (
    accession_no          text NOT NULL REFERENCES silver.form4_filings (accession_no),
    owner_cik             text NOT NULL,
    owner_name            text,
    is_director           boolean,
    is_officer            boolean,
    is_ten_percent_owner  boolean,
    is_other              boolean,
    officer_title         text,
    loaded_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_no, owner_cik)
);

-- Non-derivative transactions. Dedupe key = accession number + transaction line.
CREATE TABLE IF NOT EXISTS silver.form4_nonderiv_transactions (
    accession_no           text NOT NULL REFERENCES silver.form4_filings (accession_no),
    line_no                integer NOT NULL CHECK (line_no > 0),
    security_title         text,
    transaction_date       date,
    transaction_form_type  text,
    transaction_code       text,
    equity_swap            boolean,
    shares                 numeric,
    price_per_share        numeric,
    acquired_disposed      text,
    shares_owned_after     numeric,
    direct_indirect        text,
    loaded_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (accession_no, line_no)
);
CREATE INDEX IF NOT EXISTS ix_nonderiv_code ON silver.form4_nonderiv_transactions (transaction_code);
CREATE INDEX IF NOT EXISTS ix_nonderiv_loaded ON silver.form4_nonderiv_transactions (loaded_at);

-- Daily close prices (adjusted) from yfinance, used for the returns analysis.
CREATE TABLE IF NOT EXISTS silver.stock_prices (
    ticker      text NOT NULL,
    price_date  date NOT NULL,
    adj_close   numeric NOT NULL,
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, price_date)
);

-- Tickers we tried to price and could not (delisted, renamed, OTC ...).
CREATE TABLE IF NOT EXISTS silver.price_fetch_log (
    ticker      text PRIMARY KEY,
    status      text NOT NULL,
    rows_loaded integer NOT NULL DEFAULT 0,
    fetched_at  timestamptz NOT NULL DEFAULT now()
);

-- Operational tables -----------------------------------------------------

-- What each pipeline step did, per run date (observability + README numbers).
CREATE TABLE IF NOT EXISTS ops.ingest_audit (
    id           bigserial PRIMARY KEY,
    run_date     date NOT NULL,
    step         text NOT NULL,
    metric       text NOT NULL,
    value        bigint NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);

-- Clusters we have already alerted on (so each cluster alerts once).
CREATE TABLE IF NOT EXISTS ops.cluster_alerts (
    cluster_id     text PRIMARY KEY,
    issuer_cik     text NOT NULL,
    ticker         text,
    issuer_name    text,
    signal_date    date NOT NULL,
    n_insiders     integer NOT NULL,
    total_value    numeric NOT NULL,
    status         text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'logged')),
    detected_at    timestamptz NOT NULL DEFAULT now(),
    sent_at        timestamptz
);
