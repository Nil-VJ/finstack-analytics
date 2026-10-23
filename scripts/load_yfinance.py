"""
FinStack Analytics: Yahoo Finance -> BigQuery raw loader.

Loads three raw tables into $RAW_DATASET:
  raw_prices        5 years of daily adjusted OHLCV  (WRITE_TRUNCATE)
  raw_company_info  one profile row per ticker        (WRITE_TRUNCATE)
  raw_financials    quarterly income statements       (WRITE_APPEND, deduped in dbt staging)

Everything is fetched and validated first. If any requested table fails validation,
nothing is written and the script exits non-zero, so a partial fetch never replaces
a good table.

Usage (from the repo root):
    python scripts/load_yfinance.py --dry-run
    RAW_DATASET=finstack_raw_dev python scripts/load_yfinance.py
    python scripts/load_yfinance.py --only prices
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.environ.setdefault("GCP_PROJECT_ID", "finstack-analytics")
os.environ.setdefault("RAW_DATASET", "finstack_raw")
os.environ.setdefault(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(REPO_ROOT, "credentials", "service_account.json"),
)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
RAW_DATASET = os.environ["RAW_DATASET"]
HISTORY_PERIOD = "5y"

# 30 S&P 500 tickers. Sector labels come from yfinance at load time, not from this list.
TICKERS = [
    "AAPL", "MSFT", "GOOGL", "NVDA", "META",
    "JPM", "GS", "V", "BAC", "BRK-B",
    "JNJ", "UNH", "PFE", "ABBV", "MRK",
    "XOM", "CVX", "COP", "SLB", "EOG",
    "AMZN", "TSLA", "HD", "MCD", "NKE",
    "PG", "KO", "PEP", "COST", "WMT",
]

RETRY_DELAYS_SECONDS = [5, 15, 45, 120]
PER_TICKER_SLEEP_SECONDS = 1.5

PRICE_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume", "_loaded_at"]
COMPANY_COLUMNS = [
    "ticker", "short_name", "long_name", "sector", "industry", "country",
    "market_cap", "enterprise_value", "recommendation_key", "_loaded_at",
]
FINANCIAL_METRICS = {
    "Total Revenue": "total_revenue",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Net Income": "net_income",
    "EBITDA": "ebitda",
}
FINANCIAL_COLUMNS = ["ticker", "date", *FINANCIAL_METRICS.values(), "_loaded_at"]

log = logging.getLogger("load_yfinance")


def with_retries(description, fetch):
    """Call fetch() until it returns a non-empty result, backing off between attempts."""
    for attempt, delay in enumerate([0, *RETRY_DELAYS_SECONDS], start=1):
        if delay:
            log.warning("%s: retry %d in %ds", description, attempt - 1, delay)
            time.sleep(delay)
        try:
            result = fetch()
            if result is not None and len(result) > 0:
                return result
            log.warning("%s: empty response", description)
        except Exception as e:
            log.warning("%s: %s", description, e)
    log.error("%s: giving up after %d attempts", description, len(RETRY_DELAYS_SECONDS) + 1)
    return None


def fetch_prices(loaded_at):
    raw = with_retries(
        "prices download",
        lambda: yf.download(
            TICKERS,
            period=HISTORY_PERIOD,
            auto_adjust=True,
            group_by="ticker",
            threads=True,
            progress=False,
        ),
    )
    if raw is None:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    frames = []
    for ticker in TICKERS:
        if ticker not in raw.columns.get_level_values(0):
            log.warning("%s: no price data returned", ticker)
            continue
        df = raw[ticker].dropna(subset=["Close"]).reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        df["ticker"] = ticker
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    prices = pd.concat(frames, ignore_index=True)
    prices["_loaded_at"] = loaded_at
    return prices


def company_info(ticker):
    # A throttled .info call can return a near-empty dict; treat that as a failed fetch.
    info = yf.Ticker(ticker).info
    return info if info.get("sector") else None


def fetch_company_info(loaded_at):
    rows = []
    for ticker in TICKERS:
        info = with_retries(f"{ticker} info", lambda: company_info(ticker))
        time.sleep(PER_TICKER_SLEEP_SECONDS)
        if info is None:
            continue
        rows.append({
            "ticker": ticker,
            "short_name": info.get("shortName"),
            "long_name": info.get("longName"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "country": info.get("country"),
            "market_cap": info.get("marketCap"),
            "enterprise_value": info.get("enterpriseValue"),
            "recommendation_key": info.get("recommendationKey"),
            "_loaded_at": loaded_at,
        })
    return pd.DataFrame(rows, columns=COMPANY_COLUMNS)


def fetch_financials(loaded_at):
    rows = []
    for ticker in TICKERS:
        stmt = with_retries(
            f"{ticker} quarterly_income_stmt",
            lambda: yf.Ticker(ticker).quarterly_income_stmt,
        )
        time.sleep(PER_TICKER_SLEEP_SECONDS)
        if stmt is None:
            continue
        for quarter_end in stmt.columns:
            row = {"ticker": ticker, "date": pd.Timestamp(quarter_end).date()}
            for metric, column in FINANCIAL_METRICS.items():
                value = stmt.loc[metric, quarter_end] if metric in stmt.index else None
                row[column] = float(value) if pd.notna(value) else None
            row["_loaded_at"] = loaded_at
            rows.append(row)
    return pd.DataFrame(rows, columns=FINANCIAL_COLUMNS)


def validate(name, df, required_columns, min_tickers):
    """Return a list of problems; an empty list means the table is safe to write."""
    problems = []
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        problems.append(f"missing columns {missing}")
    tickers = df["ticker"].nunique() if "ticker" in df.columns else 0
    if tickers < min_tickers:
        problems.append(f"only {tickers}/{min_tickers} tickers returned")
    log.info("%s: %d rows, %d tickers", name, len(df), tickers)
    return problems


def write_table(client, df, table, job_config):
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table}"
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    log.info("wrote %d rows to %s (%s)", len(df), table_id, job_config.write_disposition)


def main():
    parser = argparse.ArgumentParser(description="Load Yahoo Finance data into BigQuery.")
    parser.add_argument("--dry-run", action="store_true", help="fetch and validate, write nothing")
    parser.add_argument("--only", choices=["prices", "company", "financials"], help="load one table")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    loaded_at = datetime.now(timezone.utc)
    log.info("target %s.%s, %d tickers, dry_run=%s", PROJECT_ID, RAW_DATASET, len(TICKERS), args.dry_run)

    # name -> (fetch, required columns, minimum tickers)
    tables = {
        "prices": (fetch_prices, PRICE_COLUMNS, len(TICKERS)),
        "company": (fetch_company_info, COMPANY_COLUMNS, len(TICKERS)),
        "financials": (fetch_financials, FINANCIAL_COLUMNS, 1),
    }
    selected = [args.only] if args.only else list(tables)

    fetched, failed = {}, False
    for name in selected:
        fetch, required_columns, min_tickers = tables[name]
        df = fetch(loaded_at)
        problems = validate(name, df, required_columns, min_tickers)
        if problems:
            log.error("%s failed validation: %s", name, "; ".join(problems))
            failed = True
        else:
            fetched[name] = df[required_columns]

    if failed:
        log.error("validation failed, nothing was written")
        sys.exit(1)
    if args.dry_run:
        log.info("dry run passed, nothing was written")
        return

    # Imported here so --dry-run works without GCP libraries configured.
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    configs = {
        "prices": ("raw_prices", bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")),
        "company": ("raw_company_info", bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")),
        "financials": (
            "raw_financials",
            bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                # Explicit schema matching finstack_raw.raw_financials as `bq show` reports it
                # (metrics FLOAT, all NULLABLE). Without it an all-null metric column could be
                # inferred as another type and the append would fail.
                schema=[
                    bigquery.SchemaField("ticker", "STRING"),
                    bigquery.SchemaField("date", "DATE"),
                    *[bigquery.SchemaField(c, "FLOAT") for c in FINANCIAL_METRICS.values()],
                    bigquery.SchemaField("_loaded_at", "TIMESTAMP"),
                ],
            ),
        ),
    }
    for name in selected:
        table, job_config = configs[name]
        write_table(client, fetched[name], table, job_config)


if __name__ == "__main__":
    main()
