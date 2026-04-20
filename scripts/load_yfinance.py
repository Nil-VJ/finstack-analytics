"""
FinStack Analytics — Data Loader
Fetches stock market data from Yahoo Finance and loads it into BigQuery.

Usage:
    python scripts/load_yfinance.py

Prerequisites:
    pip install yfinance google-cloud-bigquery pandas db-dtypes pyarrow
    Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path.
"""

import os
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION
# ============================================================

# Path to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "credentials",
    "service_account.json",
)

PROJECT_ID = "finstack-analytics"
DATASET_ID = "finstack_raw"
HISTORY_PERIOD = "5y"  # 5 years of daily price data

# 30 S&P 500 tickers across 6 sectors
TICKERS = [
    # Technology (5)
    "AAPL", "MSFT", "GOOGL", "NVDA", "META",
    # Financials (5)
    "JPM", "GS", "V", "BAC", "BRK-B",
    # Healthcare (5)
    "JNJ", "UNH", "PFE", "ABBV", "MRK",
    # Energy (5)
    "XOM", "CVX", "COP", "SLB", "EOG",
    # Consumer Discretionary (5)
    "AMZN", "TSLA", "HD", "MCD", "NKE",
    # Consumer Staples (5)
    "PG", "KO", "PEP", "COST", "WMT",
]

client = bigquery.Client(project=PROJECT_ID)
loaded_at = datetime.now(timezone.utc)


# ============================================================
# 1. LOAD DAILY PRICES
# ============================================================

def load_prices():
    print("Fetching daily prices...")
    raw = yf.download(TICKERS, period=HISTORY_PERIOD, group_by="ticker", threads=True)

    rows = []
    for ticker in TICKERS:
        try:
            df = raw[ticker].copy()
            df = df.dropna(subset=["Close"])
            df = df.reset_index()
            df["ticker"] = ticker
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            rows.append(df)
        except Exception as e:
            print(f"  Warning: Could not process {ticker} prices: {e}")

    prices = pd.concat(rows, ignore_index=True)
    prices["_loaded_at"] = loaded_at

    # Rename columns to match our schema
    prices = prices.rename(columns={
        "stock_splits": "stock_splits",
    })

    # Keep only the columns we need
    keep_cols = ["ticker", "date", "open", "high", "low", "close", "volume", "dividends", "stock_splits", "_loaded_at"]
    prices = prices[[c for c in keep_cols if c in prices.columns]]

    table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_prices"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(prices, table_id, job_config=job_config)
    job.result()
    print(f"  Loaded {len(prices)} rows into {table_id}")


# ============================================================
# 2. LOAD COMPANY INFO
# ============================================================

def load_company_info():
    print("Fetching company info...")
    rows = []
    for ticker in TICKERS:
        try:
            info = yf.Ticker(ticker).info
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
            print(f"  {ticker} OK")
        except Exception as e:
            print(f"  Warning: Could not fetch {ticker} info: {e}")

    company_info = pd.DataFrame(rows)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_company_info"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(company_info, table_id, job_config=job_config)
    job.result()
    print(f"  Loaded {len(company_info)} rows into {table_id}")


# ============================================================
# 3. LOAD QUARTERLY FINANCIALS
# ============================================================

def load_financials():
    print("Fetching quarterly financials...")
    rows = []
    for ticker in TICKERS:
        try:
            stmt = yf.Ticker(ticker).quarterly_income_stmt
            if stmt.empty:
                print(f"  {ticker}: no financials available, skipping")
                continue

            for col_date in stmt.columns:
                row = {
                    "ticker": ticker,
                    "date": col_date.date() if hasattr(col_date, "date") else col_date,
                }
                # Extract key financial metrics (yfinance returns rows as index)
                for metric, field_name in [
                    ("Total Revenue", "total_revenue"),
                    ("Gross Profit", "gross_profit"),
                    ("Operating Income", "operating_income"),
                    ("Net Income", "net_income"),
                    ("EBITDA", "ebitda"),
                ]:
                    if metric in stmt.index:
                        val = stmt.loc[metric, col_date]
                        row[field_name] = int(val) if pd.notna(val) else None
                    else:
                        row[field_name] = None

                row["_loaded_at"] = loaded_at
                rows.append(row)
            print(f"  {ticker} OK ({len(stmt.columns)} quarters)")
        except Exception as e:
            print(f"  Warning: Could not fetch {ticker} financials: {e}")

    financials = pd.DataFrame(rows)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_financials"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(financials, table_id, job_config=job_config)
    job.result()
    print(f"  Loaded {len(financials)} rows into {table_id}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print(f"Starting data load at {loaded_at.isoformat()}")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Tickers: {len(TICKERS)}")
    print("=" * 50)

    load_prices()
    print()
    load_company_info()
    print()
    load_financials()

    print()
    print("=" * 50)
    print("All done! Check BigQuery for your tables:")
    print(f"  {PROJECT_ID}.{DATASET_ID}.raw_prices")
    print(f"  {PROJECT_ID}.{DATASET_ID}.raw_company_info")
    print(f"  {PROJECT_ID}.{DATASET_ID}.raw_financials")