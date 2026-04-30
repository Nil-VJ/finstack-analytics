{% docs __overview__ %}

# FinStack Analytics

Welcome to the FinStack Analytics dbt project. This project models stock market data from Yahoo Finance into a BigQuery data warehouse.

## Data Sources
- **Yahoo Finance (yfinance)**: Daily OHLCV prices, company profiles, and quarterly financials for 30 S&P 500 stocks across 6 sectors.

## Project Layers
- **Staging**: Light cleaning and renaming of raw source data (views)
- **Intermediate**: Business logic — daily returns, moving averages, quarterly growth (views)
- **Marts**: Final business-ready tables — fact and dimension models (tables)
- **Snapshots**: SCD Type-2 tracking of company info changes over time

## Key Models
- `fct_prices_daily` — Incremental daily price fact table with returns and moving averages
- `fct_returns` — Monthly aggregated return metrics
- `dim_companies` — Company dimension with profile data
- `dim_sectors` — Sector-level market cap summary
- `snp_company_info` — SCD-2 snapshot tracking sector and recommendation changes

{% enddocs %}