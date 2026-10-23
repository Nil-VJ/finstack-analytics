{% docs __overview__ %}

# FinStack Analytics

Stock market data warehouse on BigQuery: 30 S&P 500 tickers, 5 years of daily adjusted prices and quarterly income statements, loaded from Yahoo Finance with `scripts/load_yfinance.py`.

## Layers
- **Staging** (views): rename, cast and deduplicate the three raw tables. 1:1 with sources.
- **Intermediate** (views): window logic. Daily returns, 20/50-day moving averages, QoQ revenue growth and margins.
- **Marts** (tables): what consumers query.
- **Snapshot**: SCD Type 2 history of company sector, industry and analyst recommendation.
- **Semantic layer**: MetricFlow semantic models and five metrics on top of the marts.

## Marts
- `fct_prices_daily`: one row per ticker per trading date. Enforced contract.
- `fct_returns`: one row per ticker per month.
- `fct_financials_quarterly`: one row per ticker per fiscal quarter.
- `dim_companies`: one row per ticker.
- `dim_sectors`: one row per Yahoo Finance sector.

Sectors are Yahoo Finance's classification, not official GICS.

{% enddocs %}
