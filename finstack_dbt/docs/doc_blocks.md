{% docs ticker_description %}
Stock ticker symbol, e.g. AAPL for Apple or BRK-B for Berkshire Hathaway class B.
{% enddocs %}

{% docs close_price_description %}
Closing price of the regular session, adjusted for splits and dividends (yfinance `auto_adjust=True`). Because it is adjusted, the whole history can be restated after a split or dividend.
{% enddocs %}

{% docs daily_return_description %}
Day-over-day change of the adjusted close in percent: (close - previous close) / previous close * 100, rounded to 4 decimals. Null on each ticker's first trading day.
{% enddocs %}
