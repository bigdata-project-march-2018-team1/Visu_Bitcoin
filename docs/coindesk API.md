# CoinDesk API

Provides endpoint for current Bitcoin Price Index (BPI) and historical values.

## Hostname : api.coindesk.com

## Current BPI (RT)

```http
GET /v1/bpi/currentprice/<CODE>.json
```

With code in [Currency List (ISO 4217)][ISO-4217]

## Historical BPI data

```http
GET /v1/bpi/historical/close.json
```

### Parameters

* `?index=[USD/CNY]` base currency default USD
* `?currency=\<VALUE>` ([Currency List][ISO-4217])
* `?start=\<VALUE>&end=\<VALUE>` range in YYYY-MM-DD format
* `?for=yesterday` single value for yesterday

[ISO-4217]:https://api.coindesk.com/v1/bpi/supported-currencies.json