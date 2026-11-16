-- Gold layer: 1-minute and 5-minute bucket Volume-Weighted Average Price
SELECT
    ticker,
    DATE_TRUNC('minute', event_timestamp) AS time_window_1min,
    time_bucket(INTERVAL 5 MINUTES, event_timestamp) AS time_window_5min,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_shares_traded,
    ROUND(SUM(notional_value), 2) AS total_notional,
    ROUND(SUM(execution_price * quantity) / NULLIF(SUM(quantity), 0), 4) AS vwap_price,
    ROUND(AVG(arrival_price), 4) AS avg_arrival_price,
    ROUND(AVG(slippage_bps), 2) AS avg_slippage_bps
FROM {{ ref('stg_market_trades') }}
GROUP BY ticker, DATE_TRUNC('minute', event_timestamp), time_bucket(INTERVAL 5 MINUTES, event_timestamp)
ORDER BY ticker, time_window_1min
