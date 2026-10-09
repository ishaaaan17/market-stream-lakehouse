-- Gold layer: Execution slippage benchmarking against arrival mid quote
SELECT
    venue,
    ticker,
    order_type,
    side,
    COUNT(*) AS total_executions,
    ROUND(SUM(notional_value), 2) AS total_notional_volume,
    ROUND(AVG(slippage_dollars), 4) AS avg_slippage_dollars,
    ROUND(AVG(slippage_bps), 2) AS avg_slippage_bps,
    ROUND(MIN(slippage_bps), 2) AS min_slippage_bps,
    ROUND(MAX(slippage_bps), 2) AS max_slippage_bps
FROM {{ ref('stg_market_trades') }}
GROUP BY venue, ticker, order_type, side
