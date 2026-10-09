-- Staging layer: Cleansed streaming trades with schema validation
SELECT
    trade_id,
    sequence_number,
    UPPER(TRIM(ticker)) AS ticker,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    CAST(arrival_price AS DOUBLE) AS arrival_price,
    CAST(execution_price AS DOUBLE) AS execution_price,
    CAST(quantity AS BIGINT) AS quantity,
    CAST(notional_value AS DOUBLE) AS notional_value,
    CAST(slippage_dollars AS DOUBLE) AS slippage_dollars,
    CAST(slippage_bps AS DOUBLE) AS slippage_bps,
    UPPER(side) AS side,
    UPPER(order_type) AS order_type,
    UPPER(venue) AS venue,
    counterparty_id,
    trader_id,
    silver_processed_at
FROM {{ ref('silver_trades_source') }}
WHERE execution_price > 0
  AND quantity > 0
  AND trade_id IS NOT NULL
