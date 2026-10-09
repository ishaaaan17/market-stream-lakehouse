-- Gold layer: Atomic Trade Execution Fact Table
SELECT
    trade_id,
    sequence_number,
    ticker,
    event_timestamp,
    arrival_price,
    execution_price,
    quantity,
    notional_value,
    slippage_dollars,
    slippage_bps,
    side,
    order_type,
    venue,
    counterparty_id,
    trader_id
FROM {{ ref('stg_market_trades') }}
