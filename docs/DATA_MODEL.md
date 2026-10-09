# Data Modeling & Schema Specifications (Kimball Star Schema)

## 1. Medallion Schemas

### 1.1 Bronze Tier: `bronze_trades`
Raw immutable event ingestion log.

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| `trade_id` | STRING | No | Unique transaction execution identifier |
| `sequence_number` | INT64 | No | Monotonically increasing producer sequence ID |
| `ticker` | STRING | No | Equity instrument symbol (e.g. `AAPL`, `NVDA`) |
| `event_timestamp` | STRING (ISO 8601) | No | Trade execution timestamp generated at venue |
| `arrival_price` | FLOAT64 | No | Prevailing mid-quote price when order arrived |
| `execution_price` | FLOAT64 | No | Executed fill price per share |
| `quantity` | INT64 | No | Executed share count |
| `notional_value` | FLOAT64 | No | Dollar value: $\text{execution\_price} \times \text{quantity}$ |
| `side` | STRING | No | Order side (`BUY` or `SELL`) |
| `order_type` | STRING | No | Execution type (`LIMIT`, `MARKET`, `IOC`, `POST_ONLY`) |
| `venue` | STRING | No | Execution venue (`XNAS`, `XNYS`, `BATS`, `ARCA`, `IEXG`) |
| `counterparty_id` | STRING | No | Institutional counterparty identifier |
| `trader_id` | STRING | No | Execution desk / algorithmic strategy ID |
| `is_anomaly` | BOOLEAN | No | Synthetic test flag indicating injected chaos |
| `ingestion_timestamp`| STRING (ISO 8601) | No | UTC timestamp when record reached Lakehouse |

---

### 1.2 Silver Tier: `silver_trades`
Cleaned, conformed fact records with data contracts enforced.

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| `trade_id` | STRING | No | Primary key (unique constraint asserted) |
| `sequence_number` | INT64 | No | Exchange sequence counter |
| `ticker` | STRING | No | Uppercase instrument ticker |
| `event_timestamp` | TIMESTAMP | No | Validated execution timestamp |
| `arrival_price` | FLOAT64 | No | Benchmark arrival mid-price |
| `execution_price` | FLOAT64 | No | Executed fill price ($> 0.0$) |
| `quantity` | INT64 | No | Executed shares ($> 0$) |
| `notional_value` | FLOAT64 | No | Total executed value in USD |
| `slippage_dollars` | FLOAT64 | No | Absolute slippage: $\text{execution\_price} - \text{arrival\_price}$ |
| `slippage_bps` | FLOAT64 | No | Relative slippage in basis points |
| `side` | STRING | No | `BUY` or `SELL` |
| `order_type` | STRING | No | `LIMIT`, `MARKET`, `IOC`, or `POST_ONLY` |
| `venue` | STRING | No | Standardized MIC code (`XNAS`, `XNYS`, etc.) |
| `counterparty_id` | STRING | No | Validated counterparty code |
| `trader_id` | STRING | No | Trading desk code |
| `silver_processed_at`| TIMESTAMP | No | Processing timestamp in Silver tier |

---

### 1.3 Dead-Letter Queue (DLQ): `dlq_quarantine`
Isolated storage for contract-violating records.

| Column | Data Type | Description |
|---|---|---|
| *Original Bronze Columns* | *Original Types* | Raw payload preserving original corrupted data |
| `dlq_reason` | STRING | Contract violation reason code (e.g. `NON_POSITIVE_EXECUTION_PRICE`) |
| `quarantined_at` | TIMESTAMP | Timestamp when row was diverted from Silver promotion |

---

## 2. Gold Dimensional Marts (Kimball Star Schema)

```
                            ┌────────────────────────┐
                            │    dim_instruments     │
                            ├────────────────────────┤
                            │ PK ticker              │
                            │    total_trade_count   │
                            │    avg_price           │
                            │    min_price           │
                            │    max_price           │
                            └───────────┬────────────┘
                                        │
                                        │ 1:N
                                        ▼
┌────────────────────────┐  N:1  ┌────────────────────────┐  1:N  ┌────────────────────────┐
│       dim_venues       │◄──────│  fct_trade_executions  │──────►│   agg_intraday_vwap    │
├────────────────────────┤       ├────────────────────────┤       ├────────────────────────┤
│ PK venue               │       │ PK trade_id            │       │ PK ticker              │
│    execution_count     │       │ FK ticker              │       │ PK time_window_1min    │
│    total_volume_traded │       │ FK venue               │       │    total_shares_traded │
│    avg_slippage_bps    │       │    execution_price     │       │    total_notional      │
└────────────────────────┘       │    quantity            │       │    vwap_price          │
                                 │    notional_value      │       │    avg_arrival_price   │
                                 │    slippage_bps        │       │    avg_slippage_bps    │
                                 │    event_timestamp     │       └────────────────────────┘
                                 └────────────────────────┘
```

---

## 3. Mathematical Metric Formulations

### Volume-Weighted Average Price (VWAP)
$$\text{VWAP}_t = \frac{\sum_{i=1}^{N_t} (P_i \times Q_i)}{\sum_{i=1}^{N_t} Q_i}$$
Where:
- $P_i$ = Execution price of trade $i$.
- $Q_i$ = Share quantity of trade $i$.
- $N_t$ = Total number of trades in time window $t$ (1-minute or 5-minute rolling interval).

### Execution Slippage (Basis Points)
$$\text{Slippage (bps)} = \left( \frac{P_{\text{execution}} - P_{\text{arrival}}}{P_{\text{arrival}}} \right) \times 10,000$$
A positive slippage on a `BUY` indicates that the order executed above the arrival price (costly execution drift).
