# PolyMetrics

Polymarket data analytics pipeline for CSE5114 at WUSTL. Ingests market
metadata, user activity, and live order flow into Snowflake, runs
streaming anomaly detection, and exposes results through a Streamlit
dashboard.

## Team

| Member | Scope                                                        | Snowflake DB  |
|--------|--------------------------------------------------------------|---------------|
| Luke   | Gamma market metadata + Streamlit analytics layer            | `PANTHER_DB`  |
| Will   | User activity batch ingestion (2.26M users, 120+ GB CSV)     | `COYOTE_DB`   |
| Andrew | Live CLOB order flow + anomaly detection                     | `DOG_DB`      |

All three databases live on Snowflake account `UNB02139`. Cross-database
queries use fully qualified names (e.g. `PANTHER_DB.CURATED.GAMMA_MARKETS`).

## Architecture

```
  Gamma REST API                      Polymarket bulk user activity
       │                                          │
       ▼                                          ▼
   [Airflow]                                 [Airflow hourly]
       │                                          │
       ▼                                          ▼
   PANTHER_DB  (Luke)                        COYOTE_DB  (Will)
   market metadata                           user transactions


  Polymarket CLOB websocket
       │
       ▼
  Kafka producer  ──►  polymarket.trades.raw
  (Python, 24/7)  ──►  polymarket.books.raw
                              │
                              ▼
                        PyFlink job
                        (4 detectors as
                         windowed operators)
                              │
                              ▼
                         DOG_DB  (Andrew)
                   ┌──────────┴──────────┐
                   ▼                     ▼
              DOG_SCHEMA           DOG_TRANSFORM
              (raw streams +       (daily aggs,
               detector output)     risk profiles)


              PANTHER_DB  +  COYOTE_DB  +  DOG_DB
                              │
                              ▼
                     PANTHER_DB.ANALYTICS
                     (Luke's joined layer)
                              │
                              ▼
                      Streamlit dashboard
```

## Andrew's streaming + anomaly layer

### Deployment

- **Host:** WUSTL `linuxlab.engr.wustl.edu` (same host running Airflow)
- **Runtime:** Kafka + Flink via `docker-compose`, `systemd`-supervised
- **Schedule:** 24/7 continuous ingestion
- **Kafka retention:** 7 days

### Pipeline

1. **Market selector (daily).** Pulls the top ~200 markets by 24h volume
   from `PANTHER_DB.CURATED.GAMMA_MARKETS`. Writes the active asset-ID
   list to a Kafka control topic; producer hot-reloads.

2. **Producer (continuous).** Python asyncio websocket client subscribed
   to Polymarket CLOB `market` and `book` channels for the active markets.
   Publishes raw JSON to Kafka topics `polymarket.trades.raw` and
   `polymarket.books.raw`. Buffers locally on Kafka-side outages.

3. **Flink job (continuous).** PyFlink streaming job:
   - sources both Kafka topics
   - passthrough sinks raw events to `DOG_SCHEMA.CLOB_TRADES_STREAM` and
     `DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS`
   - runs 4 anomaly detectors as windowed operators keyed by
     `market_condition_id`: volume spike, whale, price impact, timing burst
   - sinks detector hits to `DOG_SCHEMA.DETECTED_ANOMALIES`
   - Snowflake sink uses key-pair auth, no MFA

4. **Nightly Airflow DAGs** (after the busiest trading hours):
   - `order_flow_enrichment` — pulls REST trades (with wallet addresses)
     and joins them against the websocket stream to fill
     `CLOB_TRADES_STREAM.wallet_address`
   - `order_flow_daily_agg` — rolls up `MARKET_DAILY_STATS` and
     `WALLET_DAILY_STATS`
   - `anomaly_model_training` — retrains per-market isolation-forest
     baselines on a rolling 7-day window
   - `anomaly_attribution` — links anomalies to wallets, updates
     `TRADER_RISK_PROFILES`

### Tables in `DOG_DB`

**Raw (straight from Polymarket):**
- `DOG_SCHEMA.CLOB_TRADES_STREAM` — websocket trade events
- `DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS` — websocket orderbook snapshots
- `DOG_SCHEMA.DATA_API_TRADES` — REST trades (includes `proxy_wallet`)
- `DOG_SCHEMA.LEADERBOARD_USERS` — REST leaderboard snapshots

**Derived (built by the pipeline):**
- `DOG_SCHEMA.DETECTED_ANOMALIES` — Flink detector output
- `DOG_TRANSFORM.MARKET_DAILY_STATS` — per-market daily rollup
- `DOG_TRANSFORM.WALLET_DAILY_STATS` — per-wallet daily rollup
- `DOG_TRANSFORM.ANOMALY_ATTRIBUTION` — anomaly → wallet links
- `DOG_TRANSFORM.TRADER_RISK_PROFILES` — composite risk per wallet

## Repo layout

- `dags/` — Airflow DAGs (Luke's `gamma_markets_to_snowflake` +
  Andrew's 4 nightly DAGs)
- `src/polymarket_etl/` — Luke's Gamma file parsing + Snowflake SQL helpers
- `src/polymarket_streaming/` — Andrew's websocket client, buffer,
  Kafka/Snowflake loaders
- `src/polymarket_detection/` — Andrew's 4 detectors + composite scorer
- `sql/snowflake/` — DDL (Luke's `PANTHER_DB` bootstrap + Andrew's
  `00_bootstrap_dog_db`, `01_streaming_tables`, `02_anomaly_tables`,
  `03_trader_profiles`)
- `scripts/` — Luke's Gamma fetch scripts + Andrew's local pipeline runners
- `docs/` — operational notes
- `include/`, `plugins/` — Airflow home placeholders

## Setup

- Airflow + `PANTHER_DB` runbook for linuxlab: see [AGENTS.md](AGENTS.md)
- Kafka + Flink streaming stack: see `docs/streaming_setup.md` (coming)
- Per-member Airflow connection: named `Snowflake`, key-pair auth, points
  at the member's own database. Template: `airflow_home.env.example`.

## Status

- Done: batch Gamma ingestion to `PANTHER_DB` (Luke)
- Done: user activity ingestion to `COYOTE_DB` (Will)
- Done: streaming client, detectors, and `DOG_DB` DDL (Andrew, local only)
- Next: key-pair auth on Andrew's Snowflake user
- Next: Kafka + Flink stack on linuxlab, 24/7 ingestion
- Next: Andrew's tab in the Streamlit app

## Class

CSE5114, Washington University in St. Louis. See [PROPOSAL.md](PROPOSAL.md)
for the original project charter and [PLAN.md](PLAN.md) for the earlier
ingestion plan.
