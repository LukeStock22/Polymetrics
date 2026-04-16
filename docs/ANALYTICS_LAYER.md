# Analytics Layer

This document describes the warehouse-driven analytics layer in `PANTHER_DB.ANALYTICS`:

- what it is
- how it is built
- whether it is scheduled
- every analytics table
- each table's source, grain, and purpose

## Current status

The analytics layer is **not scheduled yet**.

Right now it is refreshed manually by running:

```bash
python src/polymarket_etl/build_analytics_layer.py --config-path .streamlit/secrets.toml
```

That script:

1. connects to Snowflake through Snowpark
2. reads source and curated tables
3. builds analytics tables in `PANTHER_DB.ANALYTICS`
4. overwrites those analytics tables with the latest build

There is currently:

- no Airflow DAG for the analytics build
- no Snowflake task/stream driving it
- no incremental refresh logic yet

So the analytics layer is materialized and warehouse-driven, but refresh orchestration is still manual.

## Why this layer exists

The analytics layer separates:

- ingestion and curation
- business-ready analytics modeling
- presentation in Streamlit

That gives the project a clearer architecture:

1. Airflow and teammate pipelines load raw/curated source tables.
2. Snowpark transforms those tables into reusable analytics tables.
3. Streamlit queries those analytics tables instead of recomputing joins in the UI.

This matters for a data management at scale class because it gives you a defensible story for:

- separation of concerns
- warehouse-native computation
- reusable semantic datasets
- scalable presentation against precomputed facts

## Source systems

The analytics layer currently draws from these upstream tables.

### Market dimension source

- `PANTHER_DB.CURATED.GAMMA_MARKETS`

Purpose:

- latest curated market metadata
- canonical market grain
- source of `condition_id`, labels, status, liquidity, and listed volume
- source of token/outcome arrays used to build asset mappings

### DOG trade sources

- `DOG_DB.DOG_SCHEMA.DATA_API_TRADES`
- `DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM`
- `DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS`
- `DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS`
- `DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES`

Purpose:

- trade facts
- market microstructure facts
- leaderboard enrichment
- anomaly detection facts

### DOG transformed sources

- `DOG_DB.DOG_TRANSFORM.MARKET_DAILY_STATS`
- `DOG_DB.DOG_TRANSFORM.WALLET_DAILY_STATS`
- `DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION`
- `DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES`

Purpose:

- possible higher-level teammate-provided transforms

Current caveat:

- at implementation time, most of these `DOG_TRANSFORM` tables were empty, so the analytics layer does **not** depend on them for its core daily market and wallet aggregations
- `TRADER_RISK_PROFILES` is joined when populated, but its fields may be null if the table remains empty

## Naming pattern

The analytics schema uses a simple model:

- `DIM_*`: dimension-like tables
- `BRIDGE_*`: mapping tables between grains
- `FACT_*`: fact/event/aggregate tables
- app-facing presentation tables: the four tables currently queried by Streamlit

## Build order

The build script creates tables in this rough order:

1. `DIM_MARKETS`
2. `BRIDGE_MARKET_ASSETS`
3. `FACT_DATA_API_TRADES`
4. `FACT_CLOB_TRADES`
5. `FACT_BOOK_SNAPSHOTS`
6. `FACT_LEADERBOARD_USER_SNAPSHOTS`
7. `DIM_LEADERBOARD_USERS`
8. `FACT_ANOMALIES`
9. `FACT_ANOMALY_ATTRIBUTION`
10. `FACT_MARKET_DAILY`
11. `FACT_WALLET_DAILY`
12. `DIM_TRADERS`
13. app-facing tables:
   - `TRACKED_MARKET_VOLUME_DAILY`
   - `HIGHEST_VOLUME_MARKETS`
   - `MARKET_TOP_TRADERS_DAILY`
   - `MARKET_TOP_TRADERS_ALL_TIME`

## Analytics tables

### `PANTHER_DB.ANALYTICS.DIM_MARKETS`

Grain:

- one row per latest known market snapshot, keyed by `CONDITION_ID`

Primary sources:

- `PANTHER_DB.CURATED.GAMMA_MARKETS`

Core fields:

- `MARKET_ID`
- `CONDITION_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `MARKET_SLUG`
- `QUESTION_TEXT`
- `START_DATE`
- `END_DATE`
- `ACTIVE`
- `CLOSED`
- `BEST_BID`
- `BEST_ASK`
- `SPREAD`
- `LAST_TRADE_PRICE`
- `LIQUIDITY_USDC`
- `VOLUME_24HR_USDC`
- `LISTED_VOLUME_USDC`
- `OUTCOMES`
- `CLOB_TOKEN_IDS`

Purpose:

- canonical market dimension for the rest of the analytics layer
- stable join target for trade facts, anomaly facts, and app-facing market lists
- carries status and label information so the app does not have to join back to `GAMMA_MARKETS`

### `PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS`

Grain:

- one row per market outcome token

Primary sources:

- `PANTHER_DB.ANALYTICS.DIM_MARKETS`
- derived by flattening `OUTCOMES` and `CLOB_TOKEN_IDS`

Core fields:

- `MARKET_ID`
- `CONDITION_ID`
- `OUTCOME_INDEX`
- `ASSET_ID`
- `OUTCOME_NAME`

Purpose:

- bridges market-level identifiers to token/outcome-level identifiers
- required for mapping `ASSET_ID` from CLOB feeds to readable market outcomes
- foundational for future token-level analytics

### `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`

Grain:

- one row per DOG data-api trade

Primary sources:

- `DOG_DB.DOG_SCHEMA.DATA_API_TRADES`
- enriched with `PANTHER_DB.ANALYTICS.DIM_MARKETS`

Core fields:

- `TRADE_DATE`
- `TRADE_TS`
- `MARKET_ID`
- `CONDITION_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `ACTIVE`
- `CLOSED`
- `PROXY_WALLET`
- `ASSET_ID`
- `TRADE_SIDE`
- `OUTCOME_NAME`
- `OUTCOME_INDEX`
- `PRICE`
- `SIZE`
- `USDC_VOLUME`
- `TRANSACTION_HASH`
- `SOURCE_SYSTEM`

Purpose:

- the current primary trade fact for warehouse analytics
- source for daily market aggregates
- source for wallet aggregates
- source for trader rankings

Important caveat:

- some DOG trades do not map to `DIM_MARKETS`, so `MARKET_ID` can be null for unmatched `CONDITION_ID`s

### `PANTHER_DB.ANALYTICS.FACT_CLOB_TRADES`

Grain:

- one row per streamed CLOB trade event

Primary sources:

- `DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM`
- `PANTHER_DB.ANALYTICS.DIM_MARKETS`
- `PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS`

Core fields:

- `STREAM_ID`
- `CONDITION_ID`
- `MARKET_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `ASSET_ID`
- `OUTCOME_INDEX`
- `OUTCOME_NAME`
- `PROXY_WALLET`
- `TRADE_SIDE`
- `PRICE`
- `SIZE`
- `USDC_VOLUME`
- `FEE_RATE_BPS`
- `TRADE_TS`
- `TRADE_DATE`

Purpose:

- future microstructure analysis
- streamed trade timing
- fee-rate analysis
- token-level trading analysis

Current status:

- structurally built
- currently empty because the upstream source table is empty

### `PANTHER_DB.ANALYTICS.FACT_BOOK_SNAPSHOTS`

Grain:

- one row per order-book snapshot

Primary sources:

- `DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS`
- `PANTHER_DB.ANALYTICS.DIM_MARKETS`
- `PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS`

Core fields:

- `SNAPSHOT_ID`
- `CONDITION_ID`
- `MARKET_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `ASSET_ID`
- `OUTCOME_INDEX`
- `OUTCOME_NAME`
- `BEST_BID`
- `BEST_ASK`
- `SPREAD`
- `SNAPSHOT_TS`
- `SNAPSHOT_DATE`

Purpose:

- spread analysis
- liquidity state analysis
- before/after market-state comparisons
- order-book monitoring dashboards later on

Current status:

- structurally built
- currently empty because the upstream source table is empty

### `PANTHER_DB.ANALYTICS.FACT_LEADERBOARD_USER_SNAPSHOTS`

Grain:

- one row per wallet per leaderboard snapshot date

Primary sources:

- `DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS`

Core fields:

- `PROXY_WALLET`
- `LEADERBOARD_RANK`
- `USER_NAME`
- `X_USERNAME`
- `VERIFIED_BADGE`
- `LEADERBOARD_VOLUME`
- `LEADERBOARD_PNL`
- `PROFILE_IMAGE`
- `SNAPSHOT_DATE`

Purpose:

- preserves leaderboard history snapshots
- supports trend analysis later
- source for current leaderboard dimension

### `PANTHER_DB.ANALYTICS.DIM_LEADERBOARD_USERS`

Grain:

- one row per wallet, taking the latest leaderboard snapshot

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_LEADERBOARD_USER_SNAPSHOTS`

Core fields:

- `PROXY_WALLET`
- latest rank and profile fields

Purpose:

- current user enrichment dimension
- supports trader profile joins without forcing the app to deduplicate snapshots

### `PANTHER_DB.ANALYTICS.FACT_ANOMALIES`

Grain:

- one row per detected anomaly

Primary sources:

- `DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES`
- `PANTHER_DB.ANALYTICS.DIM_MARKETS`
- `PANTHER_DB.ANALYTICS.BRIDGE_MARKET_ASSETS`

Core fields:

- `ANOMALY_ID`
- `DETECTOR_TYPE`
- `CONDITION_ID`
- `MARKET_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `ASSET_ID`
- `OUTCOME_INDEX`
- `OUTCOME_NAME`
- `SEVERITY_SCORE`
- `DETECTED_AT`
- `DETECTED_DATE`

Purpose:

- anomaly fact table for market surveillance questions
- later supports anomaly summaries, suspicious market days, and detector analysis

Current status:

- structurally built
- currently empty because the upstream source table is empty

### `PANTHER_DB.ANALYTICS.FACT_ANOMALY_ATTRIBUTION`

Grain:

- one row per anomaly-to-trade/wallet attribution

Primary sources:

- `DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION`
- `PANTHER_DB.ANALYTICS.DIM_MARKETS`

Core fields:

- `ATTRIBUTION_ID`
- `ANOMALY_ID`
- `PROXY_WALLET`
- `CONDITION_ID`
- `MARKET_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `TRADE_TIMESTAMP`
- `TRADE_DATE`
- `TRADE_SIDE`
- `TRADE_SIZE`
- `TRADE_PRICE`
- `TRANSACTION_HASH`

Purpose:

- bridge between anomalies and traders
- supports wallet risk metrics and anomaly involvement metrics

Current status:

- structurally built
- currently empty because the upstream source table is empty

### `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`

Grain:

- one row per market per day

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`
- `PANTHER_DB.ANALYTICS.FACT_ANOMALIES`

Core fields:

- `STAT_DATE`
- `MARKET_ID`
- `CONDITION_ID`
- `MARKET_QUESTION`
- `MARKET_LABEL`
- `ACTIVE`
- `CLOSED`
- `TRADE_COUNT`
- `BUY_COUNT`
- `SELL_COUNT`
- `TOTAL_VOLUME`
- `TOTAL_USDC_VOLUME`
- `UNIQUE_WALLETS`
- `AVG_TRADE_SIZE`
- `MAX_TRADE_SIZE`
- `VWAP`
- `FIRST_TRADE_TS`
- `LAST_TRADE_TS`
- `PRICE_OPEN`
- `PRICE_CLOSE`
- `PRICE_HIGH`
- `PRICE_LOW`
- `ANOMALY_COUNT`

Purpose:

- central daily market performance table
- answers high-level market/day questions
- source for current app question around popular markets by day

Important note:

- this table is currently derived from `FACT_DATA_API_TRADES`, not from the empty teammate `DOG_TRANSFORM.MARKET_DAILY_STATS`

### `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`

Grain:

- one row per wallet per day

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`
- `PANTHER_DB.ANALYTICS.FACT_ANOMALY_ATTRIBUTION`

Core fields:

- `PROXY_WALLET`
- `STAT_DATE`
- `TRADE_COUNT`
- `DISTINCT_MARKETS`
- `TOTAL_VOLUME`
- `TOTAL_USDC_VOLUME`
- `BUY_VOLUME`
- `SELL_VOLUME`
- `AVG_TRADE_SIZE`
- `MAX_TRADE_SIZE`
- `ANOMALY_INVOLVEMENT`

Purpose:

- reusable wallet-day fact table
- supports wallet activity trends, behavioral analysis, and future wallet pages

### `PANTHER_DB.ANALYTICS.DIM_TRADERS`

Grain:

- one row per wallet/trader

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`
- `PANTHER_DB.ANALYTICS.DIM_LEADERBOARD_USERS`
- `PANTHER_DB.ANALYTICS.FACT_ANOMALY_ATTRIBUTION`
- `DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES`

Core fields:

- `PROXY_WALLET`
- leaderboard/profile fields
- `TOTAL_TRADES`
- `TOTAL_VOLUME`
- `DISTINCT_MARKETS`
- `AVG_TRADE_SIZE`
- `MAX_SINGLE_TRADE`
- `ANOMALY_INVOLVEMENT_COUNT`
- `WIN_RATE`
- `AVG_SECONDS_BEFORE_RESOLUTION`
- `ANOMALY_INVOLVEMENT_RATE`
- `TOP_DETECTOR_TYPE`
- `RISK_SCORE`
- `FIRST_SEEN`
- `LAST_SEEN`

Purpose:

- reusable trader dimension
- combines observed trading behavior with optional risk enrichment
- supports top-trader, profile, and risk-oriented questions

Current caveat:

- risk-specific fields depend on the upstream `TRADER_RISK_PROFILES` table being populated

## App-facing tables

These four tables are still created because the Streamlit app already depends on them. They are derived from the broader facts above.

### `PANTHER_DB.ANALYTICS.TRACKED_MARKET_VOLUME_DAILY`

Grain:

- one row per market per day

Primary sources:

- derived from `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`

Purpose:

- simplified app-facing market/day ranking table
- powers the Streamlit question currently labeled `Most Popular Markets Today`

### `PANTHER_DB.ANALYTICS.HIGHEST_VOLUME_MARKETS`

Grain:

- one row per market

Primary sources:

- derived from `PANTHER_DB.ANALYTICS.DIM_MARKETS`

Purpose:

- simplified app-facing highest listed-volume table
- powers the Streamlit question currently labeled `Highest Volume Markets`

### `PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY`

Grain:

- one row per market, trader, and day

Primary sources:

- derived from `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`

Purpose:

- app-facing drill-down for daily top traders in a selected market

### `PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME`

Grain:

- one row per market and trader across all loaded history

Primary sources:

- derived from `PANTHER_DB.ANALYTICS.FACT_DATA_API_TRADES`

Purpose:

- app-facing all-time trader drill-down for a selected market

## What questions this layer can support

This v2 design is broader than the current Streamlit UI. It can support questions such as:

- Which markets have the most volume today?
- Which markets have the highest listed lifetime volume?
- Which wallets trade the most across days?
- Which traders dominate a given market?
- Which markets have the widest spreads?
- Which markets show unusual trading or anomaly counts?
- Which traders overlap with leaderboard users?
- Which outcome tokens are active within a market?

## Current data availability caveats

At the time of implementation:

- `DOG_DB.DOG_SCHEMA.DATA_API_TRADES` had data
- `DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS` had data
- `DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM` was empty
- `DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS` was empty
- `DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES` was empty
- most `DOG_DB.DOG_TRANSFORM` tables were empty

That means:

- the trade and leaderboard parts of the analytics layer are usable now
- the microstructure and anomaly parts are structurally ready but not yet populated

## How Snowpark is used

Snowpark is the transformation layer between source data and the analytics schema.

Specifically, the build script:

- opens a Snowflake session in Python
- reads upstream tables as Snowpark DataFrames
- performs joins, deduplication, flattening, and aggregation in Snowflake
- writes the results back into warehouse tables

This is important architecturally because:

- compute stays in Snowflake
- Streamlit remains a presentation layer
- business logic lives in reusable warehouse tables
- future questions can reuse the same facts and dimensions

## Future orchestration

The most natural next step is to schedule this build.

Two reasonable options:

1. Airflow DAG

- add a DAG task that runs `build_analytics_layer.py`
- schedule it after the upstream source tables refresh

2. Snowflake-native orchestration

- convert the build into Snowflake tasks or SQL models
- schedule refresh directly inside Snowflake

For this repo, Airflow is the cleaner near-term fit because the project already uses DAGs for upstream ingestion.
