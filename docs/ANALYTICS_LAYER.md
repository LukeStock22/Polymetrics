# Analytics Layer

This document describes the warehouse-driven analytics layer in `PANTHER_DB.ANALYTICS`:

For a visual lineage view with join keys, see [ANALYTICS_LAYER_DIAGRAM.md](/Users/lukestockbridge/Desktop/School/CSE5114/Polymetrics/docs/ANALYTICS_LAYER_DIAGRAM.md).

- what it is
- how it is built
- whether it is scheduled
- every analytics table
- each table's source, grain, and purpose

## Current status

The analytics layer now has two refresh paths:

1. manual build
2. scheduled Airflow build via `analytics_layer_refresh`

Manual build:

```bash
python src/polymarket_etl/build_analytics_layer.py --config-path .streamlit/secrets.toml --mode incremental
```

Scheduled build:

- DAG id: `analytics_layer_refresh`
- schedule type: Airflow dataset-aware scheduling
- subscribed assets:
  - `snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS`
  - `snowflake://COYOTE_DB/PUBLIC/CURATED_POLYMARKET_USER_ACTIVITY`
- default mode: `incremental`

The build script:

1. connects to Snowflake through Snowpark
2. reads source and curated tables
3. builds analytics tables in `PANTHER_DB.ANALYTICS`
4. incrementally refreshes or fully rebuilds those analytics tables
5. records build watermark state in `ANALYTICS.ANALYTICS_BUILD_STATE`

There is currently:

- an Airflow DAG for the analytics build
- incremental refresh logic driven by `COYOTE._LOADED_AT`
- Airflow dataset-based orchestration between upstream curated DAGs and the analytics DAG
- no Snowflake-native task/stream orchestration yet

So the analytics layer is materialized and warehouse-driven, and it now has an Airflow scheduling path that is data-aware rather than time-only. The remaining orchestration gap is Snowflake-native scheduling if you eventually want to remove Airflow from the analytics refresh step.

## Orchestration design choice

The project now uses Airflow datasets as the connection between upstream curated data publication and the downstream analytics refresh.

Upstream dataset publishers:

- `gamma_markets_daily.upsert_curated_markets` publishes `snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS`
- `polymarket_activity_pipeline_dag.process_data` publishes `snowflake://COYOTE_DB/PUBLIC/CURATED_POLYMARKET_USER_ACTIVITY`

Downstream subscriber:

- `analytics_layer_refresh` subscribes to either dataset and runs the Snowpark build when one of them is updated

Why this is a strong choice for the project:

- it matches the class recommendation for data-aware scheduling
- it is event-driven, so Airflow does not need long-running polling sensors
- it creates a clean architecture story: curated data is published first, analytics materializations follow
- it keeps Streamlit decoupled from upstream ETL timing because the app reads materialized marts

Tradeoffs:

- dataset events are logical Airflow asset updates, not native Snowflake change capture
- the DAGs must share one Airflow environment and metadata database
- a run can be triggered by only one upstream changing, so the analytics build must remain incremental and idempotent
- this still does not replace a fully Snowflake-native orchestration model built on streams and tasks

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

The analytics layer follows this source-of-truth model:

- `PANTHER` is authoritative for market metadata
- `COYOTE` is authoritative for user-level transaction activity
- `DOG` provides supplemental raw feeds and teammate-owned derived/enrichment tables

The analytics layer currently draws from these upstream tables.

### Market dimension source

- `PANTHER_DB.CURATED.GAMMA_MARKETS`

Purpose:

- latest curated market metadata
- canonical market grain
- source of `condition_id`, labels, status, liquidity, and listed volume
- source of token/outcome arrays used to build asset mappings

### Authoritative transaction source

- `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY`

Purpose:

- canonical user-level transaction feed
- source of wallet, side, outcome, price, size, and traded USDC fields
- source of truth for market-volume and top-trader analytics

### DOG raw supplemental sources

- `DOG_DB.DOG_SCHEMA.DATA_API_TRADES`
- `DOG_DB.DOG_SCHEMA.CLOB_TRADES_STREAM`
- `DOG_DB.DOG_SCHEMA.CLOB_BOOK_SNAPSHOTS`
- `DOG_DB.DOG_SCHEMA.LEADERBOARD_USERS`

Purpose:

- supplemental trade and market microstructure facts
- market microstructure facts
- leaderboard enrichment

Interpretation:

- `DATA_API_TRADES`: raw Polymarket API data
- `CLOB_TRADES_STREAM`: raw CLOB stream data
- `CLOB_BOOK_SNAPSHOTS`: raw CLOB book snapshot data
- `LEADERBOARD_USERS`: raw leaderboard snapshot data

### DOG derived supplemental sources

- `DOG_DB.DOG_SCHEMA.DETECTED_ANOMALIES`
- `DOG_DB.DOG_TRANSFORM.MARKET_DAILY_STATS`
- `DOG_DB.DOG_TRANSFORM.WALLET_DAILY_STATS`
- `DOG_DB.DOG_TRANSFORM.ANOMALY_ATTRIBUTION`
- `DOG_DB.DOG_TRANSFORM.TRADER_RISK_PROFILES`

Purpose:

- anomaly detection output
- teammate-provided higher-level transforms and risk features

Current assessment:

- there is no evidence in this repo that DOG derived tables are built from COYOTE
- they appear to be teammate-owned DOG-side derived datasets based on their own pipelines
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
2. `FACT_USER_ACTIVITY_TRADES`
3. `BRIDGE_MARKET_ASSETS`
4. `FACT_DATA_API_TRADES`
5. `FACT_CLOB_TRADES`
6. `FACT_BOOK_SNAPSHOTS`
7. `FACT_LEADERBOARD_USER_SNAPSHOTS`
8. `DIM_LEADERBOARD_USERS`
9. `FACT_ANOMALIES`
10. `FACT_ANOMALY_ATTRIBUTION`
11. `FACT_MARKET_DAILY`
12. `FACT_WALLET_DAILY`
13. `DIM_TRADERS`
14. app-facing tables:
   - `TRACKED_MARKET_VOLUME_DAILY`
   - `HIGHEST_VOLUME_MARKETS`
   - `MARKET_TOP_TRADERS_DAILY`
   - `MARKET_TOP_TRADERS_ALL_TIME`
   - `MARKET_CONCENTRATION_DAILY`
   - `PLATFORM_DAILY_SUMMARY`
   - `TRADER_SEGMENT_SNAPSHOT`
   - `TRADER_COHORT_MONTHLY`
   - `MARKET_THEME_DAILY`

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
- `MARKET_GROUP_TITLE`
- `EVENT_TITLE`
- `EVENT_SLUG`
- `MARKET_THEME`
- `MARKET_THEME_SOURCE`

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

- supplemental DOG data-api trade fact
- useful for reconciling COYOTE trades against an external API feed
- available for future comparison and data-quality checks

Important caveat:

- some DOG trades do not map to `DIM_MARKETS`, so `MARKET_ID` can be null for unmatched `CONDITION_ID`s

### `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`

Grain:

- one row per COYOTE `TRADE` activity row

Primary sources:

- `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY`
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
- `ACTIVITY_TYPE`
- `SOURCE_SYSTEM`

Purpose:

- the primary trade fact for warehouse analytics
- source for daily market aggregates
- source for wallet aggregates
- source for trader rankings
- aligns the analytics layer with COYOTE as the transaction source of truth

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

- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
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

- this table is currently derived from `FACT_USER_ACTIVITY_TRADES`, not from the empty teammate `DOG_TRANSFORM.MARKET_DAILY_STATS`

### `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`

Grain:

- one row per wallet per day

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
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

- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
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

- derived from `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`

Purpose:

- app-facing drill-down for daily top traders in a selected market

### `PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_ALL_TIME`

Grain:

- one row per market and trader across all loaded history

Primary sources:

- derived from `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`

Purpose:

- app-facing all-time trader drill-down for a selected market

### `PANTHER_DB.ANALYTICS.MARKET_CONCENTRATION_DAILY`

Grain:

- one row per market and day

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`
- `PANTHER_DB.ANALYTICS.MARKET_TOP_TRADERS_DAILY`

Purpose:

- materializes top-1 and top-5 share-of-volume metrics by market/day
- strengthens the whale-dominance story beyond the earlier `volume per trader` proxy
- powers the Streamlit whale/concentration view with a warehouse-built concentration mart

### `PANTHER_DB.ANALYTICS.PLATFORM_DAILY_SUMMARY`

Grain:

- one row per trading day

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_MARKET_DAILY`
- `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`

Purpose:

- precomputed day-level platform summary mart
- avoids live aggregation of lower-level daily facts in Streamlit
- powers the Streamlit daily-volume time-series question

### `PANTHER_DB.ANALYTICS.TRADER_SEGMENT_SNAPSHOT`

Grain:

- one row per trader snapshot

Primary sources:

- `PANTHER_DB.ANALYTICS.DIM_TRADERS`

Purpose:

- assigns percentile buckets for total volume, total trades, diversification, and average trade size
- makes trader segmentation presentation-ready
- supports the Streamlit `What Separates Big Traders From Small Traders?` question

### `PANTHER_DB.ANALYTICS.TRADER_COHORT_MONTHLY`

Grain:

- one row per trader cohort month and activity month

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_WALLET_DAILY`
- `PANTHER_DB.ANALYTICS.DIM_TRADERS`

Purpose:

- compresses wallet history into cohort/lifecycle analytics
- supports retention-style and lifecycle-style views without scanning raw trade history
- powers the Streamlit trader cohort view

### `PANTHER_DB.ANALYTICS.MARKET_THEME_DAILY`

Grain:

- one row per trade day and normalized market theme

Primary sources:

- `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
- `PANTHER_DB.ANALYTICS.DIM_MARKETS`

Purpose:

- groups markets into a reusable “bet theme” layer using PANTHER metadata
- uses `GROUP_ITEM_TITLE` first and event title as a fallback, so the theme is source-driven rather than keyword-driven
- supports analytics on what kinds of bets attract the most dollars or the broadest participation
- powers the Streamlit `Top Bet Themes Today` and `Bet Themes With Most Unique Traders Today` views

## What questions this layer can support

This v2 design is broader than the current Streamlit UI. It can support questions such as:

- Which markets have the most volume today?
- Which markets are highly concentrated among a few large traders?
- Which markets have the highest listed lifetime volume?
- How much money was traded across the platform each day?
- Which bet themes have the most traded volume today?
- Which bet themes attract the broadest trader participation?
- Which wallets trade the most across days?
- What separates high-volume traders from the rest of the population?
- How do trader cohorts behave after first activity month?
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

The later marts added for concentration, platform summary, trader segmentation, trader cohorts, and market themes are especially useful for presentation because they show a mature analytics pattern:

- lower-level facts capture reusable event or aggregate grains
- higher-level marts precompute story-ready questions for the dashboard
- the app reads marts instead of repeatedly aggregating large lower-level tables live

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

## Incremental build design decision

The original analytics-layer build used a full rebuild strategy.

That approach was correct but expensive because it repeatedly recomputed large COYOTE-derived facts from hundreds of millions of user-activity rows, even when only a small amount of new data had arrived.

To improve this, the build script was updated to support an incremental mode driven by `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY._LOADED_AT`.

The incremental strategy now does the following:

- reads only newly loaded COYOTE trade rows since the previous successful build watermark
- merges those rows into `PANTHER_DB.ANALYTICS.FACT_USER_ACTIVITY_TRADES`
- identifies the impacted trade dates, market conditions, and wallets
- recomputes only the downstream slices affected by that delta
- leaves unchanged portions of the analytics layer untouched

This is an important analytics architecture decision for the project because it demonstrates that the warehouse layer is not just storing data; it is also managing change efficiently at scale.

## Incremental benchmark result

### Full rebuild baseline

Reference full rebuild runtime:

- total runtime: `3,682.8s`
- total runtime: `61m 22.8s`

Largest steps in the full rebuild:

- `FACT_MARKET_DAILY`: `1,659.3s`
- `FACT_USER_ACTIVITY_TRADES`: `889.9s`
- `MARKET_TOP_TRADERS_DAILY`: `422.9s`
- `MARKET_TOP_TRADERS_ALL_TIME`: `333.4s`
- `FACT_WALLET_DAILY`: `214.2s`

### Incremental rebuild result

Validated incremental run:

- COYOTE trade delta: `114,066` rows
- delta as share of current trade fact: `0.0152%`
- impacted trade dates: `43`
- impacted market conditions: `3,603`
- impacted wallets: `108`

Incremental runtime by step:

- `FACT_USER_ACTIVITY_TRADES` merge: `72.9s`
- `FACT_MARKET_DAILY` replace: `366.9s`
- `FACT_WALLET_DAILY` replace: `68.9s`
- `DIM_TRADERS` replace: `42.0s`
- `TRACKED_MARKET_VOLUME_DAILY` replace: `12.2s`
- `MARKET_TOP_TRADERS_DAILY` replace: `152.0s`
- `MARKET_TOP_TRADERS_ALL_TIME` replace: `73.6s`
- `HIGHEST_VOLUME_MARKETS` rebuild: `2.9s`

Incremental total runtime:

- total runtime: `810.9s`
- total runtime: `13m 30.9s`

### Measured improvement

Compared with the full rebuild baseline:

- time saved: `2,871.9s`
- time saved: `47m 51.9s`
- speedup: `4.54x`
- runtime reduction: `77.98%`

This is the current headline performance result for the project presentation: the same analytics layer now completes in about `13.5` minutes instead of about `61.4` minutes when only a tiny fraction of source trade data has changed.

## What the benchmark tells us

The incremental system is now functional end to end.

Evidence:

- the build enters incremental mode successfully
- it detects the COYOTE delta correctly
- it updates all downstream analytics tables without failing
- it records a meaningful runtime reduction relative to the full rebuild

The remaining bottlenecks are still concentrated in a small number of downstream aggregations:

- `FACT_MARKET_DAILY`
- `MARKET_TOP_TRADERS_DAILY`

That means the next optimization work should focus on reducing recomputation inside those two tables, especially for date-scoped aggregations.

## Presentation takeaway

For a data management at scale course, the main message is:

- `PANTHER`, `COYOTE`, and `DOG` act as source systems
- `PANTHER_DB.ANALYTICS` is the derived warehouse analytics layer
- Snowpark performs large-scale transformations inside Snowflake
- Streamlit is only the presentation layer
- incremental warehouse builds materially improve performance without increasing warehouse size

This gives a defensible story about both architecture and scalability, not just about dashboard output.
