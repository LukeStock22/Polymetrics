#set document(title: "Polymetrics")
#set page(paper: "a4", margin: 1in, numbering: "1")
#set par(justify: true)

#align(center)[
  #text(size: 16pt, weight: "bold")[Polymetrics] \
  #v(4pt)
  #text(size: 12pt)[Analytic Platform for PolyMarket Data Visualizations and Anomaly Detection] \
  #v(8pt)
  Will Andelman, Luke Stockbridge, Andrew Baggio \
  #datetime.today().display("[day] [month repr:long] [year]")
]

#v(12pt)

= Abstract


= Problem and Motivation


= Data Sources


== Gamma REST API
The Gamma REST API is Polymarket's primary public API for discovery and market level data, served from `https://gamma-api.polymarket.com`.  The Gamma service sits on top of and continuously reads the underlying blockchain data and stores it in a more query-friendly form with useful metadata for organizing, browsing and filtering markets. The Polymarket documentation states that the Gamma API has endpoints for markets, events, tags series, comments, sports metadata, search, and public profiles. In this project the most important Gamma endpoint is `GET /markets`, which provides the market-level metadata that populates `PANTHER_DB.CURATED.GAMMA_MARKETS`.

For each market, Gamma returns market and condition IDs, question text, slug, start and end timestamps, category labels, liquidity and volume fields, images and icons, outcome labels and prices, resolution sources, and nested token information (needed to connect markets to tradable assets) and more.

Gamma is fully public and read-only. No authentication, API key, or wallet is required to call Gamma endpoints, so any public user can access it for research, dashboards, or analytics. 

Rate limiting does exist and is enforced through Cloudflare throttling on sliding windows. Gamma has a general limit of `4,000` requests per `10` seconds, with stricter per-endpoint limits such as `300` requests per `10` seconds for `/markets` and `500` requests per `10` seconds for `/events`. These are fairly simple to circumvent by being conservative with fetching sequentially with a brief sleep in between calls and retry logic.

== Polymarket Data API and bulk user activity


== Live CLOB websocket feed



= Data Batch Ingestion and Snowflake Storage

This is the first section where your own implementation should take center stage.

== Gamma market ingestion DAGs
We have two separate DAGs that are capable of ingesting data from the Gamma API into Snowflake. `gamma_markets_daily` is the ongoing scheduled ingestion job that runs hourly to keep the market dimension current and keep data fresh for downstream analytics and the Streamlit app. `gamma_markets_catchup` is a manually triggered recovery and backfill DAG that reconciles history from the current watermark forward, or from the beginning of available history when run in full-history mode. This split keeps the regular functioning path simple while still giving the project a reliable way to manually recover from missed runs, bootstrap a fresh environment, or backfill after a schema change without creating one oversized DAG.

The `gamma_markets_catchup` DAG was used to fetch all historical market data in bulk initially, and is now only used to manually catch up if the hourly scheduled DAG misses several runs (my Linuxlab isn't running constantly) or for backfilling if the schema of the data we are collecting changes. We do not have intentions of continually running the `gamma_markets_catchup` on any regular interval or schedule unless missed data proves to be an issue. The `gamma_markets_daily` DAG does not automatically trust that the last hourly run completed successfully, it still determines how far back to check for updates based on a watermark. Hence, occasionally skipped runs of `gamma_markets_daily` do not automatically caused missed data.

Both DAGs follow the same core data flow. They first ensure that the required Snowflake objects exist, then fetch market pages from the Gamma API for two source groups: active markets and recently closed markets within the requested window. Those JSON page files are written to disk, uploaded into the internal Snowflake stage `PANTHER_DB.RAW.GAMMA_MARKETS_STAGE`, and then loaded into the scratch table `PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE`. Then the pipeline merges the data into the raw history table `PANTHER_DB.RAW.GAMMA_MARKET_PAGES` and finally upserts the latest known version of each market into `PANTHER_DB.CURATED.GAMMA_MARKETS`. This design is intentionally idempotent. Rerunning a load does not blindly duplicate data, because the scratch table is reused each run and both the raw and curated layers are maintained through merge-based upserts and duplicate checks. In the hourly DAG, the curated publication step also emits the `snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS` Airflow dataset event, which allows the downstream analytics DAG to refresh after curated market data has been successfully published.

The main difference between the two DAGs is how they define the ingestion window. `gamma_markets_daily` is watermark-driven. Its lower bound is usually `MAX(updated_at)` from `PANTHER_DB.CURATED.GAMMA_MARKETS`, where `updated_at` is Polymarket's own `updatedAt` value copied from the API payload (not the time we inserted the row into Snowflake or the time of the last successful Airflow run). Its upper bound is the Airflow interval end for scheduled runs or `now` for manual runs. `gamma_markets_catchup` uses the same Polymarket `updatedAt` watermark as its lower bound unless full-history mode is requested, but its upper bound is the start of the current day, so it reconciles everything from the warehouse watermark through yesterday rather than just one recent interval. In both DAGs, active markets are always fetched and closed markets are only fetched for the selected window, which avoids re-scanning the entire closed-market history on every run. This improves efficiency through preventing unnecessary fetches.

The figures below summarize the task dependencies in both DAGs and the shared raw-to-curated data path they use inside `PANTHER_DB`.

#figure(
  kind: "figure",
  supplement: [Figure],
  table(
    columns: (1fr, 0.25fr, 1fr),
    stroke: none,
    inset: 0pt,
    align: center,
    column-gutter: 8pt,
    row-gutter: 6pt,

    [#box(
      width: 2.25in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`compute_window`] \
      Hourly interval or manual run plus curated `updated_at` watermark
      ]
    ]],
    [],
    [#box(
      width: 2.25in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`ensure_snowflake_objects`] \
      Create schemas, stage, and target tables if missing
      ]
    ]],

    [#align(center)[#text(size: 13pt)[↘]]],
    [],
    [#align(center)[#text(size: 13pt)[↙]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 4.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`fetch_market_pages`] \
      Fetch active markets and recently closed markets inside the requested window
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`upload_market_pages`] \
      Upload local JSON page files into the internal Snowflake stage
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 4.4in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`load_raw_market_pages`] \
      Copy staged files into the scratch table and merge raw page history
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 4.6in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`upsert_curated_markets`] \
      Merge latest market records into `CURATED.GAMMA_MARKETS` and publish the curated dataset event
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 3.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`check_stage_rows`] \
      Verify the scratch stage table is not empty
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 3.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`check_raw_dupes`] \
      Verify there are no duplicate raw page versions
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 3.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`check_curated_dupes`] \
      Verify there is only one curated row per `market_id`
      ]
    ]]],

    table.cell(colspan: 3)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 3)[#align(center)[#box(
      width: 3.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`check_freshness`] \
      Verify `max(updated_at)` covers the requested daily window
      ]
    ]]],
  ),
  caption: [Task flow for `gamma_markets_daily`. The DAG combines a window-calculation task and a Snowflake-bootstrap task before fetching market pages, then moves through stage upload, raw-page loading, curated upsert, and a final sequence of data-quality checks.]
)

#figure(
  kind: "figure",
  supplement: [Figure],
  table(
    columns: 1,
    stroke: none,
    inset: 0pt,
    align: center,
    row-gutter: 6pt,

    [#box(
      width: 4.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`ensure_snowflake_objects`] \
      Create schemas, stage, and target tables if missing
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`compute_windows`] \
      Start from the warehouse watermark, or from full-history mode, and end at the start of the current day
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`process_window.expand`] \
      For each reconciliation window: fetch pages, upload stage files, merge raw page history, and upsert curated markets
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.8in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
      #text(weight: "bold")[`final_checks`] \
      Verify no raw or curated duplicates remain and that curated data is fresh through yesterday midnight
      ]
    ]],
  ),
  caption: [Task flow for `gamma_markets_catchup`. Compared with the daily DAG, the catchup DAG collapses the fetch-load-upsert path into `process_window.expand` and finishes with one consolidated validation step.]
)

#figure(
  kind: "figure",
  supplement: [Figure],
  grid(
    columns: 1,
    row-gutter: 6pt,
    align: center,

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Gamma API] \
        `/markets`
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Local JSON page files]
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Internal Snowflake stage] \
        `RAW.GAMMA_MARKETS_STAGE`
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Scratch load table] \
        `RAW.GAMMA_MARKET_PAGES_STAGE`
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Raw page history] \
        `RAW.GAMMA_MARKET_PAGES`
      ]
    ]],

    [#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    [#box(
      width: 4.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.5pt)
      #align(center)[
        #text(weight: "bold")[Curated latest-state table] \
        `CURATED.GAMMA_MARKETS`
      ]
    ]],
  ),
  caption: [Shared raw-to-curated data path used by both Gamma ingestion DAGs inside `PANTHER_DB`. The pipeline preserves raw page history before materializing the latest known market record in the curated table.]
)

Basic data quality checks are built directly into the DAG flow rather than treated as an afterthought. The daily DAG verifies that the stage table is populated, checks for duplicate rows in both the raw and curated layers, and confirms that the newest `updated_at` value in the curated table is at least as recent as the start of the requested window. The catchup DAG performs similar duplicate checks and validates that the curated table is fresh through the prior day after a recovery run completes. Together, these checks ensure we aren't just moving JSON into Snowflake but also preserve freshness, avoid duplicates, and maintain a clean market table that maintains the latest state for downstream use.

== Snowflake table design
The Snowflake design is split into a scratch load table, a persistent raw history table, and a curated latest-state table. `PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE` is the staging table used during each load after the JSON files are uploaded into the internal Snowflake stage. It is truncated and reused each run. `PANTHER_DB.RAW.GAMMA_MARKET_PAGES` is the persistent raw history table. It stores one row per uploaded JSON page version and keeps the full API payload as a `VARIANT`, which matters because Gamma data can change over time and reruns or backfills should not destroy the record of what the API returned.

`PANTHER_DB.CURATED.GAMMA_MARKETS` serves a different purpose. It keeps the newest known version of each market keyed by `market_id`, using Polymarket's `updatedAt` value and then `raw_loaded_at` to decide which record is the latest. It also exposes the most important top-level fields as typed columns for analytics while still preserving the original `market_payload`. This gives the rest of the project a market table that is easy to query without losing access to the full source record. `PANTHER_DB.CURATED.GAMMA_MARKETS` is the market dimension for downstream use.


= Data Streaming Ingestion



= Analytics Layer

== Why the analytics layer exists

The analytics layer exists to separate three responsibilities that would otherwise become tightly coupled and difficult to scale: ingestion, modeling, and presentation. In this project, ingestion pipelines owned by different teammates land and curate source data in Snowflake, but those source tables are not ideal application inputs on their own. They are still too close to raw system structure, too heterogeneous across databases, and too expensive to re-aggregate live inside a dashboard. `PANTHER_DB.ANALYTICS` was introduced to solve that problem by turning the source tables into reusable facts, dimensions, and presentation marts that sit between ETL and the user interface.

That separation of concerns is one of the strongest architectural decisions in the project. Instead of asking Streamlit to join very large source tables on demand, the warehouse performs the expensive joins, flattening, deduplication, and aggregation ahead of time using Snowpark. The result is a system where business logic lives in the warehouse, not in the UI, and where the same modeled tables can support many questions at different grains. For a data management at scale project, this is a much stronger design than a thin dashboard that queries raw or lightly curated tables directly.

== Source systems and modeling approach

The analytics layer follows a clear source-of-truth model across the three team-owned Snowflake databases. `PANTHER_DB.CURATED.GAMMA_MARKETS` is the authoritative source for market metadata, including condition IDs, labels, status flags, liquidity, listed volume, and the token arrays needed to map markets to tradable assets. `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY` is treated as the authoritative user-transaction feed and supplies the wallet-level trade history used for market-volume, top-trader, and cohort analytics. `DOG_DB` contributes supplemental raw feeds and teammate-owned enrichments such as data-api trades, leaderboard snapshots, and anomaly-oriented tables when those sources are populated.

To keep the schema understandable, the analytics layer uses a simple naming pattern. `DIM_*` tables hold reusable dimensions such as markets and traders. `FACT_*` tables capture trade-level or aggregate-level facts. `BRIDGE_*` tables resolve mapping problems between grains, such as linking markets to asset tokens. On top of those reusable warehouse tables, the build also creates app-facing marts such as `TRACKED_MARKET_VOLUME_DAILY`, `HIGHEST_VOLUME_MARKETS`, `MARKET_CONCENTRATION_DAILY`, and `TRADER_COHORT_MONTHLY`. This layered design is important because it means the dashboard can stay simple without reducing the warehouse to a collection of one-off UI extracts.

Technically, the layer is implemented in Snowpark rather than by pulling warehouse data into local Python memory. The build script opens a Snowflake session, reads upstream tables as Snowpark DataFrames, performs the necessary transformations inside Snowflake, and writes the results back into `PANTHER_DB.ANALYTICS`. That keeps computation close to the data and makes the warehouse, rather than the Streamlit app, the main execution environment for analytical logic.

== Core tables and downstream questions

Several tables are especially important because they show how the warehouse progresses from general-purpose modeling to presentation-ready analytics. `DIM_MARKETS` is the canonical latest-state market dimension built from curated Gamma metadata, and it gives downstream tables stable market labels, status fields, and identifiers. `FACT_USER_ACTIVITY_TRADES` is the authoritative trade fact derived from COYOTE user activity and acts as the base for most wallet and market analytics. From there, `FACT_MARKET_DAILY` and `FACT_WALLET_DAILY` move the data to reusable day-level grains, while `DIM_TRADERS` compresses wallet history into a more interpretable trader profile with volume, activity, diversification, and optional risk-related features.

On top of those core facts and dimensions, the layer materializes higher-level marts that map directly to application questions. `TRACKED_MARKET_VOLUME_DAILY` supports “most popular markets today,” `HIGHEST_VOLUME_MARKETS` supports listed-volume rankings, and `MARKET_TOP_TRADERS_DAILY` and `MARKET_TOP_TRADERS_ALL_TIME` enable market drill-down views. `MARKET_CONCENTRATION_DAILY` extends the analysis by calculating top-1 and top-5 share-of-volume metrics, which makes the “whale-dominated markets” story much stronger than a simple large-trade ranking. `PLATFORM_DAILY_SUMMARY` supports platform-level trend views, `TRADER_SEGMENT_SNAPSHOT` makes trader segmentation presentation-ready, `TRADER_COHORT_MONTHLY` turns wallet history into lifecycle analytics, and `MARKET_THEME_DAILY` groups markets into reusable thematic categories.

What makes these tables useful in the report is that each one corresponds to a distinct analytical grain and therefore to a different class of question. Some questions are naturally market-by-day questions, others are wallet-by-day or wallet-all-time questions, and still others are cohort or theme questions. The analytics layer makes those grains explicit instead of forcing the application to rediscover them at runtime. A lineage diagram and a short summary table of grain, source, and question type would fit very naturally in this subsection.

== Orchestration and incremental refresh

The analytics layer is refreshed by the `analytics_layer_refresh` Airflow DAG, which subscribes to two logical dataset assets instead of relying on a fixed time-based cron alone. Those assets are the curated Gamma market table and the curated user-activity table: `snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS` and `snowflake://COYOTE_DB/PUBLIC/CURATED_POLYMARKET_USER_ACTIVITY`. The Gamma daily DAG emits the first asset when it successfully publishes curated markets, and the upstream COYOTE pipeline emits the second when user activity is refreshed. This makes the analytics layer data-aware: it runs when the input data changes, not just when the clock says it should.

Inside that DAG, the actual build is delegated to `build_analytics_layer.py`, which runs in either `full` or `incremental` mode and then validates that the required analytics tables and build-state metadata exist. The incremental path is one of the most important technical improvements in the project. Rather than fully rebuilding the warehouse every time, the build uses the `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY._LOADED_AT` watermark to identify newly loaded rows, merge only the delta into the trade fact, and recompute only the affected downstream slices. That design keeps the build idempotent while dramatically reducing unnecessary recomputation.

The measured effect is large enough to be worth highlighting in the paper. The project documentation reports that the same analytics layer can complete in about `13.5` minutes instead of about `61.4` minutes when only a tiny fraction of source trade data has changed, a runtime reduction of roughly `78%`. For a class focused on data systems at scale, that improvement strengthens the argument that the analytics layer is not merely a semantic convenience. It is also an orchestration and performance engineering contribution.

== Why this is a strong class project contribution

This part of the project maps directly onto several grading criteria at once. From a data-quality perspective, it formalizes source-of-truth rules across multiple databases and converts heterogeneous upstream data into clearly named warehouse tables with stable grains. From a code-quality perspective, it centralizes the analytical logic into a dedicated build process rather than scattering joins and aggregations across scripts or UI callbacks. From a scalability perspective, it pushes computation into Snowflake and supports incremental refresh instead of requiring repeated full recomputation.

It is also a strong contribution because it gives the entire project a coherent architecture story. Without this layer, the project would be closer to a collection of ingestion pipelines plus a dashboard. With it, the system becomes a true warehouse-backed analytics platform: curated operational data is published first, a semantic analytics layer materializes reusable business tables second, and the application reads those tables last. That is a much more defensible design for both the final report and the course’s emphasis on data management at scale.

= Streamlit App and Visualization

== Application purpose and design

The Streamlit application is the presentation layer for the warehouse and is the part of the project where the data model becomes directly visible to an end user. Its purpose is not just to display tables, but to expose a set of analysis questions that can be answered quickly using precomputed warehouse outputs. This is an important distinction. A dashboard built directly on raw tables often becomes a thin SQL client with a nicer layout, whereas this app is intentionally designed around the idea that the warehouse has already done the hard modeling work.

That design shows up most clearly in the question-driven layout. Instead of forcing the reader to navigate schemas or choose arbitrary joins, the app organizes the analysis around prompts such as “Most Popular Markets Today,” “Whale-Dominated Markets Today,” “Who Are The Biggest Traders Overall?,” and “Wallet Lifecycle / Trader Cohorts.” This makes the interface much easier to present in a final demo because each screen corresponds to a concrete analytical story. Under the hood, those screens read materialized analytics tables in `PANTHER_DB.ANALYTICS` rather than recomputing large joins from curated source tables on every interaction.

== Main analytical views

The app currently supports several families of analysis that align closely with the warehouse design. The market-centered views focus on questions such as which markets are most active, which attract the most unique traders, which have the largest average bets, and which are dominated by a small number of large traders. These views are backed primarily by `TRACKED_MARKET_VOLUME_DAILY`, `FACT_MARKET_DAILY`, `HIGHEST_VOLUME_MARKETS`, and `MARKET_CONCENTRATION_DAILY`, so they demonstrate day-level aggregation, market ranking, and concentration analysis at warehouse scale.

The trader-centered views shift the focus from markets to participants. Questions such as top traders today, most active traders, largest traders by average trade size, and most diversified traders are supported by `FACT_WALLET_DAILY` and `DIM_TRADERS`. These are useful because they show that the warehouse is not only organized around markets; it can also compress large wallet histories into reusable trader profiles. The more advanced profile and lifecycle views extend this further by using `PLATFORM_DAILY_SUMMARY`, `TRADER_SEGMENT_SNAPSHOT`, and `TRADER_COHORT_MONTHLY` to support time-series summaries, trader segmentation, and cohort-style analysis over long activity histories.

From a report perspective, the strongest views to highlight are the ones that would be difficult to compute interactively from raw data alone. `MARKET_CONCENTRATION_DAILY` is a good example because it turns a vague “whale activity” idea into a clearly defined market/day metric. `TRADER_SEGMENT_SNAPSHOT` and `TRADER_COHORT_MONTHLY` are also strong examples because they show that the warehouse can produce presentation-ready behavioral summaries rather than only raw rankings or transaction listings.

== Data availability and observability page

One particularly useful addition to the app is the `Data Availability` page. Unlike the analysis pages, this view is aimed at developers and project reviewers rather than end users. It surfaces inventories of source and analytics tables across `DOG_DB`, `COYOTE_DB`, and `PANTHER_DB`, along with row counts, timestamps, storage information, column metadata, and example records. That makes it much easier to verify what data is actually present at demo time and to distinguish between tables that are structurally defined and tables that are already populated with usable data.

This page is worth emphasizing in the paper because it strengthens the project’s story around reproducibility and transparency. It shows that the team was not only interested in producing visualizations, but also in making the state of the data platform inspectable. For a systems-oriented course project, that is a useful sign of maturity: observability is treated as part of the product, not as a separate debugging concern.

== Visualization strategy

For the final paper, this section should include screenshots that illustrate both breadth and depth. A strong set would be one market-focused screenshot such as highest-volume or whale-dominated markets, one trader-focused screenshot such as the trader profile or trader segmentation view, one lifecycle-oriented screenshot showing the cohort analysis, and one screenshot of the `Data Availability` page. Together, those images would show that the app is not a single static chart, but a front end for multiple analytical grains built on the same warehouse foundation.

The key visualization argument is that the app is valuable because it makes modeled warehouse data explorable. The visuals are therefore most persuasive when paired with a short explanation of which analytics table is powering each screen. That connection between interface and warehouse design is one of the strongest themes running through the entire project.



= Conclusion



= Future Work
