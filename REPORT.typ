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

For each market, Gamma returns market and condition IDs, question text, slug, start and end timestamps, category labels, liquidity and volume fields, images and icons, outcome labels and prices, resolution sources, nested token information (needed to connect markets to tradable assets) and more.

Gamma is fully public and read-only. No authentication, API key, or wallet is required to call Gamma endpoints, so any public user can access it for research, dashboards, or analytics. 

Rate limiting does exist and is enforced through Cloudflare throttling on sliding windows. Gamma has a general limit of `4,000` requests per `10` seconds, with stricter per-endpoint limits such as `300` requests per `10` seconds for `/markets` and `500` requests per `10` seconds for `/events`. These are fairly simple to work around by being conservative with fetching sequentially, using a brief sleep in between calls and retry logic.

== Polymarket Data API and bulk user activity


== Live CLOB websocket feed



= Data Batch Ingestion and Snowflake Storage
Figure 1 summarizes the Airflow workflows that are actively used across the project. Two DAGs maintain market metadata in `PANTHER`, one batch DAG maintains user activity in `COYOTE`, and a separate nightly chain maintains the streaming-side derived tables in `DOG`. Those warehouse outputs then feed the downstream analytics layer and dashboard.

#figure(
  kind: "figure",
  supplement: [Figure],
  table(
    columns: (1fr, 0.3fr, 1fr, 0.3fr, 1fr),
    stroke: none,
    inset: 0pt,
    align: center,
    column-gutter: 8pt,
    row-gutter: 6pt,

    [#box(
      width: 2.0in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[Gamma ingestion] \
        `gamma_markets_daily` (hourly) \
        `gamma_markets_catchup` (manual)
      ]
    ]],
    [],
    [#box(
      width: 2.0in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[User-activity ingestion] \
        `polymarket_activity_pipeline_dag`
      ]
    ]],
    [],
    [#box(
      width: 2.1in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[Streaming nightly DAGs] \
        `order_flow_enrichment` -> `order_flow_daily_agg` -> `anomaly_model_training` -> `anomaly_attribution`
      ]
    ]],

    [#align(center)[#text(size: 13pt)[↘]]],
    [],
    [#align(center)[#text(size: 13pt)[↓]]],
    [],
    [#align(center)[#text(size: 13pt)[↙]]],

    table.cell(colspan: 5)[#align(center)[#box(
      width: 5.2in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[Warehouse outputs] \
        `PANTHER_DB.CURATED.GAMMA_MARKETS`, `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY`, and `DOG_DB` derived tables
      ]
    ]]],

    table.cell(colspan: 5)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 5)[#align(center)[#box(
      width: 4.3in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[`analytics_layer_refresh`] \
        Build and incrementally refresh `PANTHER_DB.ANALYTICS`
      ]
    ]]],

    table.cell(colspan: 5)[#align(center)[#text(size: 13pt, weight: "bold")[↓]]],

    table.cell(colspan: 5)[#align(center)[#box(
      width: 3.4in,
      inset: 6pt,
      radius: 4pt,
      stroke: (paint: black, thickness: 0.8pt),
    )[
      #set par(justify: false)
      #set text(size: 8.2pt)
      #align(center)[
        #text(weight: "bold")[Streamlit application]
      ]
    ]]],
  ),
  caption: [Project-level Airflow orchestration overview. This figure shows how the active workflows across `PANTHER`, `COYOTE`, and `DOG` fit together before the analytics layer and dashboard.]
)

== Gamma market ingestion DAGs
We have two separate DAGs that are capable of ingesting data from the Gamma API into Snowflake. `gamma_markets_daily` is the ongoing scheduled ingestion job that runs hourly to keep the market dimension current and keep data fresh for downstream analytics and the Streamlit app. `gamma_markets_catchup` is a manually triggered recovery and backfill DAG that reconciles history from the current watermark forward, or from the beginning of available history when run in full-history mode. This split keeps the regular functioning path simple while still giving the project a reliable way to manually recover from missed runs, bootstrap a fresh environment, or backfill after a schema change without creating one oversized DAG.

The `gamma_markets_catchup` DAG was used to fetch all historical market data in bulk initially, and is now only used to manually catch up if the hourly scheduled DAG misses several runs (artifact of LinuxLab) or for backfilling if the schema of the data we are collecting changes. We do not have intentions of continually running the `gamma_markets_catchup` on any regular interval or schedule unless missed data proves to be an issue. The `gamma_markets_daily` DAG does not automatically trust that the last hourly run completed successfully, it still determines how far back to check for updates based on a watermark. Hence, occasionally skipped runs of `gamma_markets_daily` do not automatically caused missed data.

Both DAGs follow the same core data flow. They first ensure that the required Snowflake objects exist, then fetch market pages from the Gamma API for two source groups: active markets and recently closed markets within the requested window. Those JSON page files are written to disk, uploaded into the internal Snowflake stage `PANTHER_DB.RAW.GAMMA_MARKETS_STAGE`, and then loaded into the scratch table `PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE`. Then the pipeline merges the data into the raw history table `PANTHER_DB.RAW.GAMMA_MARKET_PAGES` and finally upserts the latest known version of each market into `PANTHER_DB.CURATED.GAMMA_MARKETS`. This design is intentionally idempotent. Rerunning a load does not blindly duplicate data, because the scratch table is reused each run and both the raw and curated layers are maintained through merge-based upserts and duplicate checks. In the hourly DAG, the curated publication step also emits the `snowflake://PANTHER_DB/CURATED/GAMMA_MARKETS` Airflow dataset event, which allows the downstream analytics DAG to refresh after curated market data has been successfully published.

The main difference between the two DAGs (not including trigger method) is how they define the ingestion window. `gamma_markets_daily` is watermark-driven. Its lower bound is usually `MAX(updated_at)` from `PANTHER_DB.CURATED.GAMMA_MARKETS`, where `updated_at` is Polymarket's own `updatedAt` value copied from the API payload (not the time we inserted the row into Snowflake or the time of the last successful Airflow run). Its upper bound is the Airflow interval end for scheduled runs. `gamma_markets_catchup` uses the same Polymarket `updatedAt` watermark as its lower bound unless full-history mode is requested, but its upper bound is the start of the current day, so it reconciles everything from the warehouse watermark through yesterday rather than just one recent interval. In both DAGs, active markets are always fetched and closed markets are only fetched for the selected window, which avoids re-scanning the entire closed-market history on every run. This improves efficiency through preventing unnecessary fetches.

If we carried this architecture into a true production environment, we would keep the split between `gamma_markets_daily` and `gamma_markets_catchup`, but we would keep the catchup DAG as an operational tool rather than a permanently scheduled second reconciliation path. The primary reason for this decision is that the hourly DAG is already watermark-driven. If a few hourly runs are missed, the next successful run does not simply continue from the previous Airflow success state. It will expand its lower bound from the warehouse watermark and therefore recovers many ordinary missed intervals automatically. Because of that design, `gamma_markets_catchup` would still be used manually in clear exception cases such as bootstrapping a fresh environment, backfilling after a schema change, or for reassurance purposes when recovering from a long-lasting outage in which the scheduled DAG missed a significant amount of runs.

We would not immediately schedule catchup as a standing reconciliation job just because missed data was theoretically possible. If we became concerned about silent data loss, we would define and measure actual operational metrics that tell us whether the hourly path is leaving gaps. The most useful signals would be freshness lag between `now` and `MAX(updated_at)` in `CURATED.GAMMA_MARKETS`, counts of consecutive failed or skipped hourly runs, unusual drops in fetched active or recently closed market counts, and periodic sample comparisons between live Gamma API results and the curated warehouse state. Only if those metrics were concerning would we start running a scheduled reconciliation job, such as a daily or weekly lookback catchup window. In this case, the production version would preserve the same logical architecture, but there would be a scheduled reconciliation job like `gamma_markets_catchup` that would be implemented based on evidence. 

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

In this market data ingestion process, each of the Snowflake tables serve a different purpose. `PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE` is the reusable load table for each run after files are uploaded into the internal Snowflake stage. `PANTHER_DB.RAW.GAMMA_MARKET_PAGES` is the persistent raw history table, which keeps one row per uploaded page version and preserves the full API payload as a `VARIANT` so reruns and backfills do not erase what Gamma returned earlier. `PANTHER_DB.CURATED.GAMMA_MARKETS` is the latest-state table, where the newest known version of each market is selected using Polymarket's `updatedAt` value and then `raw_loaded_at` as a tiebreaker. This gives the rest of the project one market table that is easy to query while still preserving the full raw history separately.


= Data Streaming Ingestion



= Analytics Layer

The analytics layer exists to separate the responsibilities of ingestion, warehouse transformations, and presentation. In this project, ingestion pipelines owned by different teammates land and curate source data in Snowflake, but those source tables are not ideal application inputs on their own. They are still not organized in a way that is convenient for useful and efficient analytics, and too expensive to transform live inside a dashboard. `PANTHER_DB.ANALYTICS` is the analytics layer schema that solves this problem by turning the source tables into reusable facts, dimensions, and presentation marts that sit between ingestion and the user interface.

That separation of concerns is a very important architectural decision. Instead of having Streamlit join very large source tables on demand, the warehouse performs the expensive joins, flattening, deduplication, and aggregation ahead of time using Snowpark. The same analytics layer tables can support various questions of interest and Streamlit queries those tables directly instead of rebuilding them on the fly based on user requests. This gives the user a much lower-latency experience.

#figure(
  image("analyticsdiagram.png", width: 95%),
  caption: [High-level analytics-layer architecture. The diagram shows how upstream `PANTHER`, `COYOTE`, and `DOG` sources feed reusable core analytics tables and then presentation marts, which are read by the Streamlit app instead of recomputing large joins live.]
)

== Source systems and modeling approach

The analytics layer uses three source systems. `PANTHER_DB.CURATED.GAMMA_MARKETS` is the source of truth for market metadata. `COYOTE_DB.PUBLIC.CURATED_POLYMARKET_USER_ACTIVITY` is the source of truth for user-level transaction activity. `DOG_DB` contributes additional raw data and derived tables when available.

`DIM_*` tables hold reusable dimensions, `FACT_*` tables hold event level tables, and `BRIDGE_*` tables connect grains such as markets and asset tokens. On top of those tables, the build also creates presentaiton marts such as `TRACKED_MARKET_VOLUME_DAILY`, `HIGHEST_VOLUME_MARKETS`, `MARKET_CONCENTRATION_DAILY`, and `TRADER_COHORT_MONTHLY`.

The analytics layer is built in Snowpark. The build script opens a Snowflake session, reads the source tables as Snowpark DataFrames, performs the joins and aggregations in Snowflake, and writes the results into `PANTHER_DB.ANALYTICS`. Snowpark uses Snowflake's computing engine directly instead of moving large amounts of data to external Spark clusters, while still having similar optimizations. For example, Snowpark still performs lazy evaluation so filters, joins, aggregations, and window functions are compiled into SQL only when the pipeline reaches an action such as `collect` or `save_as_table`. 


== Snowpark experience and tradeoffs

Snowpark ended up being a strong fit for this project for the reasons discussed above while also allowing us to write transformations in modular Python instead of one very large SQL script. The analytics build is organized as a Python program with reusable functions and table builders, while still pushing the heavy joins, window functions, and aggregations down into Snowflake compute.

Snowpark generally felt similar to Spark. The language is still python, both use DataFrames, the workflow of "load table, transform through DataFrame operations, materialize result" is the same, and the same common transformations apply (e.g. projection, filtering, joins, aggregations, window calculations).

Snowpark does however have its own DataFrame layer with its own APIs, function library, and execution environment, so it is not identical to Spark. The execution behind Snowpark also shows up as Snowflake queries rather than executor logs so the debugging process is a bit different.

== Core tables and downstream questions

`DIM_MARKETS` is the latest-state market dimension built from curated Gamma metadata. `FACT_USER_ACTIVITY_TRADES` is the main trade fact built from COYOTE user activity. `FACT_MARKET_DAILY` and `FACT_WALLET_DAILY` create day-level grains based on that activity, while `DIM_TRADERS` turns wallet history into a reusable trader profile.

On top of those tables, the layer builds marts that map directly to dashboard questions. `TRACKED_MARKET_VOLUME_DAILY` supports daily market rankings, `HIGHEST_VOLUME_MARKETS` supports listed-volume rankings, `MARKET_TOP_TRADERS_DAILY` and `MARKET_TOP_TRADERS_ALL_TIME` support market drill-down views, `MARKET_CONCENTRATION_DAILY` measures whale concentration, `PLATFORM_DAILY_SUMMARY` supports platform-level time series, `TRADER_SEGMENT_SNAPSHOT` supports trader segmentation, `TRADER_COHORT_MONTHLY` supports trader behavior analysis, and `MARKET_THEME_DAILY` supports theme-level questions.

== Orchestration and incremental refresh

The analytics layer is refreshed by the `analytics_layer_refresh` Airflow DAG and uses data aware scheduling. It subscribes to two Airflow dataset assets: curated Gamma markets and curated user activity. That means the analytics build runs when upstream data changes instead of relying on a fixed schedule or manual runs.

The DAG calls `build_analytics_layer.py`, which runs in `full` or `incremental` mode and then validates that the required analytics tables exist. The incremental path uses the COYOTE `_LOADED_AT` watermark to merge only new trade rows and recompute only the affected downstream slices. This keeps the build idempotent and avoids a full rebuild every time to improve performance (roughly 5x faster based on a few observations).


= Streamlit App and Visualization

The Streamlit app is the presentation layer for the warehouse data. It is organized around answering analysis questions rather than displaying raw tables, keeping the UI simple and intuitive. Again, the heavy joins and aggregations have already been done in `PANTHER_DB.ANALYTICS`.

Views such as “Most Popular Markets Today,” “Whale-Dominated Markets Today,” and “Who Are The Biggest Traders Overall?” each map to a clear analytical story.

== Main analytical views

The app supports market-centered views, trader-centered views, and time-series or profile views. The market views rely mainly on `TRACKED_MARKET_VOLUME_DAILY`, `FACT_MARKET_DAILY`, `HIGHEST_VOLUME_MARKETS`, and `MARKET_CONCENTRATION_DAILY`. The trader views rely mainly on `FACT_WALLET_DAILY` and `DIM_TRADERS`. The profile and lifecycle views rely on `PLATFORM_DAILY_SUMMARY`, `TRADER_SEGMENT_SNAPSHOT`, and `TRADER_COHORT_MONTHLY`.

The strongest views in the project are the ones that would be more difficult to compute directly from raw tables. `MARKET_CONCENTRATION_DAILY` is a good example because it turns whale activity into a clear market/day metric. `TRADER_SEGMENT_SNAPSHOT` and `TRADER_COHORT_MONTHLY` are also strong because they show that the warehouse can produce presentation-ready summaries rather than only rankings or lists.

== Data availability and observability page

The `Data Availability` page is aimed more at developers and reviewers than end users. It shows inventories of source and analytics tables across `DOG_DB`, `COYOTE_DB`, and `PANTHER_DB`, along with row counts, timestamps, storage information, column metadata, and example records. It also provides transparencies into how the analytics tables are built from the source tables.

This page matters because it makes the state of the platform visible. It shows users which tables are populated, which are empty, and what data is actually available and updated.

== Anomalies page


= Conclusion


= Future Work
