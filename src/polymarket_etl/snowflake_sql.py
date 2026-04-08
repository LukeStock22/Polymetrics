from __future__ import annotations

from textwrap import dedent


def quote_identifier(name: str) -> str:
    safe = name.strip()
    if not safe:
        raise ValueError("Snowflake identifier cannot be empty")
    return '"' + safe.replace('"', '""') + '"'


def fq_name(*parts: str) -> str:
    return ".".join(quote_identifier(part) for part in parts)


def bootstrap_sql(database: str) -> str:
    raw_schema = fq_name(database, "RAW")
    curated_schema = fq_name(database, "CURATED")
    raw_stage = fq_name(database, "RAW", "GAMMA_MARKETS_STAGE")
    raw_format = fq_name(database, "RAW", "JSON_ARRAY_FORMAT")
    stage_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES_STAGE")
    raw_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES")
    curated_table = fq_name(database, "CURATED", "GAMMA_MARKETS")
    curated_hash = "market_payload_hash"
    processed_at = "processed_at"

    return dedent(
        f"""
        CREATE SCHEMA IF NOT EXISTS {raw_schema};
        CREATE SCHEMA IF NOT EXISTS {curated_schema};

        CREATE FILE FORMAT IF NOT EXISTS {raw_format}
          TYPE = JSON
          STRIP_OUTER_ARRAY = FALSE
          COMPRESSION = AUTO;

        CREATE STAGE IF NOT EXISTS {raw_stage}
          FILE_FORMAT = (FORMAT_NAME = {raw_format});

        CREATE TABLE IF NOT EXISTS {stage_table} (
          source_file_name STRING NOT NULL,
          page_name STRING NOT NULL,
          page_limit NUMBER(18,0) NOT NULL,
          page_offset NUMBER(18,0) NOT NULL,
          source_is_active BOOLEAN,
          source_is_closed BOOLEAN,
          expected_row_count NUMBER(18,0),
          expected_md5_hex STRING,
          file_content_key STRING NOT NULL,
          file_last_modified TIMESTAMP_NTZ,
          payload VARIANT NOT NULL,
          staged_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        );

        CREATE TABLE IF NOT EXISTS {raw_table} (
          source_file_name STRING NOT NULL,
          page_name STRING NOT NULL,
          page_limit NUMBER(18,0) NOT NULL,
          page_offset NUMBER(18,0) NOT NULL,
          source_is_active BOOLEAN,
          source_is_closed BOOLEAN,
          expected_row_count NUMBER(18,0),
          expected_md5_hex STRING,
          file_content_key STRING NOT NULL,
          file_last_modified TIMESTAMP_NTZ,
          payload VARIANT NOT NULL,
          staged_at TIMESTAMP_NTZ NOT NULL,
          raw_loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
          CONSTRAINT gamma_market_pages_uk UNIQUE (source_file_name, file_content_key)
        );

        CREATE TABLE IF NOT EXISTS {curated_table} (
          market_id STRING NOT NULL,
          api_id STRING,
          condition_id STRING,
          question STRING,
          slug STRING,
          resolution_source STRING,
          description STRING,
          start_date TIMESTAMP_NTZ,
          end_date TIMESTAMP_NTZ,
          created_at TIMESTAMP_NTZ,
          updated_at TIMESTAMP_NTZ,
          end_date_iso STRING,
          start_date_iso STRING,
          has_reviewed_dates BOOLEAN,
          image STRING,
          icon STRING,
          active BOOLEAN,
          closed BOOLEAN,
          is_new BOOLEAN,
          archived BOOLEAN,
          featured BOOLEAN,
          restricted BOOLEAN,
          approved BOOLEAN,
          accepting_orders BOOLEAN,
          accepting_orders_timestamp TIMESTAMP_NTZ,
          ready BOOLEAN,
          funded BOOLEAN,
          enable_order_book BOOLEAN,
          automatically_active BOOLEAN,
          order_price_min_tick_size NUMBER(18,6),
          order_min_size NUMBER(18,6),
          liquidity_text STRING,
          liquidity NUMBER(38,12),
          liquidity_clob NUMBER(38,12),
          liquidity_num NUMBER(38,12),
          volume_text STRING,
          volume NUMBER(38,12),
          volume_clob NUMBER(38,12),
          volume_24hr NUMBER(38,12),
          volume_24hr_clob NUMBER(38,12),
          volume_1wk NUMBER(38,12),
          volume_1wk_clob NUMBER(38,12),
          volume_1mo NUMBER(38,12),
          volume_1mo_clob NUMBER(38,12),
          volume_1yr NUMBER(38,12),
          volume_1yr_clob NUMBER(38,12),
          volume_num NUMBER(38,12),
          best_bid NUMBER(18,6),
          best_ask NUMBER(18,6),
          last_trade_price NUMBER(18,6),
          spread NUMBER(18,6),
          one_hour_price_change NUMBER(18,6),
          one_day_price_change NUMBER(18,6),
          one_week_price_change NUMBER(18,6),
          one_month_price_change NUMBER(18,6),
          one_year_price_change NUMBER(18,6),
          rewards_min_size NUMBER(18,6),
          rewards_max_spread NUMBER(18,6),
          uma_bond NUMBER(18,6),
          uma_reward NUMBER(18,6),
          cyom BOOLEAN,
          competitive NUMBER(18,6),
          outcomes VARIANT,
          outcome_prices VARIANT,
          clob_token_ids VARIANT,
          clob_rewards VARIANT,
          events VARIANT,
          uma_resolution_statuses VARIANT,
          market_maker_address STRING,
          submitted_by STRING,
          resolved_by STRING,
          group_item_title STRING,
          group_item_threshold STRING,
          question_id STRING,
          neg_risk BOOLEAN,
          neg_risk_other BOOLEAN,
          neg_risk_market_id STRING,
          neg_risk_request_id STRING,
          custom_liveness NUMBER(18,6),
          pager_duty_notification_enabled BOOLEAN,
          fees_enabled BOOLEAN,
          fee_type STRING,
          holding_rewards_enabled BOOLEAN,
          rfq_enabled BOOLEAN,
          manual_activation BOOLEAN,
          clear_book_on_start BOOLEAN,
          show_gmp_series BOOLEAN,
          show_gmp_outcome BOOLEAN,
          series_color STRING,
          pending_deployment BOOLEAN,
          deploying BOOLEAN,
          deploying_timestamp TIMESTAMP_NTZ,
          requires_translation BOOLEAN,
          {curated_hash} STRING,
          market_payload VARIANT NOT NULL,
          source_file_name STRING NOT NULL,
          source_page_name STRING NOT NULL,
          source_page_limit NUMBER(18,0) NOT NULL,
          source_page_offset NUMBER(18,0) NOT NULL,
          raw_loaded_at TIMESTAMP_NTZ NOT NULL,
          {processed_at} TIMESTAMP_NTZ,
          transformed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
          CONSTRAINT gamma_markets_pk PRIMARY KEY (market_id)
        );

        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS {curated_hash} STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS {processed_at} TIMESTAMP_NTZ;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS api_id STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS end_date_iso STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS start_date_iso STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS has_reviewed_dates BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS is_new BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS automatically_active BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS liquidity_text STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_text STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS liquidity_num NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_num NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_24hr_clob NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_1wk_clob NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_1mo_clob NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS volume_1yr_clob NUMBER(38,12);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS rewards_min_size NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS rewards_max_spread NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS uma_bond NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS uma_reward NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS cyom BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS competitive NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS neg_risk_other BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS pending_deployment BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS deploying BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS deploying_timestamp TIMESTAMP_NTZ;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS requires_translation BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS accepting_orders_timestamp TIMESTAMP_NTZ;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS market_maker_address STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS submitted_by STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS resolved_by STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS group_item_title STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS group_item_threshold STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS question_id STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS neg_risk BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS neg_risk_market_id STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS neg_risk_request_id STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS custom_liveness NUMBER(18,6);
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS pager_duty_notification_enabled BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS fees_enabled BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS fee_type STRING;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS holding_rewards_enabled BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS rfq_enabled BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS manual_activation BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS clear_book_on_start BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS show_gmp_series BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS show_gmp_outcome BOOLEAN;
        ALTER TABLE {curated_table}
          ADD COLUMN IF NOT EXISTS series_color STRING;
        """
    ).strip()


def truncate_stage_table_sql(database: str) -> str:
    return f"TRUNCATE TABLE {fq_name(database, 'RAW', 'GAMMA_MARKET_PAGES_STAGE')};"


def copy_market_pages_into_stage_sql(database: str, manifest_rows_sql: str) -> str:
    stage_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES_STAGE")
    stage_name = fq_name(database, "RAW", "GAMMA_MARKETS_STAGE")
    file_format = fq_name(database, "RAW", "JSON_ARRAY_FORMAT")
    file_format_arg = file_format.replace('"', "")
    return dedent(
        f"""
        INSERT INTO {stage_table}
          (
            source_file_name,
            page_name,
            page_limit,
            page_offset,
            source_is_active,
            source_is_closed,
            expected_row_count,
            expected_md5_hex,
            file_content_key,
            file_last_modified,
            payload
          )
        WITH manifest AS (
          {manifest_rows_sql}
        )
        SELECT
          manifest.source_file_name,
          manifest.page_name,
          manifest.page_limit,
          manifest.page_offset,
          manifest.source_is_active,
          manifest.source_is_closed,
          manifest.expected_row_count,
          manifest.expected_md5_hex,
          src.metadata$file_content_key,
          src.metadata$file_last_modified,
          src.$1
        FROM @{stage_name} (FILE_FORMAT => '{file_format_arg}') src
        INNER JOIN manifest
          ON src.metadata$filename = manifest.source_file_name;
        """
    ).strip()


def merge_raw_market_pages_sql(database: str) -> str:
    stage_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES_STAGE")
    raw_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES")
    return dedent(
        f"""
        MERGE INTO {raw_table} target
        USING {stage_table} src
          ON target.source_file_name = src.source_file_name
         AND target.file_content_key = src.file_content_key
        WHEN NOT MATCHED THEN INSERT (
          source_file_name,
          page_name,
          page_limit,
          page_offset,
          source_is_active,
          source_is_closed,
          expected_row_count,
          expected_md5_hex,
          file_content_key,
          file_last_modified,
          payload,
          staged_at,
          raw_loaded_at
        ) VALUES (
          src.source_file_name,
          src.page_name,
          src.page_limit,
          src.page_offset,
          src.source_is_active,
          src.source_is_closed,
          src.expected_row_count,
          src.expected_md5_hex,
          src.file_content_key,
          src.file_last_modified,
          src.payload,
          src.staged_at,
          CURRENT_TIMESTAMP()
        );
        """
    ).strip()


def merge_curated_markets_sql(database: str) -> str:
    raw_table = fq_name(database, "RAW", "GAMMA_MARKET_PAGES")
    curated_table = fq_name(database, "CURATED", "GAMMA_MARKETS")
    return dedent(
        f"""
        MERGE INTO {curated_table} target
        USING (
          WITH exploded AS (
            SELECT
              market.value:id::STRING AS market_id,
              market.value:id::STRING AS api_id,
              market.value:conditionId::STRING AS condition_id,
              market.value:question::STRING AS question,
              market.value:slug::STRING AS slug,
              market.value:resolutionSource::STRING AS resolution_source,
              market.value:description::STRING AS description,
              TRY_TO_TIMESTAMP_NTZ(market.value:startDate::STRING) AS start_date,
              TRY_TO_TIMESTAMP_NTZ(market.value:endDate::STRING) AS end_date,
              TRY_TO_TIMESTAMP_NTZ(market.value:createdAt::STRING) AS created_at,
              TRY_TO_TIMESTAMP_NTZ(market.value:updatedAt::STRING) AS updated_at,
              market.value:image::STRING AS image,
              market.value:icon::STRING AS icon,
              TRY_TO_BOOLEAN(market.value:active::STRING) AS active,
              TRY_TO_BOOLEAN(market.value:closed::STRING) AS closed,
              TRY_TO_BOOLEAN(market.value:new::STRING) AS is_new,
              TRY_TO_BOOLEAN(market.value:archived::STRING) AS archived,
              TRY_TO_BOOLEAN(market.value:featured::STRING) AS featured,
              TRY_TO_BOOLEAN(market.value:restricted::STRING) AS restricted,
              TRY_TO_BOOLEAN(market.value:approved::STRING) AS approved,
              TRY_TO_BOOLEAN(market.value:acceptingOrders::STRING) AS accepting_orders,
              TRY_TO_TIMESTAMP_NTZ(market.value:acceptingOrdersTimestamp::STRING) AS accepting_orders_timestamp,
              TRY_TO_BOOLEAN(market.value:ready::STRING) AS ready,
              TRY_TO_BOOLEAN(market.value:funded::STRING) AS funded,
              TRY_TO_BOOLEAN(market.value:enableOrderBook::STRING) AS enable_order_book,
              TRY_TO_BOOLEAN(market.value:automaticallyActive::STRING) AS automatically_active,
              TRY_TO_BOOLEAN(market.value:clearBookOnStart::STRING) AS clear_book_on_start,
              TRY_TO_BOOLEAN(market.value:manualActivation::STRING) AS manual_activation,
              TRY_TO_BOOLEAN(market.value:rfqEnabled::STRING) AS rfq_enabled,
              TRY_TO_BOOLEAN(market.value:showGmpSeries::STRING) AS show_gmp_series,
              TRY_TO_BOOLEAN(market.value:showGmpOutcome::STRING) AS show_gmp_outcome,
              TRY_TO_DECIMAL(COALESCE(market.value:orderPriceMinTickSize::STRING, NULL), 18, 6) AS order_price_min_tick_size,
              TRY_TO_DECIMAL(COALESCE(market.value:orderMinSize::STRING, NULL), 18, 6) AS order_min_size,
              market.value:liquidity::STRING AS liquidity_text,
              TRY_TO_DECIMAL(COALESCE(market.value:liquidityNum::STRING, market.value:liquidity::STRING), 38, 12) AS liquidity,
              TRY_TO_DECIMAL(COALESCE(market.value:liquidityClob::STRING, NULL), 38, 12) AS liquidity_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:liquidityNum::STRING, NULL), 38, 12) AS liquidity_num,
              market.value:volume::STRING AS volume_text,
              TRY_TO_DECIMAL(COALESCE(market.value:volumeNum::STRING, market.value:volume::STRING), 38, 12) AS volume,
              TRY_TO_DECIMAL(COALESCE(market.value:volumeClob::STRING, NULL), 38, 12) AS volume_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:volume24hr::STRING, NULL), 38, 12) AS volume_24hr,
              TRY_TO_DECIMAL(COALESCE(market.value:volume24hrClob::STRING, NULL), 38, 12) AS volume_24hr_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1wk::STRING, NULL), 38, 12) AS volume_1wk,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1wkClob::STRING, NULL), 38, 12) AS volume_1wk_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1mo::STRING, NULL), 38, 12) AS volume_1mo,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1moClob::STRING, NULL), 38, 12) AS volume_1mo_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1yr::STRING, NULL), 38, 12) AS volume_1yr,
              TRY_TO_DECIMAL(COALESCE(market.value:volume1yrClob::STRING, NULL), 38, 12) AS volume_1yr_clob,
              TRY_TO_DECIMAL(COALESCE(market.value:volumeNum::STRING, NULL), 38, 12) AS volume_num,
              TRY_TO_DECIMAL(COALESCE(market.value:bestBid::STRING, NULL), 18, 6) AS best_bid,
              TRY_TO_DECIMAL(COALESCE(market.value:bestAsk::STRING, NULL), 18, 6) AS best_ask,
              TRY_TO_DECIMAL(COALESCE(market.value:lastTradePrice::STRING, NULL), 18, 6) AS last_trade_price,
              TRY_TO_DECIMAL(COALESCE(market.value:spread::STRING, NULL), 18, 6) AS spread,
              TRY_TO_DECIMAL(COALESCE(market.value:oneHourPriceChange::STRING, NULL), 18, 6) AS one_hour_price_change,
              TRY_TO_DECIMAL(COALESCE(market.value:oneDayPriceChange::STRING, NULL), 18, 6) AS one_day_price_change,
              TRY_TO_DECIMAL(COALESCE(market.value:oneWeekPriceChange::STRING, NULL), 18, 6) AS one_week_price_change,
              TRY_TO_DECIMAL(COALESCE(market.value:oneMonthPriceChange::STRING, NULL), 18, 6) AS one_month_price_change,
              TRY_TO_DECIMAL(COALESCE(market.value:oneYearPriceChange::STRING, NULL), 18, 6) AS one_year_price_change,
              TRY_TO_DECIMAL(COALESCE(market.value:rewardsMinSize::STRING, NULL), 18, 6) AS rewards_min_size,
              TRY_TO_DECIMAL(COALESCE(market.value:rewardsMaxSpread::STRING, NULL), 18, 6) AS rewards_max_spread,
              TRY_TO_DECIMAL(COALESCE(market.value:umaBond::STRING, NULL), 18, 6) AS uma_bond,
              TRY_TO_DECIMAL(COALESCE(market.value:umaReward::STRING, NULL), 18, 6) AS uma_reward,
              TRY_TO_BOOLEAN(market.value:cyom::STRING) AS cyom,
              TRY_TO_DECIMAL(COALESCE(market.value:competitive::STRING, NULL), 18, 6) AS competitive,
              TRY_PARSE_JSON(market.value:outcomes::STRING) AS outcomes,
              TRY_PARSE_JSON(market.value:outcomePrices::STRING) AS outcome_prices,
              TRY_PARSE_JSON(market.value:clobTokenIds::STRING) AS clob_token_ids,
              market.value:clobRewards AS clob_rewards,
              market.value:events AS events,
              TRY_PARSE_JSON(market.value:umaResolutionStatuses::STRING) AS uma_resolution_statuses,
              market.value:marketMakerAddress::STRING AS market_maker_address,
              market.value:submitted_by::STRING AS submitted_by,
              market.value:resolvedBy::STRING AS resolved_by,
              market.value:groupItemTitle::STRING AS group_item_title,
              market.value:groupItemThreshold::STRING AS group_item_threshold,
              market.value:questionID::STRING AS question_id,
              TRY_TO_BOOLEAN(market.value:negRisk::STRING) AS neg_risk,
              TRY_TO_BOOLEAN(market.value:negRiskOther::STRING) AS neg_risk_other,
              market.value:negRiskMarketID::STRING AS neg_risk_market_id,
              market.value:negRiskRequestID::STRING AS neg_risk_request_id,
              TRY_TO_DECIMAL(COALESCE(market.value:customLiveness::STRING, NULL), 18, 6) AS custom_liveness,
              TRY_TO_BOOLEAN(market.value:pagerDutyNotificationEnabled::STRING) AS pager_duty_notification_enabled,
              TRY_TO_BOOLEAN(market.value:feesEnabled::STRING) AS fees_enabled,
              market.value:feeType::STRING AS fee_type,
              TRY_TO_BOOLEAN(market.value:holdingRewardsEnabled::STRING) AS holding_rewards_enabled,
              market.value:seriesColor::STRING AS series_color,
              market.value:endDateIso::STRING AS end_date_iso,
              market.value:startDateIso::STRING AS start_date_iso,
              TRY_TO_BOOLEAN(market.value:hasReviewedDates::STRING) AS has_reviewed_dates,
              TRY_TO_BOOLEAN(market.value:pendingDeployment::STRING) AS pending_deployment,
              TRY_TO_BOOLEAN(market.value:deploying::STRING) AS deploying,
              TRY_TO_TIMESTAMP_NTZ(market.value:deployingTimestamp::STRING) AS deploying_timestamp,
              TRY_TO_BOOLEAN(market.value:requiresTranslation::STRING) AS requires_translation,
              MD5_HEX(TO_JSON(market.value)) AS market_payload_hash,
              market.value AS market_payload,
              pages.source_file_name,
              pages.page_name AS source_page_name,
              pages.page_limit AS source_page_limit,
              pages.page_offset AS source_page_offset,
              pages.raw_loaded_at
            FROM {raw_table} pages,
            LATERAL FLATTEN(input => pages.payload) market
          ),
          ranked AS (
            SELECT
              *,
              ROW_NUMBER() OVER (
                PARTITION BY market_id
                ORDER BY COALESCE(updated_at, raw_loaded_at) DESC, raw_loaded_at DESC, source_file_name DESC
              ) AS rn
            FROM exploded
            WHERE market_id IS NOT NULL
          )
          SELECT *
          FROM ranked
          WHERE rn = 1
        ) src
          ON target.market_id = src.market_id
        WHEN MATCHED
          AND (
            COALESCE(src.updated_at, src.raw_loaded_at) > COALESCE(target.updated_at, target.raw_loaded_at)
            OR src.market_payload_hash <> target.market_payload_hash
          )
        THEN UPDATE SET
          api_id = src.api_id,
          condition_id = src.condition_id,
          question = src.question,
          slug = src.slug,
          resolution_source = src.resolution_source,
          description = src.description,
          start_date = src.start_date,
          end_date = src.end_date,
          created_at = src.created_at,
          updated_at = src.updated_at,
          end_date_iso = src.end_date_iso,
          start_date_iso = src.start_date_iso,
          has_reviewed_dates = src.has_reviewed_dates,
          image = src.image,
          icon = src.icon,
          active = src.active,
          closed = src.closed,
          is_new = src.is_new,
          archived = src.archived,
          featured = src.featured,
          restricted = src.restricted,
          approved = src.approved,
          accepting_orders = src.accepting_orders,
          accepting_orders_timestamp = src.accepting_orders_timestamp,
          ready = src.ready,
          funded = src.funded,
          enable_order_book = src.enable_order_book,
          automatically_active = src.automatically_active,
          clear_book_on_start = src.clear_book_on_start,
          manual_activation = src.manual_activation,
          rfq_enabled = src.rfq_enabled,
          show_gmp_series = src.show_gmp_series,
          show_gmp_outcome = src.show_gmp_outcome,
          order_price_min_tick_size = src.order_price_min_tick_size,
          order_min_size = src.order_min_size,
          liquidity_text = src.liquidity_text,
          liquidity = src.liquidity,
          liquidity_clob = src.liquidity_clob,
          liquidity_num = src.liquidity_num,
          volume_text = src.volume_text,
          volume = src.volume,
          volume_clob = src.volume_clob,
          volume_24hr = src.volume_24hr,
          volume_24hr_clob = src.volume_24hr_clob,
          volume_1wk = src.volume_1wk,
          volume_1wk_clob = src.volume_1wk_clob,
          volume_1mo = src.volume_1mo,
          volume_1mo_clob = src.volume_1mo_clob,
          volume_1yr = src.volume_1yr,
          volume_1yr_clob = src.volume_1yr_clob,
          volume_num = src.volume_num,
          best_bid = src.best_bid,
          best_ask = src.best_ask,
          last_trade_price = src.last_trade_price,
          spread = src.spread,
          one_hour_price_change = src.one_hour_price_change,
          one_day_price_change = src.one_day_price_change,
          one_week_price_change = src.one_week_price_change,
          one_month_price_change = src.one_month_price_change,
          one_year_price_change = src.one_year_price_change,
          rewards_min_size = src.rewards_min_size,
          rewards_max_spread = src.rewards_max_spread,
          uma_bond = src.uma_bond,
          uma_reward = src.uma_reward,
          cyom = src.cyom,
          competitive = src.competitive,
          outcomes = src.outcomes,
          outcome_prices = src.outcome_prices,
          clob_token_ids = src.clob_token_ids,
          clob_rewards = src.clob_rewards,
          events = src.events,
          uma_resolution_statuses = src.uma_resolution_statuses,
          market_maker_address = src.market_maker_address,
          submitted_by = src.submitted_by,
          resolved_by = src.resolved_by,
          group_item_title = src.group_item_title,
          group_item_threshold = src.group_item_threshold,
          question_id = src.question_id,
          neg_risk = src.neg_risk,
          neg_risk_other = src.neg_risk_other,
          neg_risk_market_id = src.neg_risk_market_id,
          neg_risk_request_id = src.neg_risk_request_id,
          custom_liveness = src.custom_liveness,
          pager_duty_notification_enabled = src.pager_duty_notification_enabled,
          fees_enabled = src.fees_enabled,
          fee_type = src.fee_type,
          holding_rewards_enabled = src.holding_rewards_enabled,
          series_color = src.series_color,
          pending_deployment = src.pending_deployment,
          deploying = src.deploying,
          deploying_timestamp = src.deploying_timestamp,
          requires_translation = src.requires_translation,
          market_payload_hash = src.market_payload_hash,
          market_payload = src.market_payload,
          source_file_name = src.source_file_name,
          source_page_name = src.source_page_name,
          source_page_limit = src.source_page_limit,
          source_page_offset = src.source_page_offset,
          raw_loaded_at = src.raw_loaded_at,
          processed_at = CURRENT_TIMESTAMP(),
          transformed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
          market_id,
          api_id,
          condition_id,
          question,
          slug,
          resolution_source,
          description,
          start_date,
          end_date,
          created_at,
          updated_at,
          end_date_iso,
          start_date_iso,
          has_reviewed_dates,
          image,
          icon,
          active,
          closed,
          is_new,
          archived,
          featured,
          restricted,
          approved,
          accepting_orders,
          accepting_orders_timestamp,
          ready,
          funded,
          enable_order_book,
          automatically_active,
          clear_book_on_start,
          manual_activation,
          rfq_enabled,
          show_gmp_series,
          show_gmp_outcome,
          order_price_min_tick_size,
          order_min_size,
          liquidity_text,
          liquidity,
          liquidity_clob,
          liquidity_num,
          volume_text,
          volume,
          volume_clob,
          volume_24hr,
          volume_24hr_clob,
          volume_1wk,
          volume_1wk_clob,
          volume_1mo,
          volume_1mo_clob,
          volume_1yr,
          volume_1yr_clob,
          volume_num,
          best_bid,
          best_ask,
          last_trade_price,
          spread,
          one_hour_price_change,
          one_day_price_change,
          one_week_price_change,
          one_month_price_change,
          one_year_price_change,
          rewards_min_size,
          rewards_max_spread,
          uma_bond,
          uma_reward,
          cyom,
          competitive,
          outcomes,
          outcome_prices,
          clob_token_ids,
          clob_rewards,
          events,
          uma_resolution_statuses,
          market_maker_address,
          submitted_by,
          resolved_by,
          group_item_title,
          group_item_threshold,
          question_id,
          neg_risk,
          neg_risk_other,
          neg_risk_market_id,
          neg_risk_request_id,
          custom_liveness,
          pager_duty_notification_enabled,
          fees_enabled,
          fee_type,
          holding_rewards_enabled,
          series_color,
          pending_deployment,
          deploying,
          deploying_timestamp,
          requires_translation,
          market_payload_hash,
          market_payload,
          source_file_name,
          source_page_name,
          source_page_limit,
          source_page_offset,
          raw_loaded_at,
          processed_at,
          transformed_at
        ) VALUES (
          src.market_id,
          src.api_id,
          src.condition_id,
          src.question,
          src.slug,
          src.resolution_source,
          src.description,
          src.start_date,
          src.end_date,
          src.created_at,
          src.updated_at,
          src.end_date_iso,
          src.start_date_iso,
          src.has_reviewed_dates,
          src.image,
          src.icon,
          src.active,
          src.closed,
          src.is_new,
          src.archived,
          src.featured,
          src.restricted,
          src.approved,
          src.accepting_orders,
          src.accepting_orders_timestamp,
          src.ready,
          src.funded,
          src.enable_order_book,
          src.automatically_active,
          src.clear_book_on_start,
          src.manual_activation,
          src.rfq_enabled,
          src.show_gmp_series,
          src.show_gmp_outcome,
          src.order_price_min_tick_size,
          src.order_min_size,
          src.liquidity_text,
          src.liquidity,
          src.liquidity_clob,
          src.liquidity_num,
          src.volume_text,
          src.volume,
          src.volume_clob,
          src.volume_24hr,
          src.volume_24hr_clob,
          src.volume_1wk,
          src.volume_1wk_clob,
          src.volume_1mo,
          src.volume_1mo_clob,
          src.volume_1yr,
          src.volume_1yr_clob,
          src.volume_num,
          src.best_bid,
          src.best_ask,
          src.last_trade_price,
          src.spread,
          src.one_hour_price_change,
          src.one_day_price_change,
          src.one_week_price_change,
          src.one_month_price_change,
          src.one_year_price_change,
          src.rewards_min_size,
          src.rewards_max_spread,
          src.uma_bond,
          src.uma_reward,
          src.cyom,
          src.competitive,
          src.outcomes,
          src.outcome_prices,
          src.clob_token_ids,
          src.clob_rewards,
          src.events,
          src.uma_resolution_statuses,
          src.market_maker_address,
          src.submitted_by,
          src.resolved_by,
          src.group_item_title,
          src.group_item_threshold,
          src.question_id,
          src.neg_risk,
          src.neg_risk_other,
          src.neg_risk_market_id,
          src.neg_risk_request_id,
          src.custom_liveness,
          src.pager_duty_notification_enabled,
          src.fees_enabled,
          src.fee_type,
          src.holding_rewards_enabled,
          src.series_color,
          src.pending_deployment,
          src.deploying,
          src.deploying_timestamp,
          src.requires_translation,
          src.market_payload_hash,
          src.market_payload,
          src.source_file_name,
          src.source_page_name,
          src.source_page_limit,
          src.source_page_offset,
          src.raw_loaded_at,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        );
        """
    ).strip()
