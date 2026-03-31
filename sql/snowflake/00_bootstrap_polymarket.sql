CREATE SCHEMA IF NOT EXISTS PANTHER_DB.RAW;
CREATE SCHEMA IF NOT EXISTS PANTHER_DB.CURATED;

CREATE FILE FORMAT IF NOT EXISTS PANTHER_DB.RAW.JSON_ARRAY_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = AUTO;

CREATE STAGE IF NOT EXISTS PANTHER_DB.RAW.GAMMA_MARKETS_STAGE
  FILE_FORMAT = (FORMAT_NAME = PANTHER_DB.RAW.JSON_ARRAY_FORMAT);


CREATE TABLE IF NOT EXISTS PANTHER_DB.RAW.GAMMA_MARKET_PAGES_STAGE (
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

CREATE TABLE IF NOT EXISTS PANTHER_DB.RAW.GAMMA_MARKET_PAGES (
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

CREATE TABLE IF NOT EXISTS PANTHER_DB.CURATED.GAMMA_MARKETS (
  market_id STRING NOT NULL,
  condition_id STRING,
  question STRING,
  slug STRING,
  resolution_source STRING,
  description STRING,
  start_date TIMESTAMP_NTZ,
  end_date TIMESTAMP_NTZ,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  image STRING,
  icon STRING,
  active BOOLEAN,
  closed BOOLEAN,
  archived BOOLEAN,
  featured BOOLEAN,
  restricted BOOLEAN,
  approved BOOLEAN,
  accepting_orders BOOLEAN,
  ready BOOLEAN,
  funded BOOLEAN,
  enable_order_book BOOLEAN,
  order_price_min_tick_size NUMBER(18,6),
  order_min_size NUMBER(18,6),
  liquidity NUMBER(38,12),
  liquidity_clob NUMBER(38,12),
  volume NUMBER(38,12),
  volume_clob NUMBER(38,12),
  volume_24hr NUMBER(38,12),
  volume_1wk NUMBER(38,12),
  volume_1mo NUMBER(38,12),
  volume_1yr NUMBER(38,12),
  best_bid NUMBER(18,6),
  best_ask NUMBER(18,6),
  last_trade_price NUMBER(18,6),
  spread NUMBER(18,6),
  one_hour_price_change NUMBER(18,6),
  one_day_price_change NUMBER(18,6),
  one_week_price_change NUMBER(18,6),
  one_month_price_change NUMBER(18,6),
  one_year_price_change NUMBER(18,6),
  outcomes VARIANT,
  outcome_prices VARIANT,
  clob_token_ids VARIANT,
  clob_rewards VARIANT,
  events VARIANT,
  uma_resolution_statuses VARIANT,
  market_payload VARIANT NOT NULL,
  source_file_name STRING NOT NULL,
  source_page_name STRING NOT NULL,
  source_page_limit NUMBER(18,0) NOT NULL,
  source_page_offset NUMBER(18,0) NOT NULL,
  raw_loaded_at TIMESTAMP_NTZ NOT NULL,
  transformed_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT gamma_markets_pk PRIMARY KEY (market_id)
);
