from __future__ import annotations

import pytest

from polymarket_etl.snowflake_sql import (
    bootstrap_sql,
    copy_market_pages_into_stage_sql,
    fq_name,
    merge_raw_market_pages_sql,
    quote_identifier,
)


def test_quote_identifier_escapes_quotes_and_rejects_empty_names() -> None:
    assert quote_identifier('my"table') == '"my""table"'

    with pytest.raises(ValueError, match="cannot be empty"):
        quote_identifier("   ")


def test_fq_name_quotes_each_identifier_part() -> None:
    assert fq_name("PANTHER_DB", "RAW", "GAMMA_MARKETS_STAGE") == (
        '"PANTHER_DB"."RAW"."GAMMA_MARKETS_STAGE"'
    )


def test_bootstrap_sql_contains_expected_objects_without_duplicate_create_columns() -> None:
    sql = bootstrap_sql("PANTHER_DB")

    assert 'CREATE SCHEMA IF NOT EXISTS "PANTHER_DB"."RAW";' in sql
    assert 'CREATE TABLE IF NOT EXISTS "PANTHER_DB"."RAW"."GAMMA_MARKET_PAGES"' in sql
    assert 'CREATE TABLE IF NOT EXISTS "PANTHER_DB"."CURATED"."GAMMA_MARKETS"' in sql
    assert sql.count("clear_book_on_start BOOLEAN,") == 1
    assert sql.count("manual_activation BOOLEAN,") == 1
    assert sql.count("rfq_enabled BOOLEAN,") == 1
    assert sql.count("show_gmp_series BOOLEAN,") == 1
    assert sql.count("show_gmp_outcome BOOLEAN,") == 1
    assert sql.count("ADD COLUMN IF NOT EXISTS clear_book_on_start BOOLEAN;") == 1


def test_copy_market_pages_into_stage_sql_uses_manifest_and_stage_metadata() -> None:
    sql = copy_market_pages_into_stage_sql("PANTHER_DB", "SELECT 1")

    assert "WITH manifest AS (" in sql
    assert "SELECT 1" in sql
    assert 'INSERT INTO "PANTHER_DB"."RAW"."GAMMA_MARKET_PAGES_STAGE"' in sql
    assert 'FROM @"PANTHER_DB"."RAW"."GAMMA_MARKETS_STAGE"' in sql
    assert "src.metadata$file_content_key" in sql
    assert "src.metadata$filename = manifest.source_file_name" in sql


def test_merge_raw_market_pages_sql_merges_on_file_name_and_content_key() -> None:
    sql = merge_raw_market_pages_sql("PANTHER_DB")

    assert 'MERGE INTO "PANTHER_DB"."RAW"."GAMMA_MARKET_PAGES" target' in sql
    assert "target.source_file_name = src.source_file_name" in sql
    assert "target.file_content_key = src.file_content_key" in sql
