from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_etl.gamma_ingest import build_manifest_rows_sql, put_files_to_stage


def write_page(path: Path, payload) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_build_manifest_rows_sql_includes_expected_market_file_metadata(tmp_path) -> None:
    write_page(tmp_path / "active_closedfalse_limit200_offset0.json", [])
    write_page(tmp_path / "closed_true_limit200_offset200.json", [])

    sql = build_manifest_rows_sql(tmp_path)

    assert "'active_closedfalse_limit200_offset0.json'" in sql
    assert "'closed_true_limit200_offset200.json'" in sql
    assert "AS manifest(source_file_name, page_name, page_limit, page_offset" in sql
    assert "TRUE, FALSE, NULL, NULL" in sql
    assert "NULL, TRUE, NULL, NULL" in sql


def test_build_manifest_rows_sql_can_filter_to_requested_file_names(tmp_path) -> None:
    write_page(tmp_path / "active_closedfalse_limit200_offset0.json", [])
    write_page(tmp_path / "closed_true_limit200_offset200.json", [])

    sql = build_manifest_rows_sql(
        tmp_path, ["closed_true_limit200_offset200.json"]
    )

    assert "'closed_true_limit200_offset200.json'" in sql
    assert "'active_closedfalse_limit200_offset0.json'" not in sql


def test_build_manifest_rows_sql_raises_for_missing_or_unmatched_files(tmp_path) -> None:
    with pytest.raises(ValueError, match="No market files found"):
        build_manifest_rows_sql(tmp_path)

    write_page(tmp_path / "closed_true_limit200_offset200.json", [])

    with pytest.raises(ValueError, match="No matching files found"):
        build_manifest_rows_sql(tmp_path, ["does_not_exist.json"])


def test_put_files_to_stage_emits_one_put_statement_per_file(tmp_path) -> None:
    class RecordingHook:
        def __init__(self) -> None:
            self.commands: list[str] = []

        def run(self, sql: str) -> None:
            self.commands.append(sql)

    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text("[]", encoding="utf-8")
    second.write_text("[]", encoding="utf-8")

    hook = RecordingHook()
    put_files_to_stage(hook, "PANTHER_DB", [first, second])

    assert len(hook.commands) == 2
    assert all(command.startswith("PUT ") for command in hook.commands)
    assert all('@"PANTHER_DB"."RAW"."GAMMA_MARKETS_STAGE"' in command for command in hook.commands)
    assert f"file://{first}" in hook.commands[0]
    assert f"file://{second}" in hook.commands[1]
