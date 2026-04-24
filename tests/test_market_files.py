from __future__ import annotations

import hashlib
import json

import pytest

from polymarket_etl.market_files import discover_market_page_files, summarize_market_page_files


def write_json(path, payload) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_discover_market_page_files_extracts_metadata_and_content_metrics(tmp_path) -> None:
    active_path = tmp_path / "active_closedfalse_limit200_offset0.json"
    closed_path = tmp_path / "closed_true_limit200_offset200.json"
    ignored_path = tmp_path / "notes.json"

    write_json(active_path, [{"id": 1}, {"id": 2}])
    write_json(closed_path, [{"id": 3}])
    write_json(ignored_path, [{"ignored": True}])

    files = discover_market_page_files(tmp_path, include_content_metrics=True)

    assert [item.file_name for item in files] == [
        "active_closedfalse_limit200_offset0.json",
        "closed_true_limit200_offset200.json",
    ]

    active_file, closed_file = files
    assert active_file.page_name == "active_closedfalse"
    assert active_file.page_limit == 200
    assert active_file.page_offset == 0
    assert active_file.is_active is True
    assert active_file.is_closed is False
    assert active_file.row_count == 2
    assert active_file.md5_hex == hashlib.md5(active_path.read_bytes()).hexdigest()

    assert closed_file.is_active is None
    assert closed_file.is_closed is True
    assert closed_file.row_count == 1


def test_discover_market_page_files_rejects_non_array_json(tmp_path) -> None:
    invalid_path = tmp_path / "closed_true_limit200_offset0.json"
    invalid_path.write_text('{"id": 1}', encoding="utf-8")

    with pytest.raises(ValueError, match="JSON array"):
        discover_market_page_files(tmp_path, include_content_metrics=True)


def test_summarize_market_page_files_aggregates_counts_and_handles_empty_input(tmp_path) -> None:
    write_json(tmp_path / "active_closedfalse_limit200_offset0.json", [{"id": 1}, {"id": 2}])
    write_json(tmp_path / "closed_true_limit200_offset200.json", [{"id": 3}])

    summary = summarize_market_page_files(
        discover_market_page_files(tmp_path, include_content_metrics=True)
    )

    assert summary == {
        "file_count": 2,
        "total_rows": 3,
        "first_file": "active_closedfalse_limit200_offset0.json",
        "last_file": "closed_true_limit200_offset200.json",
    }

    assert summarize_market_page_files([]) == {
        "file_count": 0,
        "total_rows": 0,
        "first_file": None,
        "last_file": None,
    }
