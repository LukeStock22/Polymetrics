from __future__ import annotations

import json
from datetime import datetime, timezone

import polymarket_etl.gamma_fetch as gamma_fetch


def test_parse_iso_timestamp_normalizes_z_suffix_and_naive_values() -> None:
    assert gamma_fetch._parse_iso_timestamp(None) is None
    assert gamma_fetch._parse_iso_timestamp("not-a-timestamp") is None

    parsed_z = gamma_fetch._parse_iso_timestamp("2026-04-20T12:30:00Z")
    parsed_naive = gamma_fetch._parse_iso_timestamp("2026-04-20T12:30:00")

    assert parsed_z == datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc)
    assert parsed_naive == datetime(2026, 4, 20, 12, 30, tzinfo=timezone.utc)


def test_filter_rows_by_updated_at_uses_half_open_window_and_keeps_missing_timestamps() -> None:
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 21, 0, 0, tzinfo=timezone.utc)
    rows = [
        {"market_id": "before", "updatedAt": "2026-04-19T23:59:59Z"},
        {"market_id": "start", "updatedAt": "2026-04-20T00:00:00Z"},
        {"market_id": "inside", "updatedAt": "2026-04-20T12:00:00Z"},
        {"market_id": "end", "updatedAt": "2026-04-21T00:00:00Z"},
        {"market_id": "missing"},
    ]

    filtered = gamma_fetch._filter_rows_by_updated_at(rows, start, end)

    assert [row["market_id"] for row in filtered] == ["start", "inside", "missing"]


def test_build_url_appends_limit_and_offset() -> None:
    url = gamma_fetch._build_url(
        "https://gamma-api.polymarket.com/markets",
        {"active": "true", "closed": "false"},
        limit=200,
        offset=400,
    )

    assert url.startswith("https://gamma-api.polymarket.com/markets?")
    assert "active=true" in url
    assert "closed=false" in url
    assert "limit=200" in url
    assert "offset=400" in url


def test_write_market_pages_filters_rows_and_skips_empty_pages(tmp_path, monkeypatch) -> None:
    start = datetime(2026, 4, 20, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 21, 0, 0, tzinfo=timezone.utc)

    def fake_fetch_pages(*args, **kwargs):
        return [
            gamma_fetch.GammaPage(
                page_name="active_closedfalse",
                page_limit=2,
                page_offset=0,
                rows=[
                    {"market_id": "inside", "updatedAt": "2026-04-20T02:00:00Z"},
                    {"market_id": "missing"},
                    {"market_id": "after", "updatedAt": "2026-04-21T02:00:00Z"},
                ],
            ),
            gamma_fetch.GammaPage(
                page_name="active_closedfalse",
                page_limit=2,
                page_offset=2,
                rows=[
                    {"market_id": "before", "updatedAt": "2026-04-19T12:00:00Z"},
                ],
            ),
        ]

    monkeypatch.setattr(gamma_fetch, "_fetch_pages", fake_fetch_pages)

    files, total_rows = gamma_fetch.write_market_pages(
        run_dir=tmp_path,
        start=start,
        end=end,
        limit=2,
        sleep_seconds=0,
        sources=[
            gamma_fetch.GammaSource(
                page_name="active_closedfalse",
                base_url=gamma_fetch.GAMMA_MARKETS_BASE,
                params={"active": "true", "closed": "false"},
            )
        ],
    )

    assert total_rows == 2
    assert [path.name for path in files] == ["active_closedfalse_limit2_offset0.json"]
    assert json.loads(files[0].read_text(encoding="utf-8")) == [
        {"market_id": "inside", "updatedAt": "2026-04-20T02:00:00Z"},
        {"market_id": "missing"},
    ]
