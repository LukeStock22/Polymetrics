from __future__ import annotations

import json
import logging
import time
from urllib.error import HTTPError, URLError
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GAMMA_MARKETS_BASE = "https://gamma-api.polymarket.com/markets"

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class GammaPage:
    page_name: str
    page_limit: int
    page_offset: int
    rows: List[dict]


@dataclass(frozen=True)
class GammaSource:
    page_name: str
    base_url: str
    params: Dict[str, str]


def _parse_iso_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fetch_json(
    url: str,
    retries: int = 4,
    backoff_seconds: float = 1.5,
    timeout_seconds: float = 30.0,
) -> list:
    headers = {
        "User-Agent": "PolyMetrics/1.0 (+https://linuxlab.engr.wustl.edu/)",
        "Accept": "application/json",
    }
    attempt = 0
    while True:
        request = Request(url, headers=headers)
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
            if not isinstance(data, list):
                raise ValueError(f"Expected list response from {url}")
            return data
        except HTTPError as exc:
            if exc.code in {403, 429} and attempt < retries:
                LOG.warning("Gamma API %s -> HTTP %s, retrying (%s/%s)", url, exc.code, attempt + 1, retries)
                time.sleep(backoff_seconds * (2 ** attempt))
                attempt += 1
                continue
            raise
        except URLError:
            if attempt < retries:
                LOG.warning("Gamma API %s -> URLError, retrying (%s/%s)", url, attempt + 1, retries)
                time.sleep(backoff_seconds * (2 ** attempt))
                attempt += 1
                continue
            raise


def _filter_rows_by_updated_at(
    rows: Iterable[dict],
    start: Optional[datetime],
    end: Optional[datetime],
) -> List[dict]:
    if start is None and end is None:
        return list(rows)
    start_utc = start.astimezone(timezone.utc) if start else None
    end_utc = end.astimezone(timezone.utc) if end else None
    filtered: List[dict] = []
    for row in rows:
        updated_at = _parse_iso_timestamp(str(row.get("updatedAt") or "")) if row else None
        if updated_at is None:
            filtered.append(row)
            continue
        if start_utc and updated_at < start_utc:
            continue
        if end_utc and updated_at >= end_utc:
            continue
        filtered.append(row)
    return filtered


def _build_url(base_url: str, params: Dict[str, str], limit: int, offset: int) -> str:
    query = dict(params)
    query["limit"] = str(limit)
    query["offset"] = str(offset)
    return f"{base_url}?{urlencode(query)}"


def _fetch_pages(
    page_name: str,
    base_url: str,
    params: Dict[str, str],
    limit: int,
    sleep_seconds: float,
    start_offset: int = 0,
) -> List[GammaPage]:
    pages: List[GammaPage] = []
    offset = start_offset
    while True:
        url = _build_url(base_url, params, limit, offset)
        rows = _fetch_json(url)
        if not rows:
            break
        pages.append(GammaPage(page_name=page_name, page_limit=limit, page_offset=offset, rows=rows))
        LOG.info("Fetched %s rows for %s offset=%s", len(rows), page_name, offset)
        offset += limit
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return pages


def write_market_pages(
    run_dir: Path,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int = 200,
    sleep_seconds: float = 0.15,
    start_offset: int = 0,
    sources: Optional[List[GammaSource]] = None,
) -> Tuple[List[Path], int]:
    run_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    total_rows = 0

    if sources is None:
        sources = [
            GammaSource(
                page_name="active_closedfalse",
                base_url=GAMMA_MARKETS_BASE,
                params={"active": "true", "closed": "false"},
            ),
            GammaSource(
                page_name="closed_true",
                base_url=GAMMA_MARKETS_BASE,
                params={"closed": "true"},
            ),
        ]

    for source in sources:
        pages = _fetch_pages(
            source.page_name,
            source.base_url,
            source.params,
            limit,
            sleep_seconds,
            start_offset=start_offset,
        )
        for page in pages:
            filtered = _filter_rows_by_updated_at(page.rows, start, end)
            if not filtered:
                continue
            file_path = run_dir / f"{page.page_name}_limit{page.page_limit}_offset{page.page_offset}.json"
            file_path.write_text(json.dumps(filtered, ensure_ascii=True))
            LOG.info("Wrote %s rows to %s", len(filtered), file_path.name)
            written.append(file_path)
            total_rows += len(filtered)

    return written, total_rows
