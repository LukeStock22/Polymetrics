from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional


MARKET_PAGE_PATTERN = re.compile(
    r"^(?P<page_name>[a-z_]+)_limit(?P<page_limit>\d+)_offset(?P<page_offset>\d+)\.json$"
)


@dataclass(frozen=True)
class MarketPageFile:
    absolute_path: Path
    file_name: str
    page_name: str
    page_limit: int
    page_offset: int
    row_count: Optional[int]
    md5_hex: Optional[str]

    @property
    def is_active(self) -> Optional[bool]:
        if "active_true" in self.page_name:
            return True
        if "active_false" in self.page_name:
            return False
        return None

    @property
    def is_closed(self) -> Optional[bool]:
        if "closed_true" in self.page_name:
            return True
        if "closedfalse" in self.page_name or "closed_false" in self.page_name:
            return False
        return None


def discover_market_page_files(data_dir: Path, include_content_metrics: bool = False) -> List[MarketPageFile]:
    files: List[MarketPageFile] = []
    for path in sorted(data_dir.glob("*.json")):
        match = MARKET_PAGE_PATTERN.match(path.name)
        if not match:
            continue

        row_count: Optional[int] = None
        md5_hex: Optional[str] = None
        if include_content_metrics:
            contents = path.read_bytes()
            rows = json.loads(contents.decode("utf-8"))
            if not isinstance(rows, list):
                raise ValueError(f"Expected {path} to contain a JSON array")
            row_count = len(rows)
            md5_hex = hashlib.md5(contents).hexdigest()

        files.append(
            MarketPageFile(
                absolute_path=path.resolve(),
                file_name=path.name,
                page_name=match.group("page_name"),
                page_limit=int(match.group("page_limit")),
                page_offset=int(match.group("page_offset")),
                row_count=row_count,
                md5_hex=md5_hex,
            )
        )

    return files


def summarize_market_page_files(files: Iterable[MarketPageFile]) -> dict:
    file_list = list(files)
    total_rows = sum(item.row_count or 0 for item in file_list)
    return {
        "file_count": len(file_list),
        "total_rows": total_rows,
        "first_file": file_list[0].file_name if file_list else None,
        "last_file": file_list[-1].file_name if file_list else None,
    }
