from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from polymarket_etl.market_files import discover_market_page_files


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_nullable_string_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return sql_string_literal(value)


def sql_bool_literal(value):
    if value is None:
        return "NULL"
    return "TRUE" if value else "FALSE"


def build_manifest_rows_sql(data_dir: Path, file_names: Optional[List[str]] = None) -> str:
    files = discover_market_page_files(data_dir)
    if not files:
        raise ValueError(f"No market files found in {data_dir}")

    allowed = set(file_names or [])
    if allowed:
        files = [item for item in files if item.file_name in allowed]
    if not files:
        raise ValueError("No matching files found for the raw load manifest")

    rows = []
    for item in files:
        rows.append(
            "("
            f"{sql_string_literal(item.file_name)}, "
            f"{sql_string_literal(item.page_name)}, "
            f"{item.page_limit}, "
            f"{item.page_offset}, "
            f"{sql_bool_literal(item.is_active)}, "
            f"{sql_bool_literal(item.is_closed)}, "
            f"{item.row_count if item.row_count is not None else 'NULL'}, "
            f"{sql_nullable_string_literal(item.md5_hex)}"
            ")"
        )
    return (
        "SELECT * FROM VALUES\n"
        + ",\n".join(rows)
        + "\nAS manifest(source_file_name, page_name, page_limit, page_offset, "
        "source_is_active, source_is_closed, expected_row_count, expected_md5_hex)"
    )


def put_files_to_stage(hook, database: str, files: Iterable[Path]) -> None:
    stage_name = f'@"{database}"."RAW"."GAMMA_MARKETS_STAGE"'
    for path in files:
        put_sql = (
            f"PUT {sql_string_literal('file://' + str(path))} "
            f"{stage_name} "
            "AUTO_COMPRESS=FALSE "
            "OVERWRITE=TRUE "
            "PARALLEL=8"
        )
        hook.run(put_sql)
