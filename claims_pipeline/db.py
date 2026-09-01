"""Provide DuckDB connections, raw readers, delivery parsing, and stable row hashes."""

import re
from pathlib import Path

import duckdb

from .constants import CONTRACTED_COLS, DATA, DB_PATH, WORK


def delivery_month_from_key(key: str) -> str:
    m = re.search(r"claims_(\d{4})(\d{2})", key)
    return f"{m.group(1)}-{m.group(2)}" if m else "unknown"


def is_csv(path: Path) -> bool:
    return path.suffix.lower() == ".csv"


def connect() -> duckdb.DuckDBPyConnection:
    WORK.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def read_claims_file(con: duckdb.DuckDBPyConnection, path: Path, delivery_month: str) -> str:
    """Register one delivery as a temp view; return view name."""
    view = f"raw_{delivery_month.replace('-', '_')}"
    src = str(path).replace("'", "''")
    if is_csv(path):
        con.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT *, '{delivery_month}' AS _delivery_month, '{path.name}' AS _source_file
            FROM read_csv('{src}', delim='|', header=true, all_varchar=true)
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE VIEW {view} AS
            SELECT *, '{delivery_month}' AS _delivery_month, '{path.name}' AS _source_file
            FROM read_parquet('{src}')
        """)
    return view


def row_hash_sql(prefix: str = "") -> str:
    p = f"{prefix}." if prefix else ""
    cols = ", ".join(f"coalesce(CAST({p}{c} AS VARCHAR), '')" for c in CONTRACTED_COLS)
    return f"md5(concat_ws('|', {cols}))"
