"""Inventory physical files before safely normalizing mixed deliveries in DuckDB."""

import json

import duckdb

from .constants import CLAIMS_KEYS, CONTRACTED_COLS, DATA, TOKEN_KEY
from .db import delivery_month_from_key, is_csv, read_claims_file


def store_inventory(con: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Store common inventory attributes as columns and only variable schema as JSON."""
    con.execute("""
        CREATE OR REPLACE TABLE inventory (
            s3_key VARCHAR,
            delivery_month VARCHAR,
            file_format VARCHAR,
            row_count BIGINT,
            size_bytes BIGINT,
            columns_json VARCHAR
        )
    """)
    con.executemany(
        "INSERT INTO inventory VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                row["s3_key"],
                row["delivery_month"],
                row["format"],
                row["row_count"],
                row["size_bytes"],
                json.dumps(row["columns"], separators=(",", ":")),
            )
            for row in rows
        ],
    )


def inventory(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Physical schema inventory per delivery file."""
    rows = []
    for key in CLAIMS_KEYS:
        path = DATA / key
        dm = delivery_month_from_key(key)
        view = read_claims_file(con, path, dm)
        info = con.execute(f"SELECT COUNT(*) AS row_count FROM {view}").fetchone()
        cols = con.execute(f"DESCRIBE SELECT * FROM {view}").fetchall()
        rows.append({
            "s3_key": key,
            "delivery_month": dm,
            "format": "csv" if is_csv(path) else "parquet",
            "row_count": info[0],
            "columns": [{"name": c[0], "type": c[1]} for c in cols],
            "size_bytes": path.stat().st_size,
        })
    store_inventory(con, rows)
    return rows


def build_staging(con: duckdb.DuckDBPyConnection) -> None:
    """Union all deliveries into staging_claims with normalized types."""
    parts = []
    for key in CLAIMS_KEYS:
        path = DATA / key
        dm = delivery_month_from_key(key)
        view = read_claims_file(con, path, dm)
        parts.append(f"SELECT * FROM {view}")

    union_sql = " UNION ALL BY NAME ".join(parts)
    con.execute(f"CREATE OR REPLACE TABLE staging_claims AS {union_sql}")

    col_selects = []
    for c in CONTRACTED_COLS:
        if c in ("claim_line", "birth_year", "units"):
            col_selects.append(f"TRY_CAST({c} AS INTEGER) AS {c}")
        elif c in ("charge_amount", "allowed_amount"):
            col_selects.append(f"TRY_CAST({c} AS DECIMAL(12,2)) AS {c}")
        elif c in ("date_of_service", "paid_date"):
            col_selects.append(f"TRY_CAST({c} AS DATE) AS {c}")
        else:
            col_selects.append(f"CAST({c} AS VARCHAR) AS {c}")

    con.execute(f"""
        CREATE OR REPLACE TABLE normalized_claims AS
        SELECT
            {", ".join(col_selects)},
            _delivery_month,
            _source_file,
            CASE WHEN lower(coalesce(_source_file, '')) LIKE '%.csv' THEN 'csv' ELSE 'parquet' END AS _physical_format
        FROM staging_claims
    """)

    token_path = str(DATA / TOKEN_KEY).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TABLE member_tokens AS SELECT * FROM read_parquet('{token_path}')")
    # ponytail: one row per token; ambiguous tokens get vi_pid NULL + flag
    con.execute("""
        CREATE OR REPLACE TABLE member_tokens_resolved AS
        SELECT
            member_token,
            CASE WHEN vi_pid_count = 1 THEN any_vi_pid ELSE NULL END AS vi_pid,
            vi_pid_count > 1 AS is_ambiguous
        FROM (
            SELECT member_token, COUNT(DISTINCT vi_pid) AS vi_pid_count, MIN(vi_pid) AS any_vi_pid
            FROM member_tokens
            GROUP BY member_token
        )
    """)
