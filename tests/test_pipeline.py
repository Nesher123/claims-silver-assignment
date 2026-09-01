"""Small runnable checks for deterministic helpers and gate behavior."""

import duckdb

from claims_pipeline.config import load_config
from claims_pipeline.db import delivery_month_from_key, row_hash_sql
from claims_pipeline.inventory import store_inventory
from claims_pipeline.validate import apply_gate


def test_delivery_month_from_key():
    assert delivery_month_from_key("raw/claims/claims_202509.csv") == "2025-09"


def test_row_hash_is_stable_and_qualified():
    sql = row_hash_sql("n")
    assert sql == row_hash_sql("n")
    assert "n.claim_id" in sql
    assert "n.charge_amount" in sql


def test_inventory_exposes_common_fields_as_columns():
    con = duckdb.connect()
    store_inventory(con, [{
        "s3_key": "raw/claims/claims_202501.parquet",
        "delivery_month": "2025-01",
        "format": "parquet",
        "row_count": 28_000,
        "size_bytes": 1_130_654,
        "columns": [{"name": "claim_id", "type": "VARCHAR"}],
    }])

    assert [row[0] for row in con.execute("DESCRIBE inventory").fetchall()] == [
        "s3_key", "delivery_month", "file_format", "row_count", "size_bytes", "columns_json"
    ]
    assert con.execute("""
        SELECT delivery_month, file_format, row_count FROM inventory
    """).fetchone() == ("2025-01", "parquet", 28_000)


def test_gate_blocks_empty_and_same_month_conflict():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE expected_deliveries(delivery_month VARCHAR);
        INSERT INTO expected_deliveries VALUES ('2025-01'), ('2025-02');
        CREATE TABLE validation_results AS
        SELECT
            '2025-01'::VARCHAR AS _delivery_month,
            10::BIGINT AS total_rows,
            'PASS'::VARCHAR AS clause_2_2,
            'PASS'::VARCHAR AS clause_2_3,
            'PASS'::VARCHAR AS clause_2_4,
            'PASS'::VARCHAR AS clause_3_1,
            'PASS'::VARCHAR AS clause_3_2,
            'PASS'::VARCHAR AS clause_3_3,
            'PASS'::VARCHAR AS clause_3_4,
            'PASS'::VARCHAR AS clause_4_2,
            'PASS'::VARCHAR AS clause_5_2,
            'PASS'::VARCHAR AS clause_5_4;
        CREATE TABLE same_month_conflicts(
            claim_id VARCHAR, claim_line INTEGER, _delivery_month VARCHAR
        );
        INSERT INTO same_month_conflicts VALUES ('A', 1, '2025-01');
    """)

    apply_gate(con, load_config())

    assert con.execute("""
        SELECT decision FROM promotion_decisions WHERE _delivery_month = '2025-01'
    """).fetchone()[0] == "BLOCK"
    assert con.execute("""
        SELECT decision FROM promotion_decisions WHERE _delivery_month = '2025-02'
    """).fetchone()[0] == "BLOCK"
