"""Measure contract clauses and assign one risk-based gate decision per delivery."""

import duckdb

from .constants import CLAIMS_KEYS
from .db import delivery_month_from_key, row_hash_sql


def run_validation(con: duckdb.DuckDBPyConnection, cfg: dict) -> None:
    """Per-delivery and cross-delivery contract checks."""
    sample_factor = cfg["sample"]["factor"]
    comp = cfg["completeness"]
    ident = cfg["identity"]
    vol = cfg["volume"]

    con.execute(f"""
        CREATE OR REPLACE TABLE validation_results_base AS
        WITH base AS (
            SELECT
                _delivery_month,
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (WHERE dx_code IS NOT NULL AND trim(dx_code) != '') * 1.0 / NULLIF(COUNT(*), 0) AS dx_populated,
                COUNT(*) FILTER (WHERE npi IS NOT NULL AND trim(npi) != '') * 1.0 / NULLIF(COUNT(*), 0) AS npi_populated,
                COUNT(*) FILTER (WHERE member_token IS NOT NULL AND trim(member_token) != '') * 1.0 / NULLIF(COUNT(*), 0) AS token_populated,
                COUNT(*) FILTER (WHERE claim_id IS NOT NULL AND trim(claim_id) != ''
                    AND claim_line IS NOT NULL AND date_of_service IS NOT NULL AND charge_amount IS NOT NULL)
                    * 1.0 / NULLIF(COUNT(*), 0) AS required_populated,
                COUNT(*) FILTER (WHERE token_version = '{ident["token_version_required"]}') * 1.0 / NULLIF(COUNT(*), 0) AS token_v2_rate,
                COUNT(*) FILTER (WHERE gender IN ('M','F','U')) * 1.0 / NULLIF(COUNT(*), 0) AS gender_valid_rate,
                COUNT(*) FILTER (WHERE icd_version = '10' AND dx_code IS NOT NULL) * 1.0 / NULLIF(COUNT(*), 0) AS icd10_rate,
                COUNT(*) FILTER (WHERE length(coalesce(npi,'')) = 10) * 1.0 / NULLIF(COUNT(*), 0) AS npi_len_rate,
                COUNT(*) FILTER (WHERE length(coalesce(member_zip,'')) = 5) * 1.0 / NULLIF(COUNT(*), 0) AS zip_len_rate,
                COUNT(*) FILTER (WHERE length(coalesce(place_of_service,'')) = 2) * 1.0 / NULLIF(COUNT(*), 0) AS pos_len_rate,
                COUNT(*) FILTER (WHERE date_of_service <= paid_date OR paid_date IS NULL) * 1.0 / NULLIF(COUNT(*), 0) AS date_order_rate,
                COUNT(*) FILTER (WHERE units >= 1 OR units IS NULL) * 1.0 / NULLIF(COUNT(*), 0) AS units_valid_rate,
                MAX(CASE WHEN lower(_source_file) LIKE '%.csv' THEN 1 ELSE 0 END) AS is_csv,
                MAX(_physical_format) AS physical_format
            FROM normalized_claims
            GROUP BY _delivery_month
        ),
        resolved AS (
            SELECT
                n._delivery_month,
                COUNT(*) FILTER (WHERE m.member_token IS NOT NULL AND NOT m.is_ambiguous) * 1.0
                    / NULLIF(COUNT(*), 0) AS token_resolution_rate
            FROM normalized_claims n
            LEFT JOIN member_tokens_resolved m ON n.member_token = m.member_token
            GROUP BY n._delivery_month
        )
        SELECT
            b.*,
            r.token_resolution_rate,
            b.total_rows * {sample_factor} AS scaled_row_count,
            CASE WHEN b.total_rows = 0 THEN 'FAIL' WHEN b.total_rows * {sample_factor} < {vol['min_claim_lines']} THEN 'FAIL' ELSE 'PASS' END AS clause_2_2,
            CASE WHEN b.is_csv = 1 THEN 'FAIL' ELSE 'PASS' END AS clause_2_4,
            CASE WHEN b.dx_populated >= {comp['dx_code']} THEN 'PASS' ELSE 'FAIL' END AS clause_3_1,
            CASE WHEN b.npi_populated >= {comp['npi']} THEN 'PASS' ELSE 'FAIL' END AS clause_3_2,
            CASE WHEN b.token_populated >= {comp['member_token']} THEN 'PASS' ELSE 'FAIL' END AS clause_3_3,
            CASE WHEN b.required_populated >= {comp['required_fields']} THEN 'PASS' ELSE 'FAIL' END AS clause_3_4,
            CASE WHEN b.gender_valid_rate = 1.0 THEN 'PASS' ELSE 'FAIL' END AS clause_4_2,
            CASE WHEN b.token_v2_rate = 1.0 THEN 'PASS' ELSE 'FAIL' END AS clause_5_4,
            CASE WHEN r.token_resolution_rate >= {ident['token_resolution_min']} THEN 'PASS' ELSE 'FAIL' END AS clause_5_2
        FROM base b
        JOIN resolved r ON b._delivery_month = r._delivery_month
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE validation_results AS
        WITH measured AS (
            SELECT *,
                AVG(total_rows) OVER (
                    ORDER BY _delivery_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) AS prior_three_mean,
                ROW_NUMBER() OVER (ORDER BY _delivery_month) AS delivery_number
            FROM validation_results_base
        )
        SELECT *,
            CASE
                WHEN delivery_number < 4 THEN 'NOT_APPLICABLE'
                WHEN total_rows BETWEEN prior_three_mean * {1 - vol['band_pct']}
                    AND prior_three_mean * {1 + vol['band_pct']} THEN 'PASS'
                ELSE 'FAIL'
            END AS clause_2_3,
            'UNTESTABLE' AS clause_2_1,
            'UNTESTABLE' AS clause_2_5,
            CASE WHEN icd10_rate = 1.0 THEN 'PASS' ELSE 'FAIL' END AS clause_4_1,
            CASE WHEN npi_len_rate = 1.0 AND zip_len_rate = 1.0 AND pos_len_rate = 1.0
                THEN 'PASS' ELSE 'FAIL' END AS clause_4_3,
            CASE WHEN date_order_rate = 1.0 THEN 'PASS' ELSE 'FAIL' END AS clause_4_4,
            CASE WHEN units_valid_rate = 1.0 THEN 'PASS' ELSE 'FAIL' END AS clause_4_7
        FROM measured
    """)

    con.execute("""
        CREATE OR REPLACE TABLE global_key_dupes AS
        SELECT claim_id, claim_line, COUNT(DISTINCT _delivery_month) AS delivery_count, COUNT(*) AS row_count
        FROM normalized_claims
        WHERE claim_id IS NOT NULL AND claim_line IS NOT NULL
        GROUP BY claim_id, claim_line
        HAVING COUNT(*) > 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE token_patient_ambiguous AS
        SELECT member_token, COUNT(DISTINCT patient_id) AS patient_count
        FROM normalized_claims
        WHERE member_token IS NOT NULL
        GROUP BY member_token
        HAVING COUNT(DISTINCT patient_id) > 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE token_vi_ambiguous AS
        SELECT member_token
        FROM member_tokens_resolved
        WHERE is_ambiguous
    """)

    row_signature = row_hash_sql("n")
    con.execute(f"""
        CREATE OR REPLACE TABLE same_month_conflicts AS
        SELECT claim_id, claim_line, _delivery_month
        FROM (
            SELECT n.*, {row_signature} AS row_signature
            FROM normalized_claims n
        )
        GROUP BY claim_id, claim_line, _delivery_month
        HAVING COUNT(DISTINCT row_signature) > 1
    """)

    months = [delivery_month_from_key(k) for k in CLAIMS_KEYS]
    con.execute("CREATE OR REPLACE TABLE expected_deliveries (delivery_month VARCHAR)")
    for m in months:
        con.execute("INSERT INTO expected_deliveries VALUES (?)", [m])


def apply_gate(con: duckdb.DuckDBPyConnection, cfg: dict) -> None:
    """Risk-based promotion gate per delivery."""
    block_codes = ", ".join(f"'{code}'" for code in cfg["gate"]["block"]["reasons"])
    con.execute(f"""
        CREATE OR REPLACE TABLE promotion_decisions AS
        WITH reasons AS (
            SELECT
                e.delivery_month AS _delivery_month,
                COALESCE(v.total_rows, 0) AS total_rows,
                CASE WHEN COALESCE(v.total_rows, 0) = 0 THEN ['EMPTY_DELIVERY'] ELSE [] END AS empty_r,
                CASE WHEN v.clause_2_4 = 'FAIL' THEN ['WRONG_FORMAT'] ELSE [] END AS format_r,
                CASE WHEN v.clause_3_1 = 'FAIL' OR v.clause_3_2 = 'FAIL'
                    OR v.clause_3_3 = 'FAIL' OR v.clause_3_4 = 'FAIL' THEN ['COMPLETENESS_FAIL'] ELSE [] END AS comp_r,
                CASE WHEN v.clause_4_2 = 'FAIL' THEN ['DOMAIN_FAIL'] ELSE [] END AS domain_r,
                CASE WHEN v.clause_5_4 = 'FAIL' THEN ['TOKEN_VERSION_FAIL'] ELSE [] END AS tokver_r,
                CASE WHEN v.clause_5_2 = 'FAIL' THEN ['TOKEN_RESOLUTION_LOW'] ELSE [] END AS tokres_r,
                CASE WHEN v.clause_2_2 = 'FAIL' THEN ['VOLUME_FAIL'] ELSE [] END AS vol_r,
                CASE WHEN v.clause_2_3 = 'FAIL' THEN ['VOLUME_BAND_FAIL'] ELSE [] END AS band_r,
                CASE WHEN EXISTS (
                    SELECT 1 FROM same_month_conflicts c
                    WHERE c._delivery_month = e.delivery_month
                ) THEN ['SAME_MONTH_CONFLICT'] ELSE [] END AS conflict_r
            FROM expected_deliveries e
            LEFT JOIN validation_results v ON e.delivery_month = v._delivery_month
        ),
        merged AS (
            SELECT
                _delivery_month,
                total_rows,
                list_concat(
                    list_concat(list_concat(list_concat(empty_r, format_r), comp_r), conflict_r),
                    list_concat(list_concat(domain_r, tokver_r),
                        list_concat(list_concat(tokres_r, vol_r), band_r))
                ) AS all_reasons
            FROM reasons
        )
        SELECT
            _delivery_month,
            total_rows,
            all_reasons,
            CASE
                WHEN len(list_filter(all_reasons, r -> r IN ({block_codes}))) > 0 THEN 'BLOCK'
                WHEN len(all_reasons) > 0 THEN 'LOAD_WITH_WARNINGS'
                ELSE 'LOAD'
            END AS decision,
            array_to_string(all_reasons, '|') AS reason_codes
        FROM merged
    """)
