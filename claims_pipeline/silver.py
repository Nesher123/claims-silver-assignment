"""Resolve identity, disposition rows, version claims, and build silver tables."""

from datetime import datetime, timezone

import duckdb

from .db import row_hash_sql


def build_silver(con: duckdb.DuckDBPyConnection) -> None:
    """Build silver_claim_line, quarantine, and silver_claim_header."""
    rh = row_hash_sql("n")
    now = datetime.now(timezone.utc).isoformat()

    con.execute(f"""
        CREATE OR REPLACE TABLE claims_with_identity AS
        SELECT
            n.*,
            m.vi_pid,
            {rh} AS _row_hash,
            CASE WHEN m.is_ambiguous THEN true ELSE false END AS _token_ambiguous,
            CASE WHEN m.member_token IS NULL AND n.member_token IS NOT NULL AND trim(n.member_token) != ''
                THEN true ELSE false END AS _token_unresolved
        FROM normalized_claims n
        INNER JOIN promotion_decisions p ON n._delivery_month = p._delivery_month
            AND p.decision != 'BLOCK'
        LEFT JOIN member_tokens_resolved m ON n.member_token = m.member_token
    """)

    con.execute("""
        CREATE OR REPLACE TABLE row_disposition AS
        WITH classified AS (
            SELECT
                c.*,
                sm.claim_id IS NOT NULL AS _same_month_conflict,
                concat_ws('|',
                    CASE WHEN c.dx_code IS NULL OR trim(c.dx_code) = '' THEN 'DX_MISSING' END,
                    CASE WHEN c.npi IS NULL OR trim(c.npi) = '' THEN 'NPI_MISSING' END,
                    CASE WHEN c.member_token IS NULL OR trim(c.member_token) = '' THEN 'MEMBER_TOKEN_MISSING' END,
                    CASE WHEN c._token_unresolved THEN 'TOKEN_UNRESOLVED' END,
                    CASE WHEN c.token_version IS DISTINCT FROM 'v2' THEN 'TOKEN_VERSION_INVALID' END,
                    CASE WHEN c.gender NOT IN ('M', 'F', 'U') OR c.gender IS NULL THEN 'GENDER_INVALID' END,
                    CASE WHEN c.units < 1 THEN 'UNITS_INVALID' END,
                    CASE WHEN c.date_of_service > c.paid_date THEN 'DATE_ORDER_INVALID' END,
                    CASE WHEN c.charge_amount < 0 AND NOT (
                        c.claim_freq_code = '8' AND c.original_claim_id IS NOT NULL
                    ) THEN 'NEGATIVE_CHARGE_INVALID' END
                ) AS _warning_reasons
            FROM claims_with_identity c
            LEFT JOIN same_month_conflicts sm
                ON c.claim_id = sm.claim_id AND c.claim_line = sm.claim_line
                AND c._delivery_month = sm._delivery_month
        )
        SELECT
            * EXCLUDE (_same_month_conflict, _warning_reasons),
            CASE
                WHEN _same_month_conflict OR _token_ambiguous THEN 'QUARANTINE'
                WHEN _warning_reasons != '' THEN 'WARN'
                ELSE 'PASS'
            END AS _dq_status,
            CASE
                WHEN _same_month_conflict THEN 'SAME_MONTH_CONFLICT'
                WHEN _token_ambiguous THEN 'TOKEN_AMBIGUOUS'
                ELSE _warning_reasons
            END AS _dq_reasons
        FROM classified
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dedup_ranked AS
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY claim_id, claim_line, _row_hash
                ORDER BY _delivery_month
            ) AS dup_rank
        FROM row_disposition
        WHERE _dq_status != 'QUARANTINE'
    """)

    con.execute("CREATE OR REPLACE TABLE deduplicated_rows AS SELECT * FROM dedup_ranked WHERE dup_rank > 1")

    con.execute("""
        CREATE OR REPLACE TABLE version_ranked AS
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY claim_id, claim_line
                ORDER BY _delivery_month DESC, _row_hash DESC
            ) AS version_rank
        FROM dedup_ranked
        WHERE dup_rank = 1
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE silver_claim_line AS
        SELECT
            claim_id, claim_line, claim_freq_code, original_claim_id,
            member_token, token_version, patient_id, member_zip, gender,
            birth_year, dx_code, icd_version, procedure_code, place_of_service,
            npi, date_of_service, paid_date, charge_amount, allowed_amount,
            units, claim_type, source_system, delivery_month,
            vi_pid,
            CAST('{now}' AS TIMESTAMP) AS _ingested_at,
            _source_file,
            _delivery_month,
            _dq_status,
            _dq_reasons,
            _row_hash,
            CASE WHEN EXISTS (
                SELECT 1 FROM version_ranked replacement
                WHERE replacement.claim_freq_code = '7'
                  AND replacement.original_claim_id = version_ranked.claim_id
                  AND replacement.version_rank = 1
            ) THEN false ELSE true END AS is_current,
            CASE WHEN EXISTS (
                SELECT 1 FROM version_ranked replacement
                WHERE replacement.claim_freq_code = '7'
                  AND replacement.original_claim_id = version_ranked.claim_id
                  AND replacement.version_rank = 1
            ) THEN true ELSE false END AS is_superseded
        FROM version_ranked
        WHERE _dq_status IN ('PASS', 'WARN') AND version_rank = 1
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE silver_claim_line_quarantine AS
        SELECT
            claim_id, claim_line, claim_freq_code, original_claim_id,
            member_token, token_version, patient_id, member_zip, gender,
            birth_year, dx_code, icd_version, procedure_code, place_of_service,
            npi, date_of_service, paid_date, charge_amount, allowed_amount,
            units, claim_type, source_system, delivery_month,
            vi_pid,
            CAST('{now}' AS TIMESTAMP) AS _ingested_at,
            _source_file,
            _delivery_month,
            'QUARANTINE' AS _dq_status,
            _dq_reasons,
            _row_hash,
            false AS is_current,
            false AS is_superseded
        FROM row_disposition
        WHERE _dq_status = 'QUARANTINE'
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver_claim_header AS
        SELECT
            claim_id,
            ANY_VALUE(vi_pid) AS vi_pid,
            MIN(date_of_service) AS first_date_of_service,
            MAX(date_of_service) AS last_date_of_service,
            SUM(charge_amount) AS total_charge_amount,
            SUM(allowed_amount) AS total_allowed_amount,
            COUNT(*) AS line_count,
            MAX(_delivery_month) AS _delivery_month,
            MAX(_source_file) AS _source_file,
            MAX(_ingested_at) AS _ingested_at,
            CASE WHEN bool_or(_dq_status = 'WARN') THEN 'WARN' ELSE 'PASS' END AS _dq_status,
            string_agg(DISTINCT NULLIF(_dq_reasons, ''), '|') AS _dq_reasons,
            md5(concat_ws('|',
                claim_id,
                coalesce(ANY_VALUE(vi_pid), ''),
                CAST(MIN(date_of_service) AS VARCHAR),
                CAST(MAX(date_of_service) AS VARCHAR),
                CAST(SUM(charge_amount) AS VARCHAR),
                CAST(SUM(allowed_amount) AS VARCHAR),
                CAST(COUNT(*) AS VARCHAR)
            )) AS _row_hash,
            bool_and(is_current) AS all_lines_current
        FROM silver_claim_line
        WHERE is_current
        GROUP BY claim_id
    """)

    con.execute("""
        CREATE OR REPLACE TABLE reconciliation AS
        SELECT
            p._delivery_month,
            p.decision,
            p.total_rows AS raw_rows,
            COALESCE((SELECT COUNT(*) FROM row_disposition r
                WHERE r._delivery_month = p._delivery_month AND r._dq_status = 'QUARANTINE'), 0) AS quarantine_rows,
            COALESCE((SELECT COUNT(*) FROM deduplicated_rows d
                WHERE d._delivery_month = p._delivery_month), 0)
                + COALESCE((SELECT COUNT(*) FROM version_ranked vr
                    WHERE vr._delivery_month = p._delivery_month AND vr.version_rank > 1), 0)
                AS deduplicated_rows,
            COALESCE((SELECT COUNT(*) FROM version_ranked vr
                WHERE vr._delivery_month = p._delivery_month AND vr.version_rank = 1), 0)
                AS silver_eligible_rows
        FROM promotion_decisions p
    """)
