"""Export partitioned Parquet, upload it to S3, and register Athena tables."""

import shutil
import time
from pathlib import Path

import duckdb

from .aws import athena_client, glue_client, s3_client
from .constants import CLAIMS_KEYS, DATA
from .db import delivery_month_from_key


def _raw_columns_for_key(key: str) -> str:
    """Translate a delivery's actual DuckDB schema to Athena raw DDL."""
    path = str(DATA / key).replace("'", "''")
    con = duckdb.connect()
    if key.endswith(".csv"):
        rows = con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{path}', delim='|', header=true, all_varchar=true)"
        ).fetchall()
    else:
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    con.close()
    athena_types = {
        "VARCHAR": "string",
        "INTEGER": "int",
        "BIGINT": "bigint",
        "DOUBLE": "double",
        "DATE": "date",
        "BOOLEAN": "boolean",
    }
    return ",\n".join(
        f"`{name}` {athena_types.get(dtype, 'string')}" for name, dtype, *_ in rows
    )


def export_parquet(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    """Export silver tables partitioned by _delivery_month."""
    for table in ("silver_claim_line", "silver_claim_line_quarantine", "silver_claim_header"):
        dest = out_dir / table
        shutil.rmtree(dest, ignore_errors=True)
        dest.mkdir(parents=True)
        con.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{dest}'
            (FORMAT PARQUET, PARTITION_BY (_delivery_month), OVERWRITE_OR_IGNORE)
        """)


def export_audit_tables(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    """Export validation and promotion audit tables."""
    for table in ("validation_results", "promotion_decisions", "reconciliation", "inventory"):
        try:
            dest = out_dir / table
            shutil.rmtree(dest, ignore_errors=True)
            dest.unlink(missing_ok=True)
            dest.mkdir(parents=True)
            con.execute(
                f"COPY (SELECT * FROM {table}) TO '{dest / 'data.parquet'}' "
                "(FORMAT PARQUET, OVERWRITE_OR_IGNORE)"
            )
        except duckdb.CatalogException:
            pass


def _athena_columns(table: str) -> str:
    if table == "silver_claim_header":
        return """
            claim_id string,
            vi_pid string,
            first_date_of_service date,
            last_date_of_service date,
            total_charge_amount decimal(12,2),
            total_allowed_amount decimal(12,2),
            line_count bigint,
            `_source_file` string,
            `_ingested_at` timestamp,
            `_dq_status` string,
            `_dq_reasons` string,
            `_row_hash` string,
            all_lines_current boolean
        """
    base = """
        claim_id string, claim_line int, claim_freq_code string, original_claim_id string,
        member_token string, token_version string, patient_id string, member_zip string,
        gender string, birth_year int, dx_code string, icd_version string,
        procedure_code string, place_of_service string, npi string,
        date_of_service date, paid_date date, charge_amount decimal(12,2),
        allowed_amount decimal(12,2), units int, claim_type string, source_system string,
        delivery_month string, vi_pid string, `_ingested_at` timestamp, `_source_file` string,
        `_dq_status` string, `_dq_reasons` string, `_row_hash` string
    """
    if table in {"silver_claim_line", "silver_claim_line_quarantine"}:
        return base + ", is_current boolean, is_superseded boolean"
    return base


def _audit_columns(table: str) -> str:
    if table == "promotion_decisions":
        return """
            `_delivery_month` string, total_rows bigint, all_reasons array<string>,
            decision string, reason_codes string
        """
    if table == "reconciliation":
        return """
            `_delivery_month` string, decision string, raw_rows bigint,
            quarantine_rows bigint, deduplicated_rows bigint, silver_eligible_rows bigint
        """
    if table == "inventory":
        return """
            s3_key string, delivery_month string, file_format string,
            row_count bigint, size_bytes bigint, columns_json string
        """
    return """
        `_delivery_month` string, total_rows bigint,
        dx_populated double, npi_populated double, token_populated double,
        required_populated double, token_v2_rate double, gender_valid_rate double,
        icd10_rate double, npi_len_rate double, zip_len_rate double, pos_len_rate double,
        date_order_rate double, units_valid_rate double, is_csv int, physical_format string,
        token_resolution_rate double, scaled_row_count bigint,
        clause_2_2 string, clause_2_4 string, clause_3_1 string, clause_3_2 string,
        clause_3_3 string, clause_3_4 string, clause_4_2 string, clause_5_4 string,
        clause_5_2 string, prior_three_mean double, delivery_number bigint,
        clause_2_3 string, clause_2_1 string, clause_2_5 string,
        clause_4_1 string, clause_4_3 string, clause_4_4 string, clause_4_7 string
    """


def _run_athena_ddl(cfg: dict, sql: str) -> None:
    athena = athena_client(cfg)
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": cfg["aws"]["database"]},
        WorkGroup=cfg["aws"]["workgroup"],
    )
    query_id = resp["QueryExecutionId"]
    while True:
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            print(f"athena query {query_id} succeeded")
            return
        if state in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", "unknown error")
            raise RuntimeError(f"Athena query {query_id} {state}: {reason}")
        time.sleep(0.5)


def register_raw_athena_tables(cfg: dict) -> None:
    """Register one external raw table per delivery in Athena."""
    bucket = cfg["aws"]["bucket"]
    db = cfg["aws"]["database"]
    candidate_prefix = cfg["aws"]["candidate_prefix"]
    s3 = s3_client(cfg)
    for key in CLAIMS_KEYS:
        dm = delivery_month_from_key(key)
        table = f"raw_claims_{dm.replace('-', '_')}"
        raw_key = f"{candidate_prefix}/raw/{dm}/{key.rsplit('/', 1)[-1]}"
        s3.upload_file(str(DATA / key), bucket, raw_key)
        raw_location = f"s3://{bucket}/{candidate_prefix}/raw/{dm}/"
        physical_columns = _raw_columns_for_key(key)
        if key.endswith(".csv"):
            ddl = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
                    {physical_columns}
                )
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                WITH SERDEPROPERTIES ('separatorChar' = '|')
                STORED AS TEXTFILE
                LOCATION '{raw_location}'
                TBLPROPERTIES ('skip.header.line.count'='1')
            """
        else:
            ddl = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
                    {physical_columns}
                )
                STORED AS PARQUET
                LOCATION '{raw_location}'
            """
        _run_athena_ddl(cfg, f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        _run_athena_ddl(cfg, ddl)

    _run_athena_ddl(cfg, f"DROP TABLE IF EXISTS `{db}`.`raw_member_tokens`")
    _run_athena_ddl(cfg, f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`raw_member_tokens` (
            member_token string, vi_pid string, token_version string,
            zip3 string, state string, first_seen_date date, has_email_hash boolean
        )
        STORED AS PARQUET
        LOCATION 's3://{bucket}/reference/member_tokens/'
    """)


def upload_and_register(cfg: dict, out_dir: Path) -> None:
    """Upload parquet to candidate prefix and register Athena tables."""
    bucket = cfg["aws"]["bucket"]
    prefix = cfg["aws"]["candidate_prefix"]
    db = cfg["aws"]["database"]
    s3 = s3_client(cfg)
    glue = glue_client(cfg)

    try:
        glue.create_database(DatabaseInput={"Name": db})
        print(f"created database {db}")
    except glue.exceptions.AlreadyExistsException:
        print(f"database {db} exists")

    for table in ("silver_claim_line", "silver_claim_line_quarantine", "silver_claim_header"):
        local = out_dir / table
        if not local.exists():
            continue
        remote_prefix = f"{prefix}/{table}/"
        paginator = s3.get_paginator("list_objects_v2")
        old_objects = [
            {"Key": obj["Key"]}
            for page in paginator.paginate(Bucket=bucket, Prefix=remote_prefix)
            for obj in page.get("Contents", [])
        ]
        for start in range(0, len(old_objects), 1000):
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": old_objects[start:start + 1000], "Quiet": True},
            )
        for pq in local.rglob("*.parquet"):
            rel = pq.relative_to(out_dir)
            key = f"{prefix}/{rel}"
            print(f"upload {pq} -> s3://{bucket}/{key}")
            s3.upload_file(str(pq), bucket, str(key).replace("\\", "/"))

        s3_location = f"s3://{bucket}/{prefix}/{table}/"
        ddl = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
                {_athena_columns(table)}
            )
            COMMENT '{"one row per claim" if table == "silver_claim_header" else "one row per (claim_id, claim_line), latest delivered version only"}'
            PARTITIONED BY (`_delivery_month` string)
            STORED AS PARQUET
            LOCATION '{s3_location}'
        """
        _run_athena_ddl(cfg, f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        _run_athena_ddl(cfg, ddl)
        _run_athena_ddl(cfg, f"MSCK REPAIR TABLE `{db}`.`{table}`")

    for table in ("validation_results", "promotion_decisions", "reconciliation", "inventory"):
        local = out_dir / table
        remote_prefix = f"{prefix}/{table}/"
        old_objects = [
            {"Key": obj["Key"]}
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=remote_prefix
            )
            for obj in page.get("Contents", [])
        ]
        for start in range(0, len(old_objects), 1000):
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": old_objects[start:start + 1000], "Quiet": True},
            )
        for pq in local.rglob("*.parquet"):
            s3.upload_file(str(pq), bucket, f"{remote_prefix}{pq.name}")
        _run_athena_ddl(cfg, f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        _run_athena_ddl(cfg, f"""
            CREATE EXTERNAL TABLE `{db}`.`{table}` (
                {_audit_columns(table)}
            )
            STORED AS PARQUET
            LOCATION 's3://{bucket}/{remote_prefix}'
        """)
