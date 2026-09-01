# Meridian Claims Contract Audit and Silver Pipeline

A small DuckDB-to-Athena pipeline that inventories twelve vendor deliveries, measures the data contract, assigns one promotion decision per delivery, resolves `member_token → vi_pid`, and publishes typed, deduplicated silver tables.

## Architecture

1. Download immutable claims, token graph, and assignment documents from S3.
2. Inventory each file's physical format/schema before casting.
3. Normalize mixed Parquet and pipe-delimited CSV in DuckDB with safe casts.
4. Measure per-delivery and cumulative contract checks.
5. Apply `LOAD`, `LOAD_WITH_WARNINGS`, or `BLOCK` before silver.
6. Resolve identity, quarantine ambiguity, deduplicate/version claim lines, and reconcile every accepted row.
7. Export partitioned Parquet, upload it to the candidate prefix, and register Athena tables.

The implementation is split by responsibility under `claims_pipeline/`; `pipeline.py` is only a compatibility entrypoint.

## Setup

Prerequisites: Python 3.11+, `uv`, AWS credentials for the assignment account, and region `us-east-1`. Supply credentials through the normal AWS environment/profile chain; never commit them.

```bash
uv sync --dev
source .venv/bin/activate
```

See `.env.example` for optional environment variable names and `ACCESS.example.md` for the sanitized access-document shape. Keep populated `.env` and `ACCESS.md` local; both are Git-ignored.

## Run

```bash
# Download all supplied objects and official report templates
claims-pipeline download

# Print physical schema/row-count evidence
claims-pipeline inventory

# Build locally, upload outputs, and register Athena tables
claims-pipeline run

# Rebuild twice and assert identical counts/totals
claims-pipeline reload-test

# Unit checks
pytest -q
```

Use Athena workgroup `dataweb_assignment`; the default workgroup points at an S3 result bucket this IAM user cannot write.

The Athena inventory is directly queryable:

```sql
SELECT s3_key, delivery_month, file_format, row_count, size_bytes, columns_json
FROM dataweb_ofir.inventory
ORDER BY delivery_month;
```

## Athena tables

Database: `dataweb_ofir`

- Required: `silver_claim_line`, `silver_claim_header`
- Traceability: `silver_claim_line_quarantine`
- Audit: `inventory`, `validation_results`, `promotion_decisions`, `reconciliation`
- Raw: `raw_claims_2025_01` through `raw_claims_2025_12`, plus `raw_member_tokens`

The raw tables deliberately preserve each file's physical schema, including vendor drift. Silver normalizes identifiers to strings, money to `decimal(12,2)`, dates to `date`, and partitions on `_delivery_month`.

## Important behavior

- March and December block as whole deliveries; blocked rows never enter silver.
- Exact replays collapse by stable row hash.
- For changed versions of the same key, the later delivery wins.
- Frequency-7 replacements mark their originals non-current; frequency-8 reversals are retained.
- Unresolved tokens stay in silver with `vi_pid = NULL`; ambiguous Vi mappings quarantine.
- Build assertions enforce claim-line/header grain and row reconciliation.
