# Notes

## Airflow

One monthly DAG would run:

`discover_delivery → copy_raw → inventory_schema → normalize → [contract_checks, identity_checks, cross_delivery_checks] → promotion_gate → build_affected_partitions → reconcile → publish`

The gate always records its decision and measurements. A blocked delivery stops before silver and pages the on-call owner with the month, failed clauses, and run link. Warnings publish but appear in the notification and audit table. A rerun replaces its output partition; cross-delivery version changes also rebuild partitions containing affected claim keys. Stable row hashes collapse exact replays. Airflow retries transient S3/Athena errors, but not deterministic contract failures.

## Cost

At 100M lines, cost is driven by bytes scanned, repeated full-history windows, small files, Parquet encoding/compression, and the token-graph join. Track Athena `DataScannedInBytes`, execution time and failures per task/delivery, S3 requests, output file counts/sizes, and rows processed/quarantined.

Partition silver by delivery month and target roughly 128–512 MB compressed Parquet files. Project only required columns, compact small files, and avoid full rebuilds: identify changed claim keys and replace only affected partitions. Pre-deduplicate the token graph to one row per token and use the smaller token projection as the hash-join build side. Keep compact audit summaries separate from claim facts.

## Decisions

- The feed is a 1:80 even sample. Absolute volume uses sampled rows ×80; percentages and relative volume remain unscaled.
- Timeliness (§2.1) and advance notice (§2.5) are untestable: S3 provisioning timestamps are not vendor delivery timestamps.
- Risk-based promotion blocks unreadable/empty files and unordered same-month conflicts; transformable defects load with warnings.
- Exact same-key/same-hash replays are deduplicated. For changed same keys across deliveries, later `delivery_month` wins. Conflicting versions within one delivery have no ordering, so that whole delivery blocks.
- Frequency-7 replacements mark the referenced original non-current. Frequency-8 reversals are retained so they can offset original money.
- Unresolved tokens remain with `vi_pid = NULL`; tokens mapping to multiple `vi_pid` values quarantine.
- This exercise rebuilds the complete snapshot; production would replace only partitions affected by changed keys.

## Security

The supplied records are explicitly synthetic and contain no real people or PHI. The pipeline retains vendor `patient_id` only as a reference and never uses it as a person key; identity is resolved exclusively through `member_token → vi_pid`. It does not attempt to reverse or re-identify tokens. In production, `patient_id`, member tokens, demographics, and claims would still require least-privilege table access, audited queries, encryption, retention controls, and purpose-limited use.

## Not finished

- The optional `silver_provider` table was intentionally omitted.
- Full ICD-10-CM and CMS Place of Service reference sets were not supplied; checks therefore use transmitted version and structural conformance rather than unavailable membership lists.

## Re-load

`claims-pipeline reload-test` rebuilt identical inputs twice:

- Silver claim lines: **362,056 → 362,056**
- Current claim lines: **359,188 → 359,188**
- Current charge total: **$224,434,649.23 → $224,434,649.23**
- Quarantine lines: **1,976 → 1,976**

Athena independently returned 362,056 claim lines, 189,637 claim headers, and 1,976 quarantine lines.

## Tools

Python 3.11, DuckDB, boto3/AWS CLI, Athena/Glue, uv, pytest, and Cursor AI assistance were used. AI helped interpret the specifications, draft code/docs, and debug schema publishing; all outputs were executed and verified against supplied data.
