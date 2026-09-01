"""Define source objects, contracted columns, and local working paths."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / ".data"
WORK = ROOT / ".work"
DB_PATH = WORK / "pipeline.duckdb"

CLAIMS_KEYS = [
    f"raw/claims/claims_2025{m:02d}.{ext}"
    for m, ext in [
        (1, "parquet"), (2, "parquet"), (3, "parquet"), (4, "parquet"),
        (5, "parquet"), (6, "parquet"), (7, "parquet"), (8, "parquet"),
        (9, "csv"), (10, "parquet"), (11, "parquet"), (12, "csv"),
    ]
]
TOKEN_KEY = "reference/member_tokens/member_tokens.parquet"
DOC_KEYS = {
    "docs/CONTRACT.md": DATA / "docs" / "CONTRACT.md",
    "docs/SILVER_SPEC.md": DATA / "docs" / "SILVER_SPEC.md",
    "docs/submission/FINDINGS.md": ROOT / "FINDINGS.md",
    "docs/submission/NOTES.md": ROOT / "NOTES.md",
}

CONTRACTED_COLS = [
    "claim_id", "claim_line", "claim_freq_code", "original_claim_id",
    "member_token", "token_version", "patient_id", "member_zip", "gender",
    "birth_year", "dx_code", "icd_version", "procedure_code", "place_of_service",
    "npi", "date_of_service", "paid_date", "charge_amount", "allowed_amount",
    "units", "claim_type", "source_system", "delivery_month",
]
