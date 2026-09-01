"""Orchestrate all local pipeline stages and produce a reproducible run summary."""

import json

from .constants import WORK
from .db import connect
from .inventory import build_staging, inventory
from .publish import export_audit_tables, export_parquet, register_raw_athena_tables, upload_and_register
from .silver import build_silver
from .validate import apply_gate, run_validation


def run_inventory(cfg: dict) -> list[dict]:
    con = connect()
    inv = inventory(con)
    for r in inv:
        print(json.dumps(r, indent=2))
    con.close()
    return inv


def run_pipeline(cfg: dict, *, publish: bool = True) -> dict:
    """Full pipeline: inventory through publish."""
    con = connect()
    inventory(con)
    build_staging(con)
    run_validation(con, cfg)
    apply_gate(con, cfg)
    build_silver(con)
    duplicate_lines = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT claim_id, claim_line
            FROM silver_claim_line
            GROUP BY claim_id, claim_line
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    duplicate_headers = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT claim_id FROM silver_claim_header
            GROUP BY claim_id HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    reconciliation_failures = con.execute("""
        SELECT COUNT(*) FROM reconciliation
        WHERE decision != 'BLOCK'
          AND raw_rows != silver_eligible_rows + quarantine_rows + deduplicated_rows
    """).fetchone()[0]
    assert duplicate_lines == 0, "silver_claim_line grain is not unique"
    assert duplicate_headers == 0, "silver_claim_header grain is not unique"
    assert reconciliation_failures == 0, "row reconciliation failed"

    out_dir = WORK / "output"
    export_parquet(con, out_dir)
    export_audit_tables(con, out_dir)

    totals = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM silver_claim_line) AS silver_lines,
            (SELECT COUNT(*) FROM silver_claim_line WHERE is_current) AS current_lines,
            (SELECT SUM(charge_amount) FROM silver_claim_line WHERE is_current) AS total_charge,
            (SELECT COUNT(*) FROM silver_claim_line_quarantine) AS quarantine_lines
    """).fetchone()

    dec_cols = [d[0] for d in con.execute("DESCRIBE promotion_decisions").fetchall()]
    decisions = [
        dict(zip(dec_cols, row))
        for row in con.execute("SELECT * FROM promotion_decisions ORDER BY _delivery_month").fetchall()
    ]
    recon_cols = [d[0] for d in con.execute("DESCRIBE reconciliation").fetchall()]
    recon = [
        dict(zip(recon_cols, row))
        for row in con.execute("SELECT * FROM reconciliation ORDER BY _delivery_month").fetchall()
    ]
    con.close()

    if publish:
        upload_and_register(cfg, out_dir)
        register_raw_athena_tables(cfg)

    result = {
        "silver_lines": totals[0],
        "current_lines": totals[1],
        "total_charge": float(totals[2] or 0),
        "quarantine_lines": totals[3],
        "decisions": decisions,
        "reconciliation": recon,
    }
    (WORK / "run_summary.json").write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
    return result


def run_reload_test(cfg: dict) -> None:
    r1 = run_pipeline(cfg, publish=False)
    r2 = run_pipeline(cfg, publish=False)
    assert r1["silver_lines"] == r2["silver_lines"], "silver line count changed on replay"
    assert r1["current_lines"] == r2["current_lines"], "current line count changed on replay"
    assert abs(r1["total_charge"] - r2["total_charge"]) < 0.01, "total charge changed on replay"
    print("reload test PASSED")
