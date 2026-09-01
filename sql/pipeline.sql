-- Post-build checks for dataweb_ofir (run in workgroup dataweb_assignment).

-- Required claim-line grain.
SELECT claim_id, claim_line, count(*) AS rows_at_grain
FROM dataweb_ofir.silver_claim_line
GROUP BY claim_id, claim_line
HAVING count(*) > 1;

-- Required claim-header grain.
SELECT claim_id, count(*) AS rows_at_grain
FROM dataweb_ofir.silver_claim_header
GROUP BY claim_id
HAVING count(*) > 1;

-- Accepted-delivery reconciliation.
SELECT *
FROM dataweb_ofir.reconciliation
WHERE decision <> 'BLOCK'
  AND raw_rows <> silver_eligible_rows + quarantine_rows + deduplicated_rows;

-- Promotion decisions and supporting measurements.
SELECT p.*, v.dx_populated, v.npi_populated, v.token_resolution_rate
FROM dataweb_ofir.promotion_decisions p
LEFT JOIN dataweb_ofir.validation_results v USING (_delivery_month)
ORDER BY _delivery_month;
