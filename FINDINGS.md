# Findings — Meridian claims feed

**To:** Meridian delivery lead  
**From:** Ofir Nesher, Vi Data Web team  
**Date:** 2026-09-01  
**Re:** Agreement MCDS-VI-2024-114 Schedule B, deliveries 2025-01 to 2025-12

We tested all twelve sampled deliveries against §§2–5. The following breaches require correction:

- **Delivery and schema (§§2.2–2.5):** December contained **0 claim lines** versus the 2,000,000 minimum. November contained 112,003 sampled lines versus a prior-three-delivery mean of 28,687.3, **290.4% above the mean** and outside the ±25% band. September and December were pipe-delimited CSV rather than Parquet. April and May transmitted `member_zip`, `place_of_service`, and `npi` as integers; September and December transmitted typed fields as strings, without evidence of the required advance notice.
- **Completeness (§§3.1–3.3):** `dx_code` population was **78.54%–79.35%** in every non-empty delivery versus 95%. `npi` population was **96.21%–96.66%** versus 98%. `member_token` fell below 99% in June (**97.73%**) and November (**94.68%**). The four fields governed by §3.4 were 100% populated.
- **Conformance (§4.2):** valid HL7 gender values were only **3.85% in August, 3.25% in September, and 0% in October**.
- **Uniqueness (§5.1):** **2,861** `(claim_id, claim_line)` keys recur cumulatively. March also contains **800 same-delivery keys with conflicting values**, so that delivery was blocked rather than assigned arbitrary winners.
- **Identity (§§5.2–5.4):** token resolution was **54.08% in June** and **11.35% in November**, below 85%. Only **55.65%** of June tokens and **0%** of November tokens declared v2. Across the feed, **3,096 member tokens** map to more than one vendor `patient_id`.

Please provide within ten business days: (1) root causes by clause and affected delivery, (2) a dated remediation plan, and (3) corrected Parquet re-transmissions for March and December plus the malformed deliveries above. Re-transmissions must preserve legitimate reversals and use new claim IDs for restatements as required by §5.1.

---

**Our side — remove before sending:** The Consumer Graph contains **1,100 tokens mapping to multiple `vi_pid` values**. These rows were quarantined as ambiguous identity and require Vi-owned remediation.
