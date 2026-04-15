---
name: active-otb-analysis
description: "Generate the Active OTB (On-the-Books) HTML coverage report for an AMSExpansion fiscal quarter. Combines OTB Google Sheet accounts (via Glean), SetSail activity, and AGG_MONTHLY run rates into a filterable HTML dashboard grouped by DM. Use when: OTB report, active OTB analysis, on-the-books coverage, AE coverage report, OTB HTML report, refresh OTB data."
---

# Active OTB Analysis — HTML Report

Produces `/Users/nathomas/Downloads/Active_OTB_Analysis.html` from live Snowflake data.

## Hardcoded Constants

```
OTB_SHEET_URL = https://docs.google.com/spreadsheets/d/1rPURUkIlunN9nMICZmZLbdIbtdrZMjsrYgfrUSTvvpA/edit?gid=1793251121#gid=1793251121
THEATER       = AMSExpansion
FQ_LABEL      = Q1 FY27
FQ_START      = 2026-02-01
FQ_END        = 2026-04-30
CONNECTION    = MyConnection
OUTPUT_PATH   = ~/Downloads/Active_OTB_Analysis.html
SCRIPT        = /tmp/generate_otb_html.py
```

**Active threshold:** ≥5 activities **or** ≥3.0 meeting hours **or** ≥1 UC created **or** ≥1 UC won during coverage period.

---

## ⚠️ MANDATORY FIRST STEP — Ask Before Doing Anything

**Before running any query, opening any tool, or executing any code, you MUST ask the user which path they want.**

Use AskUserQuestion with these options:
- **Path A** — Regenerate the HTML using hardcoded data (`--no-snowflake`). No Snowflake or Glean. ~5 seconds.
- **Path B** — Pull fresh data from Snowflake and regenerate (`--refresh`). 3 queries run in parallel, ~10 seconds. Uses cache for 4h after that.
- **Path C** — Full refresh: re-read OTB sheet via Glean, re-enrich AE/DM names, then run `--refresh`. Use only for new quarter or account changes.

Do not assume Path C. Most requests are Path A or B. Only proceed once the user has confirmed their choice.

---

## Path A — Regenerate Report (no data changes)

No Glean or Snowflake needed. The script uses hardcoded data.

```bash
python3 /tmp/generate_otb_html.py --no-snowflake
open ~/Downloads/Active_OTB_Analysis.html
```

---

## Path B — Refresh Data (same accounts, new numbers)

```bash
python3 /tmp/generate_otb_html.py           # uses cache if < 4h old
python3 /tmp/generate_otb_html.py --refresh # forces fresh Snowflake queries
open ~/Downloads/Active_OTB_Analysis.html
```

---

## Path C — New Quarter or Account Changes (full refresh)

Use when the OTB sheet has been updated with new accounts or a new quarter begins.
Run steps 1–3 in order.

---

## Workflow

### Step 1: Fetch OTB Sheet

Use `mcp__glean__read_document` with the hardcoded URL:
```
https://docs.google.com/spreadsheets/d/1rPURUkIlunN9nMICZmZLbdIbtdrZMjsrYgfrUSTvvpA/edit?gid=1793251121#gid=1793251121
```

**Sheet column map** (0-indexed after splitting on `|`):
- col[1] = DM email (submitter)
- col[2] = Theater (filter to `AMSExpansion`)
- col[4] = AE name
- col[5] = Coverage start date
- col[9] = Account name
- col[10] = SFDC Account ID

**Filtering — apply both before storing any row:**
1. `col[2] == "<THEATER>"` (e.g. `AMSExpansion`)
2. `col[5] >= "<FQ_START>"` — **drop any row with a start date before the current FQ**. Prior quarter entries for the same accounts will have earlier start dates and must be excluded.

**Multi-account rows:** Some rows have multiple SFDC IDs in col[10]. Split using regex `00[A-Za-z0-9]{13,16}` to find all IDs; split account names by comma to match count.

**Known AE name aliases** (sheet → Salesforce):
```
"Ali Maahs"       → "Alessandra Maahs"
"Matt Loewel"     → "Matthew Loewel"
"danny king"      → "Danny King"
"Eric Brueninger" → "Eric Brueninger"  # verify spelling each run
```

Save parsed accounts to `/tmp/otb_accounts.json`:
```json
[{"ae": "...", "dm_email": "...", "account": "...", "account_id": "...", "start_date": "YYYY-MM-DD"}]
```

**⚠️ STOP** — verify before proceeding:
- Account count matches expectation (Q1 FY27 AMSExpansion = 45)
- No duplicate `account_id` values
- All `start_date` values are valid dates within the FQ
- **All AE names match known Snowflake employees** — cross-check every parsed AE name against the existing `AE_EMAIL` dict. Any name not in `AE_EMAIL` is a parse error (leaked header row, note cell, or adjacent text). Remove those rows before continuing.
- Spot-check 3–4 rows: confirm the account name and SFDC ID are correctly paired (multi-account rows are prone to name/ID order mismatch — verify by looking up one ID in SFDC)

---

### Step 2: Update Script + Refresh Data (automated)

Once `/tmp/otb_accounts.json` passes all Step 1 checks, run:

```bash
python3 /tmp/generate_otb_html.py --update-accounts /tmp/otb_accounts.json
```

Watch stdout for warnings — any `Warning:` line means an AE or DM lookup failed and needs manual correction before proceeding.

Expected output:
```
Script patched: /tmp/generate_otb_html.py
Fetching from Snowflake (3 queries in parallel)...
Done (Xs) — cache: ~/.otb_cache.json
Written: ~/Downloads/Active_OTB_Analysis.html
Total accounts: N | Active: X | Inactive: Y
```

---

### Step 3: Open and Verify

The HTML is already written by Step 2. Open it:

```bash
open ~/Downloads/Active_OTB_Analysis.html
```

**⚠️ STOP** — confirm before sharing:
- Account count in stdout matches expectation (Q1 FY27 AMSExpansion = 45)
- No `Warning:` lines appeared in Step 2 stdout
- Coverage End date shows `2026-04-30` for all rows

---
