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
- **Path A** — Regenerate the HTML report using existing hardcoded data (no Snowflake, no Glean, ~5 seconds)
- **Path B** — Pull fresh activity, run rate, and UC data from Snowflake, then regenerate (same accounts, new numbers)
- **Path C** — Full refresh: re-read the OTB sheet via Glean, re-enrich AE/DM names, re-run all Snowflake queries (use only for new quarter or account changes)

Do not assume Path C. Most requests are Path A or B. Only proceed once the user has confirmed their choice.

---

## Which Path to Use?

| Situation | Path |
|---|---|
| Just want to regenerate the HTML with no data changes | **Path A** |
| Accounts are the same but want fresh activity/RR/UC numbers | **Path B** |
| New quarter, or accounts were added/removed from the OTB sheet | **Path C** |

---

## Path A — Regenerate Report (no data changes)

No Glean or Snowflake needed. The script has all data hardcoded.

```bash
python3 /tmp/generate_otb_html.py
open ~/Downloads/Active_OTB_Analysis.html
```

Expected output:
```
Written: ~/Downloads/Active_OTB_Analysis.html
Total accounts: N | Active: X | Inactive: Y
DMs: D | AEs: A
```

Then run the **Step 7 verification checklist** below before sharing.

---

## Path B — Refresh Data (same accounts, new numbers)

Run Steps 3–5 to pull fresh Snowflake data, update the script dicts, then regenerate.

Skip directly to **Step 3** — do not re-run Glean or re-enrich AE/DM names.

---

## Path C — New Quarter or Account Changes (full refresh)

Use when the OTB sheet has been updated with new accounts or a new quarter begins.
Run all steps 1–7 in order.

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

### Step 2: Enrich AE Emails + DM Names

*(Path C only — skip if accounts haven't changed)*

**AE emails** — query by name (handle Salesforce name variants with `ILIKE '%last_name%'`):
```sql
SELECT NAME, EMAIL
FROM FIVETRAN.SALESFORCE.USER
WHERE IS_ACTIVE = TRUE
  AND EMAIL ILIKE '%@snowflake.com%'
  AND NAME ILIKE '%<last_name>%';
```

**DM names** — look up each unique DM email:
```sql
SELECT NAME, EMAIL
FROM FIVETRAN.SALESFORCE.USER
WHERE EMAIL = '<dm_email>';
```

Update `AE_EMAIL` and `DM_NAME` dicts in the generator script.

---

### Step 3: Query SetSail Activity

Run once per unique (AE email, account_id) pair. Use a VALUES CTE to batch all pairs.

**Note on Snowflake VALUES syntax:** Use `FROM (VALUES (...)) AS t(col1, col2, ...)` — do NOT use `WITH cte(col1) AS (VALUES (...))` which is rejected by Snowflake.

```sql
SELECT
  p.ae_email,
  p.account_id,
  COUNT(*)                                              AS total_acts,
  COUNT(CASE WHEN a.ACTIVITY_TYPE = 'MEETING' THEN 1 END) AS meetings,
  COUNT(CASE WHEN a.ACTIVITY_TYPE = 'EMAIL'   THEN 1 END) AS emails,
  COALESCE(SUM(CASE WHEN a.ACTIVITY_TYPE = 'MEETING'
                    THEN a.DURATION / 60.0 END), 0)    AS meeting_hrs
FROM (VALUES
  ('<ae1@snowflake.com>', '<sfdc_id_1>', '<start_date_1>'::DATE, '<end_date>'::DATE),
  ('<ae2@snowflake.com>', '<sfdc_id_2>', '<start_date_2>'::DATE, '<end_date>'::DATE)
  -- ... all pairs
) AS p(ae_email, account_id, start_date, end_date)
LEFT JOIN SALES.ACTIVITY.SETSAIL_RAW_ACTIVITY a
  ON  a.EMAIL      = p.ae_email
  AND a.ACCOUNT_ID = p.account_id
  AND a.ACTIVITY_DATE BETWEEN p.start_date AND LEAST(p.end_date, CURRENT_DATE())
GROUP BY 1, 2;
```

Store results as `ACTIVITY` dict keyed by `(account_id, ae_email)` → `(acts, mtgs, emails, hrs)`.

---

### Step 4: Query AGG_MONTHLY Run Rates

**RR at start** = last complete month before coverage start date:
```
RR_START_MONTH = DATEADD('month', -1, DATE_TRUNC('month', start_date))
```
e.g. Feb 1 or Feb 2 start → Jan 2026; Apr 1 start → Mar 2026.

**Current RR** = latest month where `IS_CURRENT_MONTH = FALSE`.

```sql
WITH
rr_start AS (
  SELECT
    p.account_id,
    SUM(m.AVG_DAILY_REVENUE) * 365 AS rr_start,
    MAX(m.CONSUMPTION_MONTH)        AS rr_start_month
  FROM (VALUES
    ('<sfdc_id_1>', '<start_date_1>'::DATE),
    ('<sfdc_id_2>', '<start_date_2>'::DATE)
  ) AS p(account_id, start_date)
  JOIN SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS m
    ON m.SALESFORCE_ACCOUNT_ID = p.account_id
  WHERE m.IS_CURRENT_MONTH = FALSE
    AND m.CONSUMPTION_MONTH = DATEADD('month', -1, DATE_TRUNC('month', p.start_date))
  GROUP BY 1
),
rr_current AS (
  SELECT
    p.account_id,
    SUM(m.AVG_DAILY_REVENUE) * 365 AS rr_current,
    MAX(m.CONSUMPTION_MONTH)        AS rr_current_month
  FROM (VALUES
    ('<sfdc_id_1>', '<start_date_1>'::DATE),
    ('<sfdc_id_2>', '<start_date_2>'::DATE)
  ) AS p(account_id, start_date)
  JOIN SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS m
    ON m.SALESFORCE_ACCOUNT_ID = p.account_id
  WHERE m.IS_CURRENT_MONTH = FALSE
  GROUP BY 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.account_id ORDER BY MAX(m.CONSUMPTION_MONTH) DESC) = 1
)
SELECT
  s.account_id,
  s.rr_start,
  TO_CHAR(s.rr_start_month,  'Mon YYYY') AS rr_start_month,
  c.rr_current,
  TO_CHAR(c.rr_current_month,'Mon YYYY') AS rr_current_month
FROM rr_start s
LEFT JOIN rr_current c ON c.account_id = s.account_id;
```

Store results as `RR_DATA` dict: `{account_id: (rr_start, rr_start_month, rr_current, rr_current_month)}`.

---

### Step 5: Query Use Cases

Source: `MDM.MDM_INTERFACES.DIM_USE_CASE`

- **Created**: `CREATED_DATE BETWEEN start_date AND end_date`
- **Won**: `DECISION_DATE BETWEEN start_date AND LEAST(end_date, CURRENT_DATE())` AND `STAGE_NUMBER BETWEEN 4 AND 7`
  - Stage 4 = "Use Case Won / Migration Plan", Stage 7 = "Deployed", Stage 8 = "Lost" (excluded)

```sql
SELECT
    p.account_id,
    COUNT(CASE WHEN u.CREATED_DATE BETWEEN p.start_date AND p.end_date THEN 1 END) AS uc_created,
    COUNT(CASE WHEN u.DECISION_DATE BETWEEN p.start_date AND LEAST(p.end_date, CURRENT_DATE())
                AND u.STAGE_NUMBER BETWEEN 4 AND 7 THEN 1 END)                     AS uc_won
FROM (VALUES
  ('<sfdc_id_1>', '<start_date_1>'::DATE, '<end_date>'::DATE),
  ('<sfdc_id_2>', '<start_date_2>'::DATE, '<end_date>'::DATE)
) AS p(account_id, start_date, end_date)
LEFT JOIN MDM.MDM_INTERFACES.DIM_USE_CASE u
  ON u.ACCOUNT_ID = p.account_id
GROUP BY 1
ORDER BY 1;
```

Store results as `UC_DATA` dict: `{account_id: (uc_created, uc_won)}`.

---

### Step 6: Update Generator Script

The script lives at `/tmp/generate_otb_html.py`. Update only the dicts that changed:

| Dict | When to update |
|---|---|
| `ACCOUNTS` | Path C only (account list changed) |
| `AE_EMAIL` | Path C only (account list changed) |
| `ACTIVITY` | Paths B + C (fresh activity data) |
| `RR_DATA` | Paths B + C (fresh RR data) |
| `UC_DATA` | Paths B + C (fresh UC data) |

Also update header constants (Path C only):
```python
GENERATED_AT = "<today>"
FQ_LABEL     = "<FQ_LABEL>"
FQ_START     = "<FQ_START>"
FQ_END       = "<FQ_END>"
```

---

### Step 7: Generate and Open

```bash
python3 /tmp/generate_otb_html.py
open ~/Downloads/Active_OTB_Analysis.html
```

Expected output:
```
Written: ~/Downloads/Active_OTB_Analysis.html
Total accounts: N | Active: X | Inactive: Y
DMs: D | AEs: A
```

**⚠️ STOP** — review before sharing:
- Verify: active/inactive split is plausible (not 0 active or all active)
- Verify: spot-check 2–3 accounts — confirm activity counts match SetSail manually
- Verify: UC Created/Won counts look reasonable (most accounts 0; flag any with >5 as suspicious)
- Verify: no accounts show `—` for both RR at Start and Current RR unless the account is truly new
- Verify: Coverage End date shows `2026-04-30` for all rows
- Verify: Active badge fires correctly for accounts with UC data but low activity counts

---

## Key Tables

| Table | Purpose |
|---|---|
| `FIVETRAN.SALESFORCE.USER` | AE/DM name → email lookup |
| `SALES.ACTIVITY.SETSAIL_RAW_ACTIVITY` | Activity counts and meeting hours |
| `SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS` | Run rates (AVG_DAILY_REVENUE × 365, summed across all product categories) |
| `MDM.MDM_INTERFACES.DIM_USE_CASE` | Use cases created + won during coverage period |

Warehouse: `SNOWADHOC`

---

## Output

`~/Downloads/Active_OTB_Analysis.html` — filterable HTML dashboard:
- Grouped by DM → AE → accounts
- Active/Inactive toggle (≥5 acts or ≥3 hrs or ≥1 UC created/won)
- Columns: Account, Coverage Start, Coverage End, RR at Start, Current RR, RR Change, Activities, Meetings, Emails, Mtg Hrs, UCs Created, UCs Won, Status, Action
- RR delta indicator (↑/↓/→)
- SFDC links per account
