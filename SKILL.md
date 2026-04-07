---
name: active-otb-analysis
description: "Generate the Active OTB (On-the-Books) HTML coverage report for an AMSExpansion fiscal quarter. Combines OTB Google Sheet accounts (via Glean), SetSail activity, and AGG_MONTHLY run rates into a filterable HTML dashboard grouped by DM. Use when: OTB report, active OTB analysis, on-the-books coverage, AE coverage report, OTB HTML report, refresh OTB data."
---

# Active OTB Analysis — HTML Report

Produces `/Users/nathomas/Downloads/Active_OTB_Analysis.html` from three live data sources.

## Parameters

| Parameter | Required | Example |
|---|---|---|
| `<OTB_SHEET_URL>` | Yes | Glean URL for the OTB Google Sheet |
| `<THEATER>` | Yes | `AMSExpansion` |
| `<FQ_LABEL>` | Yes | `Q1 FY27` |
| `<FQ_START>` | Yes | `2026-02-01` |
| `<FQ_END>` | Yes | `2026-04-30` |
| `<SNOWFLAKE_CONNECTION>` | No | `MyConnection` (default) |
| `<OUTPUT_PATH>` | No | `~/Downloads/Active_OTB_Analysis.html` (default) |

**Active threshold:** ≥5 activities **or** ≥3.0 meeting hours since coverage start date.

---

## Workflow

### Step 1: Fetch OTB Sheet

Use `mcp__glean__read_document` with `<OTB_SHEET_URL>`.

**Sheet column map** (0-indexed after splitting on `|`):
- col[1] = DM email (submitter)
- col[2] = Theater (filter to `<THEATER>`)
- col[4] = AE name
- col[5] = Coverage start date
- col[9] = Account name
- col[10] = SFDC Account ID

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

**⚠️ STOP** — confirm account count and spot-check a few rows before proceeding.

---

### Step 2: Enrich AE Emails + DM Names

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

Run once per unique (AE email, account_id) pair. Use a VALUES CTE to batch all pairs:

```sql
WITH pairs(ae_email, account_id, start_date) AS (
  VALUES
    ('<ae1@snowflake.com>', '<sfdc_id_1>', '<start_date_1>'::DATE),
    ('<ae2@snowflake.com>', '<sfdc_id_2>', '<start_date_2>'::DATE)
    -- ... all pairs
)
SELECT
  p.ae_email,
  p.account_id,
  COUNT(*)                                              AS total_acts,
  COUNT(CASE WHEN a.ACTIVITY_TYPE = 'MEETING' THEN 1 END) AS meetings,
  COUNT(CASE WHEN a.ACTIVITY_TYPE = 'EMAIL'   THEN 1 END) AS emails,
  COALESCE(SUM(CASE WHEN a.ACTIVITY_TYPE = 'MEETING'
                    THEN a.DURATION / 60.0 END), 0)    AS meeting_hrs
FROM pairs p
LEFT JOIN SALES.ACTIVITY.SETSAIL_RAW_ACTIVITY a
  ON  a.EMAIL      = p.ae_email
  AND a.ACCOUNT_ID = p.account_id
  AND a.ACTIVITY_DATE BETWEEN p.start_date AND CURRENT_DATE()
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
WITH accounts(account_id, start_date) AS (
  VALUES
    ('<sfdc_id_1>', '<start_date_1>'::DATE),
    ('<sfdc_id_2>', '<start_date_2>'::DATE)
    -- ...
),
rr_start AS (
  SELECT
    a.account_id,
    SUM(m.AVG_DAILY_REVENUE) * 365 AS rr_start,
    MAX(m.CONSUMPTION_MONTH)        AS rr_start_month
  FROM accounts a
  JOIN SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS m
    ON m.SALESFORCE_ACCOUNT_ID = a.account_id
  WHERE m.IS_CURRENT_MONTH = FALSE
    AND m.CONSUMPTION_MONTH = DATEADD('month', -1, DATE_TRUNC('month', a.start_date))
  GROUP BY 1
),
rr_current AS (
  SELECT
    a.account_id,
    SUM(m.AVG_DAILY_REVENUE) * 365 AS rr_current,
    MAX(m.CONSUMPTION_MONTH)        AS rr_current_month
  FROM accounts a
  JOIN SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS m
    ON m.SALESFORCE_ACCOUNT_ID = a.account_id
  WHERE m.IS_CURRENT_MONTH = FALSE
  GROUP BY 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY MAX(m.CONSUMPTION_MONTH) DESC) = 1
)
SELECT
  a.account_id,
  s.rr_start,
  TO_CHAR(s.rr_start_month,  'Mon YYYY') AS rr_start_month,
  c.rr_current,
  TO_CHAR(c.rr_current_month,'Mon YYYY') AS rr_current_month
FROM accounts a
LEFT JOIN rr_start   s ON s.account_id = a.account_id
LEFT JOIN rr_current c ON c.account_id = a.account_id;
```

Store results as `RR_DATA` dict: `{account_id: (rr_start, rr_start_month, rr_current, rr_current_month)}`.

---

### Step 5: Update Generator Script

The script lives at `/tmp/generate_otb_html.py`. Update these four hardcoded dicts:

1. **`ACCOUNTS`** — list of `(ae, dm, account, acct_id, start_date)` tuples (one per row, multi-account rows already split)
2. **`AE_EMAIL`** — `{ae_name: ae_email}` from Step 2
3. **`ACTIVITY`** — `{(acct_id, ae_email): (acts, mtgs, emails, hrs)}` from Step 3
4. **`RR_DATA`** — `{acct_id: (rr_start, rr_start_month, rr_current, rr_current_month)}` from Step 4

Also update header constants:
```python
GENERATED_AT = "<today>"
GEO          = "<THEATER>"
FQ_LABEL     = "<FQ_LABEL>"
FQ_START     = "<FQ_START>"
FQ_END       = "<FQ_END>"
```

---

### Step 6: Generate and Open

```bash
python3 /tmp/generate_otb_html.py
open ~/Downloads/Active_OTB_Analysis.html
```

Expected output line:
```
Written: ~/Downloads/Active_OTB_Analysis.html
Total accounts: N | Active: X | Inactive: Y
DMs: D | AEs: A
```

**⚠️ STOP** — review the report. Spot-check:
- Active/inactive counts look reasonable
- RR deltas (↑/↓) are plausible
- No accounts missing run rate data (shown as —)

---

## Key Tables

| Table | Purpose |
|---|---|
| `FIVETRAN.SALESFORCE.USER` | AE/DM name → email lookup |
| `SALES.ACTIVITY.SETSAIL_RAW_ACTIVITY` | Activity counts and meeting hours |
| `SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS` | Run rates (AVG_DAILY_REVENUE × 365, summed across all product categories) |

Warehouse: `SNOWADHOC`

---

## Stopping Points

- ✋ **Step 1** — confirm parsed account count before querying Snowflake
- ✋ **Step 6** — review output before sharing

## Output

`~/Downloads/Active_OTB_Analysis.html` — filterable HTML dashboard:
- Grouped by DM → AE → accounts
- Active/Inactive toggle (≥5 acts or ≥3 hrs meetings)
- RR at start, current RR, delta indicator (↑/↓/→)
- SFDC links per account
