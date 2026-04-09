#!/usr/bin/env python3
"""Active OTB Analysis - HTML Report Generator
Q1 FY27 | AMSExpansion | SetSail Activity vs OTB Coverage

Data sources:
  - Accounts:  Glean (OTB Google Sheet) + FIVETRAN.SALESFORCE.USER (AE/DM names)
  - Activity:  SALES.ACTIVITY.SETSAIL_RAW_ACTIVITY  (coverage start → today)
  - Run Rates: SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS
               (AVG_DAILY_REVENUE × 365, summed across all product categories)
               RR at start  = last complete month before coverage start date
               Current RR   = latest complete month (IS_CURRENT_MONTH = FALSE)

Generated: 2026-04-07
"""

import re
from datetime import datetime, date

# ─── Constants ────────────────────────────────────────────────────────────────
REPORT_TITLE     = "Active OTB Analysis"
REPORT_SUBTITLE  = "Q1 FY27 &nbsp;|&nbsp; AMSExpansion &nbsp;|&nbsp; SetSail Coverage"
GENERATED_AT     = "2026-04-07"
SFDC_BASE        = "https://snowforce.lightning.force.com"

ACTIVE_MIN_ACTS  = 5
ACTIVE_MIN_HRS   = 3.0

GEO     = "AMSExpansion"
FQ_END  = "2026-04-30"

# ─── Raw Data ─────────────────────────────────────────────────────────────────
# Columns: (ae, dm, account, account_id, start_date)
ACCOUNTS = [
    ("DJ Kline",          "Nicolette Mullenix",      "Trimble",                         "0013r00002EEIpOAAX", "2026-02-01"),
    ("DJ Kline",          "Nicolette Mullenix",      "Crocs",                            "0013100001nnZZjAAM", "2026-02-01"),
    ("DJ Kline",          "Nicolette Mullenix",      "Starz",                            "0010Z000025Ao5UQAS", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "TVI, Inc",                         "0013100001rsDOuAAM", "2026-02-01"),
    ("Sawyer Ramsey",     "Benjamin Short",          "Act-On Software",                  "0013100001nnXwhAAE", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Les Schwab Tire Centers, Inc.",    "001i000001R8a5VAAR", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Recordpoint",                      "0010Z00001xwkPDQAY", "2026-02-01"),
    ("John Bird",         "Benjamin Short",          "Db Franchising USA, LLC",          "0013100001nncprAAA", "2026-02-01"),
    ("David Parrish",     "Benjamin Short",          "Epiq",                             "0013r00002EEWF4AAP", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Lamb Weston Holdings, Inc.",       "0010Z00001xw65XQAQ", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Knock, Inc.",                      "0013r00002QcPGJAA3", "2026-02-01"),
    ("Sawyer Ramsey",     "Benjamin Short",          "Thrift Books Global LLC",          "0010Z000026n9KfQAI", "2026-02-01"),
    ("David Parrish",     "Benjamin Short",          "Foureyes",                         "0010Z00001tFGoKQAW", "2026-02-01"),
    ("David Parrish",     "Benjamin Short",          "ENGIE Impact",                     "0013r00002EFLhiAAH", "2026-02-01"),
    ("Matt Loewel",       "Benjamin Short",          "Archera",                          "0013r00002QbYmeAAF", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Navex Global Inc.",                "0010Z00001tFJ7nQAG", "2026-02-01"),
    ("Matt Loewel",       "Benjamin Short",          "SanMar Corporation",               "0013100001coYXQAA2", "2026-02-01"),
    ("Sawyer Ramsey",     "Benjamin Short",          "Navigating Cancer",                "0010Z0000295KoFQAU", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Truckstop.com",                    "0013100001gXxPTAA0", "2026-02-01"),
    ("David Parrish",     "Benjamin Short",          "Payscale, Inc.",                   "0013100001dGmrNAAS", "2026-02-01"),
    ("Sawyer Ramsey",     "Benjamin Short",          "Click Sales, Inc.",                "0013100001qwUTcAAM", "2026-02-01"),
    ("Alessandra Maahs",  "Benjamin Short",          "Brooks Sports, Inc",               "0013100001rvJiDAAU", "2026-02-01"),
    ("Danny King",        "Benjamin Short",          "Knock, Inc.",                      "0013r00002QcPGJAA3", "2026-02-01"),
    ("Danny King",        "Benjamin Short",          "Lamb Weston Holdings",             "0010Z00001xw65XQAQ", "2026-02-01"),
    ("Danny King",        "Benjamin Short",          "Truckstop",                        "0013100001gXxPTAA0", "2026-02-01"),
    ("Noy Bar",           "Parmeet Manchandani",     "Panther Labs Inc",                 "0013r00002IJS7WAAX", "2026-02-01"),
    ("Noy Bar",           "Parmeet Manchandani",     "Gladly Inc.",                      "0010Z00001tFUG0QAO", "2026-02-01"),
    ("Noy Bar",           "Parmeet Manchandani",     "Monte Carlo Data",                 "0010Z0000297szGQAQ", "2026-02-01"),
    ("Noy Bar",           "Parmeet Manchandani",     "Sift",                             "0013r00002EEDzCAAX", "2026-02-01"),
    ("Noy Bar",           "Parmeet Manchandani",     "Malwarebytes",                     "0013100001qyDleAAE", "2026-02-01"),
    ("Omar Ramahi",       "Parmeet Manchandani",     "Anvilogic",                        "0013r00002K9hK7AAJ", "2026-02-01"),
    ("Linsey Zelhart",    "Stephen Kalkbrenner",     "Sprinklr",                         "0013100001gxIlNAAU", "2026-04-01"),
    ("Eric Breuninger",   "Stephen Kalkbrenner",     "Sagent Lending",                   "0010Z0000259NHnQAM", "2026-04-01"),
    ("Jon Poole",         "Stephen Kalkbrenner",     "Common Securitization Solutions",  "0010Z00001uZXGYQA4", "2026-04-01"),
    ("Eric Breuninger",   "Stephen Kalkbrenner",     "Health Union",                     "0013100001qzIBSAA2", "2026-04-01"),
    ("Eric Breuninger",   "Stephen Kalkbrenner",     "SDI",                              "0013r00002EFsqEAAT", "2026-04-01"),
    ("Chad Sagmoen",      "Stephen Kalkbrenner",     "Lyric",                            "001Do00000LaFWDIA3", "2026-04-01"),
    ("Nick Ahearn",       "Cory Stratford",          "Employ",                           "0013r00002UrHHtAAN", "2026-02-01"),
    ("Shane Rinkus",      "Cory Stratford",          "National Grid USA",                "0013100001kbbJQAAY", "2026-02-02"),
    ("Arjun Mazumdar",    "Stephen Kalkbrenner",     "Everstage",                        "0013r00002YMhtnAAD", "2026-04-01"),
    ("Chad Sagmoen",      "Stephen Kalkbrenner",     "Ciena Corporation",                "0013100001oMYuCAAW", "2026-04-01"),
    ("Chad Sagmoen",      "Stephen Kalkbrenner",     "Chalice.AI",                       "0013r00002K9dB2AAJ", "2026-04-01"),
    ("Chad Sagmoen",      "Stephen Kalkbrenner",     "Utica Mutual Insurance Company",   "0013r00002Pa9dVAAR", "2026-04-01"),
    ("Chad Sagmoen",      "Stephen Kalkbrenner",     "Billtrust",                        "0013100001bn326AAA", "2026-04-01"),
    ("Noah Mahr",         "Jen O'Pry",               "Unique Travel Corp.",              "0013r00002XWxb2AAD", "2026-04-01"),
]

# SetSail activity: {(account_id, ae_email): (total_acts, meetings, emails, mtg_hrs)}
ACTIVITY = {
    ("0010Z00001tFJ7nQAG", "alessandra.maahs@snowflake.com"): (12, 3, 9, 1.5),
    ("0010Z00001xw65XQAQ", "alessandra.maahs@snowflake.com"): (2, 1, 1, 0.5),
    ("0010Z00001xwkPDQAY", "alessandra.maahs@snowflake.com"): (15, 1, 14, 0.5),
    ("0013100001gXxPTAA0", "alessandra.maahs@snowflake.com"): (1, 0, 1, 0.0),
    ("0013100001rsDOuAAM", "alessandra.maahs@snowflake.com"): (0, 0, 0, 0.0),
    ("0013100001rvJiDAAU", "alessandra.maahs@snowflake.com"): (44, 7, 37, 7.2),
    ("0013r00002QcPGJAA3", "alessandra.maahs@snowflake.com"): (20, 0, 20, 0.0),
    ("001i000001R8a5VAAR", "alessandra.maahs@snowflake.com"): (10, 2, 8, 0.8),
    ("0013r00002YMhtnAAD", "arjun.mazumdar@snowflake.com"):   (0, 0, 0, 0.0),
    ("0013100001bn326AAA", "chad.sagmoen@snowflake.com"):     (0, 0, 0, 0.0),
    ("0013100001oMYuCAAW", "chad.sagmoen@snowflake.com"):     (0, 0, 0, 0.0),
    ("0013r00002K9dB2AAJ", "chad.sagmoen@snowflake.com"):     (0, 0, 0, 0.0),
    ("0013r00002Pa9dVAAR", "chad.sagmoen@snowflake.com"):     (0, 0, 0, 0.0),
    ("001Do00000LaFWDIA3", "chad.sagmoen@snowflake.com"):     (0, 0, 0, 0.0),
    ("0010Z00001xw65XQAQ", "danny.king@snowflake.com"):       (1, 1, 0, 0.5),
    ("0013100001gXxPTAA0", "danny.king@snowflake.com"):       (0, 0, 0, 0.0),
    ("0013r00002QcPGJAA3", "danny.king@snowflake.com"):       (16, 1, 15, 0.5),
    ("0010Z00001tFGoKQAW", "david.parrish@snowflake.com"):    (2, 0, 2, 0.0),
    ("0013100001dGmrNAAS", "david.parrish@snowflake.com"):    (1, 0, 1, 0.0),
    ("0013r00002EEWF4AAP", "david.parrish@snowflake.com"):    (0, 0, 0, 0.0),
    ("0013r00002EFLhiAAH", "david.parrish@snowflake.com"):    (21, 2, 19, 1.0),
    ("0010Z000025Ao5UQAS", "dj.kline@snowflake.com"):         (116, 23, 93, 21.8),
    ("0013100001nnZZjAAM", "dj.kline@snowflake.com"):         (0, 0, 0, 0.0),
    ("0013r00002EEIpOAAX", "dj.kline@snowflake.com"):         (2, 2, 0, 1.0),
    ("0010Z0000259NHnQAM", "eric.breuninger@snowflake.com"):  (0, 0, 0, 0.0),
    ("0013100001qzIBSAA2", "eric.breuninger@snowflake.com"):  (3, 0, 3, 0.0),
    ("0013r00002EFsqEAAT", "eric.breuninger@snowflake.com"):  (0, 0, 0, 0.0),
    ("0013100001nncprAAA", "john.bird@snowflake.com"):         (12, 0, 12, 0.0),
    ("0010Z00001uZXGYQA4", "jon.poole@snowflake.com"):         (1, 1, 0, 0.5),
    ("0013100001gxIlNAAU", "lindsey.zelhart@snowflake.com"):  (9, 0, 9, 0.0),
    ("0013100001coYXQAA2", "matthew.loewel@snowflake.com"):   (0, 0, 0, 0.0),
    ("0013r00002QbYmeAAF", "matthew.loewel@snowflake.com"):   (0, 0, 0, 0.0),
    ("0013r00002UrHHtAAN", "nicholas.ahearn@snowflake.com"):  (29, 10, 19, 5.4),
    ("0013r00002XWxb2AAD", "noah.mahr@snowflake.com"):        (3, 0, 3, 0.0),
    ("0010Z00001tFUG0QAO", "noy.bar@snowflake.com"):          (1, 0, 1, 0.0),
    ("0010Z0000297szGQAQ", "noy.bar@snowflake.com"):           (8, 2, 6, 1.0),
    ("0013100001qyDleAAE", "noy.bar@snowflake.com"):           (7, 6, 1, 3.0),
    ("0013r00002EEDzCAAX", "noy.bar@snowflake.com"):           (8, 2, 6, 1.0),
    ("0013r00002IJS7WAAX", "noy.bar@snowflake.com"):           (0, 0, 0, 0.0),
    ("0013r00002K9hK7AAJ", "omar.ramahi@snowflake.com"):       (6, 5, 1, 3.3),
    ("0010Z000026n9KfQAI", "sawyer.ramsey@snowflake.com"):     (0, 0, 0, 0.0),
    ("0010Z0000295KoFQAU", "sawyer.ramsey@snowflake.com"):     (0, 0, 0, 0.0),
    ("0013100001nnXwhAAE", "sawyer.ramsey@snowflake.com"):     (1, 0, 1, 0.0),
    ("0013100001qwUTcAAM", "sawyer.ramsey@snowflake.com"):     (15, 3, 12, 1.8),
    ("0013100001kbbJQAAY", "shane.rinkus@snowflake.com"):      (26, 4, 22, 2.3),
}

# AE name → email (for ACTIVITY lookup)
AE_EMAIL = {
    "Alessandra Maahs": "alessandra.maahs@snowflake.com",
    "Andrew Keefer":    "andrew.keefer@snowflake.com",
    "Arjun Mazumdar":   "arjun.mazumdar@snowflake.com",
    "Chad Sagmoen":     "chad.sagmoen@snowflake.com",
    "DJ Kline":         "dj.kline@snowflake.com",
    "Danny King":       "danny.king@snowflake.com",
    "David Parrish":    "david.parrish@snowflake.com",
    "Eric Breuninger":  "eric.breuninger@snowflake.com",
    "John Bird":        "john.bird@snowflake.com",
    "Jon Poole":        "jon.poole@snowflake.com",
    "Linsey Zelhart":   "lindsey.zelhart@snowflake.com",
    "Matt Loewel":      "matthew.loewel@snowflake.com",
    "Nick Ahearn":      "nicholas.ahearn@snowflake.com",
    "Noah Mahr":        "noah.mahr@snowflake.com",
    "Noy Bar":          "noy.bar@snowflake.com",
    "Omar Ramahi":      "omar.ramahi@snowflake.com",
    "Sawyer Ramsey":    "sawyer.ramsey@snowflake.com",
    "Shane Rinkus":     "shane.rinkus@snowflake.com",
}

# Run rates from SALES.SE_REPORTING.AGG_MONTHLY_PRODUCT_CATEGORY_ACCOUNT_METRICS
# {account_id: (rr_start, rr_start_month, rr_current, rr_current_month)}
# rr_start  = last complete month before coverage start  (annualized, rounded $)
# rr_current = latest complete month (Mar 2026)          (annualized, rounded $)
RR_DATA = {
    "0010Z00001tFGoKQAW": (203929,  "Jan 2026", 238345,  "Mar 2026"),
    "0010Z00001tFJ7nQAG": (210088,  "Jan 2026", 225260,  "Mar 2026"),
    "0010Z00001tFUG0QAO": (1281716, "Jan 2026", 1370914, "Mar 2026"),
    "0010Z00001uZXGYQA4": (737368,  "Mar 2026", 737368,  "Mar 2026"),
    "0010Z00001xw65XQAQ": (325517,  "Jan 2026", 199091,  "Mar 2026"),
    "0010Z00001xwkPDQAY": (281623,  "Jan 2026", 317284,  "Mar 2026"),
    "0010Z0000259NHnQAM": (1384824, "Mar 2026", 1384824, "Mar 2026"),
    "0010Z000025Ao5UQAS": (1710213, "Jan 2026", 1471715, "Mar 2026"),
    "0010Z000026n9KfQAI": (367950,  "Jan 2026", 376026,  "Mar 2026"),
    "0010Z0000295KoFQAU": (382015,  "Jan 2026", 975526,  "Mar 2026"),
    "0010Z0000297szGQAQ": (800661,  "Jan 2026", 897931,  "Mar 2026"),
    "0013100001bn326AAA": (544263,  "Mar 2026", 544263,  "Mar 2026"),
    "0013100001coYXQAA2": (112253,  "Jan 2026", 125529,  "Mar 2026"),
    "0013100001dGmrNAAS": (183594,  "Jan 2026", 173409,  "Mar 2026"),
    "0013100001gXxPTAA0": (183345,  "Jan 2026", 221911,  "Mar 2026"),
    "0013100001gxIlNAAU": (304565,  "Mar 2026", 304565,  "Mar 2026"),
    "0013100001kbbJQAAY": (1105993, "Jan 2026", 783759,  "Mar 2026"),
    "0013100001nnXwhAAE": (216070,  "Jan 2026", 222400,  "Mar 2026"),
    "0013100001nnZZjAAM": (649165,  "Jan 2026", 701028,  "Mar 2026"),
    "0013100001nncprAAA": (253888,  "Jan 2026", 367295,  "Mar 2026"),
    "0013100001oMYuCAAW": (579556,  "Mar 2026", 579556,  "Mar 2026"),
    "0013100001qwUTcAAM": (149993,  "Jan 2026", 183930,  "Mar 2026"),
    "0013100001qyDleAAE": (692911,  "Jan 2026", 708428,  "Mar 2026"),
    "0013100001qzIBSAA2": (1036444, "Mar 2026", 1036444, "Mar 2026"),
    "0013100001rsDOuAAM": (683535,  "Jan 2026", 468913,  "Mar 2026"),
    "0013100001rvJiDAAU": (156717,  "Jan 2026", 177446,  "Mar 2026"),
    "0013r00002EEDzCAAX": (711632,  "Jan 2026", 722938,  "Mar 2026"),
    "0013r00002EEIpOAAX": (911943,  "Jan 2026", 991914,  "Mar 2026"),
    "0013r00002EEWF4AAP": (122139,  "Jan 2026", 147878,  "Mar 2026"),
    "0013r00002EFLhiAAH": (218560,  "Jan 2026", 242902,  "Mar 2026"),
    "0013r00002EFsqEAAT": (478042,  "Mar 2026", 478042,  "Mar 2026"),
    "0013r00002IJS7WAAX": (3038293, "Jan 2026", 3559783, "Mar 2026"),
    "0013r00002K9dB2AAJ": (823062,  "Mar 2026", 823062,  "Mar 2026"),
    "0013r00002K9hK7AAJ": (944826,  "Jan 2026", 1346345, "Mar 2026"),
    "0013r00002Pa9dVAAR": (421144,  "Mar 2026", 421144,  "Mar 2026"),
    "0013r00002QbYmeAAF": (310312,  "Jan 2026", 389984,  "Mar 2026"),
    "0013r00002QcPGJAA3": (494244,  "Jan 2026", 521741,  "Mar 2026"),
    "0013r00002UrHHtAAN": (1395270, "Jan 2026", 983750,  "Mar 2026"),
    "0013r00002XWxb2AAD": (41508,   "Mar 2026", 41508,   "Mar 2026"),
    "0013r00002YMhtnAAD": (1029170, "Mar 2026", 1029170, "Mar 2026"),
    "001Do00000LaFWDIA3": (976141,  "Mar 2026", 976141,  "Mar 2026"),
    "001i000001R8a5VAAR": (169413,  "Jan 2026", 182559,  "Mar 2026"),
}

# Use cases from MDM.MDM_INTERFACES.DIM_USE_CASE
# {account_id: (uc_created, uc_won)}
# Created: CREATED_DATE in coverage period
# Won: DECISION_DATE in coverage period (past), STAGE_NUMBER 4-7
UC_DATA = {
    "0010Z00001tFGoKQAW": (0, 0),
    "0010Z00001tFJ7nQAG": (1, 0),
    "0010Z00001tFUG0QAO": (1, 0),
    "0010Z00001uZXGYQA4": (0, 0),
    "0010Z00001xw65XQAQ": (0, 0),
    "0010Z00001xwkPDQAY": (0, 0),
    "0010Z0000259NHnQAM": (0, 0),
    "0010Z000025Ao5UQAS": (1, 2),
    "0010Z000026n9KfQAI": (0, 0),
    "0010Z0000295KoFQAU": (0, 0),
    "0010Z0000297szGQAQ": (1, 0),
    "0013100001bn326AAA": (0, 0),
    "0013100001coYXQAA2": (0, 0),
    "0013100001dGmrNAAS": (0, 0),
    "0013100001gXxPTAA0": (0, 0),
    "0013100001gxIlNAAU": (0, 0),
    "0013100001kbbJQAAY": (4, 0),
    "0013100001nnXwhAAE": (0, 0),
    "0013100001nnZZjAAM": (0, 0),
    "0013100001nncprAAA": (0, 0),
    "0013100001oMYuCAAW": (0, 0),
    "0013100001qwUTcAAM": (2, 0),
    "0013100001qyDleAAE": (1, 0),
    "0013100001qzIBSAA2": (0, 0),
    "0013100001rsDOuAAM": (0, 0),
    "0013100001rvJiDAAU": (3, 0),
    "0013r00002EEDzCAAX": (1, 0),
    "0013r00002EEIpOAAX": (1, 1),
    "0013r00002EEWF4AAP": (0, 0),
    "0013r00002EFLhiAAH": (1, 0),
    "0013r00002EFsqEAAT": (0, 0),
    "0013r00002IJS7WAAX": (1, 0),
    "0013r00002K9dB2AAJ": (0, 0),
    "0013r00002K9hK7AAJ": (0, 0),
    "0013r00002Pa9dVAAR": (0, 0),
    "0013r00002QbYmeAAF": (0, 0),
    "0013r00002QcPGJAA3": (0, 0),
    "0013r00002UrHHtAAN": (0, 0),
    "0013r00002XWxb2AAD": (0, 0),
    "0013r00002YMhtnAAD": (0, 0),
    "001Do00000LaFWDIA3": (0, 0),
    "001i000001R8a5VAAR": (0, 0),
}

# ─── Helpers ─────────────────────────────────────────────────────────────────
def is_active(acts, hrs, uc_created=0, uc_won=0):
    return (acts >= ACTIVE_MIN_ACTS or hrs >= ACTIVE_MIN_HRS
            or uc_created > 0 or uc_won > 0)

def fmt_rr(val):
    if val is None:
        return "—"
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"${int(round(val/1_000))}K"
    return f"${int(val)}"

def fmt_pct(start, latest):
    if start is None or latest is None or start == 0:
        return None
    pct = (latest - start) / start * 100
    return pct

def pct_html(pct):
    if pct is None:
        return "<span class='rr-flat'>—</span>"
    if pct > 2:
        return f"<span class='rr-up'>↑{pct:+.1f}%</span>"
    if pct < -2:
        return f"<span class='rr-dn'>↓{pct:.1f}%</span>"
    return f"<span class='rr-flat'>→{pct:+.1f}%</span>"

def sfdc_url(acct_id):
    return f"{SFDC_BASE}/lightning/r/Account/{acct_id}/view"

def rr_cell(val, month_label):
    if val is None:
        return "<span class='no-data'>—</span>"
    text = fmt_rr(val)
    if month_label:
        return f"<span title='{month_label}'>{text}</span>"
    return text


# ─── Build enriched row list ──────────────────────────────────────────────────
rows = []
for ae, dm, account, acct_id, start_date in ACCOUNTS:
    ae_email = AE_EMAIL.get(ae, "")
    act_key  = (acct_id, ae_email)
    acts, mtgs, emails, hrs = ACTIVITY.get(act_key, (0, 0, 0, 0.0))

    rr_start, rr_start_month, rr_current, rr_current_month = RR_DATA.get(acct_id, (None, None, None, None))
    uc_created, uc_won = UC_DATA.get(acct_id, (0, 0))
    end_date = FQ_END
    active = is_active(acts, hrs, uc_created, uc_won)
    pct    = fmt_pct(rr_start, rr_current)

    rows.append({
        "ae":               ae,
        "dm":               dm,
        "account":          account,
        "acct_id":          acct_id,
        "start_date":       start_date,
        "end_date":         end_date,
        "acts":             acts,
        "mtgs":             mtgs,
        "emails":           emails,
        "hrs":              hrs,
        "rr_start":         rr_start,
        "rr_start_month":   rr_start_month,
        "rr_current":       rr_current,
        "rr_current_month": rr_current_month,
        "pct":              pct,
        "uc_created":       uc_created,
        "uc_won":           uc_won,
        "active":           active,
    })

n_active   = sum(1 for r in rows if r["active"])
n_inactive = len(rows) - n_active


# ─── Group: DM → AE → [rows] ─────────────────────────────────────────────────
from collections import defaultdict

dm_order = []
seen_dms = set()
for r in rows:
    if r["dm"] not in seen_dms:
        dm_order.append(r["dm"])
        seen_dms.add(r["dm"])

grouped = defaultdict(lambda: defaultdict(list))
for r in rows:
    grouped[r["dm"]][r["ae"]].append(r)


# ─── HTML Builder ─────────────────────────────────────────────────────────────
def build_html():
    parts = []

    # ── header boilerplate ───────────────────────────────────────────────────
    parts.append(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{REPORT_TITLE}</title>
<style>
  :root {{
    --green-bg: #e8f5e9;
    --red-bg:   #ffebee;
    --green-bd: #a5d6a7;
    --red-bd:   #ef9a9a;
    --blue:     #1565c0;
    --gray:     #757575;
    --header:   #1a237e;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 13px;
    background: #f5f5f5;
    color: #212121;
  }}

  /* ─── Top bar ─────────────────────────────────────────────── */
  .topbar {{
    background: var(--header);
    color: #fff;
    padding: 14px 24px;
    display: flex;
    align-items: baseline;
    gap: 16px;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 6px rgba(0,0,0,.4);
  }}
  .topbar h1 {{ font-size: 20px; font-weight: 700; }}
  .topbar .sub {{ font-size: 12px; opacity: .8; }}
  .topbar .meta {{ margin-left: auto; font-size: 11px; opacity: .7; white-space: nowrap; }}

  /* ─── Filter bar ───────────────────────────────────────────── */
  .filterbar {{
    background: #fff;
    border-bottom: 1px solid #ddd;
    padding: 10px 24px;
    display: flex;
    align-items: center;
    gap: 8px;
    position: sticky;
    top: 53px;
    z-index: 99;
  }}
  .filterbar label {{ font-weight: 600; color: #555; margin-right: 4px; }}
  .filter-btn {{
    border: 1px solid #bbb;
    background: #fff;
    border-radius: 20px;
    padding: 4px 14px;
    cursor: pointer;
    font-size: 12px;
    font-weight: 500;
    transition: background .15s, color .15s;
  }}
  .filter-btn:hover  {{ background: #e8eaf6; }}
  .filter-btn.active {{ background: var(--header); color: #fff; border-color: var(--header); }}
  .threshold-note {{
    margin-left: auto;
    font-size: 11px;
    color: var(--gray);
    font-style: italic;
  }}

  /* ─── Summary chips ────────────────────────────────────────── */
  .summary {{
    padding: 10px 24px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }}
  .chip {{
    border-radius: 16px;
    padding: 4px 14px;
    font-size: 12px;
    font-weight: 600;
    border: 1px solid transparent;
  }}
  .chip-all     {{ background: #e8eaf6; color: #283593; border-color: #9fa8da; }}
  .chip-active  {{ background: var(--green-bg); color: #2e7d32; border-color: var(--green-bd); }}
  .chip-inactive{{ background: var(--red-bg);   color: #c62828; border-color: var(--red-bd); }}

  /* ─── DM section ───────────────────────────────────────────── */
  .dm-section  {{ margin: 8px 16px; border: 1px solid #ddd; border-radius: 6px; background: #fff; }}
  .dm-header   {{
    display: flex; align-items: center; gap: 8px;
    padding: 10px 14px;
    cursor: pointer;
    background: #eeeeee;
    border-radius: 6px 6px 0 0;
    user-select: none;
  }}
  .dm-header:hover {{ background: #e0e0e0; }}
  .toggle-icon {{ font-size: 14px; color: #555; flex-shrink: 0; }}
  .dm-name     {{ font-weight: 700; font-size: 14px; }}
  .dm-counts   {{ font-size: 11px; color: #777; margin-left: 8px; }}
  .dm-body     {{ display: none; padding: 0 12px 10px; }}
  .dm-body.open {{ display: block; }}

  /* ─── AE section ───────────────────────────────────────────── */
  .ae-section  {{ margin: 8px 0; border: 1px solid #e0e0e0; border-radius: 4px; }}
  .ae-header   {{
    display: flex; align-items: center; gap: 8px;
    padding: 7px 10px;
    cursor: pointer;
    background: #fafafa;
    border-radius: 4px 4px 0 0;
    user-select: none;
  }}
  .ae-header:hover {{ background: #f0f0f0; }}
  .ae-name     {{ font-weight: 600; font-size: 13px; }}
  .ae-counts   {{ font-size: 11px; color: #888; }}
  .ae-body     {{ display: none; overflow-x: auto; }}
  .ae-body.open {{ display: block; }}

  /* ─── Account table ────────────────────────────────────────── */
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }}
  th {{
    background: #37474f;
    color: #fff;
    padding: 6px 8px;
    text-align: left;
    white-space: nowrap;
    font-weight: 600;
    font-size: 11px;
    letter-spacing: .03em;
  }}
  td {{
    padding: 5px 8px;
    border-bottom: 1px solid #e8e8e8;
    vertical-align: middle;
    white-space: nowrap;
  }}
  tr.active-row   {{ background: var(--green-bg); }}
  tr.inactive-row {{ background: var(--red-bg); }}
  tr:hover td {{ filter: brightness(0.97); }}

  .acct-name {{ font-weight: 500; }}
  .stale {{ font-style: italic; color: #888; }}
  .no-data {{ color: #bbb; }}

  .badge {{
    display: inline-block;
    border-radius: 10px;
    padding: 2px 8px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .04em;
  }}
  .badge-active   {{ background: #c8e6c9; color: #1b5e20; }}
  .badge-inactive {{ background: #ffcdd2; color: #b71c1c; }}

  .split-btn {{
    background: #1565c0;
    color: #fff;
    border: none;
    border-radius: 4px;
    padding: 3px 9px;
    font-size: 10px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    display: inline-block;
    margin-left: 4px;
  }}
  .split-btn:hover {{ background: #0d47a1; }}

  .rr-up   {{ color: #2e7d32; font-weight: 600; }}
  .rr-dn   {{ color: #c62828; font-weight: 600; }}
  .rr-flat {{ color: #616161; }}

  td.num, th.num {{ text-align: right; }}

  /* hidden rows for filter */
  tr.hidden-row {{ display: none; }}
  .ae-section.hidden-section  {{ display: none; }}
  .dm-section.hidden-section  {{ display: none; }}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <h1>{REPORT_TITLE}</h1>
  <span class="sub">{REPORT_SUBTITLE}</span>
  <span class="meta">As of {GENERATED_AT}</span>
</div>

<!-- Filter bar -->
<div class="filterbar">
  <label>Show:</label>
  <button class="filter-btn active" id="btn-all"      onclick="setFilter('all',this)">All <span id="cnt-all">{len(rows)}</span></button>
  <button class="filter-btn"        id="btn-active"   onclick="setFilter('active',this)">Active <span id="cnt-active">{n_active}</span></button>
  <button class="filter-btn"        id="btn-inactive" onclick="setFilter('inactive',this)">Inactive <span id="cnt-inactive">{n_inactive}</span></button>
  <span class="threshold-note">Active = &ge;{ACTIVE_MIN_ACTS} activities&nbsp;&nbsp;or&nbsp;&nbsp;&ge;{ACTIVE_MIN_HRS:.0f} hrs meetings</span>
</div>

<!-- Summary chips -->
<div class="summary">
  <span class="chip chip-all">Total: {len(rows)}</span>
  <span class="chip chip-active">Active: {n_active}</span>
  <span class="chip chip-inactive">Inactive: {n_inactive}</span>
  <span style="font-size:11px;color:#888;align-self:center;">
    Run rates = monthly actual (AGG_MONTHLY, AVG_DAILY_REVENUE × 365) &nbsp;|&nbsp; Activities filtered to covering AE only
  </span>
</div>

""")

    # ── DM sections ─────────────────────────────────────────────────────────
    for dm_idx, dm in enumerate(dm_order):
        ae_map = grouped[dm]
        dm_rows = [r for ae_rows in ae_map.values() for r in ae_rows]
        dm_n_active   = sum(1 for r in dm_rows if r["active"])
        dm_n_inactive = len(dm_rows) - dm_n_active
        dm_id = f"dm-{dm_idx}"

        parts.append(f"""
<div class="dm-section" id="{dm_id}-wrap">
  <div class="dm-header" onclick="toggle('{dm_id}')">
    <span class="toggle-icon" id="{dm_id}-icon">&#9654;</span>
    <span class="dm-name">{dm}</span>
    <span class="dm-counts">{len(dm_rows)} accounts &bull; {dm_n_active} active &bull; {dm_n_inactive} inactive</span>
  </div>
  <div class="dm-body" id="{dm_id}">
""")

        # ── AE sections ──────────────────────────────────────────────────
        ae_order = []
        seen_aes = set()
        for r in rows:
            if r["dm"] == dm and r["ae"] not in seen_aes:
                ae_order.append(r["ae"])
                seen_aes.add(r["ae"])

        for ae_idx, ae in enumerate(ae_order):
            ae_rows     = ae_map[ae]
            ae_n_active = sum(1 for r in ae_rows if r["active"])
            ae_id       = f"ae-{dm_idx}-{ae_idx}"

            parts.append(f"""
    <div class="ae-section" id="{ae_id}-wrap">
      <div class="ae-header" onclick="toggle('{ae_id}')">
        <span class="toggle-icon" id="{ae_id}-icon">&#9654;</span>
        <span class="ae-name">{ae}</span>
        <span class="ae-counts">&nbsp;&nbsp;{len(ae_rows)} accounts &bull; {ae_n_active} active</span>
      </div>
      <div class="ae-body" id="{ae_id}">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Coverage Start</th>
              <th>Coverage End</th>
              <th>RR at Start</th>
              <th>Current RR</th>
              <th>RR Change</th>
              <th class="num">Activities</th>
              <th class="num">Meetings</th>
              <th class="num">Emails</th>
              <th class="num">Mtg Hrs</th>
              <th class="num">UCs Created</th>
              <th class="num">UCs Won</th>
              <th>Status</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
""")

            for r in ae_rows:
                row_cls = "active-row" if r["active"] else "inactive-row"
                badge   = ('<span class="badge badge-active">Active</span>'
                           if r["active"]
                           else '<span class="badge badge-inactive">Inactive</span>')

                action = ""
                if r["active"]:
                    url = sfdc_url(r["acct_id"])
                    action = f'<a class="split-btn" href="{url}" target="_blank">+ New Split</a>'

                rr_s = rr_cell(r["rr_start"],   r["rr_start_month"])
                rr_l = rr_cell(r["rr_current"], r["rr_current_month"])
                pct  = pct_html(r["pct"])

                data_active = "true" if r["active"] else "false"

                parts.append(f"""            <tr class="{row_cls}" data-active="{data_active}">
              <td class="acct-name">{r["account"]}</td>
              <td>{r["start_date"]}</td>
              <td>{r["end_date"]}</td>
              <td>{rr_s}</td>
              <td>{rr_l}</td>
              <td>{pct}</td>
              <td class="num">{r["acts"]}</td>
              <td class="num">{r["mtgs"]}</td>
              <td class="num">{r["emails"]}</td>
              <td class="num">{r["hrs"]:.1f}</td>
              <td class="num">{r["uc_created"]}</td>
              <td class="num">{r["uc_won"]}</td>
              <td>{badge}</td>
              <td>{action}</td>
            </tr>
""")

            parts.append("""          </tbody>
        </table>
      </div>
    </div>
""")

        parts.append("""  </div>
</div>
""")

    # ── JavaScript ──────────────────────────────────────────────────────────
    parts.append("""
<script>
function toggle(id) {
  var body = document.getElementById(id);
  var icon = document.getElementById(id + '-icon');
  if (!body) return;
  if (body.classList.contains('open')) {
    body.classList.remove('open');
    icon.innerHTML = '&#9654;';
  } else {
    body.classList.add('open');
    icon.innerHTML = '&#9660;';
  }
}

var currentFilter = 'all';

function setFilter(mode, btn) {
  currentFilter = mode;
  document.querySelectorAll('.filter-btn').forEach(function(b) {
    b.classList.remove('active');
  });
  btn.classList.add('active');
  applyFilter();
}

function applyFilter() {
  var rows = document.querySelectorAll('tr[data-active]');
  rows.forEach(function(row) {
    var isActive = row.getAttribute('data-active') === 'true';
    var show = (currentFilter === 'all') ||
               (currentFilter === 'active'   &&  isActive) ||
               (currentFilter === 'inactive' && !isActive);
    if (show) {
      row.classList.remove('hidden-row');
    } else {
      row.classList.add('hidden-row');
    }
  });

  // Auto-expand DM and AE sections that have visible rows; hide empty ones
  document.querySelectorAll('.ae-section').forEach(function(section) {
    var visibleRows = section.querySelectorAll('tr[data-active]:not(.hidden-row)');
    if (visibleRows.length === 0) {
      section.classList.add('hidden-section');
    } else {
      section.classList.remove('hidden-section');
      // Auto-expand the ae-body
      var body = section.querySelector('.ae-body');
      var icon = section.querySelector('.toggle-icon');
      if (body && currentFilter !== 'all') {
        body.classList.add('open');
        if (icon) icon.innerHTML = '&#9660;';
      }
    }
  });

  document.querySelectorAll('.dm-section').forEach(function(section) {
    var visibleAE = section.querySelectorAll('.ae-section:not(.hidden-section)');
    if (visibleAE.length === 0) {
      section.classList.add('hidden-section');
    } else {
      section.classList.remove('hidden-section');
      // Auto-expand the dm-body
      var body = section.querySelector('.dm-body');
      var icon = section.querySelector('.toggle-icon');
      if (body && currentFilter !== 'all') {
        body.classList.add('open');
        if (icon) icon.innerHTML = '&#9660;';
      }
    }
  });
}
</script>
</body>
</html>
""")

    return "".join(parts)


# ─── Write output ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    html    = build_html()
    outfile = "/Users/nathomas/Downloads/Active_OTB_Analysis.html"
    with open(outfile, "w") as f:
        f.write(html)
    print(f"Written: {outfile}")
    print(f"Total accounts: {len(rows)} | Active: {n_active} | Inactive: {n_inactive}")
    print(f"DMs: {len(dm_order)} | AEs: {len(set(r['ae'] for r in rows))}")
