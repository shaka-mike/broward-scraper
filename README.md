# Broward County Motivated Seller Lead Scraper

Automated Playwright-driven pipeline that pulls distress recordings from the
Broward Clerk (AcclaimWeb), enriches every lead with owner + mailing addresses
from the Broward County Property Appraiser (BCPA) parcel DBF, scores each
lead 0-100 on a flag-based rubric, and publishes:

- `dashboard/records.json` — machine-readable dataset for the dashboard
- `data/records.json` — archive copy
- `dashboard/leads_ghl.csv` — GoHighLevel-ready direct-mail list
- `dashboard/index.html` — live, filter/sort dashboard (auto-deployed to Pages)

## Folder structure

```
scraper/fetch.py                 # main scraper (async Playwright + requests + dbfread)
scraper/requirements.txt
dashboard/index.html             # dashboard UI (reads records.json at runtime)
dashboard/records.json           # seeded empty; populated by first scrape
data/records.json                # archive copy
data/BCPA_TAX_ROLL.DBF           # optional; downloaded by the scraper or prestaged
data/SearchResults*.csv          # optional; deterministic clerk export fallback
.github/workflows/scrape.yml
```

## Lead types collected

| Code  | Description              |
|-------|--------------------------|
| LP    | Lis Pendens              |
| NOFC  | Notice of Foreclosure    |
| FJ    | Final Judgment           |
| PALIE | Property Tax Lien        |
| PRO   | Probate Documents        |
| NOC   | Notice of Commencement   |
| RELLP | Release Lis Pendens      |
| LIE   | Lien                     |
| LIEX  | Lien Hidden from Web     |

## Scoring rubric

```
Base                      30
+10 per flag              (up to 8 flags)
+20 LP + foreclosure combo
+15 amount > $100k
+10 amount > $50k         (only if not already +15)
+5  new this week
+5  has address
```

Flags: `Lis pendens`, `Pre-foreclosure`, `Judgment lien`, `Tax lien`,
`Mechanic lien`, `Probate / estate`, `LLC / corp owner`, `New this week`.

Final score is clamped to 0-100.

## Configuration (env vars)

| Variable         | Purpose                                                   |
|------------------|-----------------------------------------------------------|
| `LOOKBACK_DAYS`  | Days of recordings to fetch (default 7)                   |
| `BCPA_DBF_URL`   | Pre-signed URL to the BCPA parcel DBF                     |
| `BCPA_FORM_URL`  | BCPA form page that serves the DBF via `__doPostBack`     |
| `BCPA_POSTBACK`  | The `__EVENTTARGET` value that triggers the download       |

The scraper tries `BCPA_DBF_URL` first; if that's missing it tries the
`__doPostBack` flow (`BCPA_FORM_URL` + `BCPA_POSTBACK`). If both are missing,
enrichment is skipped and the run still produces a (less-enriched) dataset.

## BCPA parcel index

The DBF is indexed by every owner name under three variants so a lookup
succeeds regardless of how the Clerk formats the party name:

- `FIRST LAST`
- `LAST FIRST`
- `LAST, FIRST`

Column aliases auto-detected (first match wins):

```
OWNER / OWN1
SITE_ADDR / SITEADDR
SITE_CITY
SITE_ZIP
ADDR_1 / MAILADR1
CITY / MAILCITY
STATE
ZIP / MAILZIP
```

## Running locally

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

Offline / CSV-only mode (drop an AcclaimWeb CSV export in `data/`):

```bash
# data/SearchResults.csv   -- exported from AcclaimWeb "Save as CSV" button
python scraper/fetch.py
```

## GitHub Actions

`.github/workflows/scrape.yml` runs every day at 07:00 UTC and on
`workflow_dispatch`. It:

1. Installs Python 3.11 + scraper deps + Playwright Chromium
2. Runs `python scraper/fetch.py`
3. Commits refreshed `dashboard/records.json`, `data/records.json`, and
   `dashboard/leads_ghl.csv`
4. Deploys `dashboard/` to GitHub Pages

Set repo secrets `BCPA_DBF_URL` / `BCPA_FORM_URL` / `BCPA_POSTBACK` to
enable parcel enrichment in CI.

## GHL export columns

```
First Name, Last Name,
Mailing Address, Mailing City, Mailing State, Mailing Zip,
Property Address, Property City, Property State, Property Zip,
Lead Type, Document Type, Date Filed, Document Number,
Amount/Debt Owed, Seller Score, Motivated Seller Flags,
Source, Public Records URL
```

Rows without any deliverable mailing address are skipped (GHL imports fail
on blank addresses). The dashboard's "Export GHL CSV" button produces the
same CSV but honors your current filters.

## Reliability

- **3-attempt retries** with exponential backoff on every HTTP call
- **Per-row try/except** so one malformed record never kills the run
- **Playwright timeouts** isolated per doc type -- one slow search doesn't
  stall the others
- **CSV fallback** -- if Playwright fails or you want deterministic runs,
  drop a `SearchResults*.csv` in `data/` and the scraper uses it instead

## Disclaimer

Data derives from Broward County Official Records (AcclaimWeb) and the
Broward County Property Appraiser. Informational only — not a title
search, investment advice, or legal advice.
