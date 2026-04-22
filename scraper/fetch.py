"""
Broward County Motivated Seller Lead Scraper
=============================================

End-to-end pipeline:

  1. Playwright (async) drives the Broward Clerk AcclaimWeb portal
     to pull recorded documents for a 7-day lookback window across
     9 target lead types.
  2. requests + BeautifulSoup handles the Broward County Property
     Appraiser (BCPA) bulk DBF download, including the ASP.NET
     __doPostBack POST flow.
  3. dbfread indexes every parcel by 3 owner-name variants
     ("FIRST LAST", "LAST FIRST", "LAST, FIRST") for enrichment.
  4. Each lead is scored 0-100 using the flag-based rubric.
  5. Outputs:
       dashboard/records.json          -- consumed by the dashboard
       data/records.json               -- same payload, archived
       dashboard/leads_ghl.csv         -- GoHighLevel import CSV

Retries (3 attempts with exponential backoff) guard every network
call. No single bad record kills the run.

Run:
    python scraper/fetch.py
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# =============================================================================
# CONFIGURATION
# =============================================================================

CLERK_BASE = "https://officialrecords.broward.org/AcclaimWeb"
CLERK_SEARCH_URL = f"{CLERK_BASE}/search/SearchTypeDocType"

# Broward County Property Appraiser. The bulk parcel DBF is delivered from
# a paid "commercial data services" portal at web.bcpa.net/InfoBroward; the
# download is triggered by an ASP.NET __doPostBack. The exact page and
# control ID can be overridden with env vars (see download_bcpa_dbf).
BCPA_DBF_URL   = os.environ.get("BCPA_DBF_URL", "")   # Pre-signed direct URL
BCPA_FORM_URL  = os.environ.get("BCPA_FORM_URL", "")  # Form page with postback
BCPA_POSTBACK  = os.environ.get("BCPA_POSTBACK", "")  # __EVENTTARGET value

# Lookback window.
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

# Target lead types. The AcclaimWeb doc-type picker uses long English labels;
# each internal `cat` maps to 1+ label substrings plus optional keyword rules
# applied against the legal description / case number.
LEAD_TYPES: list[dict[str, Any]] = [
    {
        "cat": "LP",
        "cat_label": "Lis Pendens",
        "clerk_doc_types": ["Lis Pendens"],
        "keywords": [],
    },
    {
        "cat": "NOFC",
        "cat_label": "Notice of Foreclosure",
        "clerk_doc_types": ["Lis Pendens", "Notice"],
        # Broward doesn't have a distinct NOFC doc type -- it's filed as
        # a Lis Pendens or Notice whose text mentions foreclosure.
        "keywords": [r"foreclos"],
    },
    {
        "cat": "FJ",
        "cat_label": "Final Judgment",
        "clerk_doc_types": ["Final Judgment", "Certified Final Judgment"],
        "keywords": [],
    },
    {
        "cat": "PALIE",
        "cat_label": "Property Tax Lien",
        "clerk_doc_types": ["Lien", "Certificate", "Notice"],
        "keywords": [
            r"tax\s+lien", r"tax\s+deed", r"tax\s+delinquen",
            r"tax\s+certificate", r"non[- ]ad\s+valorem",
            r"property\s+tax",
        ],
    },
    {
        "cat": "PRO",
        "cat_label": "Probate",
        "clerk_doc_types": ["Probate", "Death Certificate"],
        "keywords": [],
    },
    {
        "cat": "NOC",
        "cat_label": "Notice of Commencement",
        "clerk_doc_types": ["Notice of Commencement"],
        "keywords": [],
    },
    {
        "cat": "RELLP",
        "cat_label": "Release Lis Pendens",
        "clerk_doc_types": [
            "Release/Revoke/Satisfy or Terminate",
            "Release/Revoke/Satisfy or Terminate Hidden from Web",
        ],
        "keywords": [r"lis\s+pendens"],
    },
    {
        "cat": "LIE",
        "cat_label": "Lien",
        "clerk_doc_types": ["Lien"],
        "keywords": [],
    },
    {
        "cat": "LIEX",
        "cat_label": "Lien Hidden from Web",
        "clerk_doc_types": ["Lien Hidden from Web"],
        "keywords": [],
    },
]

# Unique clerk doc-type labels to query (deduplicated across lead types).
ALL_CLERK_DOC_TYPES: list[str] = sorted({
    label for lt in LEAD_TYPES for label in lt["clerk_doc_types"]
})

# Paths (relative to repo root).
REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO_ROOT / "dashboard" / "records.json"
DATA_JSON      = REPO_ROOT / "data" / "records.json"
GHL_CSV        = REPO_ROOT / "dashboard" / "leads_ghl.csv"
BCPA_DBF_PATH  = REPO_ROOT / "data" / "BCPA_TAX_ROLL.DBF"
# Pre-exported clerk CSV (optional deterministic fallback).
CLERK_CSV_DIR  = REPO_ROOT / "data"

# Network config.
REQUEST_TIMEOUT = 45
REQUEST_RETRIES = 3
REQUEST_BACKOFF = 2.0
PLAYWRIGHT_TIMEOUT_MS = 45_000

# Scoring knobs.
SCORE_BASE = 30
SCORE_PER_FLAG = 10
SCORE_LP_FC_COMBO = 20
SCORE_AMOUNT_OVER_100K = 15
SCORE_AMOUNT_OVER_50K = 10
SCORE_NEW_THIS_WEEK = 5
SCORE_HAS_ADDRESS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("broward")


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Lead:
    """Final record shape written to records.json."""
    doc_num: str = ""
    doc_type: str = ""            # Raw clerk label (e.g. "Lis Pendens")
    filed: str = ""               # ISO yyyy-mm-dd
    cat: str = ""                 # Short code (LP, NOFC, FJ, ...)
    cat_label: str = ""           # Human label
    owner: str = ""               # Motivated seller (Direct or Indirect)
    grantee: str = ""
    amount: float = 0.0
    legal: str = ""
    prop_address: str = ""
    prop_city: str = ""
    prop_state: str = "FL"
    prop_zip: str = ""
    mail_address: str = ""
    mail_city: str = ""
    mail_state: str = ""
    mail_zip: str = ""
    clerk_url: str = ""
    flags: list[str] = field(default_factory=list)
    score: int = 0


@dataclass
class RawRow:
    """Raw clerk row as it comes out of Acclaim (or a pre-saved CSV)."""
    DirectName: str = ""
    IndirectName: str = ""
    RecordDate: str = ""
    DocTypeDescription: str = ""
    InstrumentNumber: str = ""
    BookType: str = ""
    BookPage: str = ""
    DocLegalDescription: str = ""
    Consideration: str = ""
    CaseNumber: str = ""


# =============================================================================
# RETRY HELPER
# =============================================================================

def with_retries(fn, *args, attempts: int = REQUEST_RETRIES,
                 backoff: float = REQUEST_BACKOFF, label: str = "", **kwargs):
    """Call `fn(*args, **kwargs)` up to `attempts` times with exponential backoff."""
    last: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 -- want to catch truly anything
            last = exc
            wait = backoff ** i
            log.warning("%s attempt %d/%d failed: %s -- retrying in %.1fs",
                        label or fn.__name__, i, attempts, exc, wait)
            time.sleep(wait)
    raise last  # type: ignore[misc]


# =============================================================================
# CLERK PORTAL -- PLAYWRIGHT
# =============================================================================
#
# AcclaimWeb is an ASP.NET MVC app using Telerik UI (t-combobox, t-grid,
# t-grid-pager). The search form submits via Sys.Mvc.AsyncForm.
#
# Strategy (verified against live portal DOM):
#   1. Launch headless Chromium.
#   2. Navigate to /AcclaimWeb/search/SearchTypeDocType.
#   3. Dismiss any disclaimer/terms modal.
#   4. Leave DocTypes as "All" (the default) -- returns every doc type
#      in the date range. AcclaimWeb caps results at 10,000, which is
#      well above a normal 7-day window (~9,500 rows in Broward).
#   5. Fill in the Record Date range (beginDateInput / endDateInput).
#   6. Click the Search button.
#   7. Scrape the results grid at div#RsltsGrid table, then click
#      t-grid-pager "Next" until disabled.
#
# Column order in the results grid is POSITIONAL (no <th> labels):
#   [0] blank/selector   [1] DirectName        [2] IndirectName
#   [3] RecordDate       [4] DocTypeDescription [5] InstrumentNumber
#   [6] BookType         [7] BookPage           [8] DocLegalDescription
#   [9] Consideration    [10] CaseNumber
# (Order observed in live DOM; spec matches the CSV export field order.)
# =============================================================================

# Hard cap returned by AcclaimWeb per search.
ACCLAIM_RESULT_CAP = 10_000

# Positional column mapping for the results grid (no <th> tags).
RESULTS_COLUMNS = [
    None,                  # 0: row selector / blank
    "DirectName",          # 1
    "IndirectName",        # 2
    "RecordDate",          # 3
    "DocTypeDescription",  # 4
    "InstrumentNumber",    # 5
    "BookType",            # 6
    "BookPage",            # 7
    "DocLegalDescription", # 8
    "Consideration",       # 9
    "CaseNumber",          # 10
]


async def scrape_clerk_playwright(date_from: str, date_to: str) -> list[dict[str, str]]:
    """
    Drive AcclaimWeb in headless Chromium. AcclaimWeb caps results at 10,000
    per search and a full weekly DocType=All window in Broward typically
    returns ~9,500 records -- right on the edge. To stay safely under the
    cap, we split the window into 1-day slices and run one search per day,
    merging results (deduped by InstrumentNumber).
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PwTimeout
    except ImportError:
        log.warning("playwright not installed; skipping live clerk scrape.")
        return []

    rows: list[dict[str, str]] = []

    # Build the list of 1-day windows [(from, to), ...] spanning the target range.
    try:
        start_dt = datetime.strptime(date_from, "%m/%d/%Y")
        end_dt   = datetime.strptime(date_to,   "%m/%d/%Y")
    except ValueError:
        log.error("Bad date range %s..%s", date_from, date_to)
        return rows

    day_windows: list[tuple[str, str]] = []
    cur = start_dt
    while cur <= end_dt:
        d = cur.strftime("%m/%d/%Y")
        day_windows.append((d, d))
        cur += timedelta(days=1)

    log.info("Window split into %d daily searches to stay under AcclaimWeb's 10k cap.",
             len(day_windows))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()
        page.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)

        seen: set[str] = set()
        for d_from, d_to in day_windows:
            try:
                day_rows = await _run_all_doctypes_search(page, d_from, d_to)
                new_count = 0
                for r in day_rows:
                    inum = r.get("InstrumentNumber", "")
                    if inum and inum not in seen:
                        seen.add(inum)
                        rows.append(r)
                        new_count += 1
                log.info("Day %s: %d rows (%d new; grand total %d)",
                         d_from, len(day_rows), new_count, len(rows))
            except PwTimeout as exc:
                log.warning("Day %s timed out: %s", d_from, exc)
            except Exception as exc:
                log.exception("Day %s failed: %s", d_from, exc)

        await context.close()
        await browser.close()

    return rows


async def _run_all_doctypes_search(page, date_from: str, date_to: str) -> list[dict[str, str]]:
    """Single search with DocTypes=All, paginated through every result page."""
    rows: list[dict[str, str]] = []

    log.info("Navigating to AcclaimWeb search page...")
    await page.goto(CLERK_SEARCH_URL, wait_until="domcontentloaded")

    # Disclaimer handling. Broward redirects all /search/* URLs to
    # /AcclaimWeb/search/Disclaimer?st=... on first visit, and keeps
    # sending you there until you submit the accept form. The button
    # text varies across browsers / ADA modes -- could be "Accept",
    # "I Accept", "Agree", "Continue", "Submit", or just an <input>
    # with no text. The safest approach: if we're on the Disclaimer
    # URL, iterate over every submit/button on the page and click
    # whichever one causes a URL change to /SearchTypeDocType.
    for attempt in range(5):
        if "/disclaimer" not in page.url.lower():
            break
        log.info("On disclaimer page (attempt %d); trying to dismiss...", attempt + 1)

        # Snapshot all candidate "go forward" controls on the page.
        candidate_selectors = [
            "input[type='submit']",
            "input[type='button']",
            "button[type='submit']",
            "button",
            "a.button, a.btn",
        ]
        clicked_this_attempt = False
        for sel in candidate_selectors:
            handles = await page.locator(sel).all()
            for h in handles:
                try:
                    # Grab identifying info for logging.
                    val = (await h.get_attribute("value")) or ""
                    txt = (await h.text_content()) or ""
                    label = (val + " " + txt).strip()

                    # Skip obvious "Decline" / "Cancel" / "Exit" buttons.
                    if re.search(r"decline|cancel|exit|back|decline|no\b",
                                 label, re.IGNORECASE):
                        continue

                    log.info("  clicking disclaimer control: %r", label or sel)
                    await h.click(timeout=5_000)
                    await page.wait_for_load_state("domcontentloaded", timeout=10_000)
                    clicked_this_attempt = True
                    break
                except Exception as exc:
                    log.debug("  click skip: %s", exc)
                    continue
            if clicked_this_attempt:
                break

        if not clicked_this_attempt:
            log.info("  no clickable controls found; trying direct nav.")
            await page.goto(CLERK_SEARCH_URL, wait_until="domcontentloaded")

        await page.wait_for_timeout(500)

    log.info("Post-disclaimer URL: %s", page.url)

    # The DocTypes picker defaults to "All" (textarea#DocTypes has value "all").
    # We don't touch it -- "All" returns every doc type in the date range.
    log.info("Leaving DocTypes=All to fetch every type in one search.")

    # Fill the date range. Confirmed IDs from the live portal:
    #   <input id="RecordDateFrom" name="RecordDateFrom" ...>  (hidden backing)
    #   <input id="RecordDateTo"   name="RecordDateTo"   ...>  (hidden backing)
    # These are inside Telerik t-datepicker widgets. The USER-VISIBLE input
    # is a sibling with class "t-input" -- so we set BOTH: the backing
    # store via its id, and any visible t-input inside the same widget
    # so the UI reflects the value. We use state="attached" because the
    # backing input isn't visually displayed.
    try:
        await page.wait_for_selector(
            "input#RecordDateFrom", state="attached", timeout=15_000
        )
        await page.wait_for_selector(
            "input#RecordDateTo", state="attached", timeout=15_000
        )

        # Set values via JS. Telerik t-datepicker listens for input/change
        # on the backing input; the visible t-input is also updated so the
        # user-facing display is in sync.
        await page.evaluate(
            """([fromVal, toVal]) => {
                const setField = (hiddenId, val) => {
                    // Backing input (by id)
                    const hidden = document.getElementById(hiddenId);
                    if (hidden) {
                        hidden.value = val;
                        hidden.dispatchEvent(new Event('input',  {bubbles: true}));
                        hidden.dispatchEvent(new Event('change', {bubbles: true}));
                        hidden.dispatchEvent(new Event('blur',   {bubbles: true}));

                        // Also update the sibling t-input inside the same
                        // t-datepicker widget so the visible text matches.
                        const widget = hidden.closest('.t-datepicker, .t-widget');
                        if (widget) {
                            const vis = widget.querySelector('input.t-input');
                            if (vis && vis !== hidden) {
                                vis.value = val;
                                vis.dispatchEvent(new Event('input',  {bubbles: true}));
                                vis.dispatchEvent(new Event('change', {bubbles: true}));
                                vis.dispatchEvent(new Event('blur',   {bubbles: true}));
                            }
                        }
                    }
                };
                setField('RecordDateFrom', fromVal);
                setField('RecordDateTo',   toVal);
            }""",
            [date_from, date_to],
        )
        await page.wait_for_timeout(500)
    except Exception as exc:
        log.warning("Could not fill date range: %s", exc)
        # Diagnostic: what's actually on the page?
        try:
            log.info("DIAG current URL: %s", page.url)
            from_count = await page.locator("input#RecordDateFrom").count()
            to_count   = await page.locator("input#RecordDateTo").count()
            log.info("DIAG #RecordDateFrom=%d  #RecordDateTo=%d", from_count, to_count)
            # Any visible date-looking input we might be missing?
            visible_inputs = await page.locator(
                "input[type='text']:visible, input[id*='Date']:visible"
            ).count()
            log.info("DIAG visible-text/Date inputs on page: %d", visible_inputs)
            title = await page.title()
            log.info("DIAG page title: %r", title)
            # Dump a 2000-char snippet of the body for inspection.
            body_snippet = await page.evaluate(
                "() => document.body ? document.body.innerText.substring(0, 2000) : ''"
            )
            log.info("DIAG body text (first 2000 chars): %s", body_snippet)
        except Exception as diag_exc:
            log.info("DIAG failed: %s", diag_exc)
        return rows

    # Click Search. The form is an Sys.Mvc.AsyncForm, so the page doesn't
    # navigate -- it replaces divResultsPrint / #RsltsGrid inline.
    log.info("Submitting search (DocTypes=All, %s -> %s)...", date_from, date_to)
    clicked = False
    for sel in (
        "input[type='submit'][value='Search']",
        "button:has-text('Search')",
        "input#btnSearch",
        "input[id$='btnSearch']",
        "input[name$='btnSearch']",
    ):
        btn = page.locator(sel)
        if await btn.count() > 0:
            try:
                await btn.first.click()
                clicked = True
                break
            except Exception:
                continue
    if not clicked:
        log.error("Could not find a Search button on the page.")
        return rows

    # Wait for the results grid to render. The grid container is #RsltsGrid;
    # when it contains <tbody><tr>, we have results.
    # We also poll for AcclaimWeb's "maximum limit exceeded" error modal
    # so we can bail early instead of waiting 45s for nothing.
    grid_selector = (
        "div#RsltsGrid table tbody tr, div.t-grid-content table tbody tr"
    )
    error_text_selector = "text=/maximum limit|exceeded the maximum|no records/i"

    try:
        # First, wait briefly for either: a results row, or an error modal.
        await page.wait_for_selector(
            f"{grid_selector}, {error_text_selector}",
            timeout=PLAYWRIGHT_TIMEOUT_MS,
        )
    except Exception as exc:
        log.warning("Results grid never rendered: %s", exc)
        return rows

    # Check if we hit the error modal (too many results for this day).
    err = page.locator(error_text_selector)
    if await err.count() > 0:
        err_text = (await err.first.text_content()) or ""
        log.warning("AcclaimWeb returned error: %s", err_text.strip()[:200])
        return rows

    # Also check "no records" explicitly (empty days -- e.g. weekends/holidays).
    html_preview = await page.content()
    if "no records" in html_preview.lower() and "RsltsGrid" not in html_preview:
        log.info("No records returned for this window.")
        return rows

    # Page through every results page.
    max_pages = 500
    seen_instrument_ids: set[str] = set()
    for page_num in range(1, max_pages + 1):
        html = await page.content()
        page_rows = parse_clerk_results_html(html)
        if not page_rows:
            break

        new_count = 0
        for r in page_rows:
            inum = r.get("InstrumentNumber", "")
            if inum and inum not in seen_instrument_ids:
                seen_instrument_ids.add(inum)
                rows.append(r)
                new_count += 1

        log.info("  page %d: %d rows (%d new; running total %d)",
                 page_num, len(page_rows), new_count, len(rows))

        # Protective cap -- AcclaimWeb caps at 10,000 anyway.
        if len(rows) >= ACCLAIM_RESULT_CAP:
            log.info("Hit AcclaimWeb's 10,000-record cap.")
            break
        if new_count == 0:
            # Pager didn't advance -- bail to avoid an infinite loop.
            break

        # Find + click the pager Next button. Telerik t-grid-pager uses an
        # <a class="t-arrow-next"> or <input class="t-arrow-next"> inside
        # div.t-grid-pager. When we're on the last page, the link carries
        # class "t-state-disabled".
        next_sel = (
            "div.t-grid-pager a.t-arrow-next:not(.t-state-disabled), "
            "div.t-grid-pager .t-icon.t-arrow-next:not(.t-state-disabled), "
            "a[title='Go to the next page']:not(.t-state-disabled)"
        )
        try:
            nxt = page.locator(next_sel).first
            if await nxt.count() == 0:
                break
            await nxt.click()
            # Wait for the grid to refresh -- rows should change.
            await page.wait_for_timeout(1500)
        except Exception as exc:
            log.warning("Pagination stopped on page %d: %s", page_num, exc)
            break

    return rows


def parse_clerk_results_html(html: str) -> list[dict[str, str]]:
    """
    Parse the AcclaimWeb results grid.

    The grid is at div#RsltsGrid -> div.t-grid-content -> table -> tbody -> tr.
    There are NO <th> headers -- columns are positional (see RESULTS_COLUMNS).
    Rows alternate between no class and class="t-alt".
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")

    # Primary: the grid inside div#RsltsGrid.
    container = soup.find("div", id="RsltsGrid") or soup.find("div", class_="t-grid-content")
    table = container.find("table") if container else None

    # Fallback: any <table> whose rows have ~11 <td>s in the expected shape.
    if table is None:
        for t in soup.find_all("table"):
            sample_tr = t.find("tr")
            if sample_tr and 10 <= len(sample_tr.find_all("td")) <= 13:
                table = t
                break
    if table is None:
        return []

    results: list[dict[str, str]] = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < len(RESULTS_COLUMNS):
            continue

        row: dict[str, str] = {}
        for i, td in enumerate(cells):
            key = RESULTS_COLUMNS[i] if i < len(RESULTS_COLUMNS) else None
            if key is None:
                continue
            row[key] = td.get_text(" ", strip=True)
            # If this cell has a link to the document detail, capture it.
            a = td.find("a", href=True)
            if a and not row.get("__detail_href"):
                href = a["href"]
                if href.startswith("/") or "AcclaimWeb" in href or "Instrument" in href:
                    row["__detail_href"] = urljoin(CLERK_BASE + "/", href)

        if row.get("InstrumentNumber"):
            results.append(row)
    return results


# =============================================================================
# CLERK CSV FALLBACK
# =============================================================================
#
# AcclaimWeb's UI exposes a CSV export. If one or more SearchResults*.csv
# files are present in data/ at runtime, we load from them -- deterministic
# and immune to portal captchas. The CI workflow can populate these via a
# prior export step or, in practice, by committing a nightly dump.

def load_clerk_csv_fallback(csv_dir: Path) -> list[dict[str, str]]:
    """Load any SearchResults*.csv files from csv_dir."""
    rows: list[dict[str, str]] = []
    if not csv_dir.is_dir():
        return rows
    for path in sorted(csv_dir.glob("SearchResults*.csv")):
        log.info("Loading clerk CSV fallback: %s", path)
        try:
            with path.open(encoding="utf-8-sig", newline="") as f:
                for r in csv.DictReader(f):
                    rows.append(r)
        except Exception as exc:
            log.error("Failed to read %s: %s", path, exc)
    return rows


# =============================================================================
# BCPA PARCEL APPRAISER -- DBF DOWNLOAD
# =============================================================================

def download_bcpa_dbf(dest: Path) -> bool:
    """
    Acquire the BCPA bulk parcel DBF. Three routes, tried in order:

      1. If BCPA_DBF_URL is set, GET it directly (simplest -- this is the
         pre-signed URL you'd generate by subscribing to BCPA's commercial
         data service or hosting your own mirror).
      2. If BCPA_FORM_URL + BCPA_POSTBACK are set, fetch the form page,
         harvest __VIEWSTATE, and POST the __doPostBack that triggers
         the file download.
      3. Otherwise, skip BCPA enrichment entirely.

    Returns True on success. Never raises -- on failure we log and return
    False so the pipeline still produces (less-enriched) leads.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Route 1: direct URL.
    if BCPA_DBF_URL:
        log.info("Downloading BCPA parcel DBF via direct URL.")
        try:
            resp = with_retries(
                requests.get,
                BCPA_DBF_URL,
                timeout=REQUEST_TIMEOUT * 4,  # This file is big.
                stream=True,
                label="BCPA direct download",
            )
            resp.raise_for_status()
            with dest.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            log.info("BCPA DBF saved: %s (%s bytes)", dest, dest.stat().st_size)
            return True
        except Exception as exc:
            log.error("Direct BCPA download failed: %s", exc)

    # Route 2: ASP.NET __doPostBack on a form page.
    if BCPA_FORM_URL and BCPA_POSTBACK:
        log.info("Downloading BCPA parcel DBF via __doPostBack flow.")
        try:
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            })

            # Fetch the form to harvest hidden fields.
            form_resp = with_retries(
                sess.get, BCPA_FORM_URL, timeout=REQUEST_TIMEOUT,
                label="BCPA form fetch",
            )
            form_resp.raise_for_status()
            soup = BeautifulSoup(form_resp.text, "lxml")

            payload = {
                "__EVENTTARGET":     BCPA_POSTBACK,
                "__EVENTARGUMENT":   "",
            }
            for hidden in soup.find_all("input", {"type": "hidden"}):
                name = hidden.get("name")
                if name and name not in payload:
                    payload[name] = hidden.get("value", "")

            # POST the postback. The response is the DBF payload
            # (or a zip; both are handled by streaming to disk).
            dl_resp = with_retries(
                sess.post, BCPA_FORM_URL,
                data=payload, timeout=REQUEST_TIMEOUT * 4, stream=True,
                label="BCPA postback download",
            )
            dl_resp.raise_for_status()
            with dest.open("wb") as f:
                for chunk in dl_resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            log.info("BCPA DBF saved via postback: %s (%s bytes)",
                     dest, dest.stat().st_size)
            return True
        except Exception as exc:
            log.error("BCPA postback download failed: %s", exc)

    log.info("No BCPA download route configured -- skipping enrichment.")
    return False


# =============================================================================
# BCPA PARCEL INDEX
# =============================================================================

class ParcelIndex:
    """
    Owner-name index over the BCPA bulk parcel DBF.

    Keyed by 3 normalized name variants:
        "FIRST LAST", "LAST FIRST", "LAST, FIRST"

    Column aliases (first match wins):
        owner:         OWNER / OWN1
        site addr:     SITE_ADDR / SITEADDR
        site city:     SITE_CITY
        site zip:      SITE_ZIP
        mailing addr:  ADDR_1 / MAILADR1
        mailing city:  CITY / MAILCITY
        mailing state: STATE
        mailing zip:   ZIP / MAILZIP
    """

    OWNER_COLS      = ("OWNER", "OWN1", "OWNL", "OWN_NAME")
    SITE_ADDR_COLS  = ("SITE_ADDR", "SITEADDR", "PROPADDR")
    SITE_CITY_COLS  = ("SITE_CITY", "SITECITY")
    SITE_ZIP_COLS   = ("SITE_ZIP", "SITEZIP")
    MAIL_ADDR_COLS  = ("ADDR_1", "MAILADR1", "MAILADRL", "MAIL_ADDR")
    MAIL_CITY_COLS  = ("CITY", "MAILCITY", "MAIL_CITY")
    MAIL_STATE_COLS = ("STATE", "MAILSTATE", "MAIL_STATE")
    MAIL_ZIP_COLS   = ("ZIP", "MAILZIP", "MAIL_ZIP")

    def __init__(self) -> None:
        self.by_name: dict[str, dict[str, str]] = {}
        self.record_count = 0

    @staticmethod
    def _first(rec: dict[str, Any], cols: tuple[str, ...]) -> str:
        for c in cols:
            v = rec.get(c)
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""

    @staticmethod
    def _normalize(name: str) -> str:
        if not name:
            return ""
        return re.sub(r"\s+", " ", re.sub(r"[^\w\s,]", "", name.upper())).strip()

    @classmethod
    def name_variants(cls, raw_name: str) -> set[str]:
        """
        Produce the three required lookup variants.
            FIRST LAST, LAST FIRST, LAST, FIRST
        Handles middle names / initials / suffixes.
        """
        norm = cls._normalize(raw_name)
        if not norm:
            return set()

        variants: set[str] = {norm}

        if "," in norm:
            left, _, right = norm.partition(",")
            left, right = left.strip(), right.strip()
            if left and right:
                variants.add(f"{left}, {right}")      # LAST, FIRST
                variants.add(f"{left} {right}")       # LAST FIRST
                variants.add(f"{right} {left}")       # FIRST LAST
                # Drop middle names for a tighter match.
                first_only = right.split()[0] if right.split() else ""
                if first_only:
                    variants.add(f"{first_only} {left}")
                    variants.add(f"{left} {first_only}")
                    variants.add(f"{left}, {first_only}")
        else:
            parts = norm.split()
            if len(parts) >= 2:
                first, last = parts[0], parts[-1]
                variants.add(f"{first} {last}")
                variants.add(f"{last} {first}")
                variants.add(f"{last}, {first}")

        return {v for v in variants if v}

    def load(self, dbf_path: Path) -> None:
        """Load the DBF and build the index. Never raises."""
        if not dbf_path.exists():
            log.info("BCPA DBF not present at %s; parcel enrichment disabled.", dbf_path)
            return
        try:
            from dbfread import DBF  # type: ignore
        except ImportError:
            log.warning("dbfread not installed; cannot enrich parcels.")
            return

        log.info("Loading BCPA DBF: %s", dbf_path)
        try:
            table = DBF(
                str(dbf_path),
                load=False,
                ignore_missing_memofile=True,
                char_decode_errors="ignore",
            )
            for rec in table:
                try:
                    rec = {k.upper(): v for k, v in dict(rec).items()}
                    owner = self._first(rec, self.OWNER_COLS)
                    if not owner:
                        continue

                    parcel = {
                        "owner":      owner,
                        "site_addr":  self._first(rec, self.SITE_ADDR_COLS),
                        "site_city":  self._first(rec, self.SITE_CITY_COLS),
                        "site_zip":   self._first(rec, self.SITE_ZIP_COLS),
                        "mail_addr":  self._first(rec, self.MAIL_ADDR_COLS),
                        "mail_city":  self._first(rec, self.MAIL_CITY_COLS),
                        "mail_state": self._first(rec, self.MAIL_STATE_COLS),
                        "mail_zip":   self._first(rec, self.MAIL_ZIP_COLS),
                    }

                    for variant in self.name_variants(owner):
                        # First writer wins -- avoids overwriting early hits.
                        self.by_name.setdefault(variant, parcel)

                    self.record_count += 1
                    if self.record_count % 100_000 == 0:
                        log.info("  ...indexed %d parcels", self.record_count)

                except Exception as row_exc:
                    # Never let a single malformed DBF record kill the load.
                    log.debug("Skipping bad DBF record: %s", row_exc)
                    continue

        except Exception as exc:
            log.error("Failed to load BCPA DBF: %s", exc)
            return

        log.info("BCPA index ready: %d parcels, %d name variants",
                 self.record_count, len(self.by_name))

    def load_fdor_csv(self, csv_path: Path) -> None:
        """
        Load the Florida Department of Revenue NAL (Name/Address/Legal) CSV.
        Format: comma-delimited with quoted strings, one row per parcel.

        Relevant columns:
            OWN_NAME                              -> owner name
            PHY_ADDR1, PHY_ADDR2, PHY_CITY,
            PHY_ZIPCD                             -> property site address
            OWN_ADDR1, OWN_ADDR2, OWN_CITY,
            OWN_STATE, OWN_ZIPCD                  -> mailing address

        File is large (~370 MB, ~750k rows). We stream row-by-row.
        """
        if not csv_path.exists():
            return
        import csv as _csv

        log.info("Loading FDOR NAL CSV: %s", csv_path)
        try:
            # Increase field size limit -- FDOR legal descriptions can be huge.
            try:
                _csv.field_size_limit(10_000_000)
            except OverflowError:
                _csv.field_size_limit(2**31 - 1)

            with open(csv_path, "r", encoding="utf-8", errors="replace",
                     newline="") as fh:
                reader = _csv.DictReader(fh)
                for rec in reader:
                    try:
                        owner = (rec.get("OWN_NAME") or "").strip()
                        if not owner:
                            continue

                        # Property site address: join ADDR1 + ADDR2 if both present.
                        phy1 = (rec.get("PHY_ADDR1") or "").strip()
                        phy2 = (rec.get("PHY_ADDR2") or "").strip()
                        site_addr = " ".join(p for p in (phy1, phy2) if p)

                        # Mailing address: same treatment.
                        own1 = (rec.get("OWN_ADDR1") or "").strip()
                        own2 = (rec.get("OWN_ADDR2") or "").strip()
                        mail_addr = " ".join(p for p in (own1, own2) if p)

                        parcel = {
                            "owner":      owner,
                            "site_addr":  site_addr,
                            "site_city":  (rec.get("PHY_CITY") or "").strip(),
                            "site_zip":   (rec.get("PHY_ZIPCD") or "").strip(),
                            "mail_addr":  mail_addr,
                            "mail_city":  (rec.get("OWN_CITY") or "").strip(),
                            "mail_state": (rec.get("OWN_STATE") or "").strip(),
                            "mail_zip":   (rec.get("OWN_ZIPCD") or "").strip(),
                        }

                        for variant in self.name_variants(owner):
                            self.by_name.setdefault(variant, parcel)

                        self.record_count += 1
                        if self.record_count % 100_000 == 0:
                            log.info("  ...indexed %d parcels", self.record_count)

                    except Exception as row_exc:
                        log.debug("Skipping bad NAL row: %s", row_exc)
                        continue

        except Exception as exc:
            log.error("Failed to load FDOR NAL CSV: %s", exc)
            return

        log.info("FDOR NAL index ready: %d parcels, %d name variants",
                 self.record_count, len(self.by_name))

    def lookup(self, raw_name: str) -> dict[str, str] | None:
        if not self.by_name or not raw_name:
            return None
        for variant in self.name_variants(raw_name):
            hit = self.by_name.get(variant)
            if hit:
                return hit
        return None


# =============================================================================
# CLASSIFICATION + PARSING
# =============================================================================

# Institutional grantor patterns -- for LP/NOFC/FJ/LIE etc. the "DirectName"
# is typically the bank/HOA/city filing the claim, while the actual motivated
# seller is in "IndirectName". We flip the roles when the Direct name is
# obviously institutional.
# Institutional FILER patterns -- banks, servicers, HOAs, cities, law firms.
# When the DirectName matches, we flip Direct/Indirect so the motivated
# seller (homeowner or real LLC) lands in `owner`.
FILER_RE = re.compile(
    r"\b("
    # Law firms / attorneys
    r"LAW\s+(PA|PLLC|LLC|FIRM|OFFICE)|ATTORN|"
    # Banks / credit unions / mortgage servicers
    r"BANK(\s+OF|\s+NA|\s+NATIONAL|\s+USA|\s+TRUST|\s+NATIONAL\s+ASSN|$)|"
    r"CREDIT\s+UNION|MORTGAGE|FINANCIAL|CAPITAL\s+(ONE|LLC|CORP)|SYNCHRONY|"
    r"US\s+BANK|U\.?S\.?\s+BANK|DISCOVER\s+BANK|CREDIT\s+ACCEPTANCE|"
    r"PENNYMAC|WILMINGTON\s+(SAVINGS|TRUST)|FEDERAL\s+HOME\s+LOAN|"
    r"FANNIE\s+MAE|FREDDIE\s+MAC|NEWREZ|SHELLPOINT|NATIONSTAR|"
    r"MR\.?\s+COOPER|ROCKET\s+(MORTGAGE|LOANS)|CARRINGTON|"
    r"SELENE\s+FINANCE|SERVIS\s+ONE|SPECIALIZED\s+LOAN|"
    r"ASSET\s+(COMPANY|ISSUER)|LOAN\s+ASSET|"
    # Government / municipalities
    r"CITY\s+OF|TOWN\s+OF|\w+\s+CITY$|COUNTY\s+OF|"
    r"STATE\s+OF\s+FLORIDA|INTERNAL\s+REVENUE|DEPT\s+OF|DEPARTMENT\s+OF|"
    # HOAs / condo associations
    r"HOMEOWNERS?\s+ASS|CONDOMINIUM\s+(ASS|APTS)|CONDO\s+(ASS|APTS)|"
    r"PROPERTY\s+OWNERS|HOA\b|ASSN\s+INC\b|ASSOCIATION\s+INC\b|"
    r"NATIONAL\s+ASSN|"
    # Debt collectors / credit recovery (file FJs against consumers)
    r"PORTFOLIO\s+RECOVERY|MIDLAND\s+(CREDIT|FUNDING)|"
    r"JEFFERSON\s+CAPITAL|VELOCITY\s+(INVESTMENT|COMMERCIAL)|"
    r"LVNV\s+FUNDING|CAVALRY\s+SPV|VEROS\s+CREDIT|"
    r"CACH\s+LLC|UNIFIN\s+INC|CREDIT\s+COLLECTION|"
    r"ASSET\s+ACCEPTANCE|RESURGENT\s+CAPITAL|CROWN\s+ASSET|"
    # Contractors / vendors (NOC filings put contractor in Direct)
    r"GOODLEAP|LENNOX|CONTRACTORS?\s+(INC|LLC|CORP)|ROOFING|"
    r"INDUSTRIES\s+INC|CONSTRUCTION\s+(INC|LLC|CORP|CO)"
    r")\b",
    re.IGNORECASE,
)

# Owner-LLC pattern -- any corporate-style suffix. Used for the
# "LLC / corp owner" FLAG, independent of whether the owner is also
# a filer. (A real-estate LLC that owns a property and gets a lien
# filed against it counts as an LLC/corp owner lead.)
LLC_CORP_RE = re.compile(
    r"\b(LLC|L\.?L\.?C\.?|INC|INCORPORATED|CORP|CORPORATION|"
    r"LP|LLP|LTD|TRUST|TRS|REV\s+TR)\b",
    re.IGNORECASE,
)


def is_filer(name: str) -> bool:
    """True if the name looks like a bank/servicer/HOA/city/law firm filing the record."""
    return bool(name and FILER_RE.search(name))


def classify(row: dict[str, str]) -> dict[str, Any] | None:
    """Return the matching LEAD_TYPES rule, or None if the row isn't a target."""
    desc = (row.get("DocTypeDescription") or "").strip()
    searchable = " ".join([
        row.get("DocLegalDescription", ""),
        row.get("CaseNumber", ""),
        row.get("DirectName", ""),
        row.get("IndirectName", ""),
        desc,
    ]).lower()

    for rule in LEAD_TYPES:
        if desc not in rule["clerk_doc_types"]:
            continue
        if not rule["keywords"]:
            return rule
        for kw in rule["keywords"]:
            if re.search(kw, searchable, flags=re.IGNORECASE):
                return rule
    return None


def parse_filed_date(raw: str) -> str:
    if not raw:
        return ""
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw.strip()


def parse_amount(raw: str) -> float:
    if not raw:
        return 0.0
    try:
        return float(str(raw).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def build_clerk_url(instrument: str, detail_href: str = "") -> str:
    if detail_href:
        return detail_href
    if instrument:
        # Stable deep link pattern used by AcclaimWeb (opens the document).
        return f"{CLERK_BASE}/details/{instrument}"
    return CLERK_BASE


def row_to_lead(row: dict[str, str]) -> Lead | None:
    """Convert a raw clerk row into a populated Lead (pre-score)."""
    rule = classify(row)
    if not rule:
        return None

    try:
        instrument = (row.get("InstrumentNumber") or "").strip()
        direct = (row.get("DirectName") or "").strip()
        indirect = (row.get("IndirectName") or "").strip()
        cat = rule["cat"]

        # Claim-against-homeowner record types. In Broward, Direct is
        # always the plaintiff/creditor (bank, HOA, city, debt collector,
        # lienholder) and Indirect is always the defendant/debtor -- the
        # motivated seller. Flip unconditionally if Indirect has a value.
        # For RELLP we leave names as-is (release records have the owner
        # as Direct already). For PRO/NOC, no flip either.
        if cat in {"LP", "NOFC", "FJ", "LIE", "LIEX", "PALIE"} and indirect:
            direct, indirect = indirect, direct

        return Lead(
            doc_num=instrument,
            doc_type=(row.get("DocTypeDescription") or "").strip(),
            filed=parse_filed_date(row.get("RecordDate", "")),
            cat=cat,
            cat_label=rule["cat_label"],
            owner=direct,
            grantee=indirect,
            amount=parse_amount(row.get("Consideration", "")),
            legal=(row.get("DocLegalDescription") or "").strip(),
            clerk_url=build_clerk_url(instrument, row.get("__detail_href", "")),
        )
    except Exception as exc:
        log.warning("Bad row skipped: %s | %r", exc, row)
        return None


def enrich_with_parcel(lead: Lead, parcels: ParcelIndex) -> None:
    """Attach BCPA property & mailing addresses when available."""
    parcel = parcels.lookup(lead.owner)
    if not parcel:
        return
    lead.prop_address = parcel.get("site_addr", "")
    lead.prop_city    = parcel.get("site_city", "")
    lead.prop_state   = "FL"
    lead.prop_zip     = parcel.get("site_zip", "")
    lead.mail_address = parcel.get("mail_addr", "")
    lead.mail_city    = parcel.get("mail_city", "")
    lead.mail_state   = parcel.get("mail_state", "")
    lead.mail_zip     = parcel.get("mail_zip", "")


# =============================================================================
# FLAGS + SCORING
# =============================================================================
#
# Flag list (spec):
#   "Lis pendens", "Pre-foreclosure", "Judgment lien", "Tax lien",
#   "Mechanic lien", "Probate / estate", "LLC / corp owner", "New this week"
#
# Score rubric (spec):
#   Base 30
#   +10 per flag
#   +20 LP + foreclosure combo
#   +15 amount > $100k
#   +10 amount > $50k
#   +5  new this week
#   +5  has address
# =============================================================================

def derive_flags(lead: Lead, days_lookback: int = LOOKBACK_DAYS) -> list[str]:
    """Compute the flags list for a Lead based on cat + text + dates."""
    flags: list[str] = []
    text = " ".join([
        lead.doc_type, lead.legal, lead.owner, lead.grantee, lead.cat_label,
    ]).lower()

    # Lis pendens
    if lead.cat == "LP" or "lis pendens" in text:
        flags.append("Lis pendens")

    # Pre-foreclosure: NOFC, or LP text mentions foreclosure, or NOD
    if (lead.cat == "NOFC"
            or "foreclos" in text
            or "notice of default" in text
            or (lead.cat == "LP" and "foreclos" in text)):
        flags.append("Pre-foreclosure")

    # Judgment lien: FJ or wording "judgment lien"
    if lead.cat == "FJ" or "judgment lien" in text or "final judgment" in text:
        flags.append("Judgment lien")

    # Tax lien: PALIE or explicit tax-lien wording
    if lead.cat == "PALIE" or re.search(
        r"tax\s+lien|tax\s+deed|tax\s+delinquen|tax\s+certificate|non[- ]ad\s+valorem",
        text,
    ):
        flags.append("Tax lien")

    # Mechanic lien
    if re.search(r"mechanic'?s?\s+lien|claim\s+of\s+lien|construction\s+lien", text):
        flags.append("Mechanic lien")

    # Probate / estate
    if (lead.cat == "PRO"
            or re.search(r"probate|estate\s+of|deceased|personal\s+rep|executor|executrix|letters\s+testamentary|death\s+certificate", text)):
        flags.append("Probate / estate")

    # LLC / corp owner -- looking at the motivated seller (owner field).
    # Only flag real estate investor LLCs, not the bank/servicer/HOA filers
    # (those should have been flipped in row_to_lead anyway).
    if lead.owner and LLC_CORP_RE.search(lead.owner) and not is_filer(lead.owner):
        flags.append("LLC / corp owner")

    # New this week
    if lead.filed:
        try:
            filed_dt = datetime.strptime(lead.filed, "%Y-%m-%d").date()
            today = datetime.now(timezone.utc).date()
            if (today - filed_dt).days <= days_lookback:
                flags.append("New this week")
        except ValueError:
            pass

    # Dedupe while preserving order
    seen = set()
    ordered: list[str] = []
    for f in flags:
        if f not in seen:
            seen.add(f)
            ordered.append(f)
    return ordered


def score_lead(lead: Lead) -> int:
    """Apply the spec's scoring rubric and return 0-100."""
    score = SCORE_BASE

    # +10 per flag
    score += SCORE_PER_FLAG * len(lead.flags)

    # +20 combo: Lis pendens AND Pre-foreclosure
    if "Lis pendens" in lead.flags and "Pre-foreclosure" in lead.flags:
        score += SCORE_LP_FC_COMBO

    # Amount tiers (apply the bigger one only)
    if lead.amount > 100_000:
        score += SCORE_AMOUNT_OVER_100K
    elif lead.amount > 50_000:
        score += SCORE_AMOUNT_OVER_50K

    # New this week
    if "New this week" in lead.flags:
        score += SCORE_NEW_THIS_WEEK

    # Has address (property OR mailing)
    if lead.prop_address or lead.mail_address:
        score += SCORE_HAS_ADDRESS

    return max(0, min(100, int(score)))


# =============================================================================
# OUTPUT: records.json (dashboard + data copies)
# =============================================================================

def write_records_json(leads: list[Lead], date_from: str, date_to: str) -> None:
    """Write the records.json payload to both dashboard/ and data/."""
    payload = {
        "fetched_at": datetime.now(timezone.utc)
            .isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": CLERK_BASE,
        "date_range": {"from": date_from, "to": date_to},
        "total": len(leads),
        "with_address": sum(1 for l in leads if l.prop_address or l.mail_address),
        "records": [asdict(l) for l in leads],
    }
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    for target in (DASHBOARD_JSON, DATA_JSON):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        log.info("Wrote %d leads to %s", len(leads), target)


# =============================================================================
# OUTPUT: GHL CSV
# =============================================================================

GHL_HEADERS = [
    "First Name",
    "Last Name",
    "Mailing Address",
    "Mailing City",
    "Mailing State",
    "Mailing Zip",
    "Property Address",
    "Property City",
    "Property State",
    "Property Zip",
    "Lead Type",
    "Document Type",
    "Date Filed",
    "Document Number",
    "Amount/Debt Owed",
    "Seller Score",
    "Motivated Seller Flags",
    "Source",
    "Public Records URL",
]


def split_owner_name(owner: str) -> tuple[str, str]:
    """
    Split an owner string into (first, last). BCPA stores names as
    "LAST,FIRST MIDDLE" so that's the primary pattern.
    """
    if not owner:
        return "", ""
    o = owner.strip()
    if "," in o:
        last, _, rest = o.partition(",")
        rest = rest.strip()
        first = rest.split()[0] if rest else ""
        return first, last.strip()
    parts = o.split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return o, ""


def export_ghl_csv(leads: list[Lead], path: Path) -> None:
    """
    Write a GoHighLevel-ready CSV. Skips leads without any deliverable
    mailing address -- GHL imports fail on blank-address rows.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(GHL_HEADERS)
        for l in leads:
            # Prefer mailing for contact; fall back to property address.
            mail_addr = l.mail_address or l.prop_address
            mail_city = l.mail_city or l.prop_city
            mail_state = l.mail_state or l.prop_state or "FL"
            mail_zip = l.mail_zip or l.prop_zip
            if not mail_addr:
                # Without an address this row is useless for direct mail.
                continue

            first, last = split_owner_name(l.owner)
            w.writerow([
                first,
                last,
                mail_addr,
                mail_city,
                mail_state,
                mail_zip,
                l.prop_address,
                l.prop_city,
                l.prop_state or "FL",
                l.prop_zip,
                l.cat_label,
                l.doc_type,
                l.filed,
                l.doc_num,
                f"{l.amount:.2f}" if l.amount else "",
                l.score,
                "; ".join(l.flags),
                CLERK_BASE,
                l.clerk_url,
            ])
            written += 1
    log.info("Wrote %d rows to %s", written, path)


# =============================================================================
# MAIN
# =============================================================================

def date_window(lookback_days: int) -> tuple[str, str]:
    """Return (from_str, to_str) as MM/DD/YYYY suitable for the clerk form."""
    today = datetime.now(timezone.utc)
    start = today - timedelta(days=lookback_days)
    return start.strftime("%m/%d/%Y"), today.strftime("%m/%d/%Y")


async def run() -> int:
    date_from, date_to = date_window(LOOKBACK_DAYS)
    log.info("Broward scraper run  window=%s..%s  lookback=%d days",
             date_from, date_to, LOOKBACK_DAYS)

    # --- 1. Gather raw clerk rows -----------------------------------------
    raw_rows: list[dict[str, str]] = load_clerk_csv_fallback(CLERK_CSV_DIR)
    if raw_rows:
        log.info("Using %d rows from pre-exported clerk CSV(s).", len(raw_rows))
    else:
        try:
            raw_rows = await scrape_clerk_playwright(date_from, date_to)
        except Exception as exc:
            log.exception("Playwright scrape failed completely: %s", exc)
            raw_rows = []

    log.info("Total raw clerk rows: %d", len(raw_rows))

    # --- 2. Download BCPA parcel DBF + build index ------------------------
    try:
        download_bcpa_dbf(BCPA_DBF_PATH)
    except Exception as exc:
        log.error("BCPA download wrapper crashed: %s", exc)
    parcels = ParcelIndex()
    try:
        parcels.load(BCPA_DBF_PATH)
    except Exception as exc:
        log.error("BCPA index build crashed: %s", exc)

    # Fallback/primary: load FDOR NAL CSV if present. Shipped by Florida
    # Department of Revenue at https://floridarevenue.com/property/Pages/DataPortal.aspx
    # Landing at data/fdor/NAL*.csv. Works without dbfread.
    if not parcels.by_name:
        fdor_dir = REPO_ROOT / "data" / "fdor"
        if fdor_dir.is_dir():
            nal_csvs = sorted(fdor_dir.glob("NAL*.csv"))
            for nal in nal_csvs:
                try:
                    parcels.load_fdor_csv(nal)
                    # One NAL file is always complete -- stop at first.
                    if parcels.by_name:
                        break
                except Exception as exc:
                    log.error("FDOR NAL load crashed: %s", exc)

    # --- 3. Classify -> Lead, filter by date window, enrich ---------------
    window_iso = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)) \
        .strftime("%Y-%m-%d")
    leads: list[Lead] = []
    for row in raw_rows:
        try:
            lead = row_to_lead(row)
            if not lead:
                continue
            if lead.filed and lead.filed < window_iso:
                continue
            enrich_with_parcel(lead, parcels)
            leads.append(lead)
        except Exception as exc:
            log.warning("Row failed: %s", exc)

    log.info("Classified target leads: %d", len(leads))

    # --- 4. Flags + score -------------------------------------------------
    for l in leads:
        try:
            l.flags = derive_flags(l)
            l.score = score_lead(l)
        except Exception as exc:
            log.warning("Scoring failed for %s: %s", l.doc_num, exc)
            l.score = SCORE_BASE

    # Sort by score desc, filed desc
    leads.sort(key=lambda x: (x.score, x.filed), reverse=True)

    # --- 5. Write outputs -------------------------------------------------
    write_records_json(leads, date_from, date_to)
    export_ghl_csv(leads, GHL_CSV)

    hot  = sum(1 for l in leads if l.score >= 70)
    warm = sum(1 for l in leads if 50 <= l.score < 70)
    log.info("Summary  total=%d  hot(>=70)=%d  warm(50-69)=%d  "
             "with_address=%d",
             len(leads), hot, warm,
             sum(1 for l in leads if l.prop_address or l.mail_address))
    return 0


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        log.warning("Interrupted.")
        return 130
    except Exception:
        log.exception("Fatal error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
