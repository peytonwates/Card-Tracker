# pages/5_Grading.py
import json
import re
import time
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# =========================
# Page
# =========================
st.set_page_config(page_title="Grading", layout="wide")
st.title("Grading — Watchlist → Sales History")

# =========================
# Config
# =========================
WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

WATCHLIST_REQUIRED_COLS = ["Generation", "Set", "Card Name", "Card No", "Link"]

SALES_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "grade_bucket",   # ungraded / psa9 / psa10
    "sale_date",      # YYYY-MM-DD
    "price",          # numeric
    "title",
    "sale_key",       # stable dedupe key
    "updated_utc",
]

PRICE_RE = re.compile(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")

# =========================
# Helpers
# =========================
def _bs_parser():
    try:
        import lxml  # noqa
        return "lxml"
    except Exception:
        return "html.parser"

def safe_str(x):
    return "" if x is None else str(x)

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def is_blank(x) -> bool:
    return safe_str(x).strip() == ""

def _parse_sale_date(text: str):
    if not text:
        return None
    t = text.strip()
    m = ISO_DATE_RE.search(t)
    if m:
        try:
            d = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(d):
                return d.date()
        except Exception:
            return None
    try:
        d = pd.to_datetime(t, errors="coerce")
        if pd.notna(d) and d.year >= 2000:
            return d.date()
    except Exception:
        pass
    return None

def _classify_bucket_from_title(title: str) -> str:
    t = (title or "").upper()
    # We only care about psa9/psa10/ungraded in this minimal version
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*MINT\s*10\b", t) or re.search(r"\bGEM\s*MT\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"

def _gs_write_retry(fn, *args, **kwargs):
    max_tries = 6
    base_sleep = 0.8
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            raise
    raise RuntimeError("Google Sheets API quota exceeded (retries exhausted).")

# =========================
# Google Sheets auth
# =========================
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_info = json.loads(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    if "service_account_json_path" in st.secrets:
        sa_rel = st.secrets["service_account_json_path"]
        p = Path(sa_rel)
        if not p.is_absolute():
            p = Path.cwd() / sa_rel
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets: add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')

def get_ws(ws_name: str):
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    return sh.worksheet(ws_name)

def ensure_headers(ws, headers: list[str]):
    values = ws.get_all_values()
    if not values or not values[0]:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
        return
    current = [str(x).strip() for x in values[0]]
    if current != headers:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")

def read_df(ws) -> pd.DataFrame:
    vals = ws.get_all_values()
    if not vals or len(vals) < 1 or not vals[0]:
        return pd.DataFrame()
    header = [str(x).strip() for x in vals[0]]
    rows = vals[1:]
    out = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[:len(header)]
        out.append(r)
    return pd.DataFrame(out, columns=header)

# =========================
# HTTP
# =========================
@st.cache_resource
def get_http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (CardTracker; Streamlit)"})
    return s

def http_get_with_backoff(url: str, timeout=25, max_tries=6):
    sess = get_http_session()
    sleep_s = 1.0
    for _ in range(max_tries):
        r = sess.get(url, timeout=timeout)
        if r.status_code == 200:
            return r
        if r.status_code == 429:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.8, 25.0)
            continue
        if r.status_code in {500, 502, 503, 504}:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.6, 15.0)
            continue
        r.raise_for_status()
    raise requests.HTTPError(f"HTTPError: Too Many Requests (retries exhausted) for {url}")

# =========================
# PriceCharting parsing (correct $ selection)
# =========================
def _extract_sale_price_from_numeric_td(td_numeric) -> float:
    """
    PriceCharting rows can contain:
      - accepted price: <span class="js-price" title="best offer accepted price">$100.00</span>
      - list price:     <span class="js-price listed-price-inline" title="best offer list price">$119.00</span>

    Rule:
      1) If there is a js-price span whose title contains 'accepted' => THAT is sale price
      2) Else first js-price span
      3) Else first $ in td text
    """
    if td_numeric is None:
        return 0.0

    # 1) Accepted price wins
    accepted = td_numeric.select_one("span.js-price[title*='accepted']")
    if accepted:
        m = PRICE_RE.search(accepted.get_text(" ", strip=True) or "")
        if m:
            return safe_float(m.group(1), 0.0)

    # 2) First js-price span
    first_js = td_numeric.select_one("span.js-price")
    if first_js:
        m = PRICE_RE.search(first_js.get_text(" ", strip=True) or "")
        if m:
            return safe_float(m.group(1), 0.0)

    # 3) Fallback: first $ in td text
    txt = td_numeric.get_text(" ", strip=True) if hasattr(td_numeric, "get_text") else ""
    m = PRICE_RE.search(txt or "")
    if m:
        return safe_float(m.group(1), 0.0)

    return 0.0

def _parse_completed_auctions_section(soup: BeautifulSoup, section_id: str) -> list[dict]:
    """
    Parse PriceCharting 'completed auctions' table in a specific section:
      - completed-auctions-manual-only (PSA 10 tab in your screenshot)
      - completed-auctions-graded      (graded sales list; we classify by title)
    Only returns rows with valid date + price + title.
    """
    sales = []
    now = datetime.utcnow().isoformat()

    container = soup.select_one(f"#{section_id}")
    if not container:
        return sales

    # Tables in that section
    table = container.select_one("table")
    if not table:
        # sometimes table is nested
        table = container.select_one("table.hoverable-rows")
    if not table:
        return sales

    for tr in table.select("tbody tr"):
        # rows often have id="ebay-123..." etc.
        tds = tr.select("td")
        if len(tds) < 3:
            continue

        td_date = tr.select_one("td.date")
        td_title = tr.select_one("td.title")
        td_numeric = tr.select_one("td.numeric")

        sale_date = _parse_sale_date(td_date.get_text(" ", strip=True) if td_date else "")
        title = td_title.get_text(" ", strip=True) if td_title else ""
        price = _extract_sale_price_from_numeric_td(td_numeric)

        if not sale_date or not title or price <= 0:
            continue

        bucket = _classify_bucket_from_title(title)
        sale_key = f"{sale_date.isoformat()}|{price:.2f}|{bucket}|{title[:120].strip().lower()}"

        sales.append(
            {
                "sale_date": sale_date,
                "price": float(price),
                "title": title.strip(),
                "grade_bucket": bucket,
                "sale_key": sale_key,
                "updated_utc": now,
            }
        )

    # newest first
    sales.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return sales

@st.cache_data(ttl=60 * 60 * 2)
def fetch_sales_for_link(reference_link: str, per_bucket: int = 5) -> tuple[list[dict], dict]:
    """
    Pull:
      - ungraded: base page (we'll parse all completed-auctions sections we find)
      - psa9:     #completed-auctions-graded
      - psa10:    #completed-auctions-manual-only
    Then keep last N for each bucket.
    """
    debug = {
        "base_rows": 0,
        "graded_rows": 0,
        "manual_rows": 0,
        "kept_ungraded": 0,
        "kept_psa9": 0,
        "kept_psa10": 0,
        "notes": [],
    }

    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        debug["notes"].append("Invalid link")
        return [], debug

    # Fetch base HTML once (anchors are in same HTML, but we use them to target the correct section)
    try:
        r = http_get_with_backoff(link, timeout=25, max_tries=6)
    except Exception as e:
        debug["notes"].append(f"HTTP error: {e}")
        return [], debug

    soup = BeautifulSoup(r.text, _bs_parser())

    # Parse targeted sections
    manual = _parse_completed_auctions_section(soup, "completed-auctions-manual-only")   # PSA10 best-offer table
    graded = _parse_completed_auctions_section(soup, "completed-auctions-graded")       # graded table (PSA9 lives here)
    base = _parse_completed_auctions_section(soup, "completed-auctions")                # if present
    # Some pages don't have #completed-auctions; but ungraded often appears under manual-only / graded too.
    all_rows = base + graded + manual

    debug["base_rows"] = len(base)
    debug["graded_rows"] = len(graded)
    debug["manual_rows"] = len(manual)

    # Dedup
    by_key = {}
    for s in all_rows:
        by_key[s["sale_key"]] = s
    merged = list(by_key.values())
    merged.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)

    # Keep last N per bucket
    kept = []
    for bucket in ["ungraded", "psa9", "psa10"]:
        rows = [x for x in merged if x.get("grade_bucket") == bucket]
        rows = rows[: int(per_bucket)]
        kept.extend(rows)
        debug[f"kept_{bucket}"] = len(rows)

    # final newest first
    kept.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return kept, debug

# =========================
# Pipeline
# =========================
def update_sales_history_from_watchlist(per_bucket: int = 5) -> tuple[int, pd.DataFrame]:
    watch_ws = get_ws(WATCHLIST_WS_NAME)
    sales_ws = get_ws(SALES_HISTORY_WS_NAME)
    ensure_headers(sales_ws, SALES_HEADERS)

    wdf = read_df(watch_ws)
    if wdf.empty:
        return 0, pd.DataFrame([{"error": f"No rows found in {WATCHLIST_WS_NAME}"}])

    for c in WATCHLIST_REQUIRED_COLS:
        if c not in wdf.columns:
            wdf[c] = ""

    sdf = read_df(sales_ws)
    existing_keys = set()
    if not sdf.empty and "sale_key" in sdf.columns:
        existing_keys = set(sdf["sale_key"].astype(str).str.strip().tolist())

    appended = 0
    rows_to_append = []
    dbg_rows = []

    for _, w in wdf.iterrows():
        link = safe_str(w.get("Link", "")).strip()
        if is_blank(link) or "pricecharting.com" not in link.lower():
            continue

        generation = safe_str(w.get("Generation", "")).strip()
        set_name = safe_str(w.get("Set", "")).strip()
        card_name = safe_str(w.get("Card Name", "")).strip()
        card_no = safe_str(w.get("Card No", "")).strip()

        sales, dbg = fetch_sales_for_link(link, per_bucket=int(per_bucket))
        dbg_rows.append({"link": link, **dbg})

        for s in sales:
            sk = safe_str(s.get("sale_key", "")).strip()
            if not sk or sk in existing_keys:
                continue

            rows_to_append.append(
                [
                    generation,
                    set_name,
                    card_name,
                    card_no,
                    link,
                    safe_str(s.get("grade_bucket", "")).strip(),
                    s["sale_date"].isoformat() if isinstance(s.get("sale_date"), date) else safe_str(s.get("sale_date", "")),
                    f"{float(safe_float(s.get('price', 0.0), 0.0)):.2f}",
                    safe_str(s.get("title", "")).strip(),
                    sk,
                    safe_str(s.get("updated_utc", "")),
                ]
            )
            existing_keys.add(sk)
            appended += 1

        time.sleep(0.8)

    if rows_to_append:
        if hasattr(sales_ws, "append_rows"):
            _gs_write_retry(sales_ws.append_rows, rows_to_append, value_input_option="RAW")
        else:
            for r in rows_to_append:
                _gs_write_retry(sales_ws.append_row, r, value_input_option="RAW")

    return appended, pd.DataFrame(dbg_rows)

# =========================
# UI
# =========================
st.markdown(
    f"""
**Reads:** `{WATCHLIST_WS_NAME}`  
**Writes:** `{SALES_HISTORY_WS_NAME}`

Pulls **last N** sales per watchlist row for:
- **Ungraded** (from base / merged tables)
- **PSA 9** (from `#completed-auctions-graded`)
- **PSA 10** (from `#completed-auctions-manual-only`, using **accepted price** if present)
"""
)

per_bucket = st.number_input("Sales per bucket (ungraded/psa9/psa10)", min_value=1, max_value=25, value=5, step=1)
show_debug = st.checkbox("Show debug after run", value=True)

if st.button("📥 Pull sales data", use_container_width=True):
    with st.spinner("Scraping PriceCharting and writing to Google Sheet..."):
        n, dbgdf = update_sales_history_from_watchlist(per_bucket=int(per_bucket))

    st.success(f"Done. Appended {n} new row(s).")

    if show_debug and dbgdf is not None and not dbgdf.empty:
        st.dataframe(dbgdf, use_container_width=True, hide_index=True)

if st.button("🔄 Preview Sales History", use_container_width=True):
    ws = get_ws(SALES_HISTORY_WS_NAME)
    ensure_headers(ws, SALES_HEADERS)
    sdf = read_df(ws)
    if sdf.empty:
        st.info("Sales history is empty.")
    else:
        if "sale_date" in sdf.columns:
            sdf["_dt"] = pd.to_datetime(sdf["sale_date"], errors="coerce")
            sdf = sdf.sort_values("_dt", ascending=False).drop(columns=["_dt"])
        st.dataframe(sdf, use_container_width=True, hide_index=True)
