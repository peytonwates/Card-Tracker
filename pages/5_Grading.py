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
# Sold scraping (robust)
# =========================
@st.cache_data(ttl=60 * 60 * 3)
def fetch_pricecharting_sold_sales_latest(reference_link: str, limit: int = 120) -> tuple[list[dict], dict]:
    """
    Robust scrape:
    - Iterate tables/rows
    - Prefer price from LAST cell (Price column) when present
    - Date from first cell when present
    - Title from a middle cell (usually 3rd) when present
    Returns: (sales_rows, debug_info)
    """
    debug = {
        "tables_found": 0,
        "rows_scanned": 0,
        "rows_emitted": 0,
        "notes": [],
    }

    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        debug["notes"].append("Invalid link")
        return [], debug

    try:
        r = http_get_with_backoff(link, timeout=25, max_tries=6)
    except Exception as e:
        debug["notes"].append(f"HTTP error: {e}")
        return [], debug

    soup = BeautifulSoup(r.text, _bs_parser())

    sales = []
    now = datetime.utcnow().isoformat()

    def emit(sale_date: date, price: float, title: str):
        if not sale_date or not price or price <= 0 or not title:
            return
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
        debug["rows_emitted"] += 1

    tables = soup.select("table")
    debug["tables_found"] = len(tables)

    # Scan all tables (this is what made ungraded work for you)
    for table in tables:
        for tr in table.select("tr"):
            tds = tr.select("td")
            if len(tds) < 3:
                continue

            debug["rows_scanned"] += 1

            # Heuristic: PC sold tables usually:
            # td[0] = sale date, td[2] = title, td[-1] = price
            sale_date_txt = tds[0].get_text(" ", strip=True)
            title_txt = tds[2].get_text(" ", strip=True)
            price_txt = tds[-1].get_text(" ", strip=True)

            sale_date = _parse_sale_date(sale_date_txt)
            if not sale_date:
                continue

            # Price cell can contain multiple prices (e.g., "$100.00 $119.00")
            pm_all = PRICE_RE.findall(price_txt)
            if not pm_all:
                # fallback: sometimes the $ appears in the row text but not last cell
                row_text = tr.get_text(" ", strip=True)
                pm_all = PRICE_RE.findall(row_text)
            if not pm_all:
                continue

            price = safe_float(pm_all[0], 0.0)
            if price <= 0:
                continue

            title = title_txt.strip()
            if not title:
                # fallback: pick the longest non-price, non-date cell
                cell_texts = [td.get_text(" ", strip=True) for td in tds]
                candidates = []
                for c in cell_texts:
                    if not c:
                        continue
                    if _parse_sale_date(c):
                        continue
                    if PRICE_RE.search(c):
                        continue
                    candidates.append(c)
                title = max(candidates, key=len).strip() if candidates else ""

            if title:
                emit(sale_date, price, title)

    # Dedup + newest first
    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    sales2 = list(by_key.values())
    sales2.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)

    return sales2[: max(1, int(limit))], debug

def pick_last_n_per_bucket(sales: list[dict], per_bucket: int = 5) -> list[dict]:
    out = []
    for bucket in ["ungraded", "psa9", "psa10"]:
        rows = [s for s in sales if s.get("grade_bucket") == bucket]
        out.extend(rows[:per_bucket])
    return out

# =========================
# Pipeline
# =========================
def update_sales_history_from_watchlist(per_bucket: int = 5, show_debug: bool = False) -> tuple[int, list[dict]]:
    watch_ws = get_ws(WATCHLIST_WS_NAME)
    sales_ws = get_ws(SALES_HISTORY_WS_NAME)
    ensure_headers(sales_ws, SALES_HEADERS)

    wdf = read_df(watch_ws)
    if wdf.empty:
        return 0, [{"error": f"No rows found in {WATCHLIST_WS_NAME}"}]

    for c in WATCHLIST_REQUIRED_COLS:
        if c not in wdf.columns:
            wdf[c] = ""

    sdf = read_df(sales_ws)
    existing_keys = set()
    if not sdf.empty and "sale_key" in sdf.columns:
        existing_keys = set(sdf["sale_key"].astype(str).str.strip().tolist())

    appended = 0
    rows_to_append = []
    debug_rows = []

    for _, w in wdf.iterrows():
        link = safe_str(w.get("Link", "")).strip()
        if is_blank(link) or "pricecharting.com" not in link.lower():
            continue

        generation = safe_str(w.get("Generation", "")).strip()
        set_name = safe_str(w.get("Set", "")).strip()
        card_name = safe_str(w.get("Card Name", "")).strip()
        card_no = safe_str(w.get("Card No", "")).strip()

        sales_all, dbg = fetch_pricecharting_sold_sales_latest(link, limit=200)
        picked = pick_last_n_per_bucket(sales_all, per_bucket=per_bucket)

        debug_rows.append(
            {
                "link": link,
                "tables_found": dbg.get("tables_found"),
                "rows_scanned": dbg.get("rows_scanned"),
                "rows_emitted": dbg.get("rows_emitted"),
                "picked_total": len(picked),
                "picked_ungraded": sum(1 for x in picked if x.get("grade_bucket") == "ungraded"),
                "picked_psa9": sum(1 for x in picked if x.get("grade_bucket") == "psa9"),
                "picked_psa10": sum(1 for x in picked if x.get("grade_bucket") == "psa10"),
                "notes": " | ".join(dbg.get("notes", [])),
            }
        )

        for s in picked:
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

    return appended, debug_rows

# =========================
# UI
# =========================
st.markdown(
    f"""
**Reads:** `{WATCHLIST_WS_NAME}`  
**Writes:** `{SALES_HISTORY_WS_NAME}`

Pulls **last N** sales for each watchlist row for:
- `ungraded`
- `psa9`
- `psa10`
"""
)

per_bucket = st.number_input("Sales per bucket (ungraded/psa9/psa10)", min_value=1, max_value=25, value=5, step=1)
show_debug = st.checkbox("Show debug details after run", value=True)

if st.button("📥 Pull sales data", use_container_width=True):
    with st.spinner("Scraping PriceCharting and writing to Google Sheet..."):
        n, dbg = update_sales_history_from_watchlist(per_bucket=int(per_bucket), show_debug=show_debug)

    st.success(f"Done. Appended {n} new row(s).")

    if show_debug and dbg:
        with st.expander("Debug output (per link)", expanded=True):
            st.dataframe(pd.DataFrame(dbg), use_container_width=True, hide_index=True)

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
