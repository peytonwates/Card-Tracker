# pages/5_Grading.py
# ---------------------------------------------------------
# Grading > Analysis (SIMPLE VERSION)
# Goal:
# - Read rows from Google Sheet tab: grading_watch_list
# - For each row, use the "Link" (PriceCharting) to pull the most recent SOLD sales
# - Keep ONLY the latest 5 "ungraded" sales per watchlist item
# - Write those rows to Google Sheet tab: grading_sales_history
# ---------------------------------------------------------

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
# Page config
# =========================
st.set_page_config(page_title="Grading", layout="wide")
st.title("Grading — Analysis (Sales History Loader)")


# =========================
# Sheet config
# =========================
WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

WATCHLIST_HEADERS_EXPECTED = ["Generation", "Set", "Card Name", "Card No", "Link"]

SALES_HISTORY_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "sale_date",     # YYYY-MM-DD
    "price",         # numeric
    "title",
    "sale_key",      # stable key to dedupe
    "updated_utc",   # ISO timestamp
]


# =========================
# Small helpers
# =========================
def safe_str(x) -> str:
    return "" if x is None else str(x)

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = safe_str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def a1_col_letter(n: int) -> str:
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters

def _bs_parser():
    try:
        import lxml  # noqa: F401
        return "lxml"
    except Exception:
        return "html.parser"

def _classify_grade_from_title(title: str) -> str:
    """
    Very simple classifier:
    - PSA 9/10 => graded
    - else => ungraded
    """
    t = (title or "").upper()
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*(MINT|MT)\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"


# =========================
# Google Sheets auth
# =========================
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit Cloud: secrets as TOML table
    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Streamlit Cloud: secrets as JSON string
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_info = json.loads(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: file path in secrets
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

def get_sheet():
    client = get_gspread_client()
    return client.open_by_key(st.secrets["spreadsheet_id"])

def get_ws(sheet, ws_name: str):
    return sheet.worksheet(ws_name)

def ensure_headers(ws, headers: list[str]):
    values = ws.get_all_values()
    if not values:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
        return
    current = [safe_str(x).strip() for x in (values[0] or [])]
    if [c for c in current if c] != headers:
        # For this simple page, we just overwrite the header row to match our expected schema.
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")

def read_ws_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values or not values[0]:
        return pd.DataFrame()
    header = [safe_str(x).strip() for x in values[0]]
    rows = values[1:]
    out = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[:len(header)]
        out.append(r)
    df = pd.DataFrame(out, columns=header)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def write_ws_df(ws, df: pd.DataFrame, headers: list[str]):
    df2 = df.copy()
    for h in headers:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[headers].copy()

    values = [headers] + df2.astype(str).fillna("").values.tolist()
    last_col = a1_col_letter(len(headers))
    rng = f"A1:{last_col}{len(values)}"
    _gs_write_retry(ws.update, range_name=rng, values=values, value_input_option="RAW")


# =========================
# HTTP (basic backoff)
# =========================
@st.cache_resource
def get_http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (CardTracker; Streamlit)"})
    return s

def http_get_with_backoff(url: str, *, timeout=25, max_tries=6):
    sess = get_http_session()
    sleep_s = 1.0
    last_exc = None

    for _ in range(max_tries):
        try:
            r = sess.get(url, timeout=timeout)
        except Exception as e:
            last_exc = e
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.7, 20.0)
            continue

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

    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"HTTPError: retries exhausted for {url}")


# =========================
# PriceCharting sold sales scraper (simple)
# =========================
@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sold_sales(reference_link: str, limit: int = 60) -> list[dict]:
    """
    Return recent sold sales from a PriceCharting product page.

    Output:
      { sale_date: date, price: float, title: str, grade_bucket: str, sale_key: str }
    """
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    price_re = re.compile(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
    iso_date_re = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
    slash_date_re = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
    month_date_re = re.compile(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+20\d{2}\b",
        re.IGNORECASE,
    )

    def parse_any_date(text: str):
        if not text:
            return None

        m = iso_date_re.search(text)
        if m:
            d = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(d):
                return d.date()

        m = slash_date_re.search(text)
        if m:
            d = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(d):
                return d.date()

        m = month_date_re.search(text)
        if m:
            d = pd.to_datetime(m.group(0), errors="coerce")
            if pd.notna(d):
                return d.date()

        d = pd.to_datetime(text, errors="coerce")
        if pd.notna(d) and d.year >= 2000:
            return d.date()

        return None

    sales = []

    # Attempt: parse table rows
    for tr in soup.select("tr"):
        t = tr.get_text(" ", strip=True)
        if not t or "$" not in t:
            continue

        pm = price_re.search(t)
        if not pm:
            continue

        d = parse_any_date(t)
        if not d:
            continue

        price = safe_float(pm.group(1), 0.0)
        if price <= 0:
            continue

        cells = [td.get_text(" ", strip=True) for td in tr.select("td")]
        cells = [c for c in cells if c]
        title = ""
        if cells:
            filtered = []
            for c in cells:
                if "$" in c and price_re.search(c):
                    continue
                if parse_any_date(c):
                    continue
                filtered.append(c)
            title = max(filtered, key=len) if filtered else t
        else:
            title = t

        bucket = _classify_grade_from_title(title)
        sale_key = f"{d.isoformat()}|{price:.2f}|{bucket}|{title[:90].strip().lower()}"
        sales.append(
            {
                "sale_date": d,
                "price": float(price),
                "title": title.strip(),
                "grade_bucket": bucket,
                "sale_key": sale_key,
            }
        )

    # Dedup + sort newest first
    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# Core: build sales-history rows (last 5 ungraded per item)
# =========================
def build_sales_history_rows_from_watchlist(wdf: pd.DataFrame, per_item_n: int = 5) -> pd.DataFrame:
    if wdf is None or wdf.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    # Normalize columns
    for h in WATCHLIST_HEADERS_EXPECTED:
        if h not in wdf.columns:
            wdf[h] = ""

    rows_out = []
    now_utc = datetime.utcnow().isoformat()

    # Only rows with a link
    wdf2 = wdf.copy()
    wdf2["Link"] = wdf2["Link"].astype(str).str.strip()
    wdf2 = wdf2[wdf2["Link"] != ""].copy()

    for i, r in wdf2.iterrows():
        link = safe_str(r.get("Link", "")).strip()
        if "pricecharting.com" not in link.lower():
            continue

        # Small pause between items to be polite
        if len(rows_out) > 0:
            time.sleep(0.75)

        sales = fetch_pricecharting_sold_sales(link, limit=80)
        if not sales:
            continue

        ungraded = [s for s in sales if (s.get("grade_bucket") or "").lower() == "ungraded"]
        ungraded = ungraded[: int(per_item_n)]

        for s in ungraded:
            rows_out.append(
                {
                    "Generation": safe_str(r.get("Generation", "")).strip(),
                    "Set": safe_str(r.get("Set", "")).strip(),
                    "Card Name": safe_str(r.get("Card Name", "")).strip(),
                    "Card No": safe_str(r.get("Card No", "")).strip(),
                    "Link": link,
                    "sale_date": s["sale_date"].isoformat() if isinstance(s.get("sale_date"), date) else safe_str(s.get("sale_date", "")).strip(),
                    "price": float(safe_float(s.get("price", 0.0), 0.0)),
                    "title": safe_str(s.get("title", "")).strip(),
                    "sale_key": safe_str(s.get("sale_key", "")).strip(),
                    "updated_utc": now_utc,
                }
            )

    df_out = pd.DataFrame(rows_out)
    if df_out.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    # Stable ordering
    df_out["price"] = df_out["price"].apply(lambda v: safe_float(v, 0.0))
    df_out["sale_date_dt"] = pd.to_datetime(df_out["sale_date"], errors="coerce")
    df_out = df_out.sort_values(["Card Name", "Card No", "sale_date_dt"], ascending=[True, True, False]).drop(columns=["sale_date_dt"])
    return df_out[SALES_HISTORY_HEADERS].copy()


# =========================
# UI
# =========================
st.caption(
    "This page ONLY does one thing: reads `grading_watch_list`, scrapes the latest SOLD comps from PriceCharting, "
    "keeps the latest 5 **ungraded** sales per item, and writes them to `grading_sales_history`."
)

sheet = get_sheet()
watch_ws = get_ws(sheet, WATCHLIST_WS_NAME)
sales_ws = get_ws(sheet, SALES_HISTORY_WS_NAME)

# Ensure headers exist / correct (simple overwrite behavior)
ensure_headers(sales_ws, SALES_HISTORY_HEADERS)

wdf = read_ws_df(watch_ws)

c1, c2 = st.columns([1, 2])

with c1:
    per_item_n = st.number_input("Ungraded sales per item", min_value=1, max_value=20, value=5, step=1)

    if st.button("Pull sales + overwrite grading_sales_history", type="primary", use_container_width=True):
        if wdf is None or wdf.empty:
            st.warning(f"No rows found in `{WATCHLIST_WS_NAME}`.")
        else:
            out_df = build_sales_history_rows_from_watchlist(wdf, per_item_n=int(per_item_n))

            # OVERWRITE the sales history sheet with only what we just pulled
            write_ws_df(sales_ws, out_df, SALES_HISTORY_HEADERS)

            st.success(f"Wrote {len(out_df):,} row(s) to `{SALES_HISTORY_WS_NAME}`.")
            st.rerun()

with c2:
    st.markdown("#### Watchlist preview")
    if wdf is None or wdf.empty:
        st.info(f"`{WATCHLIST_WS_NAME}` is empty.")
    else:
        show_cols = [c for c in WATCHLIST_HEADERS_EXPECTED if c in wdf.columns]
        st.dataframe(wdf[show_cols], use_container_width=True, hide_index=True)

st.divider()

st.markdown("#### Sales history preview (sheet)")
sdf = read_ws_df(sales_ws)
if sdf is None or sdf.empty:
    st.info(f"`{SALES_HISTORY_WS_NAME}` is empty.")
else:
    # Make it readable
    if "price" in sdf.columns:
        sdf["price"] = sdf["price"].apply(lambda v: safe_float(v, 0.0))
    if "sale_date" in sdf.columns:
        sdf["_sale_date"] = pd.to_datetime(sdf["sale_date"], errors="coerce")
        sdf = sdf.sort_values("_sale_date", ascending=False).drop(columns=["_sale_date"])
    st.dataframe(sdf, use_container_width=True, hide_index=True)
