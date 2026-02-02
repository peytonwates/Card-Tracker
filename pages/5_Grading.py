# pages/5_Grading.py
import json
import re
import uuid
import time
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Grading", layout="wide")
st.title("Grading")


# =========================================================
# CONFIG
# =========================================================

INVENTORY_WS_NAME = st.secrets.get("inventory_worksheet", "inventory")
GRADING_WS_NAME = st.secrets.get("grading_worksheet", "grading")

WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
GEMRATES_WS_NAME = st.secrets.get("gemrates_worksheet", "gemrates")
GRADING_SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
STATUS_TRADED = "TRADED"
STATUS_GRADING = "GRADING"

ELIGIBLE_INV_STATUSES = {STATUS_ACTIVE}

DEFAULT_GRADING_FEE_PER_CARD = float(st.secrets.get("default_grading_fee_per_card", 28.0))
DEFAULT_RETURN_BUSINESS_DAYS = int(st.secrets.get("default_business_days_return", 75))

# Canonical grading columns we will use (and ONLY these get returned from loader to avoid duplicates)
GRADING_CANON_COLS = [
    "grading_row_id",
    "submission_id",
    "submission_date",
    "estimated_return_date",
    "inventory_id",
    "reference_link",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "purchased_from",
    "purchase_date",
    "purchase_total",
    "grading_company",
    "grading_fee_initial",   # canonical
    "additional_costs",      # canonical
    "psa9_price",
    "psa10_price",
    "status",
    "returned_date",
    "received_grade",
    "notes",
    "created_at",
    "updated_at",
    "synced_to_inventory",
]

WATCHLIST_BASE_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "Parallel",
    "Pic",
    "Release Year",
    "Target Buy Price",
    "Max Buy Price",
    "Notes",
]

WATCHLIST_ENRICH_HEADERS = [
    "Ungraded", "PSA 9", "PSA 10",
    "Total Graded", "# PSA 10", "Gem Rate",
    # names preserved (they now represent stats from last-10 stored sales)
    "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
    "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
    "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
    "Grading Score",
    "Image",
]

GEMRATES_HEADERS = [
    "Key",
    "Generation",
    "Set Name",
    "Parallel",
    "Card #",
    "Card Description",
    "Gems",
    "Total",
    "Gem Rate - All Time",
]

SALES_HISTORY_HEADERS_V2 = [
    "reference_link",
    "sale_key",       # stable key to detect new sales
    "sale_date",      # YYYY-MM-DD
    "price",          # numeric
    "grade_bucket",   # ungraded/psa9/psa10
    "title",          # title text
    "updated_utc",    # last refresh timestamp
]


# =========================================================
# HELPERS
# =========================================================

def safe_str(x):
    if x is None:
        return ""
    return str(x)

def is_blank(x) -> bool:
    s = safe_str(x).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = safe_str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        if s.endswith("%"):
            return float(s[:-1]) / 100.0
        return float(s)
    except Exception:
        return default

def add_business_days(start_d: date, n: int) -> date:
    d = start_d
    added = 0
    while added < n:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d

def a1_col_letter(n: int) -> str:
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters

def _strip_dups(h: str) -> str:
    return re.sub(r"(?:__dup\d+)+$", "", str(h or "").strip())

def _build_stable_unique_headers(raw_headers: list[str]) -> list[str]:
    cleaned = []
    for i, h in enumerate(raw_headers, start=1):
        hh = str(h or "").strip()
        if hh == "":
            hh = f"unnamed__col{i}"
        cleaned.append(hh)

    bases = [_strip_dups(h) for h in cleaned]
    counts = {}
    out = []
    for b in bases:
        counts[b] = counts.get(b, 0) + 1
        out.append(b if counts[b] == 1 else f"{b}__dup{counts[b]}")
    return out

def _append_missing_canon_headers(stable_headers: list[str], canon_headers: list[str]) -> list[str]:
    existing_bases = {_strip_dups(h) for h in stable_headers}
    out = list(stable_headers)
    for h in canon_headers:
        b = _strip_dups(h)
        if b not in existing_bases:
            out.append(b)
            existing_bases.add(b)
    return _build_stable_unique_headers(out)

def _norm(s) -> str:
    return "" if s is None else str(s).strip()

def _norm_set(s: str) -> str:
    t = _norm(s).lower()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("’", "'")
    return t

def _norm_key(*parts) -> str:
    out = []
    for p in parts:
        out.append(_norm(p).lower())
    return "|".join(out)

def _to_float_price(s: str) -> float:
    try:
        if s is None:
            return 0.0
        t = str(s).replace("$", "").replace(",", "").strip()
        return float(t) if t else 0.0
    except Exception:
        return 0.0

def _classify_grade_from_title(title: str) -> str:
    t = (title or "").upper()
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*MINT\s*10\b", t) or re.search(r"\bGEM\s*MT\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"


# =========================================================
# GOOGLE SHEETS AUTH
# =========================================================

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
    raise APIError("APIError: [429] Quota exceeded (retries exhausted)")

def ensure_headers(ws, needed_headers: list[str], *, write: bool = False):
    values = ws.get_all_values()
    if not values:
        if write:
            _gs_write_retry(ws.update, values=[needed_headers], range_name="1:1", value_input_option="USER_ENTERED")
        return needed_headers

    raw = values[0] if values else []
    if not raw:
        if write:
            _gs_write_retry(ws.update, values=[needed_headers], range_name="1:1", value_input_option="USER_ENTERED")
        return needed_headers

    stable = _build_stable_unique_headers(raw)
    stable = _append_missing_canon_headers(stable, needed_headers)

    if write and raw != stable:
        _gs_write_retry(ws.update, values=[stable], range_name="1:1", value_input_option="USER_ENTERED")

    return stable

def _get_ws_or_create(sheet, name: str, headers: list[str], cols_hint: int = 26, rows_hint: int = 2000):
    try:
        ws = sheet.worksheet(name)
        vals = ws.get_all_values()
        if not vals or not vals[0] or all(str(x).strip() == "" for x in vals[0]):
            ws.update(range_name="1:1", values=[headers], value_input_option="RAW")
        else:
            current = [str(x).strip() for x in vals[0]]
            missing = [h for h in headers if h not in current]
            if missing:
                ws.update(range_name="1:1", values=[current + missing], value_input_option="RAW")
        return ws
    except Exception:
        ws = sheet.add_worksheet(title=name, rows=str(rows_hint), cols=str(max(cols_hint, len(headers) + 5)))
        ws.update(range_name="1:1", values=[headers], value_input_option="RAW")
        return ws

def _read_sheet_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = [str(h).strip() for h in values[0]]
    rows = values[1:]
    if not header or all(h == "" for h in header):
        return pd.DataFrame()
    out_rows = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[:len(header)]
        out_rows.append(r)
    return pd.DataFrame(out_rows, columns=header)

def _batch_write_sheet(ws, df: pd.DataFrame, header: list[str]):
    df2 = df.copy()
    for c in header:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[header]
    values = [header] + df2.astype(str).fillna("").values.tolist()
    last_col = a1_col_letter(len(header))
    ws.update(range_name=f"A1:{last_col}{len(values)}", values=values, value_input_option="RAW")


# =========================================================
# HTTP — BACKOFF (429-safe)
# =========================================================

@st.cache_resource
def get_http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (CardTracker; Streamlit)"})
    return s

def http_get_with_backoff(url: str, *, timeout=20, max_tries=6):
    sess = get_http_session()
    sleep_s = 1.0
    for _ in range(max_tries):
        r = sess.get(url, timeout=timeout)
        if r.status_code == 200:
            return r

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    sleep_s = max(sleep_s, float(ra))
                except Exception:
                    pass
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.8, 25.0)
            continue

        if r.status_code in {500, 502, 503, 504}:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.6, 15.0)
            continue

        r.raise_for_status()

    raise requests.HTTPError(f"HTTPError: [429] Too Many Requests (retries exhausted) for {url}")


# =========================================================
# PRICECHARTING — CURRENT PRICES + IMAGE
# =========================================================

@st.cache_data(ttl=60 * 60 * 12)
def fetch_pricecharting_prices(reference_link: str) -> dict:
    out = {"raw": 0.0, "psa9": 0.0, "psa10": 0.0}
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return out

    try:
        r = http_get_with_backoff(link, timeout=25, max_tries=6)
        soup = BeautifulSoup(r.text, "lxml")
        nodes = soup.select(".price.js-price")
        prices = []
        for n in nodes:
            t = n.get_text(" ", strip=True)
            m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", t)
            prices.append(safe_float(m.group(1), 0.0) if m else 0.0)

        out["raw"] = float(prices[0] if len(prices) >= 1 else 0.0)
        out["psa9"] = float(prices[3] if len(prices) >= 4 else 0.0)
        out["psa10"] = float(prices[5] if len(prices) >= 6 else 0.0)
        return out
    except Exception:
        return out

@st.cache_data(ttl=60 * 60 * 24)
def fetch_pricecharting_image_url(reference_link: str) -> str:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return ""

    try:
        r = http_get_with_backoff(link, timeout=25, max_tries=6)
        soup = BeautifulSoup(r.text, "lxml")

        # 1) og:image (most reliable)
        meta = soup.find("meta", attrs={"property": "og:image"})
        if meta and meta.get("content"):
            return str(meta.get("content")).strip()

        # 2) common image locations (inventory-like scraping)
        selectors = [
            "img#product_image",
            "img.product_image",
            "img[itemprop='image']",
            ".product-image img",
            ".product_image img",
            "img[src*='media.pricecharting.com']",
        ]
        for sel in selectors:
            tag = soup.select_one(sel)
            if tag and tag.get("src"):
                return str(tag.get("src")).strip()

        # 3) last resort: first large-ish img
        imgs = soup.select("img[src]")
        for img in imgs[:20]:
            src = str(img.get("src")).strip()
            if "pricecharting" in src and ("jpg" in src or "png" in src or "webp" in src):
                return src

        return ""
    except Exception:
        return ""


# =========================================================
# PRICECHARTING — SOLD SALES (latest rows)
# =========================================================

@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sold_sales_latest(reference_link: str, limit: int = 15) -> list[dict]:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    try:
        r = http_get_with_backoff(link, timeout=25, max_tries=6)
    except Exception:
        return []

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    sales = []
    date_re = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
    price_re = re.compile(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")

    for i, ln in enumerate(lines):
        if ("$" in ln) and ("[EBAY]" in ln.upper() or "[GOLDIN]" in ln.upper() or "[PWCC]" in ln.upper() or "[TCGPLAYER]" in ln.upper()):
            title = ln.split("[", 1)[0].strip()
            pm = price_re.search(ln)
            price = _to_float_price(pm.group(1)) if pm else 0.0

            sale_date = None
            for j in range(i, min(i + 4, len(lines))):
                dm = date_re.search(lines[j])
                if dm:
                    sale_date = pd.to_datetime(dm.group(1), errors="coerce").date()
                    break

            if sale_date and price > 0:
                bucket = _classify_grade_from_title(title)
                sale_key = f"{sale_date.isoformat()}|{price:.2f}|{bucket}|{title[:80].strip().lower()}"
                sales.append({
                    "sale_date": sale_date,
                    "price": float(price),
                    "grade_bucket": bucket,
                    "title": title,
                    "sale_key": sale_key,
                })

    sales = sorted(sales, key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return sales[: max(1, int(limit))]


# =========================================================
# LOADERS
# =========================================================

@st.cache_data(ttl=30)
def load_inventory_df():
    ws = get_ws(INVENTORY_WS_NAME)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return df

    if "inventory_status" not in df.columns:
        df["inventory_status"] = STATUS_ACTIVE

    for c in [
        "inventory_id", "reference_link", "card_name", "set_name", "year", "total_price",
        "purchase_date", "purchased_from", "product_type", "card_number", "variant", "card_subtype"
    ]:
        if c not in df.columns:
            df[c] = ""

    df["inventory_id"] = df["inventory_id"].astype(str)
    df["year"] = df["year"].astype(str).replace({"nan": "", "None": "", "<NA>": ""}).str.strip()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0.0)
    df["product_type"] = df["product_type"].astype(str)
    df["inventory_status"] = df["inventory_status"].astype(str).replace("", STATUS_ACTIVE)

    return df

@st.cache_data(ttl=30)
def load_grading_df():
    ws = get_ws(GRADING_WS_NAME)
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    raw_header = values[0] if values else []
    if not raw_header:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    header = _build_stable_unique_headers(raw_header)

    data_rows = []
    for r in values[1:]:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[: len(header)]
        data_rows.append(r)

    df = pd.DataFrame(data_rows, columns=header)
    if df.empty:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    # Ensure canon bases exist
    for c in GRADING_CANON_COLS:
        if c not in df.columns and c not in {_strip_dups(x) for x in df.columns}:
            df[c] = ""

    # helper: return first column if duplicates exist
    def _as_series(colname: str) -> pd.Series:
        obj = df.loc[:, colname]
        if isinstance(obj, pd.DataFrame):
            return obj.iloc[:, 0].astype(str)
        return obj.astype(str)

    def _cols_named(base: str):
        cols = []
        for c in df.columns:
            if _strip_dups(c) == base:
                cols.append(c)
        return cols

    def _coalesce_into(base: str, fallbacks: list[str]):
        candidates = _cols_named(base)
        for fb in fallbacks:
            candidates += _cols_named(fb)

        # unique keep order
        seen = set()
        ordered = []
        for c in candidates:
            if c not in seen:
                ordered.append(c)
                seen.add(c)
        if not ordered:
            return

        s = _as_series(ordered[0])
        for c in ordered[1:]:
            t = _as_series(c)
            s = s.where(s.str.strip() != "", t)
        df[base] = s

    # unify legacy -> canon
    _coalesce_into("grading_fee_initial", ["grading_fee_per_card"])
    _coalesce_into("additional_costs", ["extra_costs"])
    _coalesce_into("received_grade", ["returned_grade"])

    # numeric fields
    for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

    # IMPORTANT: return ONLY canonical cols so Streamlit data_editor never sees dup column names
    out = pd.DataFrame()
    for c in GRADING_CANON_COLS:
        if c in df.columns:
            out[c] = df[c]
        else:
            # look for base match
            matches = [col for col in df.columns if _strip_dups(col) == c]
            out[c] = df[matches[0]] if matches else ""

    out["grading_row_id"] = out["grading_row_id"].astype(str)
    out["submission_id"] = out["submission_id"].astype(str)
    out["status"] = out["status"].astype(str).replace("", "SUBMITTED")

    return out

@st.cache_data(ttl=60)
def load_watchlist_gemrates_sales():
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])

    watch_ws = _get_ws_or_create(sh, WATCHLIST_WS_NAME, WATCHLIST_BASE_HEADERS + WATCHLIST_ENRICH_HEADERS, cols_hint=70)
    gem_ws = _get_ws_or_create(sh, GEMRATES_WS_NAME, GEMRATES_HEADERS, cols_hint=40)
    sales_ws = _get_ws_or_create(sh, GRADING_SALES_HISTORY_WS_NAME, SALES_HISTORY_HEADERS_V2, cols_hint=60, rows_hint=4000)

    wdf = _read_sheet_df(watch_ws)
    gdf = _read_sheet_df(gem_ws)
    sdf = _read_sheet_df(sales_ws)

    return wdf, gdf, sdf

def refresh_all():
    load_inventory_df.clear()
    load_grading_df.clear()
    load_watchlist_gemrates_sales.clear()
    fetch_pricecharting_prices.clear()
    fetch_pricecharting_image_url.clear()
    fetch_pricecharting_sold_sales_latest.clear()
    st.rerun()


# =========================================================
# WRITES (GRADING + INVENTORY)
# =========================================================

def append_grading_rows(rows: list[dict]):
    if not rows:
        return

    ws = get_ws(GRADING_WS_NAME)
    headers = ensure_headers(ws, GRADING_CANON_COLS, write=True)

    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    out_rows = []
    for row in rows:
        values = []
        for h in headers:
            base = _strip_dups(h)
            v = row.get(base, "")
            if base in NUM_COLS:
                v = _num_str(v)
            values.append(v)
        out_rows.append(values)

    if hasattr(ws, "append_rows"):
        _gs_write_retry(ws.append_rows, out_rows, value_input_option="RAW")
    else:
        for r in out_rows:
            _gs_write_retry(ws.append_row, r, value_input_option="RAW")

def update_grading_rows(df_rows: pd.DataFrame):
    if df_rows is None or df_rows.empty:
        return

    ws = get_ws(GRADING_WS_NAME)
    ensure_headers(ws, GRADING_CANON_COLS, write=True)

    values = ws.get_all_values()
    if not values or not values[0]:
        return

    sheet_header = values[0]
    id_col_idx = None
    for j, h in enumerate(sheet_header):
        if _strip_dups(h) == "grading_row_id":
            id_col_idx = j
            break
    if id_col_idx is None:
        raise ValueError("grading_row_id must exist in grading sheet header row.")

    id_to_rownum: dict[str, int] = {}
    for rownum, row in enumerate(values[1:], start=2):
        v = str(row[id_col_idx] if len(row) > id_col_idx else "" or "").strip()
        if v:
            id_to_rownum[v] = rownum

    last_col = a1_col_letter(len(sheet_header))
    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and str(v).strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    data_updates = []
    for _, r in df_rows.iterrows():
        rid = str(r.get("grading_row_id", "")).strip()
        rownum = id_to_rownum.get(rid)
        if not rownum:
            continue

        out_row = []
        for h in sheet_header:
            base = _strip_dups(h)
            v = r.get(base, "")
            if pd.isna(v):
                v = ""
            if base in NUM_COLS:
                v = _num_str(v)
            out_row.append(v)

        data_updates.append({
            "range": f"A{rownum}:{last_col}{rownum}",
            "values": [out_row],
        })

    if not data_updates:
        return

    for i in range(0, len(data_updates), 50):
        chunk = data_updates[i:i + 50]
        _gs_write_retry(ws.batch_update, chunk, value_input_option="RAW")

def update_inventory_status(inventory_id: str, new_status: str):
    inv_ws = get_ws(INVENTORY_WS_NAME)
    values = inv_ws.get_all_values()
    if not values or not values[0]:
        return

    headers = values[0]
    idx = { _strip_dups(h): j for j, h in enumerate(headers) }

    if "inventory_id" not in idx or "inventory_status" not in idx:
        ensure_headers(inv_ws, ["inventory_id", "inventory_status"], write=True)
        values = inv_ws.get_all_values()
        headers = values[0] if values else []
        idx = { _strip_dups(h): j for j, h in enumerate(headers) }

    if "inventory_id" not in idx or "inventory_status" not in idx:
        return

    id_col_idx = idx["inventory_id"]
    status_col_idx = idx["inventory_status"]

    target_row = None
    for i, row in enumerate(values[1:], start=2):
        v = str(row[id_col_idx] if len(row) > id_col_idx else "" or "").strip()
        if v == str(inventory_id).strip():
            target_row = i
            break
    if not target_row:
        return

    col_letter = a1_col_letter(status_col_idx + 1)
    _gs_write_retry(inv_ws.update, values=[[new_status]], range_name=f"{col_letter}{target_row}", value_input_option="USER_ENTERED")


# =========================================================
# SALES HISTORY (last 10 per link, incremental)
# =========================================================

def update_sales_history_incremental(wdf: pd.DataFrame, *, keep_n: int = 10) -> int:
    if wdf is None or wdf.empty or "Link" not in wdf.columns:
        return 0

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    sales_ws = _get_ws_or_create(sh, GRADING_SALES_HISTORY_WS_NAME, SALES_HISTORY_HEADERS_V2, cols_hint=60, rows_hint=4000)

    sdf = _read_sheet_df(sales_ws)
    if sdf is None or sdf.empty:
        sdf = pd.DataFrame(columns=SALES_HISTORY_HEADERS_V2)
    for c in SALES_HISTORY_HEADERS_V2:
        if c not in sdf.columns:
            sdf[c] = ""

    sdf["reference_link"] = sdf["reference_link"].astype(str).str.strip()
    sdf["sale_key"] = sdf["sale_key"].astype(str).str.strip()

    existing = {}
    if not sdf.empty:
        for lk, grp in sdf.groupby("reference_link"):
            existing[lk] = set(grp["sale_key"].astype(str).tolist())

    links = wdf["Link"].astype(str).str.strip().tolist()
    uniq = []
    seen = set()
    for lk in links:
        if not lk or "pricecharting.com" not in lk.lower():
            continue
        if lk not in seen:
            seen.add(lk)
            uniq.append(lk)

    updated_links = 0
    sdf_out = sdf.copy()

    for idx, lk in enumerate(uniq, start=1):
        # gentle throttle to reduce 429
        if idx > 1:
            time.sleep(1.1)

        latest = fetch_pricecharting_sold_sales_latest(lk, limit=15)
        if not latest:
            continue

        ex_keys = existing.get(lk, set())
        new_sales = [s for s in latest if s["sale_key"] not in ex_keys]

        if not new_sales and lk in existing:
            continue

        old = sdf_out[sdf_out["reference_link"].astype(str).str.strip() == lk].copy()
        merged = []

        if not old.empty:
            for _, r in old.iterrows():
                merged.append({
                    "reference_link": lk,
                    "sale_key": _norm(r.get("sale_key", "")),
                    "sale_date": _norm(r.get("sale_date", "")),
                    "price": safe_float(r.get("price", 0.0), 0.0),
                    "grade_bucket": _norm(r.get("grade_bucket", "")),
                    "title": _norm(r.get("title", "")),
                    "updated_utc": _norm(r.get("updated_utc", "")),
                })

        now_utc = datetime.utcnow().isoformat()
        for s in new_sales:
            merged.append({
                "reference_link": lk,
                "sale_key": s["sale_key"],
                "sale_date": s["sale_date"].isoformat(),
                "price": float(s["price"]),
                "grade_bucket": s["grade_bucket"],
                "title": s["title"],
                "updated_utc": now_utc,
            })

        by_key = {}
        for m in merged:
            if m["sale_key"]:
                by_key[m["sale_key"]] = m
        merged2 = list(by_key.values())

        def _parse_date(d):
            try:
                return pd.to_datetime(d, errors="coerce").date()
            except Exception:
                return None

        merged2.sort(key=lambda x: (_parse_date(x.get("sale_date")) or date(1900,1,1), float(x.get("price", 0.0))), reverse=True)
        merged2 = merged2[: int(keep_n)]

        sdf_out = sdf_out[sdf_out["reference_link"].astype(str).str.strip() != lk].copy()
        sdf_out = pd.concat([sdf_out, pd.DataFrame(merged2)], ignore_index=True)
        updated_links += 1

    if updated_links > 0:
        for c in SALES_HISTORY_HEADERS_V2:
            if c not in sdf_out.columns:
                sdf_out[c] = ""
        sdf_out = sdf_out[SALES_HISTORY_HEADERS_V2].copy()

        # write sheet (header + data)
        _batch_write_sheet(sales_ws, sdf_out, SALES_HISTORY_HEADERS_V2)
        load_watchlist_gemrates_sales.clear()

    return updated_links


# =========================================================
# WATCHLIST VIEW BUILD
# =========================================================

def _sales_stats_from_last10(sdf: pd.DataFrame, link: str, bucket: str) -> dict:
    if sdf is None or sdf.empty or not link:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    df = sdf.copy()
    if "reference_link" not in df.columns:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    df["reference_link"] = df["reference_link"].astype(str).str.strip()
    df = df[df["reference_link"] == link].copy()
    if df.empty:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    if "grade_bucket" not in df.columns:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    df["grade_bucket"] = df["grade_bucket"].astype(str).str.strip().str.lower()
    df = df[df["grade_bucket"] == bucket].copy()
    if df.empty:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    df["price"] = df["price"].apply(lambda v: safe_float(v, 0.0))
    prices = [float(x) for x in df["price"].tolist() if float(x) > 0]
    if not prices:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}

    return {
        "count": int(len(prices)),
        "min": float(min(prices)),
        "max": float(max(prices)),
        "avg": float(sum(prices) / len(prices)),
    }

def downside_penalty_psa9(buy_total: float, psa9_value: float) -> float:
    buy_total = float(buy_total or 0.0)
    psa9_value = float(psa9_value or 0.0)
    if buy_total <= 0:
        return 0.0
    profit9 = psa9_value - buy_total
    roi9 = profit9 / buy_total
    if roi9 >= 0:
        return 0.0
    return float(min(65.0, abs(roi9) * 200.0))

def build_watchlist_view(wdf: pd.DataFrame, gdf: pd.DataFrame, sdf: pd.DataFrame, grading_fee_assumption: float):
    if wdf is None or wdf.empty:
        return pd.DataFrame()

    out = wdf.copy()

    # Make sure no duplicate columns reach st.data_editor
    out = out.loc[:, ~out.columns.duplicated()].copy()

    for c in WATCHLIST_BASE_HEADERS:
        if c not in out.columns:
            out[c] = ""

    # ---- GemRates match: Set + Card No (Parallel optional) ----
    base_lookup = {}
    if gdf is not None and not gdf.empty:
        g = gdf.loc[:, ~gdf.columns.duplicated()].copy()
        g_set = g.get("Set Name", g.get("Set", ""))
        g_cardno = g.get("Card #", g.get("Card No", ""))
        g_par = g.get("Parallel", g.get("Variant", ""))
        g_desc = g.get("Card Description", g.get("Description", ""))

        gems = g.get("Gems", 0)
        total = g.get("Total", 0)

        tmp = pd.DataFrame({
            "__set": g_set.astype(str),
            "__cardno": g_cardno.astype(str),
            "__par": g_par.astype(str),
            "__desc": g_desc.astype(str),
            "__gems": pd.Series(gems).apply(lambda x: safe_float(x, 0.0)),
            "__total": pd.Series(total).apply(lambda x: safe_float(x, 0.0)),
        })
        tmp["__k_base"] = tmp.apply(lambda r: _norm_key(_norm_set(r["__set"]), _norm(r["__cardno"])), axis=1)
        base = tmp.groupby("__k_base", as_index=False).agg(
            total_graded=("__total", "sum"),
            psa10_count=("__gems", "sum"),
        )
        base["gem_rate"] = base.apply(lambda r: (r["psa10_count"] / r["total_graded"]) if r["total_graded"] else 0.0, axis=1)
        base_lookup = base.set_index("__k_base").to_dict("index")

        def _pick_gem_stats(wrow):
            setn = _norm_set(wrow.get("Set", ""))
            cardno = _norm(wrow.get("Card No", ""))
            par = _norm(wrow.get("Parallel", ""))

            kb = _norm_key(setn, cardno)
            base_stats = base_lookup.get(kb)
            if base_stats is None:
                return 0.0, 0.0, 0.0

            # optional filter by parallel within tmp
            m = tmp[(tmp["__set"].apply(_norm_set) == setn) & (tmp["__cardno"].astype(str).str.strip() == cardno)].copy()
            if not m.empty and par:
                par_l = par.lower()
                m2 = m[m["__par"].str.lower().str.contains(par_l, na=False) | m["__desc"].str.lower().str.contains(par_l, na=False)]
                if not m2.empty:
                    total_graded = float(m2["__total"].sum())
                    psa10_count = float(m2["__gems"].sum())
                    gem_rate = float((psa10_count / total_graded) if total_graded else 0.0)
                    return total_graded, psa10_count, gem_rate

            return float(base_stats["total_graded"]), float(base_stats["psa10_count"]), float(base_stats["gem_rate"])

        picked = out.apply(_pick_gem_stats, axis=1)
        out["Total Graded"] = picked.apply(lambda t: float(t[0]))
        out["# PSA 10"] = picked.apply(lambda t: float(t[1]))
        out["Gem Rate"] = picked.apply(lambda t: float(t[2]))
    else:
        out["Total Graded"] = 0.0
        out["# PSA 10"] = 0.0
        out["Gem Rate"] = 0.0

    # ---- PriceCharting prices ----
    def _get_prices(link):
        link = _norm(link)
        if not link or "pricecharting.com" not in link.lower():
            return (0.0, 0.0, 0.0)
        p = fetch_pricecharting_prices(link)
        return (float(p.get("raw", 0.0) or 0.0), float(p.get("psa9", 0.0) or 0.0), float(p.get("psa10", 0.0) or 0.0))

    prices = out["Link"].apply(_get_prices)
    out["Ungraded"] = prices.apply(lambda t: float(t[0]))
    out["PSA 9"] = prices.apply(lambda t: float(t[1]))
    out["PSA 10"] = prices.apply(lambda t: float(t[2]))

    # ---- Sales stats from stored sales-history sheet (last 10) ----
    for c in [
        "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
        "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
        "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
    ]:
        if c not in out.columns:
            out[c] = 0.0

    def _fill_sales(row):
        link = _norm(row.get("Link", ""))
        if not link:
            return pd.Series([0.0] * 12)

        u = _sales_stats_from_last10(sdf, link, "ungraded")
        p9 = _sales_stats_from_last10(sdf, link, "psa9")
        p10 = _sales_stats_from_last10(sdf, link, "psa10")

        return pd.Series([
            u["count"], u["min"], u["max"], u["avg"],
            p9["count"], p9["min"], p9["max"], p9["avg"],
            p10["count"], p10["min"], p10["max"], p10["avg"],
        ])

    sales_cols = [
        "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
        "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
        "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
    ]
    out[sales_cols] = out.apply(_fill_sales, axis=1)

    # ---- Image (Pic first, otherwise scrape) ----
    def _img(row):
        pic = _norm(row.get("Pic", ""))
        if pic:
            return pic
        link = _norm(row.get("Link", ""))
        if link and "pricecharting.com" in link.lower():
            return fetch_pricecharting_image_url(link)
        return ""

    out["Image"] = out.apply(_img, axis=1)

    # ---- Scoring ----
    fee = float(grading_fee_assumption or 0.0)
    out["Target Buy Price"] = out["Target Buy Price"].apply(lambda v: safe_float(v, 0.0))
    out["Max Buy Price"] = out["Max Buy Price"].apply(lambda v: safe_float(v, 0.0))

    def _buy_basis(row):
        tb = float(safe_float(row.get("Target Buy Price", 0.0), 0.0))
        if tb > 0:
            return tb
        uavg = float(safe_float(row.get("ungraded_avg_30d", 0.0), 0.0))
        if uavg > 0:
            return uavg
        return float(safe_float(row.get("Ungraded", 0.0), 0.0))

    out["Buy Basis (Raw)"] = out.apply(_buy_basis, axis=1)
    out["All-in Cost (Buy+Fee)"] = (out["Buy Basis (Raw)"].astype(float) + fee).round(2)

    out["Profit PSA 10"] = (out["PSA 10"].astype(float) - out["All-in Cost (Buy+Fee)"].astype(float)).round(2)
    out["Profit PSA 9"] = (out["PSA 9"].astype(float) - out["All-in Cost (Buy+Fee)"].astype(float)).round(2)

    def _roi(profit, cost):
        cost = float(cost or 0.0)
        if cost <= 0:
            return 0.0
        return float(profit) / cost

    out["ROI PSA 10"] = out.apply(lambda r: _roi(r["Profit PSA 10"], r["All-in Cost (Buy+Fee)"]), axis=1)
    out["ROI PSA 9"] = out.apply(lambda r: _roi(r["Profit PSA 9"], r["All-in Cost (Buy+Fee)"]), axis=1)

    current_year = date.today().year

    def _score(row) -> float:
        cost = float(safe_float(row.get("All-in Cost (Buy+Fee)"), 0.0))
        profit9 = float(safe_float(row.get("Profit PSA 9"), 0.0))
        roi10 = float(safe_float(row.get("ROI PSA 10"), 0.0))

        psa10_sales = float(safe_float(row.get("psa10_sales_30d", 0.0), 0.0))
        total_graded = float(safe_float(row.get("Total Graded", 0.0), 0.0))
        gem_rate = float(safe_float(row.get("Gem Rate", 0.0), 0.0))

        sales_conf = min(1.0, math.log1p(psa10_sales) / math.log1p(10.0)) if psa10_sales > 0 else 0.0
        gem_conf = min(1.0, math.log1p(total_graded) / math.log1p(5000.0)) if total_graded > 0 else 0.0

        roi10_clamped = max(-0.50, min(3.00, roi10))
        roi_component = (roi10_clamped + 0.50) * 40.0
        roi_weighted = roi_component * (0.60 + 0.40 * sales_conf)

        gem_component = (gem_rate * 30.0) * (0.40 + 0.60 * gem_conf)
        sales_component = 20.0 * sales_conf

        cap_component = 0.0
        if cost > 0:
            cap_component = 20.0 * (1.0 / (1.0 + (cost / 120.0)))

        rel_year = int(safe_float(row.get("Release Year", 0), 0.0) or 0)
        age_pen = 0.0
        if 1995 <= rel_year <= current_year:
            years_old = max(0, current_year - rel_year)
            age_pen = min(20.0, years_old * 2.0)

        psa9_value = float(safe_float(row.get("PSA 9", 0.0), 0.0))
        down_pen = downside_penalty_psa9(cost, psa9_value)
        if cost > 0 and profit9 < 0:
            down_pen += min(20.0, (abs(profit9) / cost) * 60.0)

        raw_score = roi_weighted + gem_component + sales_component + cap_component - age_pen - down_pen
        return float(max(0.0, raw_score))

    out["Grading Score"] = out.apply(_score, axis=1).round(2)
    out["Gem Rate"] = out["Gem Rate"].astype(float).round(4)

    front = [
        "Image",
        "Generation", "Set", "Card Name", "Card No", "Parallel",
        "Target Buy Price", "Max Buy Price",
        "Buy Basis (Raw)", "All-in Cost (Buy+Fee)",
        "Ungraded", "PSA 9", "PSA 10",
        "Profit PSA 9", "Profit PSA 10",
        "ROI PSA 9", "ROI PSA 10",
        "Total Graded", "# PSA 10", "Gem Rate",
        "psa10_sales_30d", "psa9_sales_30d", "ungraded_sales_30d",
        "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
        "Grading Score",
        "Release Year",
        "Link", "Pic", "Notes",
    ]
    cols = [c for c in front if c in out.columns] + [c for c in out.columns if c not in front]
    out = out[cols].loc[:, ~out.columns.duplicated()].copy()
    return out


def write_enriched_watchlist(view_df: pd.DataFrame):
    if view_df is None or view_df.empty:
        return

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = _get_ws_or_create(sh, WATCHLIST_WS_NAME, WATCHLIST_BASE_HEADERS + WATCHLIST_ENRICH_HEADERS, cols_hint=70)

    sheet_vals = ws.get_all_values()
    sheet_header = [str(x).strip() for x in (sheet_vals[0] if sheet_vals and sheet_vals[0] else []) if str(x).strip() != ""]
    if not sheet_header:
        sheet_header = WATCHLIST_BASE_HEADERS + WATCHLIST_ENRICH_HEADERS

    df2 = view_df.copy().loc[:, ~view_df.columns.duplicated()].copy()

    # guarantee enrich cols exist
    needed = set(WATCHLIST_BASE_HEADERS + WATCHLIST_ENRICH_HEADERS)
    for c in needed:
        if c not in df2.columns:
            df2[c] = ""

    # preserve sheet order (don’t drop cols)
    for c in sheet_header:
        if c not in df2.columns:
            df2[c] = ""

    df2 = df2[sheet_header].copy()
    _batch_write_sheet(ws, df2, sheet_header)
    load_watchlist_gemrates_sales.clear()


# =========================================================
# DATA
# =========================================================

inv_df = load_inventory_df()
grading_df = load_grading_df()

eligible_inv = inv_df.copy()
if not eligible_inv.empty:
    eligible_inv = eligible_inv[eligible_inv["inventory_status"].isin(list(ELIGIBLE_INV_STATUSES))].copy()
    if "product_type" in eligible_inv.columns:
        eligible_inv = eligible_inv[~eligible_inv["product_type"].astype(str).str.lower().str.contains("sealed", na=False)]


# =========================================================
# UI
# =========================================================

tab_analysis, tab_submit, tab_update, tab_summary = st.tabs(
    ["Analysis", "Create Submission", "Update Returns", "Summary"]
)

with tab_analysis:
    st.subheader("Grading Watch List (GemRates + PriceCharting + Last 10 Sales)")

    fee_assumption = st.number_input(
        "Assumed grading fee (per card)",
        min_value=0.0,
        value=float(DEFAULT_GRADING_FEE_PER_CARD),
        step=1.0,
        format="%.2f",
    )

    wdf, gdf, sdf = load_watchlist_gemrates_sales()

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("📈 Refresh sales history (keep last 10 per link)", width="stretch", disabled=(wdf is None or wdf.empty)):
            n = update_sales_history_incremental(wdf, keep_n=10)
            st.success(f"Updated sales history for {n} link(s).")
            st.rerun()

    with c2:
        if st.button("🔄 Refresh & write Watchlist (prices + gemrates + sales + image + score)", width="stretch", disabled=(wdf is None or wdf.empty)):
            # refresh sales first (this populates your database sheet)
            _ = update_sales_history_incremental(wdf, keep_n=10)
            # reload sales df, then compute all stats
            wdf2, gdf2, sdf2 = load_watchlist_gemrates_sales()
            view = build_watchlist_view(wdf2, gdf2, sdf2, fee_assumption)
            write_enriched_watchlist(view)
            st.success("Watchlist updated and written to sheet.")
            st.rerun()

    with c3:
        st.caption("If Image is blank, add a direct image URL in Pic, or click refresh again after throttling.")

    if wdf is None or wdf.empty:
        st.info(f"No rows found in '{WATCHLIST_WS_NAME}'. Expected headers: {', '.join(WATCHLIST_BASE_HEADERS)}")
    else:
        view = build_watchlist_view(wdf, gdf, sdf, fee_assumption)

        img_cfg = {
            "Image": st.column_config.ImageColumn("Image", width="large"),
            "Link": st.column_config.LinkColumn("Link", display_text="PriceCharting"),
        }

        edited = st.data_editor(
            view.loc[:, ~view.columns.duplicated()].copy(),
            width="stretch",
            hide_index=True,
            num_rows="dynamic",
            column_config=img_cfg,
            disabled=[
                "Image",
                "Buy Basis (Raw)", "All-in Cost (Buy+Fee)",
                "Ungraded", "PSA 9", "PSA 10",
                "Profit PSA 9", "Profit PSA 10",
                "ROI PSA 9", "ROI PSA 10",
                "Total Graded", "# PSA 10", "Gem Rate",
                "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
                "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
                "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
                "Grading Score",
            ],
        )

        if st.button("💾 Save Watch List (manual edits only)", type="primary", width="stretch"):
            # write back edited base+enrich columns; this does NOT re-scrape, it saves what you see
            write_enriched_watchlist(edited)
            st.success("Saved.")
            st.rerun()


with tab_submit:
    st.subheader("Create Submission")

    if eligible_inv.empty:
        st.info("No eligible ACTIVE inventory items to submit.")
    else:
        inv_records = eligible_inv.to_dict("records")

        def short(r):
            return f"{r.get('inventory_id','')} — {r.get('card_name','')} ({r.get('set_name','')} {r.get('year','')}) — ${safe_float(r.get('total_price'),0):,.2f}"

        choices = list(range(len(inv_records)))
        selected = st.multiselect("Select inventory items", options=choices, format_func=lambda i: short(inv_records[i]))

        c1, c2, c3, c4 = st.columns([1.1, 1.1, 1.1, 1.2])
        with c1:
            submission_date = st.date_input("Submission date", value=date.today())
        with c2:
            company = st.selectbox("Grading company", ["PSA", "CGC", "Beckett"])
        with c3:
            fee_initial = st.number_input("Initial grading fee (per card)", min_value=0.0, value=DEFAULT_GRADING_FEE_PER_CARD, step=1.0, format="%.2f")
        with c4:
            business_days = st.number_input("Estimated return (business days)", min_value=1, value=DEFAULT_RETURN_BUSINESS_DAYS, step=1)

        notes = st.text_area("Notes (optional)", height=80)

        if st.button("Create submission", type="primary", width="stretch", disabled=(len(selected) == 0)):
            sub_id = str(int(datetime.utcnow().timestamp()))
            est_return = add_business_days(submission_date, int(business_days))

            rows = []
            for i in selected:
                r = inv_records[i]
                inv_id = safe_str(r.get("inventory_id", "")).strip()
                link = safe_str(r.get("reference_link", "")).strip()

                psa9 = 0.0
                psa10 = 0.0
                if link and "pricecharting.com" in link.lower():
                    prices = fetch_pricecharting_prices(link)
                    psa9 = prices["psa9"]
                    psa10 = prices["psa10"]

                row_id = str(uuid.uuid4())[:10]
                now = datetime.utcnow().isoformat()

                rows.append({
                    "grading_row_id": row_id,
                    "submission_id": sub_id,
                    "submission_date": str(submission_date),
                    "estimated_return_date": str(est_return),
                    "inventory_id": inv_id,
                    "reference_link": link,
                    "card_name": safe_str(r.get("card_name", "")),
                    "card_number": safe_str(r.get("card_number", "")),
                    "variant": safe_str(r.get("variant", "")),
                    "card_subtype": safe_str(r.get("card_subtype", "")),
                    "purchased_from": safe_str(r.get("purchased_from", "")),
                    "purchase_date": safe_str(r.get("purchase_date", "")),
                    "purchase_total": float(safe_float(r.get("total_price", 0.0), 0.0)),
                    "grading_company": company,
                    "grading_fee_initial": float(fee_initial),
                    "additional_costs": 0.0,
                    "psa9_price": float(psa9),
                    "psa10_price": float(psa10),
                    "status": "SUBMITTED",
                    "returned_date": "",
                    "received_grade": "",
                    "notes": notes,
                    "created_at": now,
                    "updated_at": now,
                    "synced_to_inventory": "",
                })

                update_inventory_status(inv_id, STATUS_GRADING)

            append_grading_rows(rows)
            st.success(f"Created submission {sub_id} with {len(rows)} card(s).")
            refresh_all()


with tab_update:
    st.subheader("Update Returns / Add Costs")

    if grading_df.empty:
        st.info("No grading records yet.")
    else:
        df = grading_df.copy().loc[:, ~grading_df.columns.duplicated()].copy()
        df["submission_id"] = df["submission_id"].astype(str)

        open_df = df[df["status"].astype(str).str.upper().isin(["SUBMITTED", "IN_TRANSIT"])].copy()
        open_df = open_df[~open_df["submission_id"].apply(is_blank)].copy()

        if open_df.empty:
            st.info("No open submissions.")
        else:
            meta = (
                open_df.groupby("submission_id", dropna=False)
                .agg(submission_date=("submission_date", "first"), cards=("grading_row_id", "count"))
                .reset_index()
            )
            meta["label"] = meta.apply(lambda r: f"{r['submission_id']} — {r['submission_date']} — {int(r['cards'])} card(s)", axis=1)

            sub_ids = meta["submission_id"].astype(str).tolist()
            label_map = dict(zip(sub_ids, meta["label"].tolist()))

            pick = st.selectbox("Select open submission", options=sub_ids, format_func=lambda sid: label_map.get(sid, sid))
            sub_rows = open_df[open_df["submission_id"].astype(str) == str(pick)].copy()

            edit_cols = [
                "grading_row_id",
                "inventory_id",
                "card_name",
                "purchase_total",
                "grading_company",
                "grading_fee_initial",
                "additional_costs",
                "psa9_price",
                "psa10_price",
                "returned_date",
                "received_grade",
                "status",
            ]
            show = sub_rows[edit_cols].copy()

            for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
                show[c] = show[c].apply(lambda v: safe_float(v, 0.0))

            edited = st.data_editor(show, width="stretch", hide_index=True, num_rows="fixed")

            if st.button("Save updates", type="primary", width="stretch"):
                updated = sub_rows.copy()
                ed_map = {str(r["grading_row_id"]): r for _, r in edited.iterrows()}

                for idx, r in updated.iterrows():
                    rid = str(r["grading_row_id"])
                    if rid not in ed_map:
                        continue
                    e = ed_map[rid]

                    updated.at[idx, "grading_fee_initial"] = safe_float(e.get("grading_fee_initial", 0.0), 0.0)
                    updated.at[idx, "additional_costs"] = safe_float(e.get("additional_costs", 0.0), 0.0)
                    updated.at[idx, "returned_date"] = safe_str(e.get("returned_date", "")).strip()
                    updated.at[idx, "received_grade"] = safe_str(e.get("received_grade", "")).strip()

                    if (not is_blank(updated.at[idx, "returned_date"])) or (not is_blank(updated.at[idx, "received_grade"])):
                        updated.at[idx, "status"] = "RETURNED"

                    updated.at[idx, "updated_at"] = datetime.utcnow().isoformat()

                update_grading_rows(updated)
                st.success("Saved.")
                refresh_all()


with tab_summary:
    st.subheader("Summary (Submission totals)")

    if grading_df.empty:
        st.info("No grading data.")
    else:
        df = grading_df.copy()
        for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
            df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

        df["grading_total_cost"] = (df["grading_fee_initial"] + df["additional_costs"]).round(2)

        grp = (
            df.groupby(["submission_id", "submission_date", "estimated_return_date"], dropna=False)
            .agg(
                cards=("grading_row_id", "count"),
                purchase_cost=("purchase_total", "sum"),
                grading_cost=("grading_total_cost", "sum"),
                psa9_value=("psa9_price", "sum"),
                psa10_value=("psa10_price", "sum"),
            )
            .reset_index()
        )

        grp["profit_all_psa9"] = (grp["psa9_value"] - (grp["purchase_cost"] + grp["grading_cost"])).round(2)
        grp["profit_all_psa10"] = (grp["psa10_value"] - (grp["purchase_cost"] + grp["grading_cost"])).round(2)

        def money(x):
            return f"${float(x):,.2f}"

        show = grp.copy()
        for c in ["purchase_cost", "grading_cost", "psa9_value", "psa10_value", "profit_all_psa9", "profit_all_psa10"]:
            show[c] = show[c].apply(money)

        st.dataframe(show, width="stretch", hide_index=True)

        if st.button("🔄 Refresh", width="stretch"):
            refresh_all()
