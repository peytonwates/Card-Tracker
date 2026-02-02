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

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
STATUS_TRADED = "TRADED"
STATUS_GRADING = "GRADING"

ELIGIBLE_INV_STATUSES = {STATUS_ACTIVE}

DEFAULT_GRADING_FEE_PER_CARD = float(st.secrets.get("default_grading_fee_per_card", 28.0))
DEFAULT_RETURN_BUSINESS_DAYS = int(st.secrets.get("default_business_days_return", 75))

# Canonical grading columns we will USE.
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


# =========================================================
# QUOTA-SAFE WRITE WRAPPERS
# =========================================================

def _gs_write_retry(fn, *args, **kwargs):
    """
    Retry gspread write calls on 429 with exponential backoff.
    """
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


# =========================================================
# HEADER NORMALIZATION / REPAIR
# =========================================================

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


# =========================================================
# PRICECHARTING PSA9 / PSA10 (current)
# =========================================================

@st.cache_data(ttl=60 * 60 * 12)
def fetch_pricecharting_prices(reference_link: str) -> dict:
    """
    Slots (matches your old ImportXML indexing):
      raw  = slot 1 -> prices[0]
      psa9 = slot 4 -> prices[3]
      psa10= slot 6 -> prices[5]
    """
    out = {"raw": 0.0, "psa9": 0.0, "psa10": 0.0}
    if not reference_link or "pricecharting.com" not in reference_link.lower():
        return out

    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(reference_link.strip(), headers=headers, timeout=12)
        r.raise_for_status()
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


# =========================================================
# LOADERS (CACHED) — READ-ONLY (NO WRITES)
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
    df["inventory_status"] = df["inventory_status"].astype(str).replace("", STATUS_ACTIVE)

    for c in ["inventory_id", "reference_link", "card_name", "set_name", "year", "total_price", "purchase_date", "purchased_from", "product_type", "card_number", "variant", "card_subtype"]:
        if c not in df.columns:
            df[c] = ""

    df["inventory_id"] = df["inventory_id"].astype(str)
    df["year"] = df["year"].astype(str).replace({"nan": "", "None": "", "<NA>": ""}).str.strip()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0.0)
    df["product_type"] = df["product_type"].astype(str)

    return df

@st.cache_data(ttl=30)
def load_grading_df():
    ws = get_ws(GRADING_WS_NAME)
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    header_row = values[0] if values else GRADING_CANON_COLS
    if not header_row:
        header_row = GRADING_CANON_COLS

    data_rows = []
    for r in values[1:]:
        if len(r) < len(header_row):
            r = r + [""] * (len(header_row) - len(r))
        elif len(r) > len(header_row):
            r = r[: len(header_row)]
        data_rows.append(r)

    df = pd.DataFrame(data_rows, columns=header_row)
    if df.empty:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    for c in GRADING_CANON_COLS:
        if c not in df.columns:
            df[c] = ""

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

        seen = set()
        ordered = []
        for c in candidates:
            if c not in seen:
                ordered.append(c)
                seen.add(c)

        if not ordered:
            return

        s = df[ordered[0]].astype(str)
        for c in ordered[1:]:
            t = df[c].astype(str)
            s = s.where(s.str.strip() != "", t)

        df[base] = s

    _coalesce_into("grading_fee_initial", ["grading_fee_per_card"])
    _coalesce_into("additional_costs", ["extra_costs"])
    _coalesce_into("received_grade", ["returned_grade"])

    for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
        df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

    df["grading_row_id"] = df["grading_row_id"].astype(str)
    df["submission_id"] = df["submission_id"].astype(str)
    df["status"] = df["status"].astype(str).replace("", "SUBMITTED")

    return df

def refresh_all():
    load_inventory_df.clear()
    load_grading_df.clear()
    load_watchlist_gemrates_sales.clear()
    st.rerun()


# =========================================================
# WRITES (HEADER REPAIR ONLY HERE)
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

    chunk_size = 50
    for i in range(0, len(data_updates), chunk_size):
        chunk = data_updates[i:i + chunk_size]
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
# WATCH LIST + GEMRATES + SALES HISTORY (Analysis tab overhaul)
# =========================================================

WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
GEMRATES_WS_NAME = st.secrets.get("gemrates_worksheet", "gemrates")
GRADING_SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

WATCHLIST_HEADERS = [
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

SALES_HISTORY_HEADERS = [
    "run_utc",
    "generation",
    "set",
    "card_name",
    "card_no",
    "reference_link",

    "ungraded_sales_30d",
    "ungraded_min_30d",
    "ungraded_max_30d",
    "ungraded_avg_30d",

    "psa9_sales_30d",
    "psa9_min_30d",
    "psa9_max_30d",
    "psa9_avg_30d",

    "psa10_sales_30d",
    "psa10_min_30d",
    "psa10_max_30d",
    "psa10_avg_30d",
]

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

def _get_ws_or_create(sheet, name: str, headers: list[str], cols_hint: int = 26, rows_hint: int = 2000):
    try:
        ws = sheet.worksheet(name)
        vals = ws.get_all_values()
        if not vals or not vals[0] or all(str(x).strip() == "" for x in vals[0]):
            ws.update(range_name="1:1", values=[headers], value_input_option="RAW")
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
    if df is None:
        return
    df2 = df.copy()
    for c in header:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[header]
    values = [header] + df2.astype(str).fillna("").values.tolist()
    last_col = a1_col_letter(len(header))
    ws.update(range_name=f"A1:{last_col}{len(values)}", values=values, value_input_option="RAW")

def _col(df: pd.DataFrame, *names, default=""):
    if df is None or df.empty:
        return pd.Series([default] * 0)

    norm_map = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n is None:
            continue
        key = str(n).strip().lower()
        if key in norm_map:
            return df[norm_map[key]]

    for n in names:
        key = str(n).strip().lower()
        for k, real in norm_map.items():
            if key and key in k:
                return df[real]
    return pd.Series([default] * len(df))

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

@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sold_sales_last_60d(reference_link: str) -> list[dict]:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
    r = requests.get(link, headers=headers, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "lxml")
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    sales = []
    date_re = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
    price_re = re.compile(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")

    i = 0
    while i < len(lines):
        ln = lines[i]
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
                sales.append({"date": sale_date, "price": price, "grade_bucket": bucket, "title": title})
        i += 1

    return sales

def _sales_stats_for_bucket(sales: list[dict], bucket: str, days: int = 30) -> dict:
    today = date.today()
    cutoff = today - timedelta(days=days)
    prices = [
        float(x["price"])
        for x in sales
        if x.get("grade_bucket") == bucket and x.get("date") and x["date"] >= cutoff
    ]
    if not prices:
        return {"count": 0, "min": 0.0, "max": 0.0, "avg": 0.0}
    return {
        "count": int(len(prices)),
        "min": float(min(prices)),
        "max": float(max(prices)),
        "avg": float(sum(prices) / len(prices)),
    }

@st.cache_data(ttl=60 * 60 * 24)
def fetch_pricecharting_image_url(reference_link: str) -> str:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(link, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        meta = soup.find("meta", attrs={"property": "og:image"})
        if meta and meta.get("content"):
            return str(meta.get("content")).strip()
        return ""
    except Exception:
        return ""

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

@st.cache_data(ttl=60)
def load_watchlist_gemrates_sales():
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])

    watch_ws = _get_ws_or_create(sh, WATCHLIST_WS_NAME, WATCHLIST_HEADERS, cols_hint=40)
    gem_ws = _get_ws_or_create(sh, GEMRATES_WS_NAME, GEMRATES_HEADERS, cols_hint=40)
    sales_ws = _get_ws_or_create(sh, GRADING_SALES_HISTORY_WS_NAME, SALES_HISTORY_HEADERS, cols_hint=60, rows_hint=4000)

    wdf = _read_sheet_df(watch_ws)
    gdf = _read_sheet_df(gem_ws)
    sdf = _read_sheet_df(sales_ws)

    return wdf, gdf, sdf

def update_sales_history_from_watchlist(wdf: pd.DataFrame):
    if wdf is None or wdf.empty:
        return 0

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    sales_ws = _get_ws_or_create(sh, GRADING_SALES_HISTORY_WS_NAME, SALES_HISTORY_HEADERS, cols_hint=60, rows_hint=4000)

    out_rows = []
    run_utc = datetime.utcnow().isoformat()

    for c in ["Generation", "Set", "Card Name", "Card No", "Link"]:
        if c not in wdf.columns:
            wdf[c] = ""

    for _, r in wdf.iterrows():
        link = _norm(r.get("Link", ""))
        if not link or "pricecharting.com" not in link.lower():
            continue

        sales = fetch_pricecharting_sold_sales_last_60d(link)
        u = _sales_stats_for_bucket(sales, "ungraded", days=30)
        p9 = _sales_stats_for_bucket(sales, "psa9", days=30)
        p10 = _sales_stats_for_bucket(sales, "psa10", days=30)

        out_rows.append({
            "run_utc": run_utc,
            "generation": _norm(r.get("Generation", "")),
            "set": _norm(r.get("Set", "")),
            "card_name": _norm(r.get("Card Name", "")),
            "card_no": _norm(r.get("Card No", "")),
            "reference_link": link,

            "ungraded_sales_30d": u["count"],
            "ungraded_min_30d": round(u["min"], 2),
            "ungraded_max_30d": round(u["max"], 2),
            "ungraded_avg_30d": round(u["avg"], 2),

            "psa9_sales_30d": p9["count"],
            "psa9_min_30d": round(p9["min"], 2),
            "psa9_max_30d": round(p9["max"], 2),
            "psa9_avg_30d": round(p9["avg"], 2),

            "psa10_sales_30d": p10["count"],
            "psa10_min_30d": round(p10["min"], 2),
            "psa10_max_30d": round(p10["max"], 2),
            "psa10_avg_30d": round(p10["avg"], 2),
        })

    if not out_rows:
        return 0

    df = pd.DataFrame(out_rows)
    _batch_write_sheet(sales_ws, df, SALES_HISTORY_HEADERS)
    load_watchlist_gemrates_sales.clear()
    fetch_pricecharting_sold_sales_last_60d.clear()
    return len(df)

def build_watchlist_view(wdf: pd.DataFrame, gdf: pd.DataFrame, sdf: pd.DataFrame, grading_fee_assumption: float):
    if wdf is None or wdf.empty:
        return pd.DataFrame()

    out = wdf.copy()
    for c in WATCHLIST_HEADERS:
        if c not in out.columns:
            out[c] = ""

    # --- Gemrates robust matching ---
    base_lookup = {}
    tmp = None

    if gdf is not None and not gdf.empty:
        g_gen = _col(gdf, "Generation").astype(str)
        g_set = _col(gdf, "Set Name", "Set").astype(str)
        g_cardno = _col(gdf, "Card #", "Card No").astype(str)
        g_par = _col(gdf, "Parallel", "Variant").astype(str)
        g_desc = _col(gdf, "Card Description", "Description").astype(str)
        gems = _col(gdf, "Gems")
        total = _col(gdf, "Total")

        g_gems = gems.apply(lambda x: safe_float(x, 0.0))
        g_total = total.apply(lambda x: safe_float(x, 0.0))

        tmp = pd.DataFrame({
            "__gen": g_gen,
            "__set": g_set,
            "__cardno": g_cardno,
            "__par": g_par,
            "__desc": g_desc,
            "__gems": g_gems,
            "__total": g_total,
        })

        tmp["__k_base"] = tmp.apply(lambda r: _norm_key(_norm_set(r["__gen"]), _norm_set(r["__set"]), _norm(r["__cardno"])), axis=1)

        base = tmp.groupby("__k_base", as_index=False).agg(
            total_graded=("__total", "sum"),
            psa10_count=("__gems", "sum"),
        )
        base["gem_rate"] = base.apply(lambda r: (r["psa10_count"] / r["total_graded"]) if r["total_graded"] else 0.0, axis=1)
        base_lookup = base.set_index("__k_base").to_dict("index")

    def _pick_gem_stats(wrow):
        gen = _norm_set(wrow.get("Generation", ""))
        setn = _norm_set(wrow.get("Set", ""))
        cardno = _norm(wrow.get("Card No", ""))
        par = _norm(wrow.get("Parallel", ""))

        kb = _norm_key(gen, setn, cardno)
        base_stats = base_lookup.get(kb)

        if tmp is None:
            if base_stats:
                return float(base_stats["total_graded"]), float(base_stats["psa10_count"]), float(base_stats["gem_rate"])
            return 0.0, 0.0, 0.0

        m = tmp[
            (tmp["__gen"].astype(str).str.strip().str.lower().apply(_norm_set) == gen)
            & (tmp["__set"].astype(str).str.strip().str.lower().apply(_norm_set) == setn)
            & (tmp["__cardno"].astype(str).str.strip() == cardno)
        ].copy()

        if m.empty:
            if base_stats:
                return float(base_stats["total_graded"]), float(base_stats["psa10_count"]), float(base_stats["gem_rate"])
            return 0.0, 0.0, 0.0

        if par:
            par_l = par.lower()
            m2 = m[
                m["__par"].astype(str).str.lower().str.contains(par_l, na=False)
                | m["__desc"].astype(str).str.lower().str.contains(par_l, na=False)
            ].copy()
            if not m2.empty:
                m = m2

        total_graded = float(m["__total"].sum())
        psa10_count = float(m["__gems"].sum())
        gem_rate = float((psa10_count / total_graded) if total_graded else 0.0)
        return total_graded, psa10_count, gem_rate

    picked = out.apply(_pick_gem_stats, axis=1)
    out["Total Graded"] = picked.apply(lambda t: float(t[0]))
    out["# PSA 10"] = picked.apply(lambda t: float(t[1]))
    out["Gem Rate"] = picked.apply(lambda t: float(t[2]))

    # --- PriceCharting current prices ---
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

    # --- Sales history merge ---
    for c in [
        "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
        "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
        "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
    ]:
        out[c] = 0.0

    if sdf is not None and not sdf.empty and "reference_link" in sdf.columns:
        sdf2 = sdf.copy()
        sdf2["run_utc"] = sdf2.get("run_utc", "").astype(str)
        sdf2["reference_link"] = sdf2["reference_link"].astype(str).str.strip()
        sdf2 = sdf2.sort_values("run_utc", ascending=False)
        latest = sdf2.drop_duplicates(subset=["reference_link"], keep="first").set_index("reference_link")

        def _merge_sales(row, col):
            link = _norm(row.get("Link", ""))
            if not link or link not in latest.index:
                return 0.0
            return safe_float(latest.loc[link].get(col, 0.0), 0.0)

        for col in [
            "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
            "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
            "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
        ]:
            out[col] = out.apply(lambda r: _merge_sales(r, col), axis=1)

    # --- Image ---
    def _img(row):
        pic = _norm(row.get("Pic", ""))
        if pic:
            return pic
        link = _norm(row.get("Link", ""))
        if link and "pricecharting.com" in link.lower():
            return fetch_pricecharting_image_url(link)
        return ""

    out["Image"] = out.apply(_img, axis=1)

    # --- Buy basis + score ---
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

        sales_conf = min(1.0, math.log1p(psa10_sales) / math.log1p(40.0)) if psa10_sales > 0 else 0.0
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
        "Grading Score",
        "Release Year",
        "Link", "Pic", "Notes",
    ]
    cols = [c for c in front if c in out.columns] + [c for c in out.columns if c not in front]
    return out[cols]

def save_watchlist_from_editor(edited_df: pd.DataFrame):
    if edited_df is None:
        return
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = _get_ws_or_create(sh, WATCHLIST_WS_NAME, WATCHLIST_HEADERS, cols_hint=40)

    for c in WATCHLIST_HEADERS:
        if c not in edited_df.columns:
            edited_df[c] = ""

    out = edited_df[WATCHLIST_HEADERS].copy()
    _batch_write_sheet(ws, out, WATCHLIST_HEADERS)
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
# UI TABS (ONLY ONCE)
# =========================================================

tab_analysis, tab_submit, tab_update, tab_summary = st.tabs(
    ["Analysis", "Create Submission", "Update Returns", "Summary"]
)

# -------------------------
# Analysis
# -------------------------
with tab_analysis:
    st.subheader("Grading Watch List (GemRates + PriceCharting + Sales History)")

    fee_assumption = st.number_input(
        "Assumed grading fee (per card)",
        min_value=0.0,
        value=float(st.secrets.get("default_grading_fee_per_card", 28.0)),
        step=1.0,
        format="%.2f",
    )

    wdf, gdf, sdf = load_watchlist_gemrates_sales()

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("📈 Update last-30-day sales (PriceCharting → grading_sales_history)", use_container_width=True, disabled=(wdf is None or wdf.empty)):
            n = update_sales_history_from_watchlist(wdf)
            st.success(f"Updated sales history for {n} watchlist row(s).")
            st.rerun()

    with c2:
        if st.button("🔄 Refresh (re-pull GemRates + PriceCharting)", use_container_width=True):
            load_watchlist_gemrates_sales.clear()
            fetch_pricecharting_prices.clear()
            fetch_pricecharting_image_url.clear()
            st.rerun()

    with c3:
        st.caption("Tip: Fill **Parallel** (Base / Reverse Holo / SIR) to improve GemRates matching.")

    if wdf is None or wdf.empty:
        st.info(
            f"No rows found in '{WATCHLIST_WS_NAME}'. Paste your watch list data into that sheet (headers in row 1).\n\n"
            f"Expected headers: {', '.join(WATCHLIST_HEADERS)}"
        )
    else:
        view = build_watchlist_view(wdf, gdf, sdf, fee_assumption)

        img_cfg = {
            "Image": st.column_config.ImageColumn("Image", help="Pic column or PriceCharting image", width="large"),
            "Link": st.column_config.LinkColumn("Link", display_text="PriceCharting"),
        }

        edited = st.data_editor(
            view,
            use_container_width=True,
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

        b1, b2 = st.columns([1, 1])
        with b1:
            if st.button("💾 Save Watch List", type="primary", use_container_width=True):
                save_watchlist_from_editor(edited)
                st.success("Saved to Google Sheets.")
                st.rerun()
        with b2:
            st.caption("Score uses ROI10, PSA10 sales confidence, gem rate confidence, capital, age penalty (Release Year), and PSA9 downside penalty.")

# -------------------------
# Create Submission
# -------------------------
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
            fee_initial = st.number_input(
                "Initial grading fee (per card)",
                min_value=0.0,
                value=DEFAULT_GRADING_FEE_PER_CARD,
                step=1.0,
                format="%.2f",
            )
        with c4:
            business_days = st.number_input(
                "Estimated return (business days)",
                min_value=1,
                value=DEFAULT_RETURN_BUSINESS_DAYS,
                step=1,
            )

        notes = st.text_area("Notes (optional)", height=80)

        if st.button("Create submission", type="primary", use_container_width=True, disabled=(len(selected) == 0)):
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

# -------------------------
# Update Returns
# -------------------------
with tab_update:
    st.subheader("Update Returns / Add Costs")

    if grading_df.empty:
        st.info("No grading records yet.")
    else:
        df = grading_df.copy()
        df["submission_id"] = df["submission_id"].astype(str)

        open_df = df[df["status"].astype(str).str.upper().isin(["SUBMITTED", "IN_TRANSIT"])].copy()
        open_df = open_df[~open_df["submission_id"].apply(is_blank)].copy()

        if open_df.empty:
            st.info("No open submissions.")
        else:
            meta = (
                open_df.groupby("submission_id", dropna=False)
                .agg(
                    submission_date=("submission_date", "first"),
                    cards=("grading_row_id", "count"),
                )
                .reset_index()
            )
            meta["label"] = meta.apply(lambda r: f"{r['submission_id']} — {r['submission_date']} — {int(r['cards'])} card(s)", axis=1)

            sub_ids = meta["submission_id"].astype(str).tolist()
            label_map = dict(zip(sub_ids, meta["label"].tolist()))

            pick = st.selectbox("Select open submission", options=sub_ids, format_func=lambda sid: label_map.get(sid, sid))

            sub_rows = open_df[open_df["submission_id"].astype(str) == str(pick)].copy()

            st.caption("Edit the table, then click Save updates. Rows with a returned date or grade will be marked RETURNED automatically.")
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
                if c in show.columns:
                    show[c] = show[c].apply(lambda v: safe_float(v, 0.0))

            edited = st.data_editor(show, use_container_width=True, hide_index=True, num_rows="fixed")

            if st.button("Save updates", type="primary", use_container_width=True):
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

# -------------------------
# Summary
# -------------------------
with tab_summary:
    st.subheader("Summary (Submission totals)")

    if grading_df.empty:
        st.info("No grading data.")
    else:
        df = grading_df.copy()
        df["submission_id"] = df["submission_id"].astype(str)

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

        st.dataframe(show, use_container_width=True, hide_index=True)

        if st.button("🔄 Refresh", use_container_width=True):
            refresh_all()
