# pages/5_Grading.py

import json
import re
import uuid
import time
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
    # if still failing
    raise APIError("APIError: [429] Quota exceeded (retries exhausted)")


# =========================================================
# HEADER NORMALIZATION / REPAIR
# =========================================================

def _strip_dups(h: str) -> str:
    # remove ANY stacked __dup suffixes
    return re.sub(r"(?:__dup\d+)+$", "", str(h or "").strip())

def _build_stable_unique_headers(raw_headers: list[str]) -> list[str]:
    """
    Normalize:
    - trim whitespace
    - blank -> unnamed__colN
    - base name = strip stacked __dup suffixes
    - rebuild stable uniques: base, base__dup2, base__dup3...
    """
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
    """
    Ensures each canon header exists by base-name. Keeps existing duplicates.
    Adds missing canon columns at the end.
    """
    existing_bases = {_strip_dups(h) for h in stable_headers}
    out = list(stable_headers)
    for h in canon_headers:
        b = _strip_dups(h)
        if b not in existing_bases:
            out.append(b)  # add as base
            existing_bases.add(b)
    # rebuild stable unique again (in case added bases collide)
    return _build_stable_unique_headers(out)

def _prune_blank_duplicate_columns(ws, values, header_row):
    """
    Safe prune:
    - For any base that has multiple columns,
      delete duplicate columns (idx >= 2) that are entirely blank across all data rows.
    Deletion happens right-to-left.
    """
    if not values or len(values) < 2:
        return False

    raw = header_row
    data_rows = values[1:]

    # pad rows to raw length
    padded = []
    for r in data_rows:
        if len(r) < len(raw):
            r = r + [""] * (len(raw) - len(r))
        padded.append(r)

    base_to_idxs = {}
    for j, h in enumerate(raw):
        b = _strip_dups(h)
        base_to_idxs.setdefault(b, []).append(j)

    delete_1based = []
    for b, idxs in base_to_idxs.items():
        if len(idxs) <= 1:
            continue
        # consider duplicates beyond the first occurrence
        for j in idxs[1:]:
            all_blank = True
            for r in padded:
                v = str(r[j] if j < len(r) else "").strip()
                if v != "":
                    all_blank = False
                    break
            if all_blank:
                delete_1based.append(j + 1)

    deleted_any = False
    for col in sorted(delete_1based, reverse=True):
        try:
            _gs_write_retry(ws.delete_columns, col)
            deleted_any = True
        except Exception:
            pass

    return deleted_any

def ensure_headers(ws, needed_headers: list[str], *, write: bool = False, prune_blank_dups: bool = False):
    """
    IMPORTANT: This function can run in READ-ONLY mode (write=False).
    - When write=False: returns what the header *should be*, but does NOT write to the sheet.
    - When write=True: writes only if header differs. Optional safe-prune blank dup columns.

    This prevents quota blowups on page load.
    """
    # single read
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

    # stable + ensure canon exists
    stable = _build_stable_unique_headers(raw)
    stable = _append_missing_canon_headers(stable, needed_headers)

    # optional prune (requires writes)
    if prune_blank_dups and write:
        # prune based on current sheet structure (raw, not stable)
        deleted_any = _prune_blank_duplicate_columns(ws, values, raw)
        if deleted_any:
            # re-read after deletion
            values = ws.get_all_values()
            raw = values[0] if values else []
            stable = _build_stable_unique_headers(raw)
            stable = _append_missing_canon_headers(stable, needed_headers)

    if write and raw != stable:
        _gs_write_retry(ws.update, values=[stable], range_name="1:1", value_input_option="USER_ENTERED")

    return stable


# =========================================================
# PRICECHARTING PSA9 / PSA10
# =========================================================

@st.cache_data(ttl=60 * 60 * 12)
def fetch_pricecharting_prices(reference_link: str) -> dict:
    """
    Slots (matches your ImportXML indexing):
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

    for c in ["inventory_id", "reference_link", "card_name", "set_name", "year", "total_price", "purchase_date", "purchased_from", "product_type"]:
        if c not in df.columns:
            df[c] = ""

    df["inventory_id"] = df["inventory_id"].astype(str)

    # Force year to a clean string
    df["year"] = (
        df["year"]
        .astype(str)
        .replace({"nan": "", "None": "", "<NA>": ""})
        .str.strip()
    )

    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0.0)
    df["product_type"] = df["product_type"].astype(str)

    return df


@st.cache_data(ttl=30)
def load_grading_df():
    """
    READ ONLY:
    - does NOT call ensure_headers(write=True)
    - just reads values + coalesces duplicates into canonical cols
    """
    ws = get_ws(GRADING_WS_NAME)
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    header_row = values[0] if values else GRADING_CANON_COLS
    if not header_row:
        header_row = GRADING_CANON_COLS

    # Pad rows to header length
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

    # Ensure canon cols exist
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

        # dedupe candidate list keeping order
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

    # Coalesce legacy → canonical
    _coalesce_into("grading_fee_initial", ["grading_fee_per_card"])
    _coalesce_into("additional_costs", ["extra_costs"])
    _coalesce_into("received_grade", ["returned_grade"])

    # Numeric parse
    for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
        df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

    df["grading_row_id"] = df["grading_row_id"].astype(str)
    df["submission_id"] = df["submission_id"].astype(str)

    df["status"] = df["status"].astype(str).replace("", "SUBMITTED")

    return df


def refresh_all():
    load_inventory_df.clear()
    load_grading_df.clear()
    st.rerun()


# =========================================================
# WRITES (HEADER REPAIR ONLY HERE)
# =========================================================

def append_grading_rows(rows: list[dict]):
    if not rows:
        return

    ws = get_ws(GRADING_WS_NAME)
    headers = ensure_headers(ws, GRADING_CANON_COLS, write=True, prune_blank_dups=False)

    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    # Build all rows for a single append (append_rows is fewer requests than append_row loop)
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

    # append_rows is available in newer gspread; fallback to append_row if needed
    if hasattr(ws, "append_rows"):
        _gs_write_retry(ws.append_rows, out_rows, value_input_option="RAW")
    else:
        for r in out_rows:
            _gs_write_retry(ws.append_row, r, value_input_option="RAW")


def update_grading_rows(df_rows: pd.DataFrame):
    """
    Quota-safe update:
    - Repairs headers once (write=True)
    - Uses a SINGLE read (ws.get_all_values) to map grading_row_id -> rownum
    - Batch updates all rows in one request (or a few) instead of per-row ws.update
    """
    if df_rows is None or df_rows.empty:
        return

    ws = get_ws(GRADING_WS_NAME)
    headers = ensure_headers(ws, GRADING_CANON_COLS, write=True, prune_blank_dups=False)

    values = ws.get_all_values()
    if not values:
        return

    sheet_header = values[0] if values else []
    if not sheet_header:
        return

    # find grading_row_id column index
    id_col_idx = None
    for j, h in enumerate(sheet_header):
        if _strip_dups(h) == "grading_row_id":
            id_col_idx = j
            break
    if id_col_idx is None:
        raise ValueError("grading_row_id must exist in grading sheet header row.")

    # id -> rownum mapping from same read
    id_to_rownum: dict[str, int] = {}
    for rownum, row in enumerate(values[1:], start=2):
        v = str(row[id_col_idx] if len(row) > id_col_idx else "" or "").strip()
        if v:
            id_to_rownum[v] = rownum

    last_col = a1_col_letter(len(sheet_header))

    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    # Build batch update payload
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

    # batch_update in chunks to reduce request size if needed
    chunk_size = 50
    for i in range(0, len(data_updates), chunk_size):
        chunk = data_updates[i:i + chunk_size]
        _gs_write_retry(ws.batch_update, chunk, value_input_option="RAW")


def update_inventory_status(inventory_id: str, new_status: str):
    inv_ws = get_ws(INVENTORY_WS_NAME)
    values = inv_ws.get_all_values()
    if not values:
        return
    headers = values[0] if values else []
    if not headers:
        return

    # Find cols by base-name
    id_col_idx = None
    status_col_idx = None
    for j, h in enumerate(headers):
        b = _strip_dups(h)
        if b == "inventory_id":
            id_col_idx = j
        elif b == "inventory_status":
            status_col_idx = j

    if id_col_idx is None or status_col_idx is None:
        # only repair headers when we're actually writing
        ensure_headers(inv_ws, ["inventory_id", "inventory_status"], write=True, prune_blank_dups=False)
        values = inv_ws.get_all_values()
        headers = values[0] if values else []
        id_col_idx = None
        status_col_idx = None
        for j, h in enumerate(headers):
            b = _strip_dups(h)
            if b == "inventory_id":
                id_col_idx = j
            elif b == "inventory_status":
                status_col_idx = j
        if id_col_idx is None or status_col_idx is None:
            return

    # Locate row
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


def mark_inventory_as_graded(inventory_id: str, grading_company: str, grade: str):
    inv_ws = get_ws(INVENTORY_WS_NAME)

    # Only repair headers on write path
    ensure_headers(inv_ws, ["inventory_id", "product_type", "grading_company", "grade", "reference_link", "market_price", "market_value"], write=True, prune_blank_dups=False)

    values = inv_ws.get_all_values()
    if not values:
        return
    headers = values[0] if values else []
    if not headers:
        return

    # find required col idxs by base
    idx = { _strip_dups(h): j for j, h in enumerate(headers) }

    if "inventory_id" not in idx:
        return

    id_col = idx["inventory_id"]

    # locate row
    rownum = None
    for i, row in enumerate(values[1:], start=2):
        v = str(row[id_col] if len(row) > id_col else "" or "").strip()
        if v == str(inventory_id).strip():
            rownum = i
            break
    if not rownum:
        return

    def _set(base_name: str, value):
        if base_name not in idx:
            return
        c = idx[base_name] + 1
        _gs_write_retry(inv_ws.update, values=[[value]], range_name=f"{a1_col_letter(c)}{rownum}", value_input_option="USER_ENTERED")

    def _get(base_name: str) -> str:
        if base_name not in idx:
            return ""
        c = idx[base_name]
        row = values[rownum - 1] if rownum - 1 < len(values) else []
        return str(row[c] if len(row) > c else "" or "").strip()

    _set("product_type", "Graded Card")
    _set("grading_company", grading_company)
    _set("grade", grade)

    link = _get("reference_link")
    if link and "pricecharting.com" in link.lower():
        prices = fetch_pricecharting_prices(link)
        g = safe_str(grade).strip().upper()

        mv = float(prices.get("raw", 0.0) or 0.0)
        if "10" in g:
            mv = float(prices.get("psa10", 0.0) or 0.0)
        elif "9" in g:
            mv = float(prices.get("psa9", 0.0) or 0.0)

        _set("market_price", mv)
        _set("market_value", mv)


# =========================================================
# OPTIONAL: Manual header repair UI (safe + quota friendly)
# =========================================================

with st.expander("🛠️ Sheet Maintenance (fix duplicate columns / header drift)", expanded=False):
    st.caption(
        "If you previously had buggy header logic, click this once to normalize the header row and (optionally) "
        "delete fully-blank duplicate columns. This prevents columns from endlessly multiplying."
    )

    cA, cB = st.columns([1, 1])
    with cA:
        do_prune = st.checkbox("Prune fully blank duplicate columns (safe)", value=True)
    with cB:
        if st.button("Repair grading sheet headers now", use_container_width=True):
            ws = get_ws(GRADING_WS_NAME)
            ensure_headers(ws, GRADING_CANON_COLS, write=True, prune_blank_dups=do_prune)
            st.success("Repaired grading sheet headers.")
            refresh_all()


# =========================================================
# SYNC RETURNED GRADES TO INVENTORY (unchanged logic, but write calls are retried)
# =========================================================

def sync_returned_grades_to_inventory():
    STATUS_ACTIVE_LOCAL = "ACTIVE"
    STATUS_LISTED_LOCAL = "LISTED"

    def to_num(x):
        return safe_float(x, 0.0)

    def to_dt(x):
        return pd.to_datetime(x, errors="coerce")

    def norm(s):
        return "" if s is None else str(s).strip()

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])

    inv_ws_name = st.secrets.get("inventory_worksheet", "inventory")
    grd_ws_name = st.secrets.get("grading_worksheet", "grading")

    inv_ws = sh.worksheet(inv_ws_name)
    grd_ws = sh.worksheet(grd_ws_name)

    # Only repair headers if user is syncing (explicit action)
    ensure_headers(inv_ws, ["inventory_id", "inventory_status", "product_type", "grading_company", "grade", "condition", "total_price", "reference_link", "market_price", "market_value"], write=True, prune_blank_dups=False)
    ensure_headers(grd_ws, GRADING_CANON_COLS, write=True, prune_blank_dups=False)

    inv_records = inv_ws.get_all_records()
    grd_records = grd_ws.get_all_records()

    inv_df = pd.DataFrame(inv_records)
    grd_df = pd.DataFrame(grd_records)

    if inv_df.empty or grd_df.empty:
        return 0

    # Normalize missing columns safely
    for c in [
        "inventory_id",
        "inventory_status",
        "reference_link",
        "total_price",
        "product_type",
        "grading_company",
        "grade",
        "condition",
        "purchase_date",
    ]:
        if c not in inv_df.columns:
            inv_df[c] = ""

    for c in [
        "status",
        "reference_link",
        "grading_company",
        "grading_fee_initial",
        "additional_costs",
        "psa10_price",
        "psa9_price",
        "received_grade",
        "inventory_id",
        "synced_to_inventory",
    ]:
        if c not in grd_df.columns:
            grd_df[c] = ""

    inv_df["__inv_total"] = inv_df["total_price"].apply(to_num)
    inv_df["__inv_dt"] = to_dt(inv_df.get("purchase_date", ""))

    grd_df["__status"] = grd_df["status"].astype(str).str.upper().str.strip()
    returned = grd_df[grd_df["__status"] == "RETURNED"].copy()
    if returned.empty:
        return 0

    if "synced_to_inventory" in grd_df.columns:
        returned = returned[returned["synced_to_inventory"].astype(str).str.upper().str.strip() != "YES"].copy()
    if returned.empty:
        return 0

    # Map inventory_id -> sheet rownum via values read
    inv_values = inv_ws.get_all_values()
    if not inv_values or not inv_values[0]:
        return 0
    inv_headers = inv_values[0]
    inv_idx = { _strip_dups(h): j for j, h in enumerate(inv_headers) }
    if "inventory_id" not in inv_idx:
        return 0
    id_col_idx = inv_idx["inventory_id"]

    id_to_rownum = {}
    for i, row in enumerate(inv_values[1:], start=2):
        v = str(row[id_col_idx] if len(row) > id_col_idx else "" or "").strip()
        if v:
            id_to_rownum[v] = i

    # Helper set cell (single cell updates; low volume here)
    def set_inv_cell(rownum: int, col_base: str, value):
        if col_base not in inv_idx:
            return
        c = inv_idx[col_base] + 1
        _gs_write_retry(inv_ws.update, values=[[value]], range_name=f"{a1_col_letter(c)}{rownum}", value_input_option="USER_ENTERED")

    # Grading values map
    grd_values = grd_ws.get_all_values()
    if not grd_values or not grd_values[0]:
        return 0
    grd_headers = grd_values[0]
    grd_idx = { _strip_dups(h): j for j, h in enumerate(grd_headers) }
    if "grading_row_id" not in grd_idx:
        return 0
    grd_id_col = grd_idx["grading_row_id"]

    grd_id_to_rownum = {}
    for i, row in enumerate(grd_values[1:], start=2):
        v = str(row[grd_id_col] if len(row) > grd_id_col else "" or "").strip()
        if v:
            grd_id_to_rownum[v] = i

    def set_grd_cell(rownum: int, col_base: str, value):
        if col_base not in grd_idx:
            return
        c = grd_idx[col_base] + 1
        _gs_write_retry(grd_ws.update, values=[[value]], range_name=f"{a1_col_letter(c)}{rownum}", value_input_option="USER_ENTERED")

    updated_count = 0

    for _, g in returned.iterrows():
        g_ref = norm(g.get("reference_link", ""))
        g_inv_id = norm(g.get("inventory_id", ""))
        g_company = norm(g.get("grading_company", ""))
        g_grade = norm(g.get("received_grade", ""))

        grading_cost = to_num(g.get("grading_fee_initial", 0)) + to_num(g.get("additional_costs", 0))

        psa10 = to_num(g.get("psa10_price", 0))
        psa9 = to_num(g.get("psa9_price", 0))
        grade_upper = str(g_grade).upper()
        new_market = psa10 if ("10" in grade_upper) or ("PRISTINE" in grade_upper) or ("BLACK" in grade_upper) else psa9

        inv_match = None

        if g_inv_id:
            m = inv_df[inv_df["inventory_id"].astype(str).str.strip() == g_inv_id]
            if not m.empty:
                inv_match = m.iloc[0]

        if inv_match is None and g_ref:
            candidates = inv_df[
                (inv_df["reference_link"].astype(str).str.strip() == g_ref)
                & (inv_df["inventory_status"].astype(str).str.upper().isin([STATUS_ACTIVE_LOCAL, STATUS_LISTED_LOCAL]))
            ].copy()

            if not candidates.empty:
                g_dt = to_dt(g.get("purchase_date", ""))
                g_cost = to_num(g.get("purchase_total", 0)) or 0.0

                candidates["__dtdiff"] = (candidates["__inv_dt"] - g_dt).abs()
                candidates["__costdiff"] = (candidates["__inv_total"] - g_cost).abs()
                candidates = candidates.sort_values(["__dtdiff", "__costdiff"])
                inv_match = candidates.iloc[0]

        if inv_match is None:
            continue

        inv_id = str(inv_match["inventory_id"]).strip()
        rownum = id_to_rownum.get(inv_id)
        if not rownum:
            continue

        old_total = to_num(inv_match.get("total_price", 0))
        new_total = round(old_total + grading_cost, 2)

        set_inv_cell(rownum, "product_type", "Graded Card")
        set_inv_cell(rownum, "grading_company", g_company)
        set_inv_cell(rownum, "grade", g_grade)
        set_inv_cell(rownum, "condition", "Graded")
        set_inv_cell(rownum, "total_price", new_total)
        set_inv_cell(rownum, "market_price", new_market)
        set_inv_cell(rownum, "market_value", new_market)

        rid = str(g.get("grading_row_id", "")).strip()
        grd_rownum = grd_id_to_rownum.get(rid)
        if grd_rownum:
            set_grd_cell(grd_rownum, "synced_to_inventory", "YES")

        updated_count += 1

    return updated_count


if st.button("🔁 Sync RETURNED grades → Inventory", use_container_width=True):
    n = sync_returned_grades_to_inventory()
    st.success(f"Synced {n} returned submission(s) into Inventory.")
    st.rerun()


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
# UI TABS
# =========================================================

tab_analysis, tab_submit, tab_update, tab_summary = st.tabs(
    ["Analysis", "Create Submission", "Update Returns", "Summary"]
)

# -------------------------
# Analysis
# -------------------------
with tab_analysis:
    st.subheader("Analysis (pull PSA 9/10 from PriceCharting)")

    if eligible_inv.empty:
        st.info("No eligible ACTIVE inventory items to analyze.")
    else:
        records = eligible_inv.to_dict("records")

        def label(r):
            return f"{r.get('inventory_id','')} — {r.get('card_name','')} ({r.get('set_name','')} {r.get('year','')}) — Cost ${safe_float(r.get('total_price'),0):,.2f}"

        idx = st.selectbox("Select an item", options=list(range(len(records))), format_func=lambda i: label(records[i]))
        r = records[idx]

        link = safe_str(r.get("reference_link", "")).strip()
        purchase_total = safe_float(r.get("total_price", 0.0), 0.0)

        st.write("**Purchased from:**", safe_str(r.get("purchased_from", "")))
        st.write("**Purchase date:**", safe_str(r.get("purchase_date", "")))
        st.write("**Reference link:**", link if link else "(none)")

        fee = st.number_input(
            "Assumed grading fee (per card)",
            min_value=0.0,
            value=DEFAULT_GRADING_FEE_PER_CARD,
            step=1.0,
            format="%.2f",
        )

        psa9 = 0.0
        psa10 = 0.0
        if link and "pricecharting.com" in link.lower():
            prices = fetch_pricecharting_prices(link)
            psa9 = prices["psa9"]
            psa10 = prices["psa10"]

        profit9 = psa9 - (purchase_total + fee)
        profit10 = psa10 - (purchase_total + fee)

        c1, c2, c3 = st.columns(3)
        c1.metric("PSA 9", f"${psa9:,.2f}")
        c2.metric("PSA 10", f"${psa10:,.2f}")
        c3.metric("Purchase Total", f"${purchase_total:,.2f}")

        d1, d2 = st.columns(2)
        d1.metric("Profit if PSA 9", f"${profit9:,.2f}")
        d2.metric("Profit if PSA 10", f"${profit10:,.2f}")


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

            save = st.button("Save updates", type="primary", use_container_width=True)

            if save:
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

                    if (not is_blank(updated.at[idx, "returned_date"])) or (not is_blank(updated.at[idx, "received_grade"]))):
                        updated.at[idx, "status"] = "RETURNED"

                    updated.at[idx, "updated_at"] = datetime.utcnow().isoformat()

                    if str(updated.at[idx, "status"]).upper() == "RETURNED":
                        inv_id = safe_str(updated.at[idx, "inventory_id"])
                        update_inventory_status(inv_id, STATUS_ACTIVE)
                        mark_inventory_as_graded(
                            inv_id,
                            safe_str(updated.at[idx, "grading_company"]),
                            safe_str(updated.at[idx, "received_grade"]),
                        )

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

        df["purchase_total"] = df["purchase_total"].apply(lambda v: safe_float(v, 0.0))
        df["grading_fee_initial"] = df["grading_fee_initial"].apply(lambda v: safe_float(v, 0.0))
        df["additional_costs"] = df["additional_costs"].apply(lambda v: safe_float(v, 0.0))
        df["psa9_price"] = df["psa9_price"].apply(lambda v: safe_float(v, 0.0))
        df["psa10_price"] = df["psa10_price"].apply(lambda v: safe_float(v, 0.0))

        need_mask = (
            ((df["psa9_price"] == 0.0) | (df["psa10_price"] == 0.0))
            & df["reference_link"].astype(str).str.lower().str.contains("pricecharting.com", na=False)
        )
        if need_mask.any():
            for idx, r in df[need_mask].iterrows():
                prices = fetch_pricecharting_prices(safe_str(r["reference_link"]))
                if df.at[idx, "psa9_price"] == 0.0:
                    df.at[idx, "psa9_price"] = prices["psa9"]
                if df.at[idx, "psa10_price"] == 0.0:
                    df.at[idx, "psa10_price"] = prices["psa10"]
            st.caption("Some PSA 9/10 were 0 — summary is using live PriceCharting values for those rows.")

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
