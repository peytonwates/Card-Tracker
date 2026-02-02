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
# WATCH LIST + GEMRATES + SALES HISTORY (Analysis tab overhaul)
#   - Watchlist tab: grading_watch_list
#   - GemRates tab:  gemrates
#   - Sales history tab: grading_sales_history (snapshot of last-30d stats)
# =========================================================

import math

WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
GEMRATES_WS_NAME = st.secrets.get("gemrates_worksheet", "gemrates")
GRADING_SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

# --- Headers (create tabs if missing) ---
WATCHLIST_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "Parallel",
    "Pic",               # optional (if blank, we'll try to pull from PriceCharting og:image)
    "Release Year",      # optional (used for "older set" penalty)
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
    # normalize set strings so "Scarlet & Violet 151" / "151" have a better shot
    t = _norm(s).lower()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("’", "'")
    return t

def _norm_key(*parts) -> str:
    out = []
    for p in parts:
        out.append(_norm(p).lower())
    return "|".join(out)

def _safe_num(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        if s.endswith("%"):
            return float(s[:-1]) / 100.0
        return float(s)
    except Exception:
        return default

def _get_ws_or_create(sheet, name: str, headers: list[str], cols_hint: int = 26, rows_hint: int = 2000):
    """
    Creates worksheet if missing; writes header row once.
    Avoids repeated "repair" to keep writes low.
    """
    try:
        ws = sheet.worksheet(name)
        # if empty sheet, write headers
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
    """
    Quota-friendly: ONE update call for entire table.
    """
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
    """
    Pick first existing column from names (case/space tolerant).
    """
    if df is None or df.empty:
        return pd.Series([default] * 0)

    norm_map = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n is None:
            continue
        key = str(n).strip().lower()
        if key in norm_map:
            return df[norm_map[key]]

    # fuzzy contains match (useful for "Gem Rate - All Time")
    for n in names:
        key = str(n).strip().lower()
        for k, real in norm_map.items():
            if key and key in k:
                return df[real]
    return pd.Series([default] * len(df))

# -------------------------
# PriceCharting sold-sales scrape (last 30d snapshot)
# -------------------------

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
    """
    Returns list of dicts: {date, price, grade_bucket, title}
    Scrapes visible sold listing lines on the PriceCharting page.
    """
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

# -------------------------
# Card image: prefer watchlist Pic, else pull og:image from PriceCharting
# -------------------------

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

# -------------------------
# Downside protection (PSA9 break-even)
# -------------------------

def downside_penalty_psa9(buy_total: float, psa9_value: float) -> float:
    """
    Penalty if PSA9 outcome loses money.
    Break-even/profit => 0. Loss => grows quickly, capped.
    """
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
    """
    Writes a snapshot (overwrite) to grading_sales_history with last-30d sales stats for each watchlist row.
    ONE sheet write for the whole table.
    """
    if wdf is None or wdf.empty:
        return 0

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    sales_ws = _get_ws_or_create(sh, GRADING_SALES_HISTORY_WS_NAME, SALES_HISTORY_HEADERS, cols_hint=60, rows_hint=4000)

    out_rows = []
    run_utc = datetime.utcnow().isoformat()

    # Ensure required cols exist
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

def build_watchlist_view(
    wdf: pd.DataFrame,
    gdf: pd.DataFrame,
    sdf: pd.DataFrame,
    grading_fee_assumption: float,
):
    """
    Display DF includes:
      - watchlist source fields
      - Image
      - gemrates: Total Graded, # PSA 10, Gem Rate
      - pricecharting current: Ungraded, PSA 9, PSA 10
      - sales last 30d snapshot: min/max/avg/count for ungraded/psa9/psa10
      - grading score w/ downside protection
    """
    if wdf is None or wdf.empty:
        return pd.DataFrame()

    out = wdf.copy()

    # ensure required columns exist
    for c in WATCHLIST_HEADERS:
        if c not in out.columns:
            out[c] = ""

    # -------------------------
    # Build GemRates aggregation (robust matching)
    # -------------------------
    # Strategy:
    #   Try match by (generation + set + card# + parallel) where possible.
    #   If Parallel blank, match by (generation + set + card#) across all gemrate rows.
    #   Also allow Parallel matching against Card Description.
    gem_lookup = {}
    base_lookup = {}

    if gdf is not None and not gdf.empty:
        g_gen = _col(gdf, "Generation").astype(str)
        g_set = _col(gdf, "Set Name", "Set").astype(str)
        g_cardno = _col(gdf, "Card #", "Card No").astype(str)
        g_par = _col(gdf, "Parallel", "Variant").astype(str)
        g_desc = _col(gdf, "Card Description", "Description").astype(str)

        gems = _col(gdf, "Gems")
        total = _col(gdf, "Total")

        g_gems = gems.apply(lambda x: _safe_num(x, 0.0))
        g_total = total.apply(lambda x: _safe_num(x, 0.0))

        tmp = pd.DataFrame({
            "__gen": g_gen,
            "__set": g_set,
            "__cardno": g_cardno,
            "__par": g_par,
            "__desc": g_desc,
            "__gems": g_gems,
            "__total": g_total,
        })

        # base aggregation (all rows per gen/set/cardno)
        tmp["__k_base"] = tmp.apply(lambda r: _norm_key(_norm_set(r["__gen"]), _norm_set(r["__set"]), _norm(r["__cardno"])), axis=1)
        base = tmp.groupby("__k_base", as_index=False).agg(
            total_graded=("__total", "sum"),
            psa10_count=("__gems", "sum"),
        )
        base["gem_rate"] = base.apply(lambda r: (r["psa10_count"] / r["total_graded"]) if r["total_graded"] else 0.0, axis=1)
        base_lookup = base.set_index("__k_base").to_dict("index")

        # "full" lookup cannot be a simple group because watchlist parallel might match description.
        # We'll keep tmp for row-wise filtering in _pick_gem_stats().
        tmp["_tmp_idx"] = range(len(tmp))
        gem_lookup["_tmp"] = tmp

    def _pick_gem_stats(wrow) -> tuple[float, float, float]:
        """
        Returns: (total_graded, psa10_count, gem_rate)
        """
        gen = _norm_set(wrow.get("Generation", ""))
        setn = _norm_set(wrow.get("Set", ""))
        cardno = _norm(wrow.get("Card No", ""))
        par = _norm(wrow.get("Parallel", ""))

        kb = _norm_key(gen, setn, cardno)
        base_stats = base_lookup.get(kb, None)

        if not gem_lookup or "_tmp" not in gem_lookup:
            if base_stats:
                return float(base_stats["total_graded"]), float(base_stats["psa10_count"]), float(base_stats["gem_rate"])
            return 0.0, 0.0, 0.0

        tmp = gem_lookup["_tmp"]

        # filter by base first
        m = tmp[
            (tmp["__gen"].astype(str).str.strip().str.lower().apply(_norm_set) == gen)
            & (tmp["__set"].astype(str).str.strip().str.lower().apply(_norm_set) == setn)
            & (tmp["__cardno"].astype(str).str.strip() == cardno)
        ].copy()

        if m.empty:
            if base_stats:
                return float(base_stats["total_graded"]), float(base_stats["psa10_count"]), float(base_stats["gem_rate"])
            return 0.0, 0.0, 0.0

        # if parallel provided, try match on Parallel OR Card Description contains it
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

    out["Total Graded"] = 0.0
    out["# PSA 10"] = 0.0
    out["Gem Rate"] = 0.0

    picked = out.apply(_pick_gem_stats, axis=1)
    out["Total Graded"] = picked.apply(lambda t: float(t[0]))
    out["# PSA 10"] = picked.apply(lambda t: float(t[1]))
    out["Gem Rate"] = picked.apply(lambda t: float(t[2]))

    # -------------------------
    # PriceCharting pull (current raw/psa9/psa10)
    # -------------------------
    out["Ungraded"] = 0.0
    out["PSA 9"] = 0.0
    out["PSA 10"] = 0.0

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

    # -------------------------
    # Sales history merge (last-30d snapshot)
    # -------------------------
    for c in [
        "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
        "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
        "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
    ]:
        out[c] = 0.0

    if sdf is not None and not sdf.empty and "reference_link" in sdf.columns:
        # most recent run per reference_link
        sdf2 = sdf.copy()
        sdf2["run_utc"] = sdf2.get("run_utc", "").astype(str)
        sdf2["reference_link"] = sdf2["reference_link"].astype(str).str.strip()
        # sort so newest run first
        sdf2 = sdf2.sort_values("run_utc", ascending=False)

        latest = sdf2.drop_duplicates(subset=["reference_link"], keep="first").copy()
        latest = latest.set_index("reference_link")

        def _merge_sales(row, col):
            link = _norm(row.get("Link", ""))
            if not link or link not in latest.index:
                return 0.0
            v = latest.loc[link].get(col, 0.0)
            return _safe_num(v, 0.0)

        for col in [
            "ungraded_sales_30d", "ungraded_min_30d", "ungraded_max_30d", "ungraded_avg_30d",
            "psa9_sales_30d", "psa9_min_30d", "psa9_max_30d", "psa9_avg_30d",
            "psa10_sales_30d", "psa10_min_30d", "psa10_max_30d", "psa10_avg_30d",
        ]:
            out[col] = out.apply(lambda r: _merge_sales(r, col), axis=1)

    # -------------------------
    # Image column (prefer Pic, else PriceCharting og:image)
    # -------------------------
    def _img(row):
        pic = _norm(row.get("Pic", ""))
        if pic:
            return pic
        link = _norm(row.get("Link", ""))
        if link and "pricecharting.com" in link.lower():
            return fetch_pricecharting_image_url(link)
        return ""

    out["Image"] = out.apply(_img, axis=1)

    # -------------------------
    # Buy basis + risk/return
    # -------------------------
    fee = float(grading_fee_assumption or 0.0)
    out["Target Buy Price"] = out["Target Buy Price"].apply(lambda v: _safe_num(v, 0.0))
    out["Max Buy Price"] = out["Max Buy Price"].apply(lambda v: _safe_num(v, 0.0))

    # choose buy basis:
    # 1) Target Buy Price if provided
    # 2) else last-30d ungraded avg if available
    # 3) else current ungraded price
    def _buy_basis(row):
        tb = float(_safe_num(row.get("Target Buy Price", 0.0), 0.0))
        if tb > 0:
            return tb
        uavg = float(_safe_num(row.get("ungraded_avg_30d", 0.0), 0.0))
        if uavg > 0:
            return uavg
        return float(_safe_num(row.get("Ungraded", 0.0), 0.0))

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

    # -------------------------
    # Grading score (with downside protection)
    # Weighted by:
    #   - ROI10 (primary)
    #   - PSA10 sales_30d (confidence in PSA10 price)
    #   - capital required (lower = better)
    #   - gem rate (weighted by total graded = confidence)
    #   - set age penalty (older = harder to find clean)
    #   - downside penalty (PSA9 loss)
    # -------------------------
    current_year = date.today().year

    def _score(row) -> float:
        cost = float(_safe_num(row.get("All-in Cost (Buy+Fee)"), 0.0))
        profit10 = float(_safe_num(row.get("Profit PSA 10"), 0.0))
        profit9 = float(_safe_num(row.get("Profit PSA 9"), 0.0))
        roi10 = float(_safe_num(row.get("ROI PSA 10"), 0.0))

        psa10_sales = float(_safe_num(row.get("psa10_sales_30d", 0.0), 0.0))
        total_graded = float(_safe_num(row.get("Total Graded", 0.0), 0.0))
        gem_rate = float(_safe_num(row.get("Gem Rate", 0.0), 0.0))

        # Confidence scalers (0..1)
        sales_conf = 0.0
        if psa10_sales > 0:
            sales_conf = min(1.0, math.log1p(psa10_sales) / math.log1p(40.0))  # ~40 sales => 1.0

        gem_conf = 0.0
        if total_graded > 0:
            gem_conf = min(1.0, math.log1p(total_graded) / math.log1p(5000.0))  # ~5k graded => 1.0

        # ROI component (clamped) -> up to ~120
        roi10_clamped = max(-0.50, min(3.00, roi10))
        roi_component = (roi10_clamped + 0.50) * 40.0  # maps [-0.5..3.0] -> [0..140]

        # More confidence => more weight on ROI component
        roi_weighted = roi_component * (0.60 + 0.40 * sales_conf)

        # Gem component (0..~30)
        gem_component = (gem_rate * 30.0) * (0.40 + 0.60 * gem_conf)

        # Sales component (0..20)
        sales_component = 20.0 * sales_conf

        # Capital component (lower cost => higher score, 0..20)
        cap_component = 0.0
        if cost > 0:
            cap_component = 20.0 * (1.0 / (1.0 + (cost / 120.0)))  # ~$120 => ~10 pts

        # Set age penalty (0..20) only if Release Year given
        rel_year = int(_safe_num(row.get("Release Year", 0), 0.0) or 0)
        age_pen = 0.0
        if rel_year >= 1995 and rel_year <= current_year:
            years_old = max(0, current_year - rel_year)
            age_pen = min(20.0, years_old * 2.0)

        # Downside penalty based on PSA9 loss vs cost
        psa9_value = float(_safe_num(row.get("PSA 9", 0.0), 0.0))
        down_pen = downside_penalty_psa9(cost, psa9_value)

        # Extra protection: if PSA9 is negative profit, add small additional penalty tied to loss magnitude
        if cost > 0 and profit9 < 0:
            down_pen += min(20.0, (abs(profit9) / cost) * 60.0)

        raw_score = roi_weighted + gem_component + sales_component + cap_component - age_pen - down_pen
        return float(max(0.0, raw_score))

    out["Grading Score"] = out.apply(_score, axis=1).round(2)

    # nice % display helpers (keep numeric for sorting; UI will show as % with formatting if desired)
    out["Gem Rate"] = out["Gem Rate"].astype(float).round(4)

    # Clean up: put useful columns up front
    front = [
        "Image",
        "Generation", "Set", "Card Name", "Card No", "Parallel",
        "Target Buy Price", "Max Buy Price",
        "Buy Basis (Raw)", "All-in Cost (Buy+Fee)",
        "Ungraded", "PSA 9", "PSA 10",
        "Profit PSA 9", "Profit PSA 10",
        "ROI PSA 9", "ROI PSA 10",
        "Total Graded", "# PSA 10", "Gem Rate",
        "psa10_sales_30d",
        "psa9_sales_30d",
        "ungraded_sales_30d",
        "Grading Score",
        "Release Year",
        "Link",
        "Pic",
        "Notes",
    ]
    # include any missing columns to avoid KeyError
    cols = [c for c in front if c in out.columns] + [c for c in out.columns if c not in front]
    out = out[cols]

    return out

def save_watchlist_from_editor(edited_df: pd.DataFrame):
    if edited_df is None:
        return
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = _get_ws_or_create(sh, WATCHLIST_WS_NAME, WATCHLIST_HEADERS, cols_hint=40)

    # Only write back "source" columns (not computed columns)
    source_cols = WATCHLIST_HEADERS[:]  # exact headers we created
    for c in source_cols:
        if c not in edited_df.columns:
            edited_df[c] = ""

    out = edited_df[source_cols].copy()
    _batch_write_sheet(ws, out, source_cols)

    load_watchlist_gemrates_sales.clear()


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
    st.subheader("Grading Watch List (GemRates + PriceCharting + Sales History)")

    fee_assumption = st.number_input(
        "Assumed grading fee (per card)",
        min_value=0.0,
        value=float(st.secrets.get("default_grading_fee_per_card", 28.0)),
        step=1.0,
        format="%.2f",
    )

    wdf, gdf, sdf = load_watchlist_gemrates_sales()

    # Controls
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
        st.caption("Tip: Fill **Parallel** (e.g., Base / Reverse Holo / SIR) to improve GemRates matching.")

    if wdf is None or wdf.empty:
        st.info(
            f"No rows found in '{WATCHLIST_WS_NAME}'.\n\n"
            f"Paste your watch list data into that sheet (headers in row 1). "
            f"Recommended columns: {', '.join(WATCHLIST_HEADERS)}"
        )
    else:
        view = build_watchlist_view(wdf, gdf, sdf, fee_assumption)

        st.caption(
            "Edit the watch list fields (Generation/Set/Card No/Link/Parallel/Target Buy/etc). "
            "GemRates, PriceCharting, Sales History, and Score columns are computed."
        )

        # Image column config (big enough to see the card)
        img_cfg = {}
        try:
            img_cfg = {
                "Image": st.column_config.ImageColumn(
                    "Image",
                    help="Card image (Pic column or PriceCharting)",
                    width="large",
                ),
                "Link": st.column_config.LinkColumn("Link", display_text="PriceCharting"),
            }
        except Exception:
            # column_config can vary by Streamlit version; safe fallback
            img_cfg = {}

        edited = st.data_editor(
            view,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config=img_cfg,
            disabled=[
                # lock computed columns so you only edit source fields
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

        s1, s2 = st.columns([1, 1])
        with s1:
            if st.button("💾 Save Watch List", type="primary", use_container_width=True):
                save_watchlist_from_editor(edited)
                st.success("Saved to Google Sheets.")
                st.rerun()

        with s2:
            st.caption(
                "Score includes: ROI in PSA10 (weighted by PSA10 sales_30d), gem rate (weighted by total graded), "
                "capital required, set age penalty (Release Year), and PSA9 downside penalty."
            )


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

                    if (not is_blank(updated.at[idx, "returned_date"])) or (not is_blank(updated.at[idx, "received_grade"])):
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
