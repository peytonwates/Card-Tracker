# pages/5_Grading.py

import json
import re
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Grading", layout="wide")
st.title("Grading")


def sync_returned_grades_to_inventory():
    import json
    from pathlib import Path
    import pandas as pd
    import gspread
    from google.oauth2.service_account import Credentials
    import streamlit as st

    STATUS_ACTIVE = "ACTIVE"
    STATUS_LISTED = "LISTED"

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
            sa_path = Path(sa_rel)
            if not sa_path.is_absolute():
                sa_path = Path.cwd() / sa_rel
            sa_info = json.loads(sa_path.read_text(encoding="utf-8"))
            creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
            return gspread.authorize(creds)

        raise KeyError('Missing secrets: add "gcp_service_account" or "service_account_json_path".')

    def to_num(x):
        try:
            if x is None or x == "":
                return 0.0
            return float(str(x).replace("$", "").replace(",", "").strip())
        except Exception:
            return 0.0

    def to_dt(x):
        return pd.to_datetime(x, errors="coerce")

    def norm(s):
        return "" if s is None else str(s).strip()

    # --- normalize / de-duplicate header row in-place (no new helpers; kept local) ---
    def _repair_headers_in_place(ws):
        raw = ws.row_values(1)
        if not raw:
            return raw

        cleaned = []
        for i, h in enumerate(raw, start=1):
            hh = "" if h is None else str(h).strip()
            if hh == "":
                hh = f"unnamed__col{i}"
            cleaned.append(hh)

        counts = {}
        unique = []
        for h in cleaned:
            if h not in counts:
                counts[h] = 1
                unique.append(h)
            else:
                counts[h] += 1
                unique.append(f"{h}__dup{counts[h]}")

        if unique != raw:
            ws.update("1:1", [unique], value_input_option="USER_ENTERED")

        return unique

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])

    inv_ws_name = st.secrets.get("inventory_worksheet", "inventory")
    grd_ws_name = st.secrets.get("grading_worksheet", "grading")

    inv_ws = sh.worksheet(inv_ws_name)
    grd_ws = sh.worksheet(grd_ws_name)

    # Repair headers once so we don't keep creating duplicates across page loads
    inv_headers = _repair_headers_in_place(inv_ws)
    grd_headers = _repair_headers_in_place(grd_ws)

    # --- READ ONCE (quota-friendly) ---
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
    ]:
        if c not in inv_df.columns:
            inv_df[c] = ""

    for c in [
        "status",
        "reference_link",
        "grading_company",
        "grading_fee_initial",
        "additional_costs",
        "grading_fee_per_card",
        "extra_costs",
        "total_grading_cost",
        "psa10_price",
        "psa9_price",
        "received_grade",
        "returned_grade",
        "inventory_id",
        "updated_at_utc",
    ]:
        if c not in grd_df.columns:
            grd_df[c] = ""

    # ---- COALESCE legacy / duplicate grading fields into canonical ones ----
    # grading_fee_initial: prefer it, else use grading_fee_per_card, else (if present) total_grading_cost (per-row) minus extra_costs
    def _first_nonblank_series(a: pd.Series, b: pd.Series) -> pd.Series:
        a_s = a.astype(str)
        b_s = b.astype(str)
        return a_s.where(a_s.str.strip() != "", b_s)

    grd_df["grading_fee_initial"] = _first_nonblank_series(grd_df["grading_fee_initial"], grd_df["grading_fee_per_card"])

    # additional_costs: prefer it, else extra_costs
    grd_df["additional_costs"] = _first_nonblank_series(grd_df["additional_costs"], grd_df["extra_costs"])

    # received_grade: prefer it, else returned_grade
    grd_df["received_grade"] = _first_nonblank_series(grd_df["received_grade"], grd_df["returned_grade"])

    inv_df["__inv_total"] = inv_df["total_price"].apply(to_num)
    inv_df["__inv_dt"] = to_dt(inv_df.get("purchase_date", ""))

    grd_df["__status"] = grd_df["status"].astype(str).str.upper().str.strip()
    returned = grd_df[grd_df["__status"] == "RETURNED"].copy()
    if returned.empty:
        return 0

    # If you have a "synced" column, respect it (optional)
    if "synced_to_inventory" in grd_df.columns:
        returned = returned[returned["synced_to_inventory"].astype(str).str.upper().str.strip() != "YES"].copy()

    if returned.empty:
        return 0

    # Map inventory_id -> sheet rownum (read col A once)
    col_a = inv_ws.col_values(1)
    id_to_rownum = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_rownum[str(val).strip()] = idx

    # Header index based on repaired headers (unique, stripped)
    inv_header_index = {h: i for i, h in enumerate(inv_headers)}
    grd_header_index = {h: i for i, h in enumerate(grd_headers)}

    def set_inv_cell(rownum: int, col_name: str, value):
        # allow matching if sheet header had been suffixed; use base-name match
        target = None
        if col_name in inv_header_index:
            target = col_name
        else:
            for h in inv_headers:
                if re.sub(r"__dup\d+$", "", h) == col_name:
                    target = h
                    break
        if not target:
            return False
        col_idx_1based = inv_header_index[target] + 1
        a1 = gspread.utils.rowcol_to_a1(rownum, col_idx_1based)
        inv_ws.update(a1, value, value_input_option="USER_ENTERED")
        return True

    def set_grd_cell(rownum: int, col_name: str, value):
        target = None
        if col_name in grd_header_index:
            target = col_name
        else:
            for h in grd_headers:
                if re.sub(r"__dup\d+$", "", h) == col_name:
                    target = h
                    break
        if not target:
            return False
        col_idx_1based = grd_header_index[target] + 1
        a1 = gspread.utils.rowcol_to_a1(rownum, col_idx_1based)
        grd_ws.update(a1, value, value_input_option="USER_ENTERED")
        return True

    # get_all_records loses row numbers, so rownum = df index + 2 (header row at 1)
    def grd_rownum_from_df_index(i):
        return int(i) + 2

    updated_count = 0

    for i, g in returned.iterrows():
        g_ref = norm(g.get("reference_link", ""))
        g_inv_id = norm(g.get("inventory_id", ""))  # best case
        g_company = norm(g.get("grading_company", ""))

        g_grade = (
            norm(g.get("received_grade", ""))
            or norm(g.get("returned_grade", ""))
            or norm(g.get("grade", ""))
        )

        grading_cost = to_num(g.get("grading_fee_initial", 0)) + to_num(g.get("additional_costs", 0))

        # Market value: use psa10 if grade is a 10-type, else psa9
        psa10 = to_num(g.get("psa10_price", 0))
        psa9 = to_num(g.get("psa9_price", 0))
        grade_upper = str(g_grade).upper()
        if ("10" in grade_upper) or ("PRISTINE" in grade_upper) or ("BLACK" in grade_upper):
            new_market = psa10
        else:
            new_market = psa9

        # Find inventory match
        inv_match = None

        # 1) if inventory_id exists
        if g_inv_id:
            m = inv_df[inv_df["inventory_id"].astype(str).str.strip() == g_inv_id]
            if not m.empty:
                inv_match = m.iloc[0]

        # 2) else match by reference_link + active/listed
        if inv_match is None and g_ref:
            candidates = inv_df[
                (inv_df["reference_link"].astype(str).str.strip() == g_ref)
                & (inv_df["inventory_status"].astype(str).str.upper().isin([STATUS_ACTIVE, STATUS_LISTED]))
            ].copy()

            if not candidates.empty:
                # pick closest by purchase_date then by total
                g_dt = to_dt(g.get("purchase_date", ""))
                g_cost = to_num(g.get("purchase_total", 0)) or to_num(g.get("purchase_price", 0)) or 0.0

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

        # Update inventory fields
        old_total = to_num(inv_match.get("total_price", 0))
        new_total = round(old_total + grading_cost, 2)

        set_inv_cell(rownum, "product_type", "Graded Card")
        set_inv_cell(rownum, "grading_company", g_company)
        set_inv_cell(rownum, "grade", g_grade)
        set_inv_cell(rownum, "condition", "Graded")
        set_inv_cell(rownum, "total_price", new_total)

        # Store market price if you have this column
        set_inv_cell(rownum, "market_price", new_market)
        set_inv_cell(rownum, "market_value", new_market)

        # Mark grading row as synced so we don't add grading cost again
        grd_rownum = grd_rownum_from_df_index(i)
        set_grd_cell(grd_rownum, "synced_to_inventory", "YES")

        updated_count += 1

    return updated_count


if st.button("🔁 Sync RETURNED grades → Inventory", use_container_width=True):
    n = sync_returned_grades_to_inventory()
    st.success(f"Synced {n} returned submission(s) into Inventory.")
    st.rerun()


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
    "grading_fee_initial",   # <-- use THIS
    "additional_costs",      # <-- use THIS
    "psa9_price",
    "psa10_price",
    "status",
    "returned_date",
    "received_grade",
    "notes",
    "created_at",
    "updated_at",
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

def ensure_headers(ws, needed_headers: list[str]):
    """
    Fixes your duplicate header problem without adding new helper modules:
    - strips whitespace on row 1
    - makes headers unique if duplicates exist (suffix __dupN)
    - prevents re-appending the same column due to whitespace/case drift
    - then ensures canonical columns exist (by base-name, ignoring __dupN)
    """
    existing_raw = ws.row_values(1)
    if not existing_raw:
        ws.append_row(needed_headers, value_input_option="USER_ENTERED")
        return needed_headers

    # strip + fill blanks
    cleaned = []
    for i, h in enumerate(existing_raw, start=1):
        hh = "" if h is None else str(h).strip()
        if hh == "":
            hh = f"unnamed__col{i}"
        cleaned.append(hh)

    # make unique in place if duplicates exist
    counts = {}
    unique = []
    for h in cleaned:
        if h not in counts:
            counts[h] = 1
            unique.append(h)
        else:
            counts[h] += 1
            unique.append(f"{h}__dup{counts[h]}")

    if unique != existing_raw:
        ws.update("1:1", [unique], value_input_option="USER_ENTERED")

    # compare by base-name (ignore __dupN)
    base_existing = {re.sub(r"__dup\d+$", "", h) for h in unique}
    missing = [h for h in needed_headers if h not in base_existing]

    if missing:
        ws.update("1:1", [unique + missing], value_input_option="USER_ENTERED")
        return unique + missing

    return unique

# =========================================================
# PRICECHARTING PSA9 / PSA10
# =========================================================

@st.cache_data(ttl=60 * 60 * 12)
def fetch_pricecharting_psa_prices(reference_link: str) -> dict:
    out = {"psa9": 0.0, "psa10": 0.0}
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

        psa9 = prices[3] if len(prices) >= 4 else 0.0
        psa10 = prices[5] if len(prices) >= 6 else 0.0
        out["psa9"] = float(psa9 or 0.0)
        out["psa10"] = float(psa10 or 0.0)
        return out
    except Exception:
        return out

# =========================================================
# LOADERS (CACHED)
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

    # Force year to a clean string (prevents Arrow trying to cast to int64 and choking on "")
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
    ws = get_ws(GRADING_WS_NAME)

    # Ensure headers exist AND repair duplicates/whitespace drift
    _ = ensure_headers(ws, GRADING_CANON_COLS)

    # Read raw values so we respect the actual header row (post-repair)
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    header_row = values[0]
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
        return pd.DataFrame(columns=header_row)

    # Make sure all canon cols exist in df (even if sheet has more)
    for c in GRADING_CANON_COLS:
        if c not in df.columns:
            df[c] = ""

    # ---- COALESCE legacy / duplicate fields (including __dupN versions) into canonical ones ----
    def _cols_named(base: str):
        cols = []
        for c in df.columns:
            if re.sub(r"__dup\d+$", "", str(c)) == base:
                cols.append(c)
        return cols

    def _coalesce_into(base: str, fallbacks: list[str]):
        # base may exist multiple times due to __dupN; coalesce all of them left->right
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

        # start with first candidate, fill blanks from others
        s = df[ordered[0]].astype(str)
        for c in ordered[1:]:
            t = df[c].astype(str)
            s = s.where(s.str.strip() != "", t)

        # write into canonical base column (exact name)
        df[base] = s

    # grading fee: grading_fee_initial <- grading_fee_per_card
    _coalesce_into("grading_fee_initial", ["grading_fee_per_card"])

    # additional costs: additional_costs <- extra_costs
    _coalesce_into("additional_costs", ["extra_costs"])

    # received grade: received_grade <- returned_grade
    _coalesce_into("received_grade", ["returned_grade"])

    # Numeric parse
    for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
        df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

    # Ensure IDs are strings
    df["grading_row_id"] = df["grading_row_id"].astype(str)
    df["submission_id"] = df["submission_id"].astype(str)

    # status default
    df["status"] = df["status"].astype(str).replace("", "SUBMITTED")

    return df

def refresh_all():
    load_inventory_df.clear()
    load_grading_df.clear()
    st.rerun()

# =========================================================
# WRITES
# =========================================================

def append_grading_rows(rows: list[dict]):
    if not rows:
        return
    ws = get_ws(GRADING_WS_NAME)
    headers = ws.row_values(1)
    if not headers:
        headers = ensure_headers(ws, GRADING_CANON_COLS)
    else:
        headers = ensure_headers(ws, GRADING_CANON_COLS)  # also repairs duplicates

    for row in rows:
        ws.append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")

def update_grading_rows(df_rows: pd.DataFrame):
    if df_rows.empty:
        return
    ws = get_ws(GRADING_WS_NAME)
    headers = ws.row_values(1)
    if not headers:
        headers = ensure_headers(ws, GRADING_CANON_COLS)
    else:
        headers = ensure_headers(ws, GRADING_CANON_COLS)  # also repairs duplicates

    # locate grading_row_id in column (by base-name match)
    id_header = None
    for h in headers:
        if re.sub(r"__dup\d+$", "", h) == "grading_row_id":
            id_header = h
            break
    if not id_header:
        raise ValueError("grading_row_id must exist in grading sheet header row.")

    id_col = headers.index(id_header) + 1
    id_vals = ws.col_values(id_col)

    id_to_rownum = {}
    for idx, val in enumerate(id_vals[1:], start=2):
        if val:
            id_to_rownum[str(val).strip()] = idx

    last_col = a1_col_letter(len(headers))

    for _, r in df_rows.iterrows():
        rid = str(r.get("grading_row_id", "")).strip()
        rownum = id_to_rownum.get(rid)
        if not rownum:
            continue

        values = []
        for h in headers:
            base = re.sub(r"__dup\d+$", "", h)
            v = r.get(base, "")  # always pull from canonical base name
            if pd.isna(v):
                v = ""
            values.append(v)

        ws.update(f"A{rownum}:{last_col}{rownum}", [values], value_input_option="USER_ENTERED")

def update_inventory_status(inventory_id: str, new_status: str):
    inv_ws = get_ws(INVENTORY_WS_NAME)
    headers = inv_ws.row_values(1)
    if not headers:
        return

    # repair header drift (strip/dupes) without changing UI
    _ = ensure_headers(inv_ws, ["inventory_id", "inventory_status"])

    headers = inv_ws.row_values(1)
    # find inventory_id header by base-name
    id_header = None
    for h in headers:
        if re.sub(r"__dup\d+$", "", h) == "inventory_id":
            id_header = h
            break
    if not id_header:
        return

    id_col = headers.index(id_header) + 1
    ids = inv_ws.col_values(id_col)

    rownum = None
    for i, v in enumerate(ids[1:], start=2):
        if str(v).strip() == str(inventory_id).strip():
            rownum = i
            break
    if not rownum:
        return

    status_header = None
    for h in headers:
        if re.sub(r"__dup\d+$", "", h) == "inventory_status":
            status_header = h
            break
    if status_header:
        c = headers.index(status_header) + 1
        inv_ws.update(
            f"{a1_col_letter(c)}{rownum}",
            [[new_status]],
            value_input_option="USER_ENTERED",
        )


def mark_inventory_as_graded(inventory_id: str, grading_company: str, grade: str):
    inv_ws = get_ws(INVENTORY_WS_NAME)
    headers = inv_ws.row_values(1)
    if not headers:
        return

    # repair header drift
    _ = ensure_headers(inv_ws, ["inventory_id", "product_type", "grading_company", "grade"])

    headers = inv_ws.row_values(1)

    id_header = None
    for h in headers:
        if re.sub(r"__dup\d+$", "", h) == "inventory_id":
            id_header = h
            break
    if not id_header:
        return

    id_col = headers.index(id_header) + 1
    ids = inv_ws.col_values(id_col)

    rownum = None
    for i, v in enumerate(ids[1:], start=2):
        if str(v).strip() == str(inventory_id).strip():
            rownum = i
            break
    if not rownum:
        return

    def _set(base_name: str, value):
        target = None
        for h in headers:
            if re.sub(r"__dup\d+$", "", h) == base_name:
                target = h
                break
        if not target:
            return
        c = headers.index(target) + 1
        inv_ws.update(f"{a1_col_letter(c)}{rownum}", value, value_input_option="USER_ENTERED")

    _set("product_type", "Graded Card")
    _set("grading_company", grading_company)
    _set("grade", grade)

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
            prices = fetch_pricecharting_psa_prices(link)
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
                    prices = fetch_pricecharting_psa_prices(link)
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

            st.caption("You can (1) add additional costs, or (2) mark returned date/grade.")
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

            edited = st.data_editor(show, use_container_width=True, hide_index=True, num_rows="fixed")

            c1, c2 = st.columns([1, 1])
            save = c1.button("Save updates", type="primary", use_container_width=True)
            mark_returned = c2.button("Mark rows with a grade/date as RETURNED", use_container_width=True)

            if save or mark_returned:
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

                    if mark_returned:
                        if not is_blank(updated.at[idx, "returned_date"]) or not is_blank(updated.at[idx, "received_grade"]):
                            updated.at[idx, "status"] = "RETURNED"

                    updated.at[idx, "updated_at"] = datetime.utcnow().isoformat()

                    if str(updated.at[idx, "status"]).upper() == "RETURNED":
                        inv_id = safe_str(updated.at[idx, "inventory_id"])
                        update_inventory_status(inv_id, STATUS_ACTIVE)
                        mark_inventory_as_graded(inv_id, safe_str(updated.at[idx, "grading_company"]), safe_str(updated.at[idx, "received_grade"]))

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
                prices = fetch_pricecharting_psa_prices(safe_str(r["reference_link"]))
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
