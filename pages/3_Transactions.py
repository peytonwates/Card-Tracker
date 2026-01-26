# pages/3_Transactions.py
# ---------------------------------------------------------
# Transactions page (Google Sheets-backed)
# Supports:
# - Create Listing (Auction / Buy It Now)  -> inventory marked LISTED
# - Trade In (no listing stage)            -> inventory marked SOLD immediately
# - Mark Sold for open listings            -> inventory marked SOLD
# - Delete/Cancel listing                  -> inventory returns to ACTIVE (transaction kept as CANCELLED)
# - Rate-limit safe reads (cache + single-read + backoff)
#
# Alignments/Fixes vs latest Inventory + Dashboard + Grading:
# - Robust header normalization (prevents "Product Type" vs product_type duplicates)
# - card_type normalized to ONLY Pokemon/Sports (never "Other")
# - Writes fees_total so Dashboard net (sold_price - fees_total) matches Transactions net
# ---------------------------------------------------------

import json
import re
import time
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# CONFIG
# =========================================================

st.set_page_config(page_title="Transactions", layout="wide")

INVENTORY_WS_DEFAULT = "inventory"
TRANSACTIONS_WS_DEFAULT = "transactions"
GRADING_WS_DEFAULT = "grading"

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"

TX_STATUS_LISTED = "LISTED"
TX_STATUS_SOLD = "SOLD"
TX_STATUS_CANCELLED = "CANCELLED"

TX_TYPES = ["Auction", "Buy It Now", "Trade In"]

# --- Inventory columns (internal canonical names) ---
INV_COLUMNS = [
    "inventory_id",
    "product_type",
    "sealed_product_type",
    "card_type",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "reference_link",
    "image_url",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "condition",
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
    # Grading + market
    "grading_company",
    "grade",
    "market_price",
    "market_price_updated_at",
]

# --- Transactions columns (internal canonical names) ---
TX_COLUMNS = [
    "transaction_id",
    "inventory_id",
    "transaction_type",     # Auction / Buy It Now / Trade In
    "platform",             # eBay / Whatnot / LCS / Trade-in shop / etc.

    "list_date",
    "list_price",

    "sold_date",
    "sold_price",

    # Keep your inputs:
    "fees",                 # platform fees (input)
    "shipping_charged",     # shipping collected from buyer (input)

    # Add for Dashboard alignment:
    "fees_total",           # = fees - shipping_charged (so Dashboard net matches)

    "net_proceeds",
    "profit",

    "notes",
    "status",               # LISTED / SOLD / CANCELLED
    "created_at",
    "updated_at",

    # Snapshot fields from inventory at time of listing/sale
    "product_type",
    "sealed_product_type",
    "card_type",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "reference_link",
    "image_url",
    "purchase_date",
    "purchased_from",

    # Cost basis snapshots
    "purchase_total",       # purchase all-in (tax+ship included)
    "grading_fee_total",    # pulled from grading sheet (best available)
    "all_in_cost",          # purchase_total + grading_fee_total

    # Grading snapshots
    "grading_company",
    "grade",

    # Condition snapshot
    "condition",
]

# --- Grading columns (internal canonical names) ---
GRADING_COLUMNS = [
    "grading_id",
    "inventory_id",
    "grading_company",
    "grading_fee_initial",
    "grading_fee_per_card",
    "additional_costs",
    "extra_costs",
    "total_grading_cost",
    "status",
    "estimated_return_date",
    "returned_date",
    "received_grade",
    "returned_grade",
    "updated_at",
    "updated_at_utc",
    "created_at",
]

NUMERIC_INV = ["purchase_price", "shipping", "tax", "total_price", "market_price"]
NUMERIC_TX = [
    "list_price", "sold_price",
    "fees", "shipping_charged", "fees_total",
    "net_proceeds", "profit",
    "purchase_total", "grading_fee_total", "all_in_cost",
]
NUMERIC_GR = ["grading_fee_initial", "grading_fee_per_card", "additional_costs", "extra_costs", "total_grading_cost"]


# =========================================================
# HEADER ALIASES (prevents duplicates / schema drift)
# =========================================================

HEADER_ALIASES = {
    # Inventory
    "inventory_id": ["inventory_id", "Inventory ID"],
    "product_type": ["product_type", "Product Type"],
    "sealed_product_type": ["sealed_product_type", "Sealed Product Type"],
    "image_url": ["image_url", "Image URL", "image", "Image"],
    "inventory_status": ["inventory_status", "Status", "inventoryStatus"],
    "listed_transaction_id": ["listed_transaction_id", "Listed Transaction ID"],

    # Inventory grading/market
    "grading_company": ["grading_company", "Grading Company", "grading company", "company"],
    "grade": ["grade", "Grade", "graded", "received_grade", "returned_grade"],
    "market_price": ["market_price", "Market Price", "Market price", "market price"],
    "market_price_updated_at": ["market_price_updated_at", "Market Price Updated At", "Market Price Update", "market_price_updated_at_utc"],

    # Transactions (old/new)
    "transaction_id": ["transaction_id", "Transaction ID"],
    "transaction_type": ["transaction_type", "Transaction Type", "listing_type"],
    "status": ["status", "tx_status", "TX Status"],

    # keep inputs
    "fees": ["fees", "platform_fees", "fee", "Fees"],
    "shipping_charged": ["shipping_charged", "Shipping Charged"],

    # IMPORTANT: dashboard prefers fees_total if present
    "fees_total": ["fees_total", "fees_total_calc", "Fees Total", "fees_total_dashboard"],

    "profit": ["profit", "profit_loss", "Profit", "Profit/Loss"],
    "net_proceeds": ["net_proceeds", "Net Proceeds"],
    "purchase_total": ["purchase_total", "cost_basis", "Cost Basis", "purchase_total_allin"],

    "grading_fee_total": ["grading_fee_total", "Grading Fee", "grading_fee", "grading_fee_per_card", "total_grading_cost"],
    "all_in_cost": ["all_in_cost", "All In Cost", "all_in"],
    "condition": ["condition", "Condition"],

    # Grading sheet duplicates
    "grading_id": ["grading_id", "submission_id", "Grading ID", "Submission ID"],
    "additional_costs": ["additional_costs", "extra_costs", "Additional Costs", "Extra Costs"],
    "received_grade": ["received_grade", "returned_grade", "Grade", "Returned Grade", "Received Grade"],
    "returned_grade": ["returned_grade", "received_grade", "Returned Grade", "Received Grade"],
    "updated_at": ["updated_at", "updated_at_utc"],
}


def _norm_header(s: str) -> str:
    """
    Normalize header strings so:
      "Product Type" == "product_type" == "product type"
    """
    s = str(s or "").strip().lower()
    s = re.sub(r"\s+", "_", s)
    return s


def sheet_header_to_internal(h: str) -> str:
    h_raw = str(h or "").strip()
    h_norm = _norm_header(h_raw)
    for internal, aliases in HEADER_ALIASES.items():
        for a in aliases:
            if _norm_header(a) == h_norm:
                return internal
    # fall back to normalized raw header (keeps stable if you add new fields)
    return _norm_header(h_raw) if h_norm else h_raw


def internal_to_sheet_header(internal: str, existing_headers: list[str]) -> str:
    """
    Prefer an existing alias header if present to avoid creating duplicates.
    """
    aliases = HEADER_ALIASES.get(internal, [internal])

    # Prefer exact existing match by normalized comparison
    existing_norm = {_norm_header(x): x for x in existing_headers}
    for a in aliases:
        if _norm_header(a) in existing_norm:
            return existing_norm[_norm_header(a)]

    # reasonable defaults
    defaults = {
        "product_type": "Product Type",
        "sealed_product_type": "Sealed Product Type",
        "image_url": "Image URL",
        "grading_company": "Grading Company",
        "grade": "Grade",
        "market_price": "Market Price",
        "market_price_updated_at": "Market Price Updated At",
        "grading_fee_total": "Grading Fee",
        "all_in_cost": "All In Cost",
        "fees_total": "Fees Total",
    }
    return defaults.get(internal, internal)


# =========================================================
# GOOGLE SHEETS CLIENT + SAFE READS
# =========================================================

def _is_quota_429(e: Exception) -> bool:
    try:
        return isinstance(e, gspread.exceptions.APIError) and getattr(e, "response", None) and e.response.status_code == 429
    except Exception:
        return False


def _with_backoff(fn, tries: int = 6, base_sleep: float = 0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if _is_quota_429(e):
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last


@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Cloud: TOML table
    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Cloud: JSON string
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_json_str = st.secrets["gcp_service_account"]
        sa_info = json.loads(sa_json_str)
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: JSON file path
    if "service_account_json_path" in st.secrets:
        sa_rel = st.secrets["service_account_json_path"]
        sa_path = Path(sa_rel)
        if not sa_path.is_absolute():
            sa_path = Path.cwd() / sa_rel
        if not sa_path.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {sa_path}")
        sa_info = json.loads(sa_path.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets: add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')


@st.cache_resource
def _get_spreadsheet(spreadsheet_id: str):
    client = get_gspread_client()
    return _with_backoff(lambda: client.open_by_key(spreadsheet_id))


@st.cache_resource
def _get_ws(spreadsheet_id: str, worksheet_name: str):
    sh = _get_spreadsheet(spreadsheet_id)
    return _with_backoff(lambda: sh.worksheet(worksheet_name))



def _ensure_headers(ws, internal_headers: list[str]) -> list[str]:
    """
    Quota-safer header ensure:
    - single read via get_all_values() (instead of row_values(1))
    - only updates header row if missing columns exist
    """
    values = _with_backoff(lambda: ws.get_all_values())
    first_row = values[0] if values else []

    # Sheet is empty -> write header row once
    if not first_row:
        sheet_headers = [internal_to_sheet_header(h, []) for h in internal_headers]
        _with_backoff(lambda: ws.update("1:1", [sheet_headers], value_input_option="USER_ENTERED"))
        return sheet_headers

    existing_sheet_headers = first_row
    existing_internal = [sheet_header_to_internal(h) for h in existing_sheet_headers]
    existing_internal_set = set(existing_internal)

    missing_internal = [h for h in internal_headers if h not in existing_internal_set]
    if missing_internal:
        additions = [internal_to_sheet_header(h, existing_sheet_headers) for h in missing_internal]
        new_headers = existing_sheet_headers + additions
        _with_backoff(lambda: ws.update("1:1", [new_headers], value_input_option="USER_ENTERED"))
        return new_headers

    return existing_sheet_headers



@st.cache_data(ttl=45, show_spinner=False)
def _read_sheet_values_cached(spreadsheet_id: str, worksheet_name: str) -> list[list[str]]:
    ws = _get_ws(spreadsheet_id, worksheet_name)
    return _with_backoff(lambda: ws.get_all_values())


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    If df has duplicate column names (after alias normalization),
    collapse them into a single column by taking first non-empty value per row.
    """
    if df.columns.duplicated().any():
        new = pd.DataFrame(index=df.index)
        for col in pd.unique(df.columns):
            cols = df.loc[:, df.columns == col]
            if cols.shape[1] == 1:
                new[col] = cols.iloc[:, 0]
            else:
                stacked = cols.astype(str).replace("nan", "").replace("None", "")
                new[col] = stacked.apply(lambda r: next((v for v in r.tolist() if str(v).strip() != ""), ""), axis=1)
        return new
    return df


def _sheet_to_df(values: list[list[str]], internal_cols: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """
    values includes header row.
    Returns (df_internal, sheet_headers)
    """
    if not values:
        return pd.DataFrame(columns=internal_cols), []

    sheet_headers = values[0]
    rows = values[1:] if len(values) > 1 else []

    df = pd.DataFrame(rows, columns=sheet_headers)

    # Normalize to internal names (may cause duplicates)
    df = df.rename(columns={h: sheet_header_to_internal(h) for h in df.columns})

    # Coalesce duplicates created by alias normalization
    df = _coalesce_duplicate_columns(df)

    # Ensure all internal cols exist
    for c in internal_cols:
        if c not in df.columns:
            df[c] = ""

    # Keep only requested internal cols (in order)
    df = df[internal_cols].copy()
    return df, sheet_headers


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def load_inventory_df(force_refresh: bool = False) -> pd.DataFrame:
    ss_key = "inv_df_cache_tx"
    if force_refresh:
        _read_sheet_values_cached.clear()
        st.session_state.pop(ss_key, None)

    if ss_key in st.session_state and isinstance(st.session_state[ss_key], pd.DataFrame):
        return st.session_state[ss_key].copy()

    spreadsheet_id = st.secrets["spreadsheet_id"]
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)

    ws = _get_ws(spreadsheet_id, inv_ws_name)
    _ensure_headers(ws, INV_COLUMNS)

    values = _read_sheet_values_cached(spreadsheet_id, inv_ws_name)
    df, _headers = _sheet_to_df(values, INV_COLUMNS)
    df = _coerce_numeric(df, NUMERIC_INV)

    # Default inventory_status if blank
    if "inventory_status" in df.columns:
        df["inventory_status"] = df["inventory_status"].replace("", STATUS_ACTIVE).fillna(STATUS_ACTIVE)

    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    st.session_state[ss_key] = df.copy()
    return df


def load_transactions_df(force_refresh: bool = False) -> pd.DataFrame:
    ss_key = "tx_df_cache"
    if force_refresh:
        _read_sheet_values_cached.clear()
        st.session_state.pop(ss_key, None)

    if ss_key in st.session_state and isinstance(st.session_state[ss_key], pd.DataFrame):
        return st.session_state[ss_key].copy()

    spreadsheet_id = st.secrets["spreadsheet_id"]
    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

    ws = _get_ws(spreadsheet_id, tx_ws_name)
    _ensure_headers(ws, TX_COLUMNS)

    values = _read_sheet_values_cached(spreadsheet_id, tx_ws_name)
    df, _headers = _sheet_to_df(values, TX_COLUMNS)
    df = _coerce_numeric(df, NUMERIC_TX)

    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()

    st.session_state[ss_key] = df.copy()
    return df


def load_grading_df(force_refresh: bool = False) -> pd.DataFrame:
    ss_key = "gr_df_cache_tx"
    if force_refresh:
        _read_sheet_values_cached.clear()
        st.session_state.pop(ss_key, None)

    if ss_key in st.session_state and isinstance(st.session_state[ss_key], pd.DataFrame):
        return st.session_state[ss_key].copy()

    spreadsheet_id = st.secrets["spreadsheet_id"]
    gr_ws_name = st.secrets.get("grading_worksheet", GRADING_WS_DEFAULT)

    ws = _get_ws(spreadsheet_id, gr_ws_name)
    _ensure_headers(ws, GRADING_COLUMNS)

    values = _read_sheet_values_cached(spreadsheet_id, gr_ws_name)
    df, _headers = _sheet_to_df(values, GRADING_COLUMNS)
    df = _coerce_numeric(df, NUMERIC_GR)

    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()

    # prefer updated_at_utc then updated_at for ordering
    sort_col = "updated_at_utc" if "updated_at_utc" in df.columns else "updated_at"
    if sort_col in df.columns:
        df[sort_col] = pd.to_datetime(df[sort_col], errors="coerce")

    st.session_state[ss_key] = df.copy()
    return df


# =========================================================
# NORMALIZATIONS (match Dashboard behavior)
# =========================================================

def _safe_str(x) -> str:
    if x is None:
        return ""
    return str(x)


def _normalize_card_type(val: str) -> str:
    """
    ONLY Pokemon or Sports. Never show 'Other'.
    Default unknown/blank to Pokemon (matches Dashboard fix).
    """
    s = _safe_str(val).strip().lower()
    if s == "sports" or "sport" in s:
        return "Sports"
    if s == "pokemon" or "pok" in s:
        return "Pokemon"
    return "Pokemon"


# =========================================================
# GRADING LOOKUPS
# =========================================================

def _best_grade_from_grading_row(gr: pd.Series) -> str:
    for k in ["received_grade", "returned_grade"]:
        v = str(gr.get(k, "")).strip()
        if v and v.lower() not in ["nan", "none"]:
            return v
    return ""


def _best_fee_from_grading_row(gr: pd.Series) -> float:
    base = 0.0
    for k in ["total_grading_cost", "grading_fee_per_card", "grading_fee_initial"]:
        try:
            v = float(gr.get(k, 0.0) or 0.0)
            if v > 0:
                base = v
                break
        except Exception:
            continue

    add = 0.0
    for k in ["additional_costs", "extra_costs"]:
        try:
            add += float(gr.get(k, 0.0) or 0.0)
        except Exception:
            pass

    return float(round(base + add, 2))


def _lookup_grading_for_inventory(gr_df: pd.DataFrame, inventory_id: str) -> dict:
    inv_id = str(inventory_id).strip()
    if gr_df.empty or not inv_id:
        return {"grading_company": "", "grade": "", "grading_fee_total": 0.0}

    sub = gr_df[gr_df["inventory_id"].astype(str).str.strip() == inv_id].copy()
    if sub.empty:
        return {"grading_company": "", "grade": "", "grading_fee_total": 0.0}

    returned = sub[sub["status"].astype(str).str.upper().isin(["RETURNED", "COMPLETED", "DONE"])]
    if not returned.empty:
        pick = returned.sort_values(
            by=[c for c in ["updated_at_utc", "updated_at", "created_at"] if c in returned.columns],
            ascending=False
        ).iloc[0]
    else:
        pick = sub.sort_values(
            by=[c for c in ["updated_at_utc", "updated_at", "created_at"] if c in sub.columns],
            ascending=False
        ).iloc[0]

    company = str(pick.get("grading_company", "")).strip()
    grade = _best_grade_from_grading_row(pick)
    fee = _best_fee_from_grading_row(pick)
    return {"grading_company": company, "grade": grade, "grading_fee_total": float(fee)}


# =========================================================
# SHEET WRITE HELPERS
# =========================================================

def _find_rownum_by_id(values: list[list[str]], id_col_index_1based: int, ids: list[str]) -> dict[str, int]:
    mapping = {}
    for i, row in enumerate(values[1:], start=2):
        val = row[id_col_index_1based - 1] if len(row) >= id_col_index_1based else ""
        if val:
            mapping[str(val).strip()] = i
    return {str(_id).strip(): mapping.get(str(_id).strip()) for _id in ids}


def _append_row(ws, sheet_headers: list[str], row_internal: dict):
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}
    ordered = []
    for sheet_h in sheet_headers:
        internal = header_to_internal.get(sheet_h, sheet_h)
        v = row_internal.get(internal, "")
        if isinstance(v, (pd.Series, pd.DataFrame)):
            v = ""
        ordered.append(v)
    _with_backoff(lambda: ws.append_row(ordered, value_input_option="USER_ENTERED"))


def _update_row(ws, sheet_headers: list[str], rownum: int, row_internal: dict):
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}
    values = []
    for sheet_h in sheet_headers:
        internal = header_to_internal.get(sheet_h, sheet_h)
        v = row_internal.get(internal, "")
        if isinstance(v, (pd.Series, pd.DataFrame)):
            v = ""
        values.append(v)

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(sheet_headers)).split("1")[0]
    rng = f"A{rownum}:{last_col_letter}{rownum}"
    _with_backoff(lambda: ws.update(rng, [values], value_input_option="USER_ENTERED"))


def _compute_net_and_profit(all_in_cost: float, sold_price: float, fees: float, shipping_charged: float) -> tuple[float, float]:
    """
    Transactions page definition (kept):
      net_proceeds = sold_price - fees + shipping_charged
      profit      = net_proceeds - all_in_cost

    Dashboard definition (current):
      net = sold_price - fees_total

    To align:
      fees_total = fees - shipping_charged  => sold_price - fees_total == sold_price - fees + shipping_charged
    """
    sold_price = float(sold_price or 0.0)
    fees = float(fees or 0.0)
    shipping_charged = float(shipping_charged or 0.0)
    all_in_cost = float(all_in_cost or 0.0)

    net = round(sold_price - fees + shipping_charged, 2)
    profit = round(net - all_in_cost, 2)
    return net, profit


def _compute_fees_total_for_dashboard(fees: float, shipping_charged: float) -> float:
    fees = float(fees or 0.0)
    shipping_charged = float(shipping_charged or 0.0)
    return float(round(fees - shipping_charged, 2))


# =========================================================
# IMAGE SCRAPE (OG:IMAGE)
# =========================================================

@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def scrape_image_url(reference_link: str) -> str:
    if not reference_link:
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(reference_link, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            return og["content"].strip()

        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"].strip()
            if src.startswith("//"):
                src = "https:" + src
            if src.startswith("/"):
                parsed = urlparse(reference_link)
                src = f"{parsed.scheme}://{parsed.netloc}{src}"
            return src
    except Exception:
        return ""
    return ""


# =========================================================
# UI HELPERS (UI unchanged)
# =========================================================

def _money(x) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""


def _inv_label(r: pd.Series) -> str:
    name = str(r.get("card_name", "")).strip()
    set_name = str(r.get("set_name", "")).strip()
    year = str(r.get("year", "")).strip()
    inv_id = str(r.get("inventory_id", "")).strip()

    parts = [inv_id]
    if year:
        parts.append(year)
    if set_name:
        parts.append(set_name)
    if name:
        parts.append(name)
    return " — ".join(parts)


def _tx_label(r: pd.Series) -> str:
    tx_id = str(r.get("transaction_id", "")).strip()
    inv_id = str(r.get("inventory_id", "")).strip()
    name = str(r.get("card_name", "")).strip()
    set_name = str(r.get("set_name", "")).strip()
    ttype = str(r.get("transaction_type", "")).strip()
    return f"{tx_id} — {ttype} — {inv_id} — {set_name} — {name}"


# =========================================================
# PAGE
# =========================================================

st.title("Transactions")

top = st.columns([2, 1])
with top[1]:
    refresh = st.button("🔄 Refresh from Sheets", use_container_width=True)

inv_df = load_inventory_df(force_refresh=refresh)
tx_df = load_transactions_df(force_refresh=refresh)
gr_df = load_grading_df(force_refresh=refresh)

tab_create, tab_update, tab_history = st.tabs(
    ["Create Listing / Trade In", "Mark Sold / Update Listing", "Transactions History"]
)


# =========================================================
# TAB 1: CREATE LISTING / TRADE IN
# =========================================================
with tab_create:
    st.subheader("Create a Listing (Auction/Buy It Now) or record a Trade In")

    inv_available = inv_df[inv_df["inventory_status"].isin([STATUS_ACTIVE])].copy()

    if inv_available.empty:
        st.info("No ACTIVE inventory available. Add inventory or cancel a listing to make it ACTIVE again.")
    else:
        inv_available["__label"] = inv_available.apply(_inv_label, axis=1)

        col_left, col_right = st.columns([1, 3])

        with col_right:
            selected_label = st.selectbox(
                "Select item from Inventory (ACTIVE only)",
                options=inv_available["__label"].tolist(),
                index=0,
            )

        selected_row = inv_available[inv_available["__label"] == selected_label].iloc[0]
        inv_id = str(selected_row.get("inventory_id", "")).strip()

        # pull grading info (fee/company/grade) from grading sheet
        gr_info = _lookup_grading_for_inventory(gr_df, inv_id)

        # Image + quick identification info
        with col_left:
            img_url = str(selected_row.get("image_url", "")).strip()
            if not img_url:
                img_url = scrape_image_url(str(selected_row.get("reference_link", "")).strip())
            if img_url:
                st.image(img_url, use_container_width=True)
            else:
                st.caption("No image found.")

        st.markdown("#### Purchase details")
        p1, p2, p3 = st.columns(3)
        p1.write(f"**Purchase date:** {selected_row.get('purchase_date', '')}")
        p2.write(f"**Purchased from:** {selected_row.get('purchased_from', '')}")
        p3.write(f"**Total cost:** {_money(selected_row.get('total_price', 0.0))}")

        st.markdown("---")

        # Transaction form (UI unchanged)
        with st.form("create_tx_form", clear_on_submit=False):
            c1, c2, c3 = st.columns([1.2, 1.2, 2.0])

            with c1:
                tx_type = st.selectbox("Transaction type*", TX_TYPES, index=0)

            with c2:
                platform = st.text_input(
                    "Platform*",
                    value="eBay" if tx_type in ["Auction", "Buy It Now"] else "Trade In",
                    placeholder="eBay, Whatnot, LCS, Trade-in shop, etc.",
                )

            with c3:
                notes = st.text_input("Notes (optional)", value="", placeholder="Anything helpful...")

            if tx_type in ["Auction", "Buy It Now"]:
                l1, l2, l3 = st.columns(3)
                with l1:
                    list_date = st.date_input("List date*", value=date.today())
                with l2:
                    list_price = st.number_input("List price*", min_value=0.0, step=1.0, format="%.2f")
                with l3:
                    st.caption("Sale fields get filled in later under 'Mark Sold / Update Listing'.")

                sold_date = ""
                sold_price = 0.0
                fees = 0.0
                shipping_charged = 0.0

            else:
                l1, l2, l3, l4 = st.columns([1.2, 1.2, 1.2, 1.2])
                with l1:
                    sold_date = st.date_input("Trade date*", value=date.today())
                with l2:
                    sold_price = st.number_input("Trade-in amount (sold price)*", min_value=0.0, step=1.0, format="%.2f")
                with l3:
                    fees = st.number_input("Fees (optional)", min_value=0.0, step=1.0, format="%.2f")
                with l4:
                    shipping_charged = st.number_input("Shipping charged (optional)", min_value=0.0, step=1.0, format="%.2f")

                list_date = ""
                list_price = 0.0

            submit = st.form_submit_button("Save", type="primary", use_container_width=True)

        if submit:
            if not platform.strip():
                st.error("Platform is required.")
            else:
                spreadsheet_id = st.secrets["spreadsheet_id"]
                inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
                tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

                inv_ws = _get_ws(spreadsheet_id, inv_ws_name)
                tx_ws = _get_ws(spreadsheet_id, tx_ws_name)

                inv_sheet_headers = _ensure_headers(inv_ws, INV_COLUMNS)
                tx_sheet_headers = _ensure_headers(tx_ws, TX_COLUMNS)

                inv_values = _with_backoff(lambda: inv_ws.get_all_values())
                tx_values = _with_backoff(lambda: tx_ws.get_all_values())

                inv_header = inv_values[0] if inv_values else inv_sheet_headers
                inv_id_col_idx = next((i for i, h in enumerate(inv_header, start=1) if sheet_header_to_internal(h) == "inventory_id"), None)
                if inv_id_col_idx is None:
                    st.error("Could not find 'inventory_id' column in the inventory sheet header.")
                    st.stop()

                # Ensure inventory has an image_url (optional write back)
                img_url_final = str(selected_row.get("image_url", "")).strip()
                if not img_url_final:
                    img_url_final = scrape_image_url(str(selected_row.get("reference_link", "")).strip())

                purchase_total = float(selected_row.get("total_price", 0.0) or 0.0)
                grading_fee_total = float(gr_info.get("grading_fee_total", 0.0) or 0.0)
                all_in_cost = float(round(purchase_total + grading_fee_total, 2))

                tx_id = str(uuid.uuid4())
                now_iso = pd.Timestamp.utcnow().isoformat()

                # Align card_type to Dashboard (Pokemon/Sports only)
                card_type_norm = _normalize_card_type(selected_row.get("card_type", ""))

                if tx_type == "Trade In":
                    net, profit = _compute_net_and_profit(
                        all_in_cost=all_in_cost,
                        sold_price=sold_price,
                        fees=fees,
                        shipping_charged=shipping_charged,
                    )
                    fees_total = _compute_fees_total_for_dashboard(fees, shipping_charged)
                    tx_status = TX_STATUS_SOLD
                else:
                    net, profit = 0.0, 0.0
                    fees_total = 0.0
                    tx_status = TX_STATUS_LISTED

                tx_row = {
                    "transaction_id": tx_id,
                    "inventory_id": inv_id,
                    "transaction_type": tx_type,
                    "platform": platform.strip(),
                    "list_date": str(list_date) if list_date else "",
                    "list_price": float(list_price or 0.0),
                    "sold_date": str(sold_date) if sold_date else "",
                    "sold_price": float(sold_price or 0.0),
                    "fees": float(fees or 0.0),
                    "shipping_charged": float(shipping_charged or 0.0),
                    "fees_total": float(fees_total),
                    "net_proceeds": float(net),
                    "profit": float(profit),
                    "notes": notes.strip(),
                    "status": tx_status,
                    "created_at": now_iso,
                    "updated_at": now_iso,

                    # snapshots
                    "product_type": str(selected_row.get("product_type", "")).strip(),
                    "sealed_product_type": str(selected_row.get("sealed_product_type", "")).strip(),
                    "card_type": card_type_norm,
                    "brand_or_league": str(selected_row.get("brand_or_league", "")).strip(),
                    "set_name": str(selected_row.get("set_name", "")).strip(),
                    "year": str(selected_row.get("year", "")).strip(),
                    "card_name": str(selected_row.get("card_name", "")).strip(),
                    "card_number": str(selected_row.get("card_number", "")).strip(),
                    "variant": str(selected_row.get("variant", "")).strip(),
                    "card_subtype": str(selected_row.get("card_subtype", "")).strip(),
                    "reference_link": str(selected_row.get("reference_link", "")).strip(),
                    "image_url": img_url_final,
                    "purchase_date": str(selected_row.get("purchase_date", "")).strip(),
                    "purchased_from": str(selected_row.get("purchased_from", "")).strip(),

                    # cost basis snapshots
                    "purchase_total": float(purchase_total),
                    "grading_fee_total": float(grading_fee_total),
                    "all_in_cost": float(all_in_cost),

                    # grading snapshots
                    "grading_company": str(gr_info.get("grading_company", "")).strip(),
                    "grade": str(gr_info.get("grade", "")).strip(),

                    # condition snapshot
                    "condition": str(selected_row.get("condition", "")).strip(),
                }

                _append_row(tx_ws, tx_sheet_headers, tx_row)

                # Update inventory status + listed_transaction_id (and image_url if missing)
                inv_rownum = _find_rownum_by_id(inv_values, inv_id_col_idx, [inv_id]).get(inv_id)
                if not inv_rownum:
                    st.warning("Transaction saved, but could not locate inventory row to update status.")
                else:
                    row_vals = inv_values[inv_rownum - 1] if len(inv_values) >= inv_rownum else []
                    if len(row_vals) < len(inv_header):
                        row_vals = row_vals + [""] * (len(inv_header) - len(row_vals))
                    row_dict_sheet = dict(zip(inv_header, row_vals))
                    row_internal = {sheet_header_to_internal(k): v for k, v in row_dict_sheet.items()}
                    row_internal = _coalesce_duplicate_columns(pd.DataFrame([row_internal])).iloc[0].to_dict()

                    if tx_type == "Trade In":
                        row_internal["inventory_status"] = STATUS_SOLD
                    else:
                        row_internal["inventory_status"] = STATUS_LISTED

                    row_internal["listed_transaction_id"] = tx_id

                    if "image_url" in row_internal and (not str(row_internal.get("image_url", "")).strip()):
                        row_internal["image_url"] = img_url_final

                    _update_row(inv_ws, inv_sheet_headers, inv_rownum, row_internal)

                # clear caches
                st.session_state.pop("inv_df_cache_tx", None)
                st.session_state.pop("tx_df_cache", None)
                st.session_state.pop("gr_df_cache_tx", None)
                _read_sheet_values_cached.clear()

                if tx_type == "Trade In":
                    st.success("Trade In recorded and inventory marked SOLD.")
                else:
                    st.success("Listing created and inventory marked LISTED.")

                # IMPORTANT: tx_df was loaded earlier in this run.
                # Force a rerun so Tab 2 reloads the fresh transactions sheet.
                st.rerun()



# =========================================================
# TAB 2: MARK SOLD / UPDATE LISTING
# =========================================================
with tab_update:
    st.subheader("Mark Sold / Update Listing (or Cancel Listing)")

    open_listings = tx_df[tx_df["status"].isin([TX_STATUS_LISTED])].copy()
    open_listings = open_listings[open_listings["transaction_type"].isin(["Auction", "Buy It Now"])]

    if open_listings.empty:
        st.info("No open listings found.")
    else:
        open_listings["__label"] = open_listings.apply(_tx_label, axis=1)
        chosen = st.selectbox("Select an open listing", open_listings["__label"].tolist(), index=0)
        tx_row = open_listings[open_listings["__label"] == chosen].iloc[0]

        left, right = st.columns([1, 3])

        with left:
            img = str(tx_row.get("image_url", "")).strip()
            if not img:
                img = scrape_image_url(str(tx_row.get("reference_link", "")).strip())
            if img:
                st.image(img, use_container_width=True)
            else:
                st.caption("No image found.")

        with right:
            st.markdown("#### Item + purchase info")
            c1, c2, c3 = st.columns(3)
            c1.write(f"**Purchase date:** {tx_row.get('purchase_date', '')}")
            c2.write(f"**Purchased from:** {tx_row.get('purchased_from', '')}")
            c3.write(f"**All-in cost:** {_money(tx_row.get('all_in_cost', 0.0) or 0.0)}")

            st.markdown("---")

            with st.form("mark_sold_form", clear_on_submit=False):
                s1, s2, s3, s4 = st.columns(4)
                with s1:
                    sold_date = st.date_input("Sold date*", value=date.today())
                with s2:
                    sold_price = st.number_input("Sold price*", min_value=0.0, step=1.0, format="%.2f")
                with s3:
                    fees = st.number_input("Fees*", min_value=0.0, step=1.0, format="%.2f")
                with s4:
                    shipping_charged = st.number_input("Shipping charged (optional)", min_value=0.0, step=1.0, format="%.2f")

                notes = st.text_input("Notes (optional)", value=str(tx_row.get("notes", "") or ""))

                b1, b2 = st.columns(2)
                mark_btn = b1.form_submit_button("Mark Sold", type="primary", use_container_width=True)
                cancel_btn = b2.form_submit_button("Delete/Cancel Listing", use_container_width=True)

            if mark_btn or cancel_btn:
                spreadsheet_id = st.secrets["spreadsheet_id"]
                inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
                tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

                inv_ws = _get_ws(spreadsheet_id, inv_ws_name)
                tx_ws = _get_ws(spreadsheet_id, tx_ws_name)

                inv_sheet_headers = _ensure_headers(inv_ws, INV_COLUMNS)
                tx_sheet_headers = _ensure_headers(tx_ws, TX_COLUMNS)

                inv_values = _with_backoff(lambda: inv_ws.get_all_values())
                tx_values = _with_backoff(lambda: tx_ws.get_all_values())

                inv_header = inv_values[0] if inv_values else inv_sheet_headers
                tx_header = tx_values[0] if tx_values else tx_sheet_headers

                inv_id_col_idx = next((i for i, h in enumerate(inv_header, start=1) if sheet_header_to_internal(h) == "inventory_id"), None)
                tx_id_col_idx = next((i for i, h in enumerate(tx_header, start=1) if sheet_header_to_internal(h) == "transaction_id"), None)

                if inv_id_col_idx is None or tx_id_col_idx is None:
                    st.error("Could not locate required ID columns in one of the sheets.")
                    st.stop()

                tx_id = str(tx_row["transaction_id"]).strip()
                inv_id = str(tx_row["inventory_id"]).strip()

                tx_rownum = _find_rownum_by_id(tx_values, tx_id_col_idx, [tx_id]).get(tx_id)
                inv_rownum = _find_rownum_by_id(inv_values, inv_id_col_idx, [inv_id]).get(inv_id)

                if not tx_rownum:
                    st.error("Could not locate the transaction row to update.")
                    st.stop()

                tx_vals = tx_values[tx_rownum - 1] if len(tx_values) >= tx_rownum else []
                if len(tx_vals) < len(tx_header):
                    tx_vals = tx_vals + [""] * (len(tx_header) - len(tx_vals))
                tx_sheet_dict = dict(zip(tx_header, tx_vals))
                tx_internal = {sheet_header_to_internal(k): v for k, v in tx_sheet_dict.items()}
                tx_internal = _coalesce_duplicate_columns(pd.DataFrame([tx_internal])).iloc[0].to_dict()

                now_iso = pd.Timestamp.utcnow().isoformat()

                if cancel_btn:
                    tx_internal["status"] = TX_STATUS_CANCELLED
                    tx_internal["updated_at"] = now_iso

                    tx_internal["sold_date"] = ""
                    tx_internal["sold_price"] = 0.0
                    tx_internal["fees"] = 0.0
                    tx_internal["shipping_charged"] = 0.0
                    tx_internal["fees_total"] = 0.0
                    tx_internal["net_proceeds"] = 0.0
                    tx_internal["profit"] = 0.0

                    _update_row(tx_ws, tx_sheet_headers, tx_rownum, tx_internal)

                    if inv_rownum:
                        inv_vals = inv_values[inv_rownum - 1] if len(inv_values) >= inv_rownum else []
                        if len(inv_vals) < len(inv_header):
                            inv_vals = inv_vals + [""] * (len(inv_header) - len(inv_vals))
                        inv_sheet_dict = dict(zip(inv_header, inv_vals))
                        inv_internal = {sheet_header_to_internal(k): v for k, v in inv_sheet_dict.items()}
                        inv_internal = _coalesce_duplicate_columns(pd.DataFrame([inv_internal])).iloc[0].to_dict()

                        inv_internal["inventory_status"] = STATUS_ACTIVE
                        inv_internal["listed_transaction_id"] = ""
                        _update_row(inv_ws, inv_sheet_headers, inv_rownum, inv_internal)

                    st.session_state.pop("inv_df_cache_tx", None)
                    st.session_state.pop("tx_df_cache", None)
                    st.session_state.pop("gr_df_cache_tx", None)
                    _read_sheet_values_cached.clear()
                    st.success("Listing cancelled. Item returned to ACTIVE inventory.")
                    st.rerun()


                if mark_btn:
                    all_in_cost = float(tx_internal.get("all_in_cost", 0.0) or 0.0)
                    if all_in_cost <= 0:
                        all_in_cost = float(tx_internal.get("purchase_total", 0.0) or 0.0)

                    net, profit = _compute_net_and_profit(
                        all_in_cost=all_in_cost,
                        sold_price=sold_price,
                        fees=fees,
                        shipping_charged=shipping_charged,
                    )
                    fees_total = _compute_fees_total_for_dashboard(fees, shipping_charged)

                    tx_internal["sold_date"] = str(sold_date)
                    tx_internal["sold_price"] = float(sold_price)
                    tx_internal["fees"] = float(fees)
                    tx_internal["shipping_charged"] = float(shipping_charged)
                    tx_internal["fees_total"] = float(fees_total)
                    tx_internal["net_proceeds"] = float(net)
                    tx_internal["profit"] = float(profit)
                    tx_internal["status"] = TX_STATUS_SOLD
                    tx_internal["notes"] = notes.strip()
                    tx_internal["updated_at"] = now_iso

                    _update_row(tx_ws, tx_sheet_headers, tx_rownum, tx_internal)

                    if inv_rownum:
                        inv_vals = inv_values[inv_rownum - 1] if len(inv_values) >= inv_rownum else []
                        if len(inv_vals) < len(inv_header):
                            inv_vals = inv_vals + [""] * (len(inv_header) - len(inv_vals))
                        inv_sheet_dict = dict(zip(inv_header, inv_vals))
                        inv_internal = {sheet_header_to_internal(k): v for k, v in inv_sheet_dict.items()}
                        inv_internal = _coalesce_duplicate_columns(pd.DataFrame([inv_internal])).iloc[0].to_dict()

                        inv_internal["inventory_status"] = STATUS_SOLD
                        inv_internal["listed_transaction_id"] = tx_id
                        _update_row(inv_ws, inv_sheet_headers, inv_rownum, inv_internal)

                    st.session_state.pop("inv_df_cache_tx", None)
                    st.session_state.pop("tx_df_cache", None)
                    st.session_state.pop("gr_df_cache_tx", None)
                    _read_sheet_values_cached.clear()
                    st.success("Marked sold. Inventory updated to SOLD.")
                    st.rerun()



# =========================================================
# TAB 3: TRANSACTIONS HISTORY
# =========================================================
with tab_history:
    st.subheader("Transactions History")

    if tx_df.empty:
        st.info("No transactions yet.")
    else:
        f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.2, 2.4])
        with f1:
            type_filter = st.multiselect("Type", sorted(tx_df["transaction_type"].dropna().unique().tolist()), default=[])
        with f2:
            status_filter = st.multiselect("Status", sorted(tx_df["status"].dropna().unique().tolist()), default=[])
        with f3:
            platform_filter = st.multiselect("Platform", sorted(tx_df["platform"].dropna().unique().tolist()), default=[])
        with f4:
            search = st.text_input("Search (name/set/id/platform)", placeholder="Type to filter…")

        hist = tx_df.copy()

        if type_filter:
            hist = hist[hist["transaction_type"].isin(type_filter)]
        if status_filter:
            hist = hist[hist["status"].isin(status_filter)]
        if platform_filter:
            hist = hist[hist["platform"].isin(platform_filter)]
        if search.strip():
            s = search.strip().lower()
            hist = hist[
                hist.apply(
                    lambda r: (
                        s in str(r.get("transaction_id", "")).lower()
                        or s in str(r.get("inventory_id", "")).lower()
                        or s in str(r.get("card_name", "")).lower()
                        or s in str(r.get("set_name", "")).lower()
                        or s in str(r.get("platform", "")).lower()
                    ),
                    axis=1,
                )
            ]

        show_cols = [
            "image_url",
            "transaction_id",
            "status",
            "transaction_type",
            "platform",
            "inventory_id",
            "set_name",
            "card_name",
            "list_date",
            "list_price",
            "sold_date",
            "sold_price",
            "fees",
            "shipping_charged",
            "fees_total",
            "net_proceeds",
            "profit",
            "purchase_total",
            "grading_fee_total",
            "all_in_cost",
            "grading_company",
            "grade",
            "reference_link",
            "notes",
        ]
        for c in show_cols:
            if c not in hist.columns:
                hist[c] = ""

        st.caption(f"Showing {len(hist):,} transaction(s)")

        display = hist[show_cols].copy()
        for c in [
            "list_price", "sold_price", "fees", "shipping_charged", "fees_total",
            "net_proceeds", "profit", "purchase_total", "grading_fee_total", "all_in_cost"
        ]:
            display[c] = display[c].apply(lambda x: _money(x) if str(x).strip() != "" else "")

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "image_url": st.column_config.ImageColumn("Image", width="small"),
                "reference_link": st.column_config.LinkColumn("Link"),
            },
        )

        csv = hist.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download transactions CSV",
            data=csv,
            file_name="transactions.csv",
            mime="text/csv",
            use_container_width=True,
        )
