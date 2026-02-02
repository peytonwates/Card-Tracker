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
# NEW (2026-01): Bulk Listing Creator
# - Select multiple ACTIVE inventory items
# - Edit listing fields in a table (type/platform/notes/list date/list price)
# - Create all listings in one shot (append_rows) + batch update inventory rows
#
# Alignments/Fixes:
# - Robust header normalization (prevents duplicates)
# - card_type normalized to ONLY Pokemon/Sports (never "Other")
# - Writes fees_total so Dashboard net (sold_price - fees_total) matches Transactions net
#
# NEW (2026-02): Listing Market + Profit/Loss Estimate (Create tab)
# - Pull market_price from inventory
# - Estimate profit/loss after fees beside list price
# - If platform is eBay -> require shipping_type (Ebay Envelope / Ground Advantage)
#   * eBay fee: 13.25% of (item + shipping + sales tax)
#   * sales tax assumed: 9%
#   * shipping costs: envelope $1.32, ground advantage $5
# - Non-eBay platforms assume $0 fees/tax/shipping for estimate
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

# =========================================================
# LISTING PROFIT ESTIMATE (assumptions)
# =========================================================
ASSUMED_TAX_RATE = 0.09          # 9%
EBAY_FEE_RATE = 0.1325           # 13.25% of (item + ship + tax)
EBAY_ENV_COST = 1.32
GROUND_ADV_COST = 5.00

EBAY_SHIPPING_TYPES = ["Ebay Envelope", "Ground Advantage"]


def _is_ebay_platform(platform: str) -> bool:
    return "ebay" in str(platform or "").strip().lower()


def _shipping_cost_from_type(shipping_type: str) -> float:
    s = str(shipping_type or "").strip().lower()
    if "envelope" in s:
        return EBAY_ENV_COST
    if "ground" in s:
        return GROUND_ADV_COST
    return 0.0


def _estimate_profit_loss(platform: str, list_price: float, all_in_cost: float, shipping_type: str = "") -> dict:
    """
    Estimate profit/loss at LISTING time using requested assumptions.

    If platform is eBay:
      - shipping charged to buyer assumed = shipping label cost (by shipping_type)
      - tax = 9% of (item + shipping)
      - ebay fee = 13.25% of (item + shipping + tax)
      - net = (item + shipping + tax) - ebay_fee - shipping_cost
      - profit = net - all_in_cost

    Else (non-eBay):
      - fees = 0, tax = 0, shipping_cost = 0
      - net = list_price
      - profit = net - all_in_cost
    """
    lp = float(list_price or 0.0)
    cost = float(all_in_cost or 0.0)

    if lp <= 0:
        return {
            "shipping_cost": 0.0, "shipping_charged": 0.0, "tax": 0.0, "fee": 0.0,
            "total_paid": 0.0, "net": 0.0, "profit": round(-cost, 2) if cost else 0.0
        }

    if _is_ebay_platform(platform):
        ship_cost = _shipping_cost_from_type(shipping_type)
        ship_charged = ship_cost  # assumption: pass-through
        tax = round(ASSUMED_TAX_RATE * (lp + ship_charged), 2)
        total_paid = round(lp + ship_charged + tax, 2)
        fee = round(EBAY_FEE_RATE * total_paid, 2)
        net = round(total_paid - fee - ship_cost, 2)
        profit = round(net - cost, 2)
        return {
            "shipping_cost": ship_cost,
            "shipping_charged": ship_charged,
            "tax": tax,
            "fee": fee,
            "total_paid": total_paid,
            "net": net,
            "profit": profit,
        }

    net = round(lp, 2)
    profit = round(net - cost, 2)
    return {
        "shipping_cost": 0.0,
        "shipping_charged": 0.0,
        "tax": 0.0,
        "fee": 0.0,
        "total_paid": round(lp, 2),
        "net": net,
        "profit": profit,
    }


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
    "inventory_status": ["inventory_status", "Inventory Status", "inventoryStatus"],
    "listed_transaction_id": ["listed_transaction_id", "Listed Transaction ID"],

    # Inventory grading/market
    "grading_company": ["grading_company", "Grading Company", "grading company", "company"],
    "grade": ["grade", "Grade", "graded", "received_grade", "returned_grade"],
    "market_price": ["market_price", "Market Price", "Market price", "market price"],
    "market_price_updated_at": ["market_price_updated_at", "Market Price Updated At", "Market Price Update", "market_price_updated_at_utc"],

    # Transactions (old/new)
    "transaction_id": ["transaction_id", "Transaction ID"],
    "transaction_type": ["transaction_type", "Transaction Type", "listing_type"],
    "status": ["status", "TX Status", "tx_status", "Status"],

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
    return _norm_header(h_raw) if h_norm else h_raw


def internal_to_sheet_header(internal: str, existing_headers: list[str]) -> str:
    aliases = HEADER_ALIASES.get(internal, [internal])

    existing_norm = {_norm_header(x): x for x in existing_headers}
    for a in aliases:
        if _norm_header(a) in existing_norm:
            return existing_norm[_norm_header(a)]

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
        "status": "TX Status",
        "inventory_status": "Inventory Status",
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

    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_json_str = st.secrets["gcp_service_account"]
        sa_info = json.loads(sa_json_str)
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

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
    values = _with_backoff(lambda: ws.get_all_values())
    first_row = values[0] if values else []

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
    if not values:
        return pd.DataFrame(columns=internal_cols), []

    sheet_headers = values[0]
    rows = values[1:] if len(values) > 1 else []

    df = pd.DataFrame(rows, columns=sheet_headers)
    df = df.rename(columns={h: sheet_header_to_internal(h) for h in df.columns})
    df = _coalesce_duplicate_columns(df)

    for c in internal_cols:
        if c not in df.columns:
            df[c] = ""

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


def _append_rows(ws, sheet_headers: list[str], rows_internal: list[dict]):
    """
    Append multiple rows in one call (quota friendly).
    """
    if not rows_internal:
        return
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}
    batch = []
    for row_internal in rows_internal:
        ordered = []
        for sheet_h in sheet_headers:
            internal = header_to_internal.get(sheet_h, sheet_h)
            v = row_internal.get(internal, "")
            if isinstance(v, (pd.Series, pd.DataFrame)):
                v = ""
            ordered.append(v)
        batch.append(ordered)

    def _do():
        return ws.append_rows(batch, value_input_option="USER_ENTERED")

    _with_backoff(_do)


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


def _batch_update_rows(ws, sheet_headers: list[str], updates: list[tuple[int, dict]]):
    """
    Batch update multiple full rows in one API call (quota friendly).
    updates = [(rownum, row_internal_dict), ...]
    """
    if not updates:
        return

    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(sheet_headers)).split("1")[0]

    data = []
    for rownum, row_internal in updates:
        row_values = []
        for sheet_h in sheet_headers:
            internal = header_to_internal.get(sheet_h, sheet_h)
            v = row_internal.get(internal, "")
            if isinstance(v, (pd.Series, pd.DataFrame)):
                v = ""
            row_values.append(v)

        rng = f"A{rownum}:{last_col_letter}{rownum}"
        data.append({"range": rng, "values": [row_values]})

    _with_backoff(lambda: ws.batch_update(data, value_input_option="USER_ENTERED"))


def _compute_net_and_profit(all_in_cost: float, sold_price: float, fees: float, shipping_charge: float) -> tuple[float, float]:
    """
    SOLD LOGIC (per your definition):
    - sold_price = total the buyer paid (all-in)
    - fees = platform fees
    - shipping_charge = what YOU paid for shipping
    - net_proceeds = sold_price - fees - shipping_charge
    - profit = net_proceeds - all_in_cost
    """
    sold_price = float(sold_price or 0.0)
    fees = float(fees or 0.0)
    shipping_charge = float(shipping_charge or 0.0)
    all_in_cost = float(all_in_cost or 0.0)

    net = round(sold_price - fees - shipping_charge, 2)
    profit = round(net - all_in_cost, 2)
    return net, profit


def _compute_fees_total_for_dashboard(fees: float, shipping_charge: float) -> float:
    """
    Dashboard alignment:
    If Dashboard does net as (sold_price - fees_total),
    then fees_total must include ALL deductions: fees + shipping_charge.
    """
    fees = float(fees or 0.0)
    shipping_charge = float(shipping_charge or 0.0)
    return float(round(fees + shipping_charge, 2))


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
# UI HELPERS
# =========================================================
def _to_float_money(x) -> float:
    """
    Safely convert common Sheet/Excel numeric strings to float.
    Handles: $1,234.56  (1,234.56)  0  ""  None
    Returns 0.0 if it can't parse.
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)

    s = str(x).strip()
    if s == "":
        return 0.0

    s = s.replace("$", "").replace(",", "").strip()

    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1].strip()

    try:
        return float(s)
    except Exception:
        return 0.0


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

        mode = st.radio(
            "Create mode",
            options=["Single", "Bulk (mass listings)"],
            index=0,
            horizontal=True,
        )

        st.markdown("---")

        # -----------------------------------------
        # BULK MODE
        # -----------------------------------------
        if mode.startswith("Bulk"):
            st.markdown("### Bulk listing creator")

            selected_labels = st.multiselect(
                "Select ACTIVE items to list",
                options=inv_available["__label"].tolist(),
                default=[],
            )

            if not selected_labels:
                st.info("Select one or more items above to build your listings table.")
            else:
                chosen_df = inv_available[inv_available["__label"].isin(selected_labels)].copy()
                chosen_df = chosen_df.sort_values(by=["year", "set_name", "card_name", "inventory_id"], na_position="last")

                # Build base editor df
                base_editor = pd.DataFrame({
                    "inventory_id": chosen_df["inventory_id"].astype(str).str.strip(),
                    "year": chosen_df.get("year", ""),
                    "set_name": chosen_df.get("set_name", ""),
                    "card_name": chosen_df.get("card_name", ""),
                    "variant": chosen_df.get("variant", ""),

                    # read-only cost + market (all_in_cost overwritten below with grading)
                    "all_in_cost": chosen_df.get("total_price", 0.0),
                    "market_price": chosen_df.get("market_price", 0.0),

                    # editable fields
                    "transaction_type": ["Buy It Now"] * len(chosen_df),
                    "platform": ["eBay"] * len(chosen_df),
                    "shipping_type": ["Ebay Envelope"] * len(chosen_df),
                    "list_date": [date.today()] * len(chosen_df),
                    "list_price": [0.0] * len(chosen_df),
                    "notes": [""] * len(chosen_df),

                    # computed preview (read-only)
                    "est_profit_loss": [0.0] * len(chosen_df),
                })

                # Overwrite all_in_cost with purchase_total + best grading fee (if any)
                inv_map_tmp = inv_df.set_index("inventory_id", drop=False).to_dict("index")
                all_in_list = []
                for _inv_id in base_editor["inventory_id"].astype(str).str.strip().tolist():
                    inv_rec = inv_map_tmp.get(_inv_id, {})
                    gr_info_tmp = _lookup_grading_for_inventory(gr_df, _inv_id)
                    purchase_total = float(inv_rec.get("total_price", 0.0) or 0.0)
                    grading_fee_total = float(gr_info_tmp.get("grading_fee_total", 0.0) or 0.0)
                    all_in_list.append(float(round(purchase_total + grading_fee_total, 2)))
                base_editor["all_in_cost"] = all_in_list

                # If user has already edited, seed from session_state and preserve their edits
                prior = st.session_state.get("bulk_listing_editor", None)
                if isinstance(prior, pd.DataFrame) and not prior.empty:
                    # align by inventory_id (keep prior edits for editable columns)
                    editable_cols = ["transaction_type", "platform", "shipping_type", "list_date", "list_price", "notes"]
                    prior2 = prior.copy()
                    prior2["inventory_id"] = prior2["inventory_id"].astype(str).str.strip()
                    merged = base_editor.merge(
                        prior2[["inventory_id"] + [c for c in editable_cols if c in prior2.columns]],
                        on="inventory_id",
                        how="left",
                        suffixes=("", "_prior"),
                    )
                    for c in editable_cols:
                        pc = f"{c}_prior"
                        if pc in merged.columns:
                            merged[c] = merged[pc].where(merged[pc].notna() & (merged[pc].astype(str) != ""), merged[c])
                            merged = merged.drop(columns=[pc])
                    base_editor = merged

                # Compute preview estimates BEFORE editor renders (so they appear beside list price)
                est_list = []
                for _, r in base_editor.iterrows():
                    est = _estimate_profit_loss(
                        platform=str(r.get("platform", "")),
                        list_price=float(r.get("list_price", 0.0) or 0.0),
                        all_in_cost=float(r.get("all_in_cost", 0.0) or 0.0),
                        shipping_type=str(r.get("shipping_type", "")),
                    )
                    est_list.append(float(est["profit"]))
                base_editor["est_profit_loss"] = est_list

                st.caption("Edit fields below, then click **Create Listings**. Inventory items will be marked LISTED.")
                st.caption("Est Profit/Loss uses: eBay fee 13.25% of (item + ship + 9% tax). Non-eBay assumes $0 fees/tax/shipping.")

                edited = st.data_editor(
                    base_editor,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    column_config={
                        "inventory_id": st.column_config.TextColumn("Inventory ID", disabled=True),
                        "year": st.column_config.TextColumn("Year", disabled=True),
                        "set_name": st.column_config.TextColumn("Set", disabled=True),
                        "card_name": st.column_config.TextColumn("Card", disabled=True),
                        "variant": st.column_config.TextColumn("Variant", disabled=True),

                        "all_in_cost": st.column_config.NumberColumn("All-in Cost", format="$%.2f", disabled=True),
                        "market_price": st.column_config.NumberColumn("Market Price", format="$%.2f", disabled=True),

                        "transaction_type": st.column_config.SelectboxColumn(
                            "Transaction Type",
                            options=["Auction", "Buy It Now"],
                            required=True,
                        ),
                        "platform": st.column_config.TextColumn("Platform", required=True),

                        "shipping_type": st.column_config.SelectboxColumn(
                            "Shipping Type (eBay only)",
                            options=["", "Ebay Envelope", "Ground Advantage"],
                            required=False,
                        ),

                        "list_date": st.column_config.DateColumn("List Date", required=True),
                        "list_price": st.column_config.NumberColumn("List Price", min_value=0.0, step=1.0, format="$%.2f", required=True),

                        "est_profit_loss": st.column_config.NumberColumn("Est Profit/Loss", format="$%.2f", disabled=True),

                        "notes": st.column_config.TextColumn("Notes"),
                    },
                    key="bulk_listing_editor",
                )

                # Recompute estimates from user edits and show a quick summary underneath
                # (The editor will reflect computed column on next interaction/rerun.)
                if isinstance(edited, pd.DataFrame) and not edited.empty:
                    recomputed = edited.copy()
                    est_list2 = []
                    for _, r in recomputed.iterrows():
                        est = _estimate_profit_loss(
                            platform=str(r.get("platform", "")),
                            list_price=float(r.get("list_price", 0.0) or 0.0),
                            all_in_cost=float(r.get("all_in_cost", 0.0) or 0.0),
                            shipping_type=str(r.get("shipping_type", "")),
                        )
                        est_list2.append(float(est["profit"]))
                    recomputed["est_profit_loss"] = est_list2

                    # Show preview table (keeps "next to list price" in the main editor, plus accurate numbers here)
                    st.dataframe(
                        recomputed[["inventory_id", "platform", "shipping_type", "list_price", "market_price", "all_in_cost", "est_profit_loss"]].copy(),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "list_price": st.column_config.NumberColumn("List Price", format="$%.2f"),
                            "market_price": st.column_config.NumberColumn("Market Price", format="$%.2f"),
                            "all_in_cost": st.column_config.NumberColumn("All-in Cost", format="$%.2f"),
                            "est_profit_loss": st.column_config.NumberColumn("Est Profit/Loss", format="$%.2f"),
                        },
                    )

                colA, colB = st.columns([1, 2])
                with colA:
                    create_bulk = st.button("✅ Create Listings", type="primary", use_container_width=True)
                with colB:
                    st.caption("Tip: Any row with missing platform or list_price <= 0 will be rejected. If platform is eBay, Shipping Type is required.")

                if create_bulk:
                    problems = []
                    rows_to_create = []

                    for i, r in edited.iterrows():
                        inv_id = str(r.get("inventory_id", "")).strip()
                        ttype = str(r.get("transaction_type", "")).strip()
                        platform = str(r.get("platform", "")).strip()
                        shipping_type = str(r.get("shipping_type", "")).strip()
                        list_date = r.get("list_date", None)
                        list_price = float(r.get("list_price", 0.0) or 0.0)
                        notes = str(r.get("notes", "")).strip()

                        if not inv_id:
                            problems.append(f"Row {i+1}: missing inventory_id.")
                            continue
                        if ttype not in ["Auction", "Buy It Now"]:
                            problems.append(f"Row {i+1} (Inv {inv_id}): transaction_type must be Auction or Buy It Now.")
                            continue
                        if not platform:
                            problems.append(f"Row {i+1} (Inv {inv_id}): platform is required.")
                            continue
                        if _is_ebay_platform(platform) and shipping_type not in EBAY_SHIPPING_TYPES:
                            problems.append(f"Row {i+1} (Inv {inv_id}): choose Shipping Type (Ebay Envelope or Ground Advantage).")
                            continue
                        if list_date in [None, ""]:
                            problems.append(f"Row {i+1} (Inv {inv_id}): list_date is required.")
                            continue
                        if list_price <= 0:
                            problems.append(f"Row {i+1} (Inv {inv_id}): list_price must be > 0.")
                            continue

                        rows_to_create.append({
                            "inventory_id": inv_id,
                            "transaction_type": ttype,
                            "platform": platform,
                            "shipping_type": shipping_type,
                            "list_date": list_date,
                            "list_price": list_price,
                            "notes": notes,
                        })

                    if problems:
                        st.error("Fix these issues and try again:\n- " + "\n- ".join(problems))
                        st.stop()

                    # Safety: ensure still ACTIVE
                    active_set = set(inv_available["inventory_id"].astype(str).str.strip().tolist())
                    bad = [x["inventory_id"] for x in rows_to_create if x["inventory_id"] not in active_set]
                    if bad:
                        st.error(
                            "Some selected items are no longer ACTIVE (maybe listed in another session). "
                            f"Remove these and try again: {', '.join(bad)}"
                        )
                        st.stop()

                    spreadsheet_id = st.secrets["spreadsheet_id"]
                    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
                    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

                    inv_ws = _get_ws(spreadsheet_id, inv_ws_name)
                    tx_ws = _get_ws(spreadsheet_id, tx_ws_name)

                    inv_sheet_headers = _ensure_headers(inv_ws, INV_COLUMNS)
                    tx_sheet_headers = _ensure_headers(tx_ws, TX_COLUMNS)

                    inv_values = _with_backoff(lambda: inv_ws.get_all_values())
                    inv_header = inv_values[0] if inv_values else inv_sheet_headers
                    inv_id_col_idx = next((i for i, h in enumerate(inv_header, start=1) if sheet_header_to_internal(h) == "inventory_id"), None)
                    if inv_id_col_idx is None:
                        st.error("Could not find 'inventory_id' column in the inventory sheet header.")
                        st.stop()

                    inv_ids = [x["inventory_id"] for x in rows_to_create]
                    inv_rownums = _find_rownum_by_id(inv_values, inv_id_col_idx, inv_ids)

                    inv_map = inv_df.set_index("inventory_id", drop=False).to_dict("index")

                    now_iso = pd.Timestamp.utcnow().isoformat()
                    tx_rows = []
                    inv_updates = []

                    for x in rows_to_create:
                        inv_id = x["inventory_id"]
                        inv_rec = inv_map.get(inv_id, {})

                        tx_id = str(uuid.uuid4())

                        gr_info = _lookup_grading_for_inventory(gr_df, inv_id)

                        img_url_final = str(inv_rec.get("image_url", "")).strip()

                        purchase_total = float(inv_rec.get("total_price", 0.0) or 0.0)
                        grading_fee_total = float(gr_info.get("grading_fee_total", 0.0) or 0.0)
                        all_in_cost = float(round(purchase_total + grading_fee_total, 2))

                        card_type_norm = _normalize_card_type(inv_rec.get("card_type", ""))

                        tx_row = {
                            "transaction_id": tx_id,
                            "inventory_id": inv_id,
                            "transaction_type": x["transaction_type"],
                            "platform": str(x["platform"]).strip(),
                            "list_date": str(x["list_date"]) if x.get("list_date") else "",
                            "list_price": float(x["list_price"] or 0.0),

                            # sale fields blank
                            "sold_date": "",
                            "sold_price": 0.0,
                            "fees": 0.0,
                            "shipping_charged": 0.0,
                            "fees_total": 0.0,
                            "net_proceeds": 0.0,
                            "profit": 0.0,

                            "notes": str(x.get("notes", "")).strip(),
                            "status": TX_STATUS_LISTED,
                            "created_at": now_iso,
                            "updated_at": now_iso,

                            # snapshots
                            "product_type": str(inv_rec.get("product_type", "")).strip(),
                            "sealed_product_type": str(inv_rec.get("sealed_product_type", "")).strip(),
                            "card_type": card_type_norm,
                            "brand_or_league": str(inv_rec.get("brand_or_league", "")).strip(),
                            "set_name": str(inv_rec.get("set_name", "")).strip(),
                            "year": str(inv_rec.get("year", "")).strip(),
                            "card_name": str(inv_rec.get("card_name", "")).strip(),
                            "card_number": str(inv_rec.get("card_number", "")).strip(),
                            "variant": str(inv_rec.get("variant", "")).strip(),
                            "card_subtype": str(inv_rec.get("card_subtype", "")).strip(),
                            "reference_link": str(inv_rec.get("reference_link", "")).strip(),
                            "image_url": img_url_final,
                            "purchase_date": str(inv_rec.get("purchase_date", "")).strip(),
                            "purchased_from": str(inv_rec.get("purchased_from", "")).strip(),

                            "purchase_total": float(purchase_total),
                            "grading_fee_total": float(grading_fee_total),
                            "all_in_cost": float(all_in_cost),

                            "grading_company": str(gr_info.get("grading_company", "")).strip(),
                            "grade": str(gr_info.get("grade", "")).strip(),
                            "condition": str(inv_rec.get("condition", "")).strip(),
                        }
                        tx_rows.append(tx_row)

                        # Inventory update (status + listed_transaction_id; keep other fields as-is)
                        rownum = inv_rownums.get(inv_id)
                        if not rownum:
                            continue

                        row_vals = inv_values[rownum - 1] if len(inv_values) >= rownum else []
                        if len(row_vals) < len(inv_header):
                            row_vals = row_vals + [""] * (len(inv_header) - len(row_vals))
                        row_dict_sheet = dict(zip(inv_header, row_vals))
                        row_internal = {sheet_header_to_internal(k): v for k, v in row_dict_sheet.items()}
                        row_internal = _coalesce_duplicate_columns(pd.DataFrame([row_internal])).iloc[0].to_dict()

                        row_internal["inventory_status"] = STATUS_LISTED
                        row_internal["listed_transaction_id"] = tx_id

                        if "image_url" in row_internal and (not str(row_internal.get("image_url", "")).strip()) and img_url_final:
                            row_internal["image_url"] = img_url_final

                        inv_updates.append((rownum, row_internal))

                    if not tx_rows:
                        st.error("No valid rows to create (unexpected).")
                        st.stop()

                    _append_rows(tx_ws, tx_sheet_headers, tx_rows)
                    _batch_update_rows(inv_ws, inv_sheet_headers, inv_updates)

                    st.session_state.pop("inv_df_cache_tx", None)
                    st.session_state.pop("tx_df_cache", None)
                    st.session_state.pop("gr_df_cache_tx", None)
                    _read_sheet_values_cached.clear()

                    st.success(f"Created {len(tx_rows)} listing(s). Inventory marked LISTED.")
                    st.rerun()

        # -----------------------------------------
        # SINGLE MODE
        # -----------------------------------------
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

            gr_info = _lookup_grading_for_inventory(gr_df, inv_id)

            with col_left:
                img_url = str(selected_row.get("image_url", "")).strip()
                if not img_url:
                    img_url = scrape_image_url(str(selected_row.get("reference_link", "")).strip())
                if img_url:
                    st.image(img_url, use_container_width=True)
                else:
                    st.caption("No image found.")

            st.markdown("#### Purchase details")
            p1, p2, p3, p4 = st.columns(4)
            p1.write(f"**Purchase date:** {selected_row.get('purchase_date', '')}")
            p2.write(f"**Purchased from:** {selected_row.get('purchased_from', '')}")
            p3.write(f"**Total cost:** {_money(selected_row.get('total_price', 0.0))}")
            p4.write(f"**Market price:** {_money(selected_row.get('market_price', 0.0))}")

            st.markdown("---")

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
                        # Show shipping type input; required only if platform is eBay
                        if _is_ebay_platform(platform):
                            shipping_type = st.selectbox("Shipping type (eBay only)*", EBAY_SHIPPING_TYPES, index=0)
                        else:
                            shipping_type = st.selectbox("Shipping type (eBay only)", [""] + EBAY_SHIPPING_TYPES, index=0)
                            st.caption("Only used for eBay estimates.")

                    # Live estimate preview
                    purchase_total_tmp = float(selected_row.get("total_price", 0.0) or 0.0)
                    grading_fee_total_tmp = float(gr_info.get("grading_fee_total", 0.0) or 0.0)
                    all_in_cost_tmp = float(round(purchase_total_tmp + grading_fee_total_tmp, 2))

                    est = _estimate_profit_loss(
                        platform=platform,
                        list_price=list_price,
                        all_in_cost=all_in_cost_tmp,
                        shipping_type=shipping_type,
                    )

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Est Profit/Loss", _money(est["profit"]))
                    if _is_ebay_platform(platform):
                        m2.metric("Est eBay fee", _money(est["fee"]))
                        m3.metric("Est tax (9%)", _money(est["tax"]))
                        m4.metric("Total buyer pays", _money(est["total_paid"]))
                    else:
                        m2.metric("Fees", _money(0))
                        m3.metric("Tax", _money(0))
                        m4.metric("Buyer pays", _money(est["total_paid"]))

                    st.caption("Estimate uses assumptions; actual fees/tax/shipping can differ. Enter actuals when sold in Tab 2.")

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
                        shipping_charged = st.number_input(
                            "Shipping charge (what you paid) (optional)",
                            min_value=0.0,
                            step=1.0,
                            format="%.2f"
                        )

                    # define for consistency
                    shipping_type = ""
                    list_date = ""
                    list_price = 0.0

                submit = st.form_submit_button("Save", type="primary", use_container_width=True)

            if submit:
                if not platform.strip():
                    st.error("Platform is required.")
                elif tx_type in ["Auction", "Buy It Now"] and _is_ebay_platform(platform) and shipping_type not in EBAY_SHIPPING_TYPES:
                    st.error("For eBay listings, Shipping type is required (Ebay Envelope or Ground Advantage).")
                else:
                    spreadsheet_id = st.secrets["spreadsheet_id"]
                    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
                    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

                    inv_ws = _get_ws(spreadsheet_id, inv_ws_name)
                    tx_ws = _get_ws(spreadsheet_id, tx_ws_name)

                    inv_sheet_headers = _ensure_headers(inv_ws, INV_COLUMNS)
                    tx_sheet_headers = _ensure_headers(tx_ws, TX_COLUMNS)

                    inv_values = _with_backoff(lambda: inv_ws.get_all_values())

                    inv_header = inv_values[0] if inv_values else inv_sheet_headers
                    inv_id_col_idx = next((i for i, h in enumerate(inv_header, start=1) if sheet_header_to_internal(h) == "inventory_id"), None)
                    if inv_id_col_idx is None:
                        st.error("Could not find 'inventory_id' column in the inventory sheet header.")
                        st.stop()

                    img_url_final = str(selected_row.get("image_url", "")).strip()
                    if not img_url_final:
                        img_url_final = scrape_image_url(str(selected_row.get("reference_link", "")).strip())

                    purchase_total = float(selected_row.get("total_price", 0.0) or 0.0)
                    grading_fee_total = float(gr_info.get("grading_fee_total", 0.0) or 0.0)
                    all_in_cost = float(round(purchase_total + grading_fee_total, 2))

                    tx_id = str(uuid.uuid4())
                    now_iso = pd.Timestamp.utcnow().isoformat()

                    card_type_norm = _normalize_card_type(selected_row.get("card_type", ""))

                    if tx_type == "Trade In":
                        net, profit = _compute_net_and_profit(
                            all_in_cost=all_in_cost,
                            sold_price=sold_price,
                            fees=fees,
                            shipping_charge=shipping_charged,
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

                        "purchase_total": float(purchase_total),
                        "grading_fee_total": float(grading_fee_total),
                        "all_in_cost": float(all_in_cost),

                        "grading_company": str(gr_info.get("grading_company", "")).strip(),
                        "grade": str(gr_info.get("grade", "")).strip(),

                        "condition": str(selected_row.get("condition", "")).strip(),
                    }

                    _append_row(tx_ws, tx_sheet_headers, tx_row)

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

                    st.session_state.pop("inv_df_cache_tx", None)
                    st.session_state.pop("tx_df_cache", None)
                    st.session_state.pop("gr_df_cache_tx", None)
                    _read_sheet_values_cached.clear()

                    if tx_type == "Trade In":
                        st.success("Trade In recorded and inventory marked SOLD.")
                    else:
                        st.success("Listing created and inventory marked LISTED.")

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
                    shipping_charged = st.number_input(
                        "Shipping charge (what you paid) (optional)",
                        min_value=0.0,
                        step=1.0,
                        format="%.2f"
                    )

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

                # sanitize numeric fields from Sheets (e.g., "$0.00")
                for c in NUMERIC_TX:
                    if c in tx_internal:
                        tx_internal[c] = _to_float_money(tx_internal.get(c))

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
                    all_in_cost = _to_float_money(tx_internal.get("all_in_cost", 0.0))
                    if all_in_cost <= 0:
                        all_in_cost = _to_float_money(tx_internal.get("purchase_total", 0.0))

                    net, profit = _compute_net_and_profit(
                        all_in_cost=all_in_cost,
                        sold_price=sold_price,
                        fees=fees,
                        shipping_charge=shipping_charged,
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
