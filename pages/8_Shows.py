# pages/4_Shows.py
# ---------------------------------------------------------
# Shows page (Google Sheets-backed)
#
# Tabs:
# 1) Show Inventory Summary
#    - ACTIVE inventory only
#    - Inventory Type = Show Inventory
#    - KPIs: # items, Total Cost, Total Market Value
#    - Pulls sticker pricing from the next upcoming show snapshot
#
# 2) Manage Shows
#    - Create shows with date/location/description
#    - When a show is created, snapshot current Show Inventory
#      into show_inventory_snapshots and store snapshot totals
#      on the shows sheet.
#
# 3) Pricing for the Show
#    - Finds the next upcoming show
#    - Exports that show inventory with a sticker_price column
#    - Re-upload syncs sticker prices back to the show snapshot
#
# 4) Show Sales Sync
#    - Finds the next upcoming show
#    - Displays inventory for that show
#    - Allows sell_price entry in-app or via Excel/CSV export/import
#    - Sync Sales marks inventory SOLD and appends SOLD rows to transactions
# ---------------------------------------------------------

import io
import json
import re
import time
import uuid
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# CONFIG
# =========================================================

st.set_page_config(page_title="Shows", layout="wide")

INVENTORY_WS_DEFAULT = "inventory"
TRANSACTIONS_WS_DEFAULT = "transactions"
SHOWS_WS_DEFAULT = "shows"
SHOW_SNAPSHOTS_WS_DEFAULT = "show_inventory_snapshots"

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
TX_STATUS_SOLD = "SOLD"

SHOW_STATUS_OPTIONS = ["Planned", "Completed", "Cancelled"]

# Inventory columns used by this page. These match your Inventory page schema.
INV_COLUMNS = [
    "inventory_id",
    "image_url",
    "product_type",
    "sealed_product_type",
    "card_type",
    "inventory_type",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "grading_company",
    "grade",
    "reference_link",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "grading_fee",
    "total_cost",
    "condition",
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
    "market_price",
    "market_value",
    "market_price_updated_at",
]

# Transaction columns. Header aliases below allow existing sheet headers like
# "Fees Total", "TX Status", "Product Type", "Image URL", etc.
TX_COLUMNS = [
    "transaction_id",
    "inventory_id",
    "transaction_type",
    "platform",
    "list_date",
    "list_price",
    "sold_date",
    "sold_price",
    "fees",
    "shipping_charged",
    "fees_total",
    "net_proceeds",
    "profit",
    "notes",
    "status",
    "created_at",
    "updated_at",
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
    "purchase_total",
    "grading_fee_total",
    "all_in_cost",
    "grading_company",
    "grade",
    "condition",
]

SHOW_COLUMNS = [
    "show_id",
    "show_name",
    "show_date",
    "location",
    "description",
    "status",
    "snapshot_item_count",
    "snapshot_total_cost",
    "snapshot_total_market_value",
    "snapshot_created_at",
    "created_at",
    "updated_at",
]

SNAPSHOT_COLUMNS = [
    "snapshot_id",
    "show_id",
    "show_name",
    "show_date",
    "snapshot_created_at",
    "inventory_id",
    "product_type",
    "sealed_product_type",
    "card_type",
    "inventory_type",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "grading_company",
    "grade",
    "reference_link",
    "image_url",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "grading_fee",
    "total_cost",
    "market_price",
    "market_value",
    "sticker_price",
    "condition",
    "inventory_status_at_snapshot",
    "sold_price",
    "synced_at",
    "synced_transaction_id",
]

NUMERIC_INV = [
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "grading_fee",
    "total_cost",
    "market_price",
    "market_value",
]
NUMERIC_TX = [
    "list_price",
    "sold_price",
    "fees",
    "shipping_charged",
    "fees_total",
    "net_proceeds",
    "profit",
    "purchase_total",
    "grading_fee_total",
    "all_in_cost",
]
NUMERIC_SHOW = [
    "snapshot_item_count",
    "snapshot_total_cost",
    "snapshot_total_market_value",
]
NUMERIC_SNAPSHOT = [
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "grading_fee",
    "total_cost",
    "market_price",
    "market_value",
    "sticker_price",
    "sold_price",
]


# =========================================================
# HEADER NORMALIZATION / ALIASES
# =========================================================

HEADER_ALIASES = {
    # IDs / statuses
    "inventory_id": ["inventory_id", "Inventory ID"],
    "transaction_id": ["transaction_id", "Transaction ID"],
    "show_id": ["show_id", "Show ID"],
    "snapshot_id": ["snapshot_id", "Snapshot ID"],
    "status": ["status", "TX Status", "tx_status", "Status", "Show Status"],
    "inventory_status": ["inventory_status", "Inventory Status", "inventoryStatus"],
    "listed_transaction_id": ["listed_transaction_id", "Listed Transaction ID"],

    # Inventory / product fields
    "product_type": ["product_type", "Product Type"],
    "sealed_product_type": ["sealed_product_type", "Sealed Product Type"],
    "inventory_type": ["inventory_type", "Inventory Type"],
    "image_url": ["image_url", "Image URL", "image", "Image"],
    "brand_or_league": ["brand_or_league", "Brand/League", "Brand / League", "Brand or League"],
    "set_name": ["set_name", "Set", "Set Name"],
    "card_name": ["card_name", "Card Name", "Item Name"],
    "card_number": ["card_number", "Card #", "Card Number"],
    "card_subtype": ["card_subtype", "Card Subtype"],
    "reference_link": ["reference_link", "Reference Link", "Reference link"],
    "purchase_date": ["purchase_date", "Purchase Date"],
    "purchased_from": ["purchased_from", "Purchased From", "Purchased from"],
    "purchase_price": ["purchase_price", "Purchase Price"],
    "total_price": ["total_price", "Total Price", "Purchase Total", "purchase_total"],
    "grading_fee": ["grading_fee", "Grading Fee"],
    "total_cost": ["total_cost", "Total Cost", "All In Cost", "all_in_cost"],
    "grading_company": ["grading_company", "Grading Company"],
    "grade": ["grade", "Grade"],
    "market_price": ["market_price", "Market Price", "Market price", "market price"],
    "market_value": ["market_value", "Market Value", "market value"],
    "market_price_updated_at": ["market_price_updated_at", "Market Price Updated At"],

    # Transactions
    "transaction_type": ["transaction_type", "Transaction Type", "listing_type"],
    "fees": ["fees", "Fees", "platform_fees", "fee"],
    "shipping_charged": ["shipping_charged", "Shipping Charged"],
    "fees_total": ["fees_total", "Fees Total", "fees_total_calc", "fees_total_dashboard"],
    "net_proceeds": ["net_proceeds", "Net Proceeds"],
    "profit": ["profit", "Profit", "Profit/Loss", "profit_loss"],
    "purchase_total": ["purchase_total", "Purchase Total", "cost_basis", "Cost Basis", "purchase_total_allin"],
    "grading_fee_total": ["grading_fee_total", "Grading Fee", "grading_fee", "total_grading_cost"],
    "all_in_cost": ["all_in_cost", "All In Cost", "all_in"],

    # Shows
    "show_name": ["show_name", "Show Name"],
    "show_date": ["show_date", "Show Date"],
    "snapshot_item_count": ["snapshot_item_count", "Snapshot Item Count"],
    "snapshot_total_cost": ["snapshot_total_cost", "Snapshot Total Cost"],
    "snapshot_total_market_value": ["snapshot_total_market_value", "Snapshot Total Market Value"],
    "snapshot_created_at": ["snapshot_created_at", "Snapshot Created At"],
    "inventory_status_at_snapshot": ["inventory_status_at_snapshot", "Inventory Status At Snapshot"],
    "sold_price": ["sold_price", "Sold Price", "sell_price", "Sell Price"],
    "sticker_price": ["sticker_price", "Sticker Price"],
    "synced_at": ["synced_at", "Synced At"],
    "synced_transaction_id": ["synced_transaction_id", "Synced Transaction ID"],
}


def _norm_header(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def sheet_header_to_internal(header: str) -> str:
    h_norm = _norm_header(header)
    for internal, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if _norm_header(alias) == h_norm:
                return internal
    return h_norm


def internal_to_sheet_header(internal: str, existing_headers: list[str] | None = None) -> str:
    existing_headers = existing_headers or []
    aliases = HEADER_ALIASES.get(internal, [internal])
    existing_norm = {_norm_header(x): x for x in existing_headers}

    for alias in aliases:
        if _norm_header(alias) in existing_norm:
            return existing_norm[_norm_header(alias)]

    defaults = {
        "product_type": "Product Type",
        "sealed_product_type": "Sealed Product Type",
        "inventory_type": "Inventory Type",
        "image_url": "Image URL",
        "inventory_status": "Inventory Status",
        "transaction_id": "transaction_id",
        "fees_total": "Fees Total",
        "status": "TX Status",
        "profit": "Profit",
        "grading_fee_total": "Grading Fee",
        "all_in_cost": "All In Cost",
        "grading_company": "Grading Company",
        "show_id": "show_id",
        "show_name": "show_name",
        "show_date": "show_date",
    }
    return defaults.get(internal, internal)


# =========================================================
# MONEY / DATE HELPERS
# =========================================================


def _money_float(x) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, float) and pd.isna(x):
            return 0.0
        s = str(x).strip()
        if s == "":
            return 0.0
        neg = s.startswith("(") and s.endswith(")")
        s = s.replace("$", "").replace(",", "")
        s = re.sub(r"[^0-9.\-]", "", s)
        if s in {"", ".", "-", "-."}:
            return 0.0
        val = float(s)
        return -val if neg and val > 0 else val
    except Exception:
        return 0.0


def _coerce_money_series(s: pd.Series) -> pd.Series:
    return s.apply(_money_float).astype(float)


def _money_display(x) -> str:
    return f"${_money_float(x):,.2f}"


def _clean_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _utc_now_iso() -> str:
    return pd.Timestamp.utcnow().isoformat()


def _date_str(x) -> str:
    parsed = pd.to_datetime(x, errors="coerce")
    if pd.isna(parsed):
        return ""
    return str(parsed.date())


def _parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def _normalize_status(x) -> str:
    return _clean_text(x).upper()


def _normalize_inventory_type(x) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(x).lower())


# =========================================================
# GOOGLE SHEETS CLIENT + READ/WRITE HELPERS
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
        sa_info = json.loads(st.secrets["gcp_service_account"])
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

    raise KeyError('Missing secrets: add "gcp_service_account" or "service_account_json_path".')


@st.cache_resource
def _get_spreadsheet(spreadsheet_id: str):
    client = get_gspread_client()
    return _with_backoff(lambda: client.open_by_key(spreadsheet_id))


def _get_or_create_ws(spreadsheet_id: str, worksheet_name: str, internal_headers: list[str]):
    sh = _get_spreadsheet(spreadsheet_id)
    try:
        return _with_backoff(lambda: sh.worksheet(worksheet_name))
    except gspread.exceptions.WorksheetNotFound:
        rows = 1000
        cols = max(len(internal_headers) + 5, 20)
        ws = _with_backoff(lambda: sh.add_worksheet(title=worksheet_name, rows=rows, cols=cols))
        sheet_headers = [internal_to_sheet_header(h, []) for h in internal_headers]
        _with_backoff(lambda: ws.update("1:1", [sheet_headers], value_input_option="USER_ENTERED"))
        return ws


def _ensure_headers(ws, internal_headers: list[str]) -> list[str]:
    values = _with_backoff(lambda: ws.get_all_values())
    first_row = values[0] if values else []

    if not first_row:
        sheet_headers = [internal_to_sheet_header(h, []) for h in internal_headers]
        _with_backoff(lambda: ws.update("1:1", [sheet_headers], value_input_option="USER_ENTERED"))
        _read_sheet_values_cached.clear()
        return sheet_headers

    existing_sheet_headers = first_row
    existing_internal = [sheet_header_to_internal(h) for h in existing_sheet_headers]
    missing_internal = [h for h in internal_headers if h not in set(existing_internal)]

    if missing_internal:
        additions = [internal_to_sheet_header(h, existing_sheet_headers) for h in missing_internal]
        new_headers = existing_sheet_headers + additions
        _with_backoff(lambda: ws.update("1:1", [new_headers], value_input_option="USER_ENTERED"))
        _read_sheet_values_cached.clear()
        return new_headers

    return existing_sheet_headers


@st.cache_data(ttl=45, show_spinner=False)
def _read_sheet_values_cached(spreadsheet_id: str, worksheet_name: str) -> list[list[str]]:
    # The worksheet should already exist before this function is called.
    sh = _get_spreadsheet(spreadsheet_id)
    ws = _with_backoff(lambda: sh.worksheet(worksheet_name))
    return _with_backoff(lambda: ws.get_all_values())


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df

    new = pd.DataFrame(index=df.index)
    for col in pd.unique(df.columns):
        cols = df.loc[:, df.columns == col]
        if cols.shape[1] == 1:
            new[col] = cols.iloc[:, 0]
        else:
            stacked = cols.astype(str).replace("nan", "").replace("None", "")
            new[col] = stacked.apply(lambda r: next((v for v in r.tolist() if str(v).strip() != ""), ""), axis=1)
    return new


def _sheet_to_df(values: list[list[str]], internal_cols: list[str]) -> tuple[pd.DataFrame, list[str]]:
    if not values:
        return pd.DataFrame(columns=internal_cols), []

    sheet_headers = values[0]
    raw_rows = values[1:] if len(values) > 1 else []

    rows = []
    for row in raw_rows:
        if len(row) < len(sheet_headers):
            rows.append(row + [""] * (len(sheet_headers) - len(row)))
        elif len(row) > len(sheet_headers):
            rows.append(row[:len(sheet_headers)])
        else:
            rows.append(row)

    df = pd.DataFrame(rows, columns=sheet_headers)
    df = df.rename(columns={h: sheet_header_to_internal(h) for h in df.columns})
    df = _coalesce_duplicate_columns(df)

    for c in internal_cols:
        if c not in df.columns:
            df[c] = ""

    return df[internal_cols].copy(), sheet_headers


def _coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = _coerce_money_series(df[c])
    return df


def _load_sheet_df(worksheet_name: str, internal_cols: list[str], numeric_cols: list[str]) -> pd.DataFrame:
    spreadsheet_id = st.secrets["spreadsheet_id"]
    ws = _get_or_create_ws(spreadsheet_id, worksheet_name, internal_cols)
    _ensure_headers(ws, internal_cols)
    values = _read_sheet_values_cached(spreadsheet_id, worksheet_name)
    df, _headers = _sheet_to_df(values, internal_cols)
    df = _coerce_numeric(df, numeric_cols)
    return df


def load_inventory_df() -> pd.DataFrame:
    ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    df = _load_sheet_df(ws_name, INV_COLUMNS, NUMERIC_INV)
    if df.empty:
        return df

    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    df["inventory_status"] = df["inventory_status"].replace("", STATUS_ACTIVE).fillna(STATUS_ACTIVE)
    df["inventory_type"] = df["inventory_type"].astype(str).str.strip()

    # Keep totals reliable even if the sheet has blanks.
    df["total_price"] = (
        _coerce_money_series(df["purchase_price"])
        + _coerce_money_series(df["shipping"])
        + _coerce_money_series(df["tax"])
    ).round(2)

    df["total_cost"] = (
        _coerce_money_series(df["total_price"])
        + _coerce_money_series(df["grading_fee"])
    ).round(2)

    # Resolve market value from market_value first, then market_price.
    df["market_value_resolved"] = _coerce_money_series(df["market_value"])
    fallback_market = _coerce_money_series(df["market_price"])
    df["market_value_resolved"] = df["market_value_resolved"].where(df["market_value_resolved"] > 0, fallback_market)

    return df


def load_transactions_df() -> pd.DataFrame:
    ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)
    df = _load_sheet_df(ws_name, TX_COLUMNS, NUMERIC_TX)
    if df.empty:
        return df
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    return df


def load_shows_df() -> pd.DataFrame:
    ws_name = st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT)
    df = _load_sheet_df(ws_name, SHOW_COLUMNS, NUMERIC_SHOW)
    if df.empty:
        return df
    df["show_id"] = df["show_id"].astype(str).str.strip()
    df["show_date"] = df["show_date"].apply(_date_str)
    return df


def load_snapshots_df() -> pd.DataFrame:
    ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)
    df = _load_sheet_df(ws_name, SNAPSHOT_COLUMNS, NUMERIC_SNAPSHOT)
    if df.empty:
        return df
    df["show_id"] = df["show_id"].astype(str).str.strip()
    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    return df


def _append_rows(worksheet_name: str, internal_headers: list[str], rows_internal: list[dict]):
    if not rows_internal:
        return

    spreadsheet_id = st.secrets["spreadsheet_id"]
    ws = _get_or_create_ws(spreadsheet_id, worksheet_name, internal_headers)
    sheet_headers = _ensure_headers(ws, internal_headers)
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}

    payload = []
    for row in rows_internal:
        ordered = []
        for sheet_h in sheet_headers:
            internal = header_to_internal.get(sheet_h, sheet_h)
            v = row.get(internal, "")
            if isinstance(v, (pd.Series, pd.DataFrame)):
                v = ""
            if pd.isna(v) if not isinstance(v, str) else False:
                v = ""
            ordered.append(v)
        payload.append(ordered)

    _with_backoff(lambda: ws.append_rows(payload, value_input_option="USER_ENTERED"))
    _read_sheet_values_cached.clear()


def _find_rownums_by_id(values: list[list[str]], id_internal_col: str, ids: list[str]) -> dict[str, int | None]:
    if not values:
        return {str(i): None for i in ids}

    headers = values[0]
    id_col_idx = None
    for i, h in enumerate(headers, start=1):
        if sheet_header_to_internal(h) == id_internal_col:
            id_col_idx = i
            break

    if id_col_idx is None:
        return {str(i): None for i in ids}

    found = {}
    for rownum, row in enumerate(values[1:], start=2):
        val = row[id_col_idx - 1] if len(row) >= id_col_idx else ""
        if val:
            found[str(val).strip()] = rownum

    return {str(i).strip(): found.get(str(i).strip()) for i in ids}


def _batch_update_full_rows(worksheet_name: str, internal_headers: list[str], updates: list[tuple[int, dict]]):
    if not updates:
        return

    spreadsheet_id = st.secrets["spreadsheet_id"]
    ws = _get_or_create_ws(spreadsheet_id, worksheet_name, internal_headers)
    sheet_headers = _ensure_headers(ws, internal_headers)
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(sheet_headers)).split("1")[0]

    data = []
    for rownum, row_internal in updates:
        values = []
        for sheet_h in sheet_headers:
            internal = header_to_internal.get(sheet_h, sheet_h)
            v = row_internal.get(internal, "")
            if isinstance(v, (pd.Series, pd.DataFrame)):
                v = ""
            if pd.isna(v) if not isinstance(v, str) else False:
                v = ""
            values.append(v)
        data.append({"range": f"A{rownum}:{last_col_letter}{rownum}", "values": [values]})

    _with_backoff(lambda: ws.batch_update(data, value_input_option="USER_ENTERED"))
    _read_sheet_values_cached.clear()


def _delete_rows_by_filter(worksheet_name: str, internal_headers: list[str], filter_fn):
    spreadsheet_id = st.secrets["spreadsheet_id"]
    ws = _get_or_create_ws(spreadsheet_id, worksheet_name, internal_headers)
    _ensure_headers(ws, internal_headers)
    values = _with_backoff(lambda: ws.get_all_values())
    if not values or len(values) <= 1:
        return 0

    df, headers = _sheet_to_df(values, internal_headers)
    rownums_to_delete = []
    for idx, row in df.iterrows():
        if filter_fn(row):
            # idx 0 corresponds to sheet row 2
            rownums_to_delete.append(idx + 2)

    for rownum in sorted(rownums_to_delete, reverse=True):
        _with_backoff(lambda rn=rownum: ws.delete_rows(rn))

    if rownums_to_delete:
        _read_sheet_values_cached.clear()
    return len(rownums_to_delete)


def _row_from_sheet_values(values: list[list[str]], rownum: int) -> dict:
    if not values or rownum is None or rownum < 2:
        return {}

    headers = values[0]
    row_vals = values[rownum - 1] if len(values) >= rownum else []
    if len(row_vals) < len(headers):
        row_vals = row_vals + [""] * (len(headers) - len(row_vals))
    elif len(row_vals) > len(headers):
        row_vals = row_vals[:len(headers)]

    out = {sheet_header_to_internal(h): v for h, v in zip(headers, row_vals)}
    out = _coalesce_duplicate_columns(pd.DataFrame([out])).iloc[0].to_dict()
    return out


# =========================================================
# BUSINESS LOGIC HELPERS
# =========================================================


def get_active_show_inventory(inv_df: pd.DataFrame) -> pd.DataFrame:
    if inv_df.empty:
        return inv_df.copy()

    df = inv_df.copy()
    df["inventory_status_norm"] = df["inventory_status"].apply(_normalize_status)
    df["inventory_type_norm"] = df["inventory_type"].apply(_normalize_inventory_type)

    df = df[
        (df["inventory_status_norm"] == STATUS_ACTIVE)
        & (df["inventory_type_norm"] == "showinventory")
    ].copy()

    df["total_cost"] = _coerce_money_series(df["total_cost"])
    df["market_value_resolved"] = _coerce_money_series(df.get("market_value_resolved", df.get("market_value", 0.0)))

    return df


def _snapshot_totals(show_inv_df: pd.DataFrame) -> tuple[int, float, float]:
    if show_inv_df.empty:
        return 0, 0.0, 0.0
    item_count = int(len(show_inv_df))
    total_cost = float(round(_coerce_money_series(show_inv_df["total_cost"]).sum(), 2))
    total_market = float(round(_coerce_money_series(show_inv_df["market_value_resolved"]).sum(), 2))
    return item_count, total_cost, total_market


def build_snapshot_rows(show_row: dict, show_inv_df: pd.DataFrame) -> list[dict]:
    now_iso = _utc_now_iso()
    rows = []

    for _, inv in show_inv_df.iterrows():
        market_value = _money_float(inv.get("market_value_resolved", inv.get("market_value", inv.get("market_price", 0.0))))
        rows.append({
            "snapshot_id": str(uuid.uuid4()),
            "show_id": show_row["show_id"],
            "show_name": show_row["show_name"],
            "show_date": show_row["show_date"],
            "snapshot_created_at": now_iso,
            "inventory_id": str(inv.get("inventory_id", "")).strip(),
            "product_type": _clean_text(inv.get("product_type")),
            "sealed_product_type": _clean_text(inv.get("sealed_product_type")),
            "card_type": _clean_text(inv.get("card_type")),
            "inventory_type": _clean_text(inv.get("inventory_type")),
            "brand_or_league": _clean_text(inv.get("brand_or_league")),
            "set_name": _clean_text(inv.get("set_name")),
            "year": _clean_text(inv.get("year")),
            "card_name": _clean_text(inv.get("card_name")),
            "card_number": _clean_text(inv.get("card_number")),
            "variant": _clean_text(inv.get("variant")),
            "card_subtype": _clean_text(inv.get("card_subtype")),
            "grading_company": _clean_text(inv.get("grading_company")),
            "grade": _clean_text(inv.get("grade")),
            "reference_link": _clean_text(inv.get("reference_link")),
            "image_url": _clean_text(inv.get("image_url")),
            "purchase_date": _clean_text(inv.get("purchase_date")),
            "purchased_from": _clean_text(inv.get("purchased_from")),
            "purchase_price": _money_float(inv.get("purchase_price")),
            "shipping": _money_float(inv.get("shipping")),
            "tax": _money_float(inv.get("tax")),
            "total_price": _money_float(inv.get("total_price")),
            "grading_fee": _money_float(inv.get("grading_fee")),
            "total_cost": _money_float(inv.get("total_cost")),
            "market_price": _money_float(inv.get("market_price")),
            "market_value": market_value,
            "sticker_price": 0.0,
            "condition": _clean_text(inv.get("condition")),
            "inventory_status_at_snapshot": _clean_text(inv.get("inventory_status")),
            "sold_price": 0.0,
            "synced_at": "",
            "synced_transaction_id": "",
        })

    return rows


def _choose_next_show(shows_df: pd.DataFrame) -> pd.Series | None:
    if shows_df.empty:
        return None

    df = shows_df.copy()
    df["_date"] = pd.to_datetime(df["show_date"], errors="coerce")
    df["_status"] = df["status"].astype(str).str.strip().str.lower()
    today_ts = pd.Timestamp(date.today())

    active_future = df[
        (df["_date"].notna())
        & (df["_date"] >= today_ts)
        & (~df["_status"].isin(["completed", "cancelled", "canceled"]))
    ].copy()

    if not active_future.empty:
        return active_future.sort_values(["_date", "show_name"]).iloc[0]

    # Fallback: most recent non-cancelled show. This keeps the page usable
    # after the show date has passed but before you mark it completed.
    active_any = df[(~df["_status"].isin(["cancelled", "canceled"])) & df["_date"].notna()].copy()
    if not active_any.empty:
        return active_any.sort_values(["_date", "show_name"], ascending=[False, True]).iloc[0]

    return None


def _build_sales_editor_df(show: pd.Series, snapshots_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    show_id = str(show.get("show_id", "")).strip()
    show_snaps = snapshots_df[snapshots_df["show_id"].astype(str).str.strip() == show_id].copy()

    if show_snaps.empty:
        # Fallback for old shows or if snapshot sheet was cleared.
        base = get_active_show_inventory(inv_df).copy()
        base["market_value"] = base.get("market_value_resolved", base.get("market_value", 0.0))
        base["sold_price"] = 0.0
    else:
        base = show_snaps.copy()

    if base.empty:
        return pd.DataFrame(columns=["inventory_id", "card_name", "purchase_price", "sell_price"])

    # Merge in current inventory status so we do not sell something already sold/listed.
    current = inv_df[["inventory_id", "inventory_status", "inventory_type", "listed_transaction_id"]].copy()
    current["inventory_id"] = current["inventory_id"].astype(str).str.strip()
    base["inventory_id"] = base["inventory_id"].astype(str).str.strip()
    base = base.merge(current, on="inventory_id", how="left", suffixes=("", "_current"))

    status_col = "inventory_status_current" if "inventory_status_current" in base.columns else "inventory_status"
    type_col = "inventory_type_current" if "inventory_type_current" in base.columns else "inventory_type"

    base["current_status"] = base[status_col].apply(_normalize_status)
    base["current_inventory_type"] = base[type_col].apply(_normalize_inventory_type)

    base = base[
        (base["current_status"] == STATUS_ACTIVE)
        & (base["current_inventory_type"] == "showinventory")
    ].copy()

    base["sell_price"] = _coerce_money_series(base.get("sold_price", pd.Series(0.0, index=base.index)))

    # Keep the sales-entry table simple, but include inventory_id for safe sync/upload.
    out_cols = [
        "inventory_id",
        "card_name",
        "set_name",
        "variant",
        "card_number",
        "purchase_price",
        "total_cost",
        "market_value",
        "sell_price",
    ]
    for c in out_cols:
        if c not in base.columns:
            base[c] = "" if c not in ["purchase_price", "total_cost", "market_value", "sell_price"] else 0.0

    out = base[out_cols].copy()
    for c in ["purchase_price", "total_cost", "market_value", "sell_price"]:
        out[c] = _coerce_money_series(out[c])

    return out.sort_values(["card_name", "set_name", "inventory_id"], na_position="last").reset_index(drop=True)



def _build_pricing_editor_df(show: pd.Series, snapshots_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    show_id = str(show.get("show_id", "")).strip()
    show_snaps = snapshots_df[snapshots_df["show_id"].astype(str).str.strip() == show_id].copy()

    if show_snaps.empty:
        return pd.DataFrame(columns=[
            "inventory_id",
            "card_name",
            "set_name",
            "variant",
            "card_number",
            "total_cost",
            "market_value",
            "sticker_price",
        ])

    base = show_snaps.copy()

    current = inv_df[["inventory_id", "inventory_status", "inventory_type"]].copy()
    current["inventory_id"] = current["inventory_id"].astype(str).str.strip()
    base["inventory_id"] = base["inventory_id"].astype(str).str.strip()
    base = base.merge(current, on="inventory_id", how="left", suffixes=("", "_current"))

    status_col = "inventory_status_current" if "inventory_status_current" in base.columns else "inventory_status"
    type_col = "inventory_type_current" if "inventory_type_current" in base.columns else "inventory_type"

    base["current_status"] = base[status_col].apply(_normalize_status)
    base["current_inventory_type"] = base[type_col].apply(_normalize_inventory_type)

    base = base[
        (base["current_status"] == STATUS_ACTIVE)
        & (base["current_inventory_type"] == "showinventory")
    ].copy()

    base["sticker_price"] = _coerce_money_series(base.get("sticker_price", pd.Series(0.0, index=base.index)))

    out_cols = [
        "inventory_id",
        "card_name",
        "set_name",
        "variant",
        "card_number",
        "total_cost",
        "market_value",
        "sticker_price",
    ]
    for c in out_cols:
        if c not in base.columns:
            base[c] = "" if c not in ["total_cost", "market_value", "sticker_price"] else 0.0

    out = base[out_cols].copy()
    for c in ["total_cost", "market_value", "sticker_price"]:
        out[c] = _coerce_money_series(out[c])

    return out.sort_values(["card_name", "set_name", "inventory_id"], na_position="last").reset_index(drop=True)


def _build_pricing_template_bytes(pricing_df: pd.DataFrame) -> tuple[bytes, str, str]:
    export_cols = [
        "inventory_id",
        "card_name",
        "set_name",
        "variant",
        "card_number",
        "total_cost",
        "market_value",
        "sticker_price",
    ]
    df = pricing_df.copy()
    for c in export_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[export_cols]

    try:
        import xlsxwriter  # noqa: F401
        engine = "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa: F401
            engine = "openpyxl"
        except Exception:
            engine = ""

    if engine:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine=engine) as writer:
            df.to_excel(writer, index=False, sheet_name="show_pricing")
        output.seek(0)
        return (
            output.getvalue(),
            "show_pricing_template.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return df.to_csv(index=False).encode("utf-8"), "show_pricing_template.csv", "text/csv"


def _read_pricing_upload(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()

    file_name = (uploaded_file.name or "").lower()
    if file_name.endswith(".csv"):
        raw = pd.read_csv(uploaded_file)
    else:
        try:
            raw = pd.read_excel(uploaded_file, engine="openpyxl")
        except Exception as exc:
            raise ValueError(f"Could not read Excel upload. Try saving as CSV. Error: {exc}")

    if raw.empty:
        return pd.DataFrame()

    rename = {}
    for col in raw.columns:
        internal = sheet_header_to_internal(col)
        if internal in {"inventory_id", "sticker_price"}:
            rename[col] = internal
        elif _norm_header(col) in {"stickerprice", "price_sticker"}:
            rename[col] = "sticker_price"

    df = raw.rename(columns=rename).copy()

    if "inventory_id" not in df.columns:
        raise ValueError("Upload must include inventory_id. Use the export template so each sticker price maps to the right item.")
    if "sticker_price" not in df.columns:
        raise ValueError("Upload must include sticker_price / Sticker Price.")

    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    df["sticker_price"] = _coerce_money_series(df["sticker_price"])
    df = df[df["inventory_id"] != ""].copy()
    return df[["inventory_id", "sticker_price"]]


def build_show_sticker_price_lookup(show: pd.Series | None, snapshots_df: pd.DataFrame, inv_df: pd.DataFrame) -> dict[str, float]:
    if show is None or snapshots_df.empty or inv_df.empty:
        return {}

    show_id = _clean_text(show.get("show_id"))
    if not show_id:
        return {}

    snaps = snapshots_df[snapshots_df["show_id"].astype(str).str.strip() == show_id].copy()
    if snaps.empty:
        return {}

    active_show_ids = set(get_active_show_inventory(inv_df)["inventory_id"].astype(str).str.strip().tolist())
    snaps["inventory_id"] = snaps["inventory_id"].astype(str).str.strip()
    snaps = snaps[snaps["inventory_id"].isin(active_show_ids)].copy()
    if snaps.empty:
        return {}

    snaps["sticker_price"] = _coerce_money_series(snaps.get("sticker_price", pd.Series(0.0, index=snaps.index)))
    snaps = snaps.drop_duplicates(subset=["inventory_id"], keep="last")

    return {str(r["inventory_id"]): float(r["sticker_price"]) for _, r in snaps.iterrows()}


def sync_show_pricing(show: pd.Series, edited_pricing_df: pd.DataFrame, inv_df: pd.DataFrame) -> tuple[int, list[str]]:
    if edited_pricing_df.empty:
        return 0, ["No pricing rows found."]

    pricing = edited_pricing_df.copy()
    pricing["inventory_id"] = pricing["inventory_id"].astype(str).str.strip()
    pricing["sticker_price"] = _coerce_money_series(pricing["sticker_price"])
    pricing = pricing[pricing["inventory_id"] != ""].copy()

    if pricing.empty:
        return 0, ["No pricing rows were found to sync."]

    pricing = pricing.drop_duplicates(subset=["inventory_id"], keep="last")

    spreadsheet_id = st.secrets["spreadsheet_id"]
    snap_ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)
    snap_ws = _get_or_create_ws(spreadsheet_id, snap_ws_name, SNAPSHOT_COLUMNS)
    _ensure_headers(snap_ws, SNAPSHOT_COLUMNS)
    snap_values = _with_backoff(lambda: snap_ws.get_all_values())

    if not snap_values or len(snap_values) <= 1:
        return 0, ["No show snapshot rows exist yet. Create or refresh the show snapshot first."]

    current_lookup = (
        inv_df[["inventory_id", "inventory_status", "inventory_type"]]
        .copy()
        .assign(inventory_id=lambda d: d["inventory_id"].astype(str).str.strip())
        .drop_duplicates(subset=["inventory_id"], keep="last")
        .set_index("inventory_id")
        .to_dict("index")
    )

    snap_df, _ = _sheet_to_df(snap_values, SNAPSHOT_COLUMNS)
    snap_row_lookup = {}
    show_id = _clean_text(show.get("show_id"))
    for idx, snap_row in snap_df.iterrows():
        key = (_clean_text(snap_row.get("show_id")), _clean_text(snap_row.get("inventory_id")))
        if key[0] and key[1]:
            snap_row_lookup[key] = idx + 2

    updates = []
    warnings = []

    for _, price_row in pricing.iterrows():
        inv_id = _clean_text(price_row.get("inventory_id"))
        sticker_price = float(round(_money_float(price_row.get("sticker_price")), 2))

        current = current_lookup.get(inv_id)
        if not current:
            warnings.append(f"Skipped {inv_id}: inventory row not found.")
            continue

        current_status = _normalize_status(current.get("inventory_status", STATUS_ACTIVE))
        current_type = _normalize_inventory_type(current.get("inventory_type"))
        if current_status != STATUS_ACTIVE:
            warnings.append(f"Skipped {inv_id}: inventory status is {current_status}, not ACTIVE.")
            continue
        if current_type != "showinventory":
            warnings.append(f"Skipped {inv_id}: Inventory Type is not Show Inventory.")
            continue

        snap_rownum = snap_row_lookup.get((show_id, inv_id))
        if not snap_rownum:
            warnings.append(f"Skipped {inv_id}: no snapshot row found for this show. Refresh the show snapshot first.")
            continue

        snap_rec = _row_from_sheet_values(snap_values, snap_rownum)
        snap_rec["sticker_price"] = sticker_price
        updates.append((snap_rownum, snap_rec))

    if not updates:
        return 0, warnings or ["No sticker price rows were synced."]

    _batch_update_full_rows(snap_ws_name, SNAPSHOT_COLUMNS, updates)
    _read_sheet_values_cached.clear()
    return len(updates), warnings


def _build_sales_template_bytes(sales_df: pd.DataFrame) -> tuple[bytes, str, str]:
    export_cols = [
        "inventory_id",
        "card_name",
        "set_name",
        "variant",
        "card_number",
        "purchase_price",
        "total_cost",
        "market_value",
        "sell_price",
    ]
    df = sales_df.copy()
    for c in export_cols:
        if c not in df.columns:
            df[c] = ""
    df = df[export_cols]

    # Prefer Excel. Fall back to CSV if the environment does not have an engine.
    try:
        import xlsxwriter  # noqa: F401
        engine = "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa: F401
            engine = "openpyxl"
        except Exception:
            engine = ""

    if engine:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine=engine) as writer:
            df.to_excel(writer, index=False, sheet_name="show_sales")
        output.seek(0)
        return (
            output.getvalue(),
            "show_sales_template.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return df.to_csv(index=False).encode("utf-8"), "show_sales_template.csv", "text/csv"


def _read_sales_upload(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()

    file_name = (uploaded_file.name or "").lower()
    if file_name.endswith(".csv"):
        raw = pd.read_csv(uploaded_file)
    else:
        try:
            raw = pd.read_excel(uploaded_file, engine="openpyxl")
        except Exception as exc:
            raise ValueError(f"Could not read Excel upload. Try saving as CSV. Error: {exc}")

    if raw.empty:
        return pd.DataFrame()

    rename = {}
    for col in raw.columns:
        internal = sheet_header_to_internal(col)
        if internal in {"inventory_id", "sold_price", "card_name"}:
            rename[col] = "sell_price" if internal == "sold_price" else internal
        elif _norm_header(col) in {"sell_price", "sale_price", "show_sale_price"}:
            rename[col] = "sell_price"

    df = raw.rename(columns=rename).copy()

    if "inventory_id" not in df.columns:
        raise ValueError("Upload must include inventory_id. Use the export template so each sale maps to the right item.")
    if "sell_price" not in df.columns:
        raise ValueError("Upload must include sell_price / Sell Price.")

    df["inventory_id"] = df["inventory_id"].astype(str).str.strip()
    df["sell_price"] = _coerce_money_series(df["sell_price"])
    df = df[df["inventory_id"] != ""].copy()
    return df[["inventory_id", "sell_price"]]


def _tx_row_from_inventory(inv_rec: dict, show: pd.Series, sell_price: float, tx_id: str) -> dict:
    show_name = _clean_text(show.get("show_name"))
    show_date = _date_str(show.get("show_date")) or str(date.today())
    now_iso = _utc_now_iso()

    purchase_total = _money_float(inv_rec.get("total_price"))
    grading_fee_total = _money_float(inv_rec.get("grading_fee"))
    all_in_cost = _money_float(inv_rec.get("total_cost"))
    if all_in_cost <= 0:
        all_in_cost = round(purchase_total + grading_fee_total, 2)

    sold_price = round(float(sell_price or 0.0), 2)
    fees = 0.0
    shipping_charged = 0.0
    fees_total = 0.0
    net_proceeds = round(sold_price - fees - shipping_charged, 2)
    profit = round(net_proceeds - all_in_cost, 2)

    return {
        "transaction_id": tx_id,
        "inventory_id": _clean_text(inv_rec.get("inventory_id")),
        "transaction_type": "Card Show Sale",
        "platform": f"Card Show - {show_name}" if show_name else "Card Show",
        "list_date": show_date,
        "list_price": sold_price,
        "sold_date": show_date,
        "sold_price": sold_price,
        "fees": fees,
        "shipping_charged": shipping_charged,
        "fees_total": fees_total,
        "net_proceeds": net_proceeds,
        "profit": profit,
        "notes": f"{show_name} | show_id={_clean_text(show.get('show_id'))}",
        "status": TX_STATUS_SOLD,
        "created_at": now_iso,
        "updated_at": now_iso,
        "product_type": _clean_text(inv_rec.get("product_type")),
        "sealed_product_type": _clean_text(inv_rec.get("sealed_product_type")),
        "card_type": _clean_text(inv_rec.get("card_type")),
        "brand_or_league": _clean_text(inv_rec.get("brand_or_league")),
        "set_name": _clean_text(inv_rec.get("set_name")),
        "year": _clean_text(inv_rec.get("year")),
        "card_name": _clean_text(inv_rec.get("card_name")),
        "card_number": _clean_text(inv_rec.get("card_number")),
        "variant": _clean_text(inv_rec.get("variant")),
        "card_subtype": _clean_text(inv_rec.get("card_subtype")),
        "reference_link": _clean_text(inv_rec.get("reference_link")),
        "image_url": _clean_text(inv_rec.get("image_url")),
        "purchase_date": _clean_text(inv_rec.get("purchase_date")),
        "purchased_from": _clean_text(inv_rec.get("purchased_from")),
        "purchase_total": purchase_total,
        "grading_fee_total": grading_fee_total,
        "all_in_cost": all_in_cost,
        "grading_company": _clean_text(inv_rec.get("grading_company")),
        "grade": _clean_text(inv_rec.get("grade")),
        "condition": _clean_text(inv_rec.get("condition")),
    }


def sync_show_sales(show: pd.Series, edited_sales_df: pd.DataFrame) -> tuple[int, list[str]]:
    if edited_sales_df.empty:
        return 0, ["No sales rows found."]

    sales = edited_sales_df.copy()
    sales["inventory_id"] = sales["inventory_id"].astype(str).str.strip()
    sales["sell_price"] = _coerce_money_series(sales["sell_price"])
    sales = sales[(sales["inventory_id"] != "") & (sales["sell_price"] > 0)].copy()

    if sales.empty:
        return 0, ["No sell_price values greater than $0 were entered."]

    # One sale per inventory item.
    sales = sales.drop_duplicates(subset=["inventory_id"], keep="last")

    spreadsheet_id = st.secrets["spreadsheet_id"]
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)
    snap_ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)

    inv_ws = _get_or_create_ws(spreadsheet_id, inv_ws_name, INV_COLUMNS)
    tx_ws = _get_or_create_ws(spreadsheet_id, tx_ws_name, TX_COLUMNS)
    snap_ws = _get_or_create_ws(spreadsheet_id, snap_ws_name, SNAPSHOT_COLUMNS)

    inv_headers = _ensure_headers(inv_ws, INV_COLUMNS)
    tx_headers = _ensure_headers(tx_ws, TX_COLUMNS)
    snap_headers = _ensure_headers(snap_ws, SNAPSHOT_COLUMNS)

    inv_values = _with_backoff(lambda: inv_ws.get_all_values())
    snap_values = _with_backoff(lambda: snap_ws.get_all_values())

    inv_ids = sales["inventory_id"].tolist()
    inv_rownums = _find_rownums_by_id(inv_values, "inventory_id", inv_ids)

    # Existing transactions safety: skip if already SOLD in transactions.
    try:
        tx_df = load_transactions_df()
        existing_sold_ids = set(
            tx_df[
                (tx_df["inventory_id"].astype(str).str.strip().isin(inv_ids))
                & (tx_df["status"].astype(str).str.upper() == TX_STATUS_SOLD)
            ]["inventory_id"].astype(str).str.strip().tolist()
        )
    except Exception:
        existing_sold_ids = set()

    tx_rows = []
    inv_updates = []
    snapshot_updates = []
    warnings = []
    now_iso = _utc_now_iso()
    show_id = _clean_text(show.get("show_id"))

    # Build snapshot row lookup by show_id + inventory_id.
    snap_row_lookup = {}
    if snap_values and len(snap_values) > 1:
        snap_df, _ = _sheet_to_df(snap_values, SNAPSHOT_COLUMNS)
        for idx, snap_row in snap_df.iterrows():
            key = (_clean_text(snap_row.get("show_id")), _clean_text(snap_row.get("inventory_id")))
            if key[0] and key[1]:
                snap_row_lookup[key] = idx + 2

    for _, sale in sales.iterrows():
        inv_id = _clean_text(sale.get("inventory_id"))
        sell_price = _money_float(sale.get("sell_price"))

        if inv_id in existing_sold_ids:
            warnings.append(f"Skipped {inv_id}: already has a SOLD transaction.")
            continue

        inv_rownum = inv_rownums.get(inv_id)
        if not inv_rownum:
            warnings.append(f"Skipped {inv_id}: inventory row not found.")
            continue

        inv_rec = _row_from_sheet_values(inv_values, inv_rownum)
        current_status = _normalize_status(inv_rec.get("inventory_status", STATUS_ACTIVE))
        current_type = _normalize_inventory_type(inv_rec.get("inventory_type"))

        if current_status != STATUS_ACTIVE:
            warnings.append(f"Skipped {inv_id}: inventory status is {current_status}, not ACTIVE.")
            continue
        if current_type != "showinventory":
            warnings.append(f"Skipped {inv_id}: Inventory Type is not Show Inventory.")
            continue

        tx_id = str(uuid.uuid4())
        tx_rows.append(_tx_row_from_inventory(inv_rec, show, sell_price, tx_id))

        inv_rec["inventory_status"] = STATUS_SOLD
        inv_rec["listed_transaction_id"] = tx_id
        inv_updates.append((inv_rownum, inv_rec))

        snap_rownum = snap_row_lookup.get((show_id, inv_id))
        if snap_rownum:
            snap_rec = _row_from_sheet_values(snap_values, snap_rownum)
            snap_rec["sold_price"] = float(round(sell_price, 2))
            snap_rec["synced_at"] = now_iso
            snap_rec["synced_transaction_id"] = tx_id
            snapshot_updates.append((snap_rownum, snap_rec))

    if not tx_rows:
        return 0, warnings or ["No valid sales were synced."]

    # Write transactions first, then inventory updates. If inventory update fails, at least
    # the transaction row exists and can be manually reconciled.
    _append_rows(tx_ws_name, TX_COLUMNS, tx_rows)
    _batch_update_full_rows(inv_ws_name, INV_COLUMNS, inv_updates)
    if snapshot_updates:
        _batch_update_full_rows(snap_ws_name, SNAPSHOT_COLUMNS, snapshot_updates)

    _read_sheet_values_cached.clear()
    return len(tx_rows), warnings



# =========================================================
# SHOW RESULTS / DASHBOARD HELPERS
# =========================================================

PRICE_BUCKET_LABELS = [
    "$1-$10",
    "$10-$25",
    "$25-$50",
    "$50-$100",
    "$100-$250",
    "$250-$500",
    "$500-$1,000",
    "$1,000+",
]

PRICE_BUCKET_BINS = [-0.01, 10, 25, 50, 100, 250, 500, 1000, float("inf")]


def _safe_pct(numerator: float, denominator: float) -> float:
    numerator = float(numerator or 0.0)
    denominator = float(denominator or 0.0)
    if denominator == 0:
        return 0.0
    return float(round(numerator / denominator, 4))


def _pct_display(x) -> str:
    try:
        return f"{float(x or 0.0) * 100:.1f}%"
    except Exception:
        return "0.0%"


def _normalize_match_text(x) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(x).lower())


def _extract_show_id_from_notes(notes: str) -> str:
    text = _clean_text(notes)
    if not text:
        return ""
    match = re.search(r"show_id\s*=\s*([A-Za-z0-9_\-]+)", text)
    return match.group(1).strip() if match else ""


def _show_name_from_platform(platform: str) -> str:
    text = _clean_text(platform)
    if not text:
        return ""
    lowered = text.lower()
    if lowered.startswith("card show -"):
        return text.split("-", 1)[1].strip()
    if lowered.startswith("card show:"):
        return text.split(":", 1)[1].strip()
    return ""


def _build_show_lookup_maps(shows_df: pd.DataFrame) -> tuple[dict, dict]:
    by_id = {}
    by_date_name = {}

    if shows_df.empty:
        return by_id, by_date_name

    for _, show in shows_df.iterrows():
        show_id = _clean_text(show.get("show_id"))
        show_name = _clean_text(show.get("show_name"))
        show_date = _date_str(show.get("show_date"))

        if show_id:
            by_id[show_id] = {
                "show_id": show_id,
                "show_name": show_name,
                "show_date": show_date,
            }

        if show_date and show_name:
            by_date_name[(show_date, _normalize_match_text(show_name))] = show_id

    return by_id, by_date_name


def _infer_show_id_for_tx(tx_row: pd.Series, by_date_name: dict) -> str:
    show_id = _extract_show_id_from_notes(tx_row.get("notes", ""))
    if show_id:
        return show_id

    platform_show_name = _show_name_from_platform(tx_row.get("platform", ""))
    if not platform_show_name:
        return ""

    candidate_dates = [
        _date_str(tx_row.get("sold_date")),
        _date_str(tx_row.get("list_date")),
    ]

    normalized_name = _normalize_match_text(platform_show_name)
    for candidate_date in candidate_dates:
        if not candidate_date:
            continue
        matched = by_date_name.get((candidate_date, normalized_name))
        if matched:
            return matched

    return ""


def build_show_sales_detail(tx_df: pd.DataFrame, shows_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a clean transaction-level table for card show sales.

    Primary match:
    - notes contains show_id=...
    Fallback match:
    - platform looks like "Card Show - {show_name}" and sold/list date matches show date.
    """
    out_cols = [
        "show_id",
        "show_name",
        "show_date",
        "transaction_id",
        "inventory_id",
        "card_name",
        "set_name",
        "brand_or_league",
        "product_type",
        "sold_price",
        "all_in_cost",
        "net_proceeds",
        "profit",
        "profit_margin",
        "price_bucket",
    ]

    if tx_df.empty:
        return pd.DataFrame(columns=out_cols)

    df = tx_df.copy()

    for col in ["status", "transaction_type", "platform", "notes"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["sold_price", "fees", "shipping_charged", "fees_total", "net_proceeds", "profit", "purchase_total", "grading_fee_total", "all_in_cost"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = _coerce_money_series(df[col])

    df["status_norm"] = df["status"].astype(str).str.strip().str.upper()
    df["transaction_type_norm"] = df["transaction_type"].astype(str).str.strip().str.lower()
    df["platform_norm"] = df["platform"].astype(str).str.strip().str.lower()
    df["notes_show_id"] = df["notes"].apply(_extract_show_id_from_notes)

    is_card_show_tx = (
        (df["notes_show_id"] != "")
        | df["transaction_type_norm"].str.contains("card show", na=False)
        | df["platform_norm"].str.contains("card show", na=False)
    )

    df = df[
        (df["status_norm"] == TX_STATUS_SOLD)
        & is_card_show_tx
        & (_coerce_money_series(df["sold_price"]) > 0)
    ].copy()

    if df.empty:
        return pd.DataFrame(columns=out_cols)

    by_id, by_date_name = _build_show_lookup_maps(shows_df)

    df["show_id"] = df.apply(lambda r: _infer_show_id_for_tx(r, by_date_name), axis=1)

    def _show_name_for_row(r: pd.Series) -> str:
        show_id = _clean_text(r.get("show_id"))
        if show_id in by_id:
            return by_id[show_id].get("show_name", "")
        from_platform = _show_name_from_platform(r.get("platform", ""))
        return from_platform or "Unmatched Card Show"

    def _show_date_for_row(r: pd.Series) -> str:
        show_id = _clean_text(r.get("show_id"))
        if show_id in by_id:
            return by_id[show_id].get("show_date", "")
        return _date_str(r.get("sold_date")) or _date_str(r.get("list_date"))

    df["show_name"] = df.apply(_show_name_for_row, axis=1)
    df["show_date"] = df.apply(_show_date_for_row, axis=1)

    # Cost basis fallback.
    df["all_in_cost_calc"] = _coerce_money_series(df["all_in_cost"])
    purchase_plus_grading = _coerce_money_series(df["purchase_total"]) + _coerce_money_series(df["grading_fee_total"])
    df["all_in_cost_calc"] = df["all_in_cost_calc"].where(df["all_in_cost_calc"] > 0, purchase_plus_grading)

    # Show sales are usually no fee/no shipping, but this keeps it correct if you add fees later.
    fees_total_calc = _coerce_money_series(df["fees_total"])
    fees_plus_ship = _coerce_money_series(df["fees"]) + _coerce_money_series(df["shipping_charged"])
    fees_total_calc = fees_total_calc.where(fees_total_calc > 0, fees_plus_ship)

    df["net_proceeds_calc"] = (_coerce_money_series(df["sold_price"]) - fees_total_calc).round(2)
    df["profit_calc"] = (df["net_proceeds_calc"] - df["all_in_cost_calc"]).round(2)
    df["profit_margin"] = df.apply(lambda r: _safe_pct(r.get("profit_calc", 0.0), r.get("sold_price", 0.0)), axis=1)

    df["price_bucket"] = pd.cut(
        _coerce_money_series(df["sold_price"]),
        bins=PRICE_BUCKET_BINS,
        labels=PRICE_BUCKET_LABELS,
        include_lowest=True,
        right=True,
    ).astype(str)

    df.loc[df["price_bucket"].isin(["nan", "NaN"]), "price_bucket"] = "Unbucketed"

    for col in ["transaction_id", "inventory_id", "card_name", "set_name", "brand_or_league", "product_type"]:
        if col not in df.columns:
            df[col] = ""

    out = pd.DataFrame({
        "show_id": df["show_id"].astype(str),
        "show_name": df["show_name"].astype(str),
        "show_date": df["show_date"].astype(str),
        "transaction_id": df["transaction_id"].astype(str),
        "inventory_id": df["inventory_id"].astype(str),
        "card_name": df["card_name"].astype(str),
        "set_name": df["set_name"].replace("", "(No set)").astype(str),
        "brand_or_league": df["brand_or_league"].replace("", "(Blank)").astype(str),
        "product_type": df["product_type"].replace("", "(Blank)").astype(str),
        "sold_price": _coerce_money_series(df["sold_price"]),
        "all_in_cost": _coerce_money_series(df["all_in_cost_calc"]),
        "net_proceeds": _coerce_money_series(df["net_proceeds_calc"]),
        "profit": _coerce_money_series(df["profit_calc"]),
        "profit_margin": df["profit_margin"].astype(float),
        "price_bucket": df["price_bucket"].astype(str),
    })

    return out[out_cols].copy()


def build_show_summary_table(shows_df: pd.DataFrame, snapshots_df: pd.DataFrame, sales_detail: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "show_id",
        "show_name",
        "show_date",
        "status",
        "incoming_items",
        "incoming_inventory_cost",
        "incoming_market_value",
        "items_sold",
        "total_sales",
        "profit",
        "profit_margin",
        "sell_through_pct",
        "sales_to_market_pct",
    ]

    if shows_df.empty:
        return pd.DataFrame(columns=cols)

    base = shows_df.copy()
    for c in ["snapshot_item_count", "snapshot_total_cost", "snapshot_total_market_value"]:
        if c not in base.columns:
            base[c] = 0.0
        base[c] = _coerce_money_series(base[c])

    base["incoming_items"] = base["snapshot_item_count"].astype(float)
    base["incoming_inventory_cost"] = base["snapshot_total_cost"].astype(float)
    base["incoming_market_value"] = base["snapshot_total_market_value"].astype(float)

    if not snapshots_df.empty:
        snap = snapshots_df.copy()
        for c in ["total_cost", "market_value"]:
            if c not in snap.columns:
                snap[c] = 0.0
            snap[c] = _coerce_money_series(snap[c])

        snap_group = (
            snap.groupby("show_id", dropna=False)
            .agg(
                snapshot_items_from_rows=("inventory_id", "count"),
                snapshot_cost_from_rows=("total_cost", "sum"),
                snapshot_market_from_rows=("market_value", "sum"),
            )
            .reset_index()
        )

        base = base.merge(snap_group, on="show_id", how="left")

        base["incoming_items"] = base["incoming_items"].where(
            base["incoming_items"] > 0,
            base["snapshot_items_from_rows"].fillna(0),
        )
        base["incoming_inventory_cost"] = base["incoming_inventory_cost"].where(
            base["incoming_inventory_cost"] > 0,
            base["snapshot_cost_from_rows"].fillna(0),
        )
        base["incoming_market_value"] = base["incoming_market_value"].where(
            base["incoming_market_value"] > 0,
            base["snapshot_market_from_rows"].fillna(0),
        )

    if sales_detail.empty:
        base["items_sold"] = 0
        base["total_sales"] = 0.0
        base["profit"] = 0.0
    else:
        sales_group = (
            sales_detail.groupby("show_id", dropna=False)
            .agg(
                items_sold=("inventory_id", "count"),
                total_sales=("sold_price", "sum"),
                profit=("profit", "sum"),
            )
            .reset_index()
        )
        base = base.merge(sales_group, on="show_id", how="left")
        base["items_sold"] = base["items_sold"].fillna(0).astype(int)
        base["total_sales"] = base["total_sales"].fillna(0.0)
        base["profit"] = base["profit"].fillna(0.0)

    base["profit_margin"] = base.apply(lambda r: _safe_pct(r.get("profit", 0.0), r.get("total_sales", 0.0)), axis=1)
    base["sell_through_pct"] = base.apply(lambda r: _safe_pct(r.get("items_sold", 0), r.get("incoming_items", 0)), axis=1)
    base["sales_to_market_pct"] = base.apply(lambda r: _safe_pct(r.get("total_sales", 0.0), r.get("incoming_market_value", 0.0)), axis=1)

    for c in cols:
        if c not in base.columns:
            base[c] = ""

    out = base[cols].copy()
    out["incoming_items"] = _coerce_money_series(out["incoming_items"]).astype(int)
    out["items_sold"] = _coerce_money_series(out["items_sold"]).astype(int)
    out = out.sort_values(["show_date", "show_name"], ascending=[False, True], na_position="last")

    return out


def build_price_bucket_summary(sales_detail: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "price_bucket",
        "items_sold",
        "total_sales",
        "total_cost",
        "profit",
        "profit_margin",
        "avg_sale_price",
    ]

    if sales_detail.empty:
        return pd.DataFrame(columns=cols)

    df = sales_detail.copy()
    df = df[df["sold_price"] > 0].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    grouped = (
        df.groupby("price_bucket", dropna=False)
        .agg(
            items_sold=("inventory_id", "count"),
            total_sales=("sold_price", "sum"),
            total_cost=("all_in_cost", "sum"),
            profit=("profit", "sum"),
            avg_sale_price=("sold_price", "mean"),
        )
        .reset_index()
    )

    order_df = pd.DataFrame({"price_bucket": PRICE_BUCKET_LABELS})
    grouped = order_df.merge(grouped, on="price_bucket", how="left")
    grouped[["items_sold", "total_sales", "total_cost", "profit", "avg_sale_price"]] = grouped[
        ["items_sold", "total_sales", "total_cost", "profit", "avg_sale_price"]
    ].fillna(0.0)

    grouped["items_sold"] = grouped["items_sold"].astype(int)
    grouped["profit_margin"] = grouped.apply(lambda r: _safe_pct(r.get("profit", 0.0), r.get("total_sales", 0.0)), axis=1)

    return grouped[cols].copy()


def build_set_summary(sales_detail: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "set_name",
        "brand_or_league",
        "items_sold",
        "total_sales",
        "total_cost",
        "profit",
        "profit_margin",
        "avg_sale_price",
    ]

    if sales_detail.empty:
        return pd.DataFrame(columns=cols)

    df = sales_detail.copy()
    df["set_name"] = df["set_name"].replace("", "(No set)")
    df["brand_or_league"] = df["brand_or_league"].replace("", "(Blank)")

    grouped = (
        df.groupby(["set_name", "brand_or_league"], dropna=False)
        .agg(
            items_sold=("inventory_id", "count"),
            total_sales=("sold_price", "sum"),
            total_cost=("all_in_cost", "sum"),
            profit=("profit", "sum"),
            avg_sale_price=("sold_price", "mean"),
        )
        .reset_index()
    )

    grouped["profit_margin"] = grouped.apply(lambda r: _safe_pct(r.get("profit", 0.0), r.get("total_sales", 0.0)), axis=1)

    return grouped[cols].sort_values(["total_sales", "items_sold"], ascending=[False, False]).copy()


def build_product_type_summary(sales_detail: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "product_type",
        "items_sold",
        "total_sales",
        "total_cost",
        "profit",
        "profit_margin",
        "avg_sale_price",
    ]

    if sales_detail.empty:
        return pd.DataFrame(columns=cols)

    df = sales_detail.copy()
    df["product_type"] = df["product_type"].replace("", "(Blank)")

    grouped = (
        df.groupby("product_type", dropna=False)
        .agg(
            items_sold=("inventory_id", "count"),
            total_sales=("sold_price", "sum"),
            total_cost=("all_in_cost", "sum"),
            profit=("profit", "sum"),
            avg_sale_price=("sold_price", "mean"),
        )
        .reset_index()
    )

    grouped["profit_margin"] = grouped.apply(lambda r: _safe_pct(r.get("profit", 0.0), r.get("total_sales", 0.0)), axis=1)

    return grouped[cols].sort_values(["total_sales", "items_sold"], ascending=[False, False]).copy()


# =========================================================
# UI
# =========================================================

st.title("Shows")

refresh_col1, refresh_col2 = st.columns([4, 1])
with refresh_col2:
    if st.button("🔄 Refresh", use_container_width=True):
        _read_sheet_values_cached.clear()
        st.rerun()

inv_df = load_inventory_df()
tx_df = load_transactions_df()
shows_df = load_shows_df()
snapshots_df = load_snapshots_df()
show_inv_df = get_active_show_inventory(inv_df)


tab_summary, tab_manage, tab_pricing, tab_sales, tab_results = st.tabs([
    "Show Inventory Summary",
    "Manage Shows",
    "Pricing for the Show",
    "Show Sales Sync",
    "Show Results",
])


# =========================================================
# TAB 1: SHOW INVENTORY SUMMARY
# =========================================================
with tab_summary:
    st.subheader("Show Inventory Summary")
    next_show_for_pricing = _choose_next_show(shows_df)
    if next_show_for_pricing is None:
        st.caption("ACTIVE items where Inventory Type = Show Inventory. Sticker columns populate once a show exists and pricing is synced.")
    else:
        st.caption(
            f"ACTIVE items where Inventory Type = Show Inventory. Sticker columns are pulled from the next show snapshot: "
            f"{_clean_text(next_show_for_pricing.get('show_name'))} ({_date_str(next_show_for_pricing.get('show_date'))})."
        )

    item_count, total_cost, total_market = _snapshot_totals(show_inv_df)

    k1, k2, k3 = st.columns(3)
    k1.metric("# of Items", f"{item_count:,}")
    k2.metric("Total Cost", _money_display(total_cost))
    k3.metric("Total Market Value", _money_display(total_market))

    st.markdown("---")

    if show_inv_df.empty:
        st.info("No ACTIVE Show Inventory found. Set items to Inventory Type = Show Inventory in the Inventory page.")
    else:
        sticker_price_lookup = build_show_sticker_price_lookup(next_show_for_pricing, snapshots_df, inv_df)

        display_cols = [
            "image_url",
            "inventory_id",
            "card_name",
            "set_name",
            "variant",
            "card_number",
            "product_type",
            "purchase_price",
            "total_cost",
            "market_value_resolved",
            "sticker_price",
            "sticker_vs_market",
            "sticker_vs_market_pct",
            "sticker_vs_cost",
            "sticker_vs_cost_pct",
            "condition",
            "reference_link",
        ]
        for c in display_cols:
            if c not in show_inv_df.columns:
                show_inv_df[c] = ""

        display = show_inv_df[display_cols].copy()
        display = display.rename(columns={"market_value_resolved": "market_value"})
        display["inventory_id"] = display["inventory_id"].astype(str).str.strip()
        display["sticker_price"] = display["inventory_id"].map(sticker_price_lookup).fillna(0.0)
        display["sticker_price"] = _coerce_money_series(display["sticker_price"])
        display["sticker_vs_market"] = (display["sticker_price"] - _coerce_money_series(display["market_value"])).round(2)
        display["sticker_vs_cost"] = (display["sticker_price"] - _coerce_money_series(display["total_cost"])).round(2)
        display["sticker_vs_market_pct"] = display.apply(
            lambda r: _pct_display(_safe_pct(r.get("sticker_vs_market", 0.0), r.get("market_value", 0.0))),
            axis=1,
        )
        display["sticker_vs_cost_pct"] = display.apply(
            lambda r: _pct_display(_safe_pct(r.get("sticker_vs_cost", 0.0), r.get("total_cost", 0.0))),
            axis=1,
        )

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            height=700,
            column_config={
                "image_url": st.column_config.ImageColumn("Image", width="small"),
                "reference_link": st.column_config.LinkColumn("Reference"),
                "purchase_price": st.column_config.NumberColumn("Purchase Price", format="$%.2f"),
                "total_cost": st.column_config.NumberColumn("Total Cost", format="$%.2f"),
                "market_value": st.column_config.NumberColumn("Market Value", format="$%.2f"),
                "sticker_price": st.column_config.NumberColumn("Sticker Price", format="$%.2f"),
                "sticker_vs_market": st.column_config.NumberColumn("Sticker vs Market", format="$%.2f"),
                "sticker_vs_cost": st.column_config.NumberColumn("Sticker vs Cost", format="$%.2f"),
                "sticker_vs_market_pct": st.column_config.TextColumn("Sticker vs Market %"),
                "sticker_vs_cost_pct": st.column_config.TextColumn("Sticker vs Cost %"),
            },
        )

        st.download_button(
            "Download Show Inventory CSV",
            data=display.to_csv(index=False).encode("utf-8"),
            file_name="show_inventory_summary.csv",
            mime="text/csv",
            use_container_width=True,
        )


# =========================================================
# TAB 2: MANAGE SHOWS
# =========================================================
with tab_manage:
    st.subheader("Create / Manage Shows")
    st.caption("Creating a show saves a snapshot of the ACTIVE Show Inventory totals and item-level inventory at that moment.")

    with st.form("create_show_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1.5, 1.0, 1.4])
        with c1:
            show_name = st.text_input("Show name*", placeholder="Example: Huntsville Card Show")
        with c2:
            show_date = st.date_input("Show date*", value=date.today())
        with c3:
            location = st.text_input("Location", placeholder="City / venue")

        description = st.text_area("Description / notes", placeholder="Table number, promoter, setup notes, etc.")
        submitted = st.form_submit_button("Create Show + Snapshot Current Show Inventory", type="primary", use_container_width=True)

    if submitted:
        if not show_name.strip():
            st.error("Show name is required.")
        else:
            item_count, total_cost, total_market = _snapshot_totals(show_inv_df)
            now_iso = _utc_now_iso()
            show_id = str(uuid.uuid4())[:8]
            show_row = {
                "show_id": show_id,
                "show_name": show_name.strip(),
                "show_date": str(show_date),
                "location": location.strip(),
                "description": description.strip(),
                "status": "Planned",
                "snapshot_item_count": item_count,
                "snapshot_total_cost": total_cost,
                "snapshot_total_market_value": total_market,
                "snapshot_created_at": now_iso,
                "created_at": now_iso,
                "updated_at": now_iso,
            }

            snap_rows = build_snapshot_rows(show_row, show_inv_df)
            _append_rows(st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT), SHOW_COLUMNS, [show_row])
            _append_rows(st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT), SNAPSHOT_COLUMNS, snap_rows)

            st.success(f"Created {show_name.strip()} and snapshotted {item_count:,} Show Inventory item(s).")
            st.rerun()

    st.markdown("---")
    st.markdown("### Shows")

    if shows_df.empty:
        st.info("No shows created yet.")
    else:
        show_display = shows_df.copy()
        show_display = show_display.sort_values("show_date", ascending=True)
        st.dataframe(
            show_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "snapshot_total_cost": st.column_config.NumberColumn("Snapshot Total Cost", format="$%.2f"),
                "snapshot_total_market_value": st.column_config.NumberColumn("Snapshot Total Market", format="$%.2f"),
            },
        )

        st.markdown("---")
        st.markdown("### Update an existing show")
        st.caption("Use this if you add/remove show inventory after creating a show and want the saved snapshot refreshed.")

        labels = []
        for _, r in show_display.iterrows():
            labels.append(f"{r.get('show_date', '')} — {r.get('show_name', '')} — {r.get('show_id', '')}")

        selected_label = st.selectbox("Select show", labels, index=0)
        selected_show = show_display.iloc[labels.index(selected_label)]
        selected_show_id = _clean_text(selected_show.get("show_id"))

        c1, c2 = st.columns([1, 1])
        with c1:
            new_status = st.selectbox(
                "Show status",
                SHOW_STATUS_OPTIONS,
                index=SHOW_STATUS_OPTIONS.index(selected_show.get("status")) if selected_show.get("status") in SHOW_STATUS_OPTIONS else 0,
            )
            update_status = st.button("Update Show Status", use_container_width=True)
        with c2:
            replace_snapshot = st.button("Replace Snapshot with Current Show Inventory", type="secondary", use_container_width=True)
            st.caption("This deletes the old snapshot rows for this show and saves today’s ACTIVE Show Inventory.")

        if update_status:
            show_ws_name = st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT)
            spreadsheet_id = st.secrets["spreadsheet_id"]
            show_ws = _get_or_create_ws(spreadsheet_id, show_ws_name, SHOW_COLUMNS)
            _ensure_headers(show_ws, SHOW_COLUMNS)
            values = _with_backoff(lambda: show_ws.get_all_values())
            rownums = _find_rownums_by_id(values, "show_id", [selected_show_id])
            rownum = rownums.get(selected_show_id)
            if not rownum:
                st.error("Could not find selected show row to update.")
            else:
                show_rec = _row_from_sheet_values(values, rownum)
                show_rec["status"] = new_status
                show_rec["updated_at"] = _utc_now_iso()
                _batch_update_full_rows(show_ws_name, SHOW_COLUMNS, [(rownum, show_rec)])
                st.success("Show status updated.")
                st.rerun()

        if replace_snapshot:
            deleted = _delete_rows_by_filter(
                st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT),
                SNAPSHOT_COLUMNS,
                lambda r: _clean_text(r.get("show_id")) == selected_show_id,
            )

            item_count, total_cost, total_market = _snapshot_totals(show_inv_df)
            now_iso = _utc_now_iso()
            refreshed_show_row = selected_show.to_dict()
            refreshed_show_row["snapshot_item_count"] = item_count
            refreshed_show_row["snapshot_total_cost"] = total_cost
            refreshed_show_row["snapshot_total_market_value"] = total_market
            refreshed_show_row["snapshot_created_at"] = now_iso
            refreshed_show_row["updated_at"] = now_iso

            snap_rows = build_snapshot_rows(refreshed_show_row, show_inv_df)
            _append_rows(st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT), SNAPSHOT_COLUMNS, snap_rows)

            # Update show totals.
            show_ws_name = st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT)
            spreadsheet_id = st.secrets["spreadsheet_id"]
            show_ws = _get_or_create_ws(spreadsheet_id, show_ws_name, SHOW_COLUMNS)
            _ensure_headers(show_ws, SHOW_COLUMNS)
            values = _with_backoff(lambda: show_ws.get_all_values())
            rownums = _find_rownums_by_id(values, "show_id", [selected_show_id])
            rownum = rownums.get(selected_show_id)
            if rownum:
                show_rec = _row_from_sheet_values(values, rownum)
                show_rec.update({
                    "snapshot_item_count": item_count,
                    "snapshot_total_cost": total_cost,
                    "snapshot_total_market_value": total_market,
                    "snapshot_created_at": now_iso,
                    "updated_at": now_iso,
                })
                _batch_update_full_rows(show_ws_name, SHOW_COLUMNS, [(rownum, show_rec)])

            st.success(f"Snapshot replaced. Deleted {deleted:,} old row(s), saved {item_count:,} current item(s).")
            st.rerun()



# =========================================================
# TAB 3: PRICING FOR THE SHOW
# =========================================================
with tab_pricing:
    pricing_show = _choose_next_show(shows_df)

    if pricing_show is None:
        st.info("Create a show first in the Manage Shows tab.")
    else:
        show_id = _clean_text(pricing_show.get("show_id"))
        show_name = _clean_text(pricing_show.get("show_name"))
        show_date_text = _date_str(pricing_show.get("show_date"))

        st.subheader(f"Pricing for {show_name}")
        st.caption(
            f"Show date: {show_date_text or 'No date'} | Show ID: {show_id}. "
            "Sticker prices save to this show's snapshot so pricing stays show-specific."
        )

        pricing_base = _build_pricing_editor_df(pricing_show, snapshots_df, inv_df)

        if pricing_base.empty:
            st.info("No snapshot inventory is available for this show. Create or refresh the show snapshot in Manage Shows first.")
        else:
            upload_key = f"uploaded_show_pricing_{show_id}"
            editor_key = f"show_pricing_editor_{show_id}"

            uploaded_price_map = st.session_state.get(upload_key, {})
            if uploaded_price_map:
                pricing_base["sticker_price"] = pricing_base.apply(
                    lambda r: uploaded_price_map.get(str(r["inventory_id"]).strip(), r["sticker_price"]),
                    axis=1,
                )

            c1, c2 = st.columns([1, 1])
            with c1:
                pricing_bytes, pricing_file_name, pricing_mime = _build_pricing_template_bytes(pricing_base)
                st.download_button(
                    "Export Show Pricing File",
                    data=pricing_bytes,
                    file_name=pricing_file_name,
                    mime=pricing_mime,
                    use_container_width=True,
                )
            with c2:
                uploaded_pricing_file = st.file_uploader(
                    "Re-upload completed pricing file",
                    type=["xlsx", "csv"],
                    key=f"pricing_upload_{show_id}",
                )

            if uploaded_pricing_file is not None:
                try:
                    uploaded_pricing = _read_pricing_upload(uploaded_pricing_file)
                    if uploaded_pricing.empty:
                        st.warning("Upload did not contain any pricing rows.")
                    else:
                        price_map = {
                            str(r["inventory_id"]).strip(): float(r["sticker_price"] or 0.0)
                            for _, r in uploaded_pricing.iterrows()
                        }
                        st.session_state[upload_key] = price_map
                        st.session_state.pop(editor_key, None)
                        st.success(
                            f"Loaded {len(price_map):,} sticker price row(s) from upload. "
                            "Review below, then sync to save them."
                        )
                        st.rerun()
                except Exception as exc:
                    st.error(str(exc))

            st.markdown("---")
            st.caption("Enter sticker prices below or upload them from Excel/CSV. Sync saves them to the show snapshot.")

            edited_pricing = st.data_editor(
                pricing_base,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                height=700,
                key=editor_key,
                column_order=[
                    "inventory_id",
                    "card_name",
                    "set_name",
                    "variant",
                    "card_number",
                    "total_cost",
                    "market_value",
                    "sticker_price",
                ],
                column_config={
                    "inventory_id": st.column_config.TextColumn("Inventory ID", disabled=True),
                    "card_name": st.column_config.TextColumn("Card Name", disabled=True),
                    "set_name": st.column_config.TextColumn("Set", disabled=True),
                    "variant": st.column_config.TextColumn("Variant", disabled=True),
                    "card_number": st.column_config.TextColumn("Card #", disabled=True),
                    "total_cost": st.column_config.NumberColumn("Total Cost", format="$%.2f", disabled=True),
                    "market_value": st.column_config.NumberColumn("Market Value", format="$%.2f", disabled=True),
                    "sticker_price": st.column_config.NumberColumn("Sticker Price", min_value=0.0, step=1.0, format="$%.2f"),
                },
            )

            priced_count = int((_coerce_money_series(edited_pricing["sticker_price"]) > 0).sum())
            priced_total = float(round(_coerce_money_series(edited_pricing["sticker_price"]).sum(), 2))
            st.caption(f"Current pricing: {priced_count:,} item(s) with a sticker price, {_money_display(priced_total)} total sticker value.")

            st.markdown("---")
            sync_pricing_clicked = st.button("Sync Sticker Prices", type="primary", use_container_width=True)

            if sync_pricing_clicked:
                synced_count, warnings = sync_show_pricing(pricing_show, edited_pricing, inv_df)
                if synced_count > 0:
                    st.success(f"Saved sticker pricing for {synced_count:,} row(s) to the show snapshot.")
                    if upload_key in st.session_state:
                        del st.session_state[upload_key]
                    st.session_state.pop(editor_key, None)
                    _read_sheet_values_cached.clear()
                    if warnings:
                        st.warning("Some rows were skipped:\n- " + "\n- ".join(warnings))
                    st.rerun()
                else:
                    st.error("No sticker prices were synced.")
                    if warnings:
                        st.warning("Details:\n- " + "\n- ".join(warnings))


# =========================================================
# TAB 4: SHOW SALES SYNC
# =========================================================
with tab_sales:
    next_show = _choose_next_show(shows_df)

    if next_show is None:
        st.info("Create a show first in the Manage Shows tab.")
    else:
        show_id = _clean_text(next_show.get("show_id"))
        show_name = _clean_text(next_show.get("show_name"))
        show_date_text = _date_str(next_show.get("show_date"))

        st.subheader(f"Inventory for {show_name} show")
        st.caption(f"Show date: {show_date_text or 'No date'} | Show ID: {show_id}")

        sales_base = _build_sales_editor_df(next_show, snapshots_df, inv_df)

        if sales_base.empty:
            st.info("No ACTIVE Show Inventory is available for this show. If needed, refresh the show snapshot in Manage Shows.")
        else:
            upload_key = f"uploaded_sales_prices_{show_id}"
            editor_key = f"show_sales_editor_{show_id}"

            # Apply uploaded prices to the starting table when present.
            uploaded_price_map = st.session_state.get(upload_key, {})
            if uploaded_price_map:
                sales_base["sell_price"] = sales_base.apply(
                    lambda r: uploaded_price_map.get(str(r["inventory_id"]).strip(), r["sell_price"]),
                    axis=1,
                )

            c1, c2 = st.columns([1, 1])
            with c1:
                sales_bytes, file_name, mime = _build_sales_template_bytes(sales_base)
                st.download_button(
                    "Export Show Inventory for Excel",
                    data=sales_bytes,
                    file_name=file_name,
                    mime=mime,
                    use_container_width=True,
                )
            with c2:
                uploaded_file = st.file_uploader(
                    "Re-upload completed Excel/CSV",
                    type=["xlsx", "csv"],
                    key=f"sales_upload_{show_id}",
                )

            if uploaded_file is not None:
                try:
                    uploaded_sales = _read_sales_upload(uploaded_file)
                    if uploaded_sales.empty:
                        st.warning("Upload did not contain any sale rows.")
                    else:
                        price_map = {
                            str(r["inventory_id"]).strip(): float(r["sell_price"] or 0.0)
                            for _, r in uploaded_sales.iterrows()
                        }
                        st.session_state[upload_key] = price_map
                        # Force the editor to rebuild from the uploaded sell prices.
                        st.session_state.pop(editor_key, None)
                        st.success(f"Loaded {len(price_map):,} sell price row(s) from upload. Review below, then Sync Sales.")
                        st.rerun()
                except Exception as exc:
                    st.error(str(exc))

            st.markdown("---")
            st.caption("Enter sell prices for items sold at the show. Rows with blank/$0 sell_price will not sync.")

            edited_sales = st.data_editor(
                sales_base,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                height=700,
                key=editor_key,
                column_order=[
                    "inventory_id",
                    "card_name",
                    "set_name",
                    "variant",
                    "card_number",
                    "purchase_price",
                    "sell_price",
                ],
                column_config={
                    "inventory_id": st.column_config.TextColumn("Inventory ID", disabled=True),
                    "card_name": st.column_config.TextColumn("Card Name", disabled=True),
                    "set_name": st.column_config.TextColumn("Set", disabled=True),
                    "variant": st.column_config.TextColumn("Variant", disabled=True),
                    "card_number": st.column_config.TextColumn("Card #", disabled=True),
                    "purchase_price": st.column_config.NumberColumn("Purchase Price", format="$%.2f", disabled=True),
                    "sell_price": st.column_config.NumberColumn("Sell Price", min_value=0.0, step=1.0, format="$%.2f"),
                },
            )

            sale_count = int((_coerce_money_series(edited_sales["sell_price"]) > 0).sum())
            sale_total = float(round(_coerce_money_series(edited_sales["sell_price"]).sum(), 2))
            st.caption(f"Pending sync: {sale_count:,} sold item(s), {_money_display(sale_total)} sold price total.")

            st.markdown("---")
            sync_clicked = st.button("Sync Sales", type="primary", use_container_width=True)

            if sync_clicked:
                synced_count, warnings = sync_show_sales(next_show, edited_sales)
                if synced_count > 0:
                    st.success(f"Synced {synced_count:,} sale(s). Inventory marked SOLD and transaction rows created.")
                    if upload_key in st.session_state:
                        del st.session_state[upload_key]
                    st.session_state.pop(editor_key, None)
                    _read_sheet_values_cached.clear()
                    if warnings:
                        st.warning("Some rows were skipped:\n- " + "\n- ".join(warnings))
                    st.rerun()
                else:
                    st.error("No sales were synced.")
                    if warnings:
                        st.warning("Details:\n- " + "\n- ".join(warnings))


# =========================================================
# TAB 5: SHOW RESULTS
# =========================================================
with tab_results:
    st.subheader("Show Results Dashboard")
    st.caption("Summarizes show snapshots, synced card-show sales, profit, price buckets, and best-selling sets.")

    sales_detail = build_show_sales_detail(tx_df, shows_df)
    show_summary_table = build_show_summary_table(shows_df, snapshots_df, sales_detail)

    if shows_df.empty:
        st.info("Create a show first in the Manage Shows tab. Once you sync sales, this dashboard will populate.")
    else:
        show_options = ["All Shows"]
        show_option_to_id = {"All Shows": ""}

        summary_for_options = show_summary_table.copy()
        summary_for_options = summary_for_options.sort_values(["show_date", "show_name"], ascending=[False, True], na_position="last")

        for _, show in summary_for_options.iterrows():
            label = f"{show.get('show_date', '')} — {show.get('show_name', '')}"
            show_id = _clean_text(show.get("show_id"))
            if show_id:
                show_options.append(label)
                show_option_to_id[label] = show_id

        selected_show_label = st.selectbox("View results for", show_options, index=0)
        selected_show_id = show_option_to_id.get(selected_show_label, "")

        if selected_show_id:
            scoped_sales = sales_detail[sales_detail["show_id"].astype(str).str.strip() == selected_show_id].copy()
            scoped_show_summary = show_summary_table[show_summary_table["show_id"].astype(str).str.strip() == selected_show_id].copy()
        else:
            scoped_sales = sales_detail.copy()
            scoped_show_summary = show_summary_table.copy()

        total_sales = float(round(_coerce_money_series(scoped_sales["sold_price"]).sum(), 2)) if not scoped_sales.empty else 0.0
        total_profit = float(round(_coerce_money_series(scoped_sales["profit"]).sum(), 2)) if not scoped_sales.empty else 0.0
        items_sold = int(len(scoped_sales)) if not scoped_sales.empty else 0
        avg_margin = _safe_pct(total_profit, total_sales)

        if selected_show_id and not scoped_show_summary.empty:
            incoming_cost_kpi = float(scoped_show_summary.iloc[0].get("incoming_inventory_cost", 0.0) or 0.0)
            incoming_market_kpi = float(scoped_show_summary.iloc[0].get("incoming_market_value", 0.0) or 0.0)
        else:
            incoming_cost_kpi = float(round(_coerce_money_series(scoped_show_summary["incoming_inventory_cost"]).sum(), 2)) if not scoped_show_summary.empty else 0.0
            incoming_market_kpi = float(round(_coerce_money_series(scoped_show_summary["incoming_market_value"]).sum(), 2)) if not scoped_show_summary.empty else 0.0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Inventory Cost Brought", _money_display(incoming_cost_kpi))
        k2.metric("Inventory Market Value Brought", _money_display(incoming_market_kpi))
        k3.metric("Total Sales", _money_display(total_sales))
        k4.metric("Profit / Margin", f"{_money_display(total_profit)} / {_pct_display(avg_margin)}")

        st.markdown("---")
        st.markdown("### Show-by-Show Performance")

        if show_summary_table.empty:
            st.info("No show summary data yet.")
        else:
            display_summary = show_summary_table.copy()
            for pct_col in ["profit_margin", "sell_through_pct", "sales_to_market_pct"]:
                display_summary[f"{pct_col}_display"] = display_summary[pct_col].apply(_pct_display)

            display_summary = display_summary.rename(columns={
                "show_name": "Show",
                "show_date": "Date",
                "status": "Status",
                "incoming_items": "Inventory Items Brought",
                "incoming_inventory_cost": "Inventory Cost Brought",
                "incoming_market_value": "Inventory Market Value Brought",
                "items_sold": "Items Sold",
                "total_sales": "$ Total Sales",
                "profit": "Profit",
                "profit_margin_display": "Profit Margin",
                "sell_through_pct_display": "Sell-Through %",
                "sales_to_market_pct_display": "Sales / Market Value %",
            })

            summary_cols = [
                "Date",
                "Show",
                "Status",
                "Inventory Items Brought",
                "Inventory Cost Brought",
                "Inventory Market Value Brought",
                "Items Sold",
                "$ Total Sales",
                "Profit",
                "Profit Margin",
                "Sell-Through %",
                "Sales / Market Value %",
            ]

            st.dataframe(
                display_summary[summary_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Inventory Cost Brought": st.column_config.NumberColumn("Inventory Cost Brought", format="$%.2f"),
                    "Inventory Market Value Brought": st.column_config.NumberColumn("Inventory Market Value Brought", format="$%.2f"),
                    "$ Total Sales": st.column_config.NumberColumn("$ Total Sales", format="$%.2f"),
                    "Profit": st.column_config.NumberColumn("Profit", format="$%.2f"),
                },
            )

            st.download_button(
                "Download Show Performance CSV",
                data=show_summary_table.to_csv(index=False).encode("utf-8"),
                file_name="show_performance_summary.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.markdown("---")

        if scoped_sales.empty:
            st.info("No synced sales found for this selection yet. Use the Show Sales Sync tab first.")
        else:
            bucket_summary = build_price_bucket_summary(scoped_sales)
            set_summary = build_set_summary(scoped_sales)
            product_summary = build_product_type_summary(scoped_sales)

            c1, c2 = st.columns([1, 1])
            with c1:
                st.markdown("### Sales by Price Bucket")
                bucket_display = bucket_summary.copy()
                bucket_display["profit_margin_display"] = bucket_display["profit_margin"].apply(_pct_display)
                bucket_display = bucket_display.rename(columns={
                    "price_bucket": "Price Bucket",
                    "items_sold": "# Sold",
                    "total_sales": "$ Sales",
                    "total_cost": "$ Cost",
                    "profit": "$ Profit",
                    "profit_margin_display": "Profit Margin",
                    "avg_sale_price": "Avg Sale",
                })

                st.dataframe(
                    bucket_display[["Price Bucket", "# Sold", "$ Sales", "$ Cost", "$ Profit", "Profit Margin", "Avg Sale"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "$ Sales": st.column_config.NumberColumn("$ Sales", format="$%.2f"),
                        "$ Cost": st.column_config.NumberColumn("$ Cost", format="$%.2f"),
                        "$ Profit": st.column_config.NumberColumn("$ Profit", format="$%.2f"),
                        "Avg Sale": st.column_config.NumberColumn("Avg Sale", format="$%.2f"),
                    },
                )

                chart_bucket = bucket_summary[["price_bucket", "items_sold"]].copy()
                chart_bucket = chart_bucket.set_index("price_bucket")
                st.bar_chart(chart_bucket)

            with c2:
                st.markdown("### Sales by Product Type")
                product_display = product_summary.copy()
                product_display["profit_margin_display"] = product_display["profit_margin"].apply(_pct_display)
                product_display = product_display.rename(columns={
                    "product_type": "Product Type",
                    "items_sold": "# Sold",
                    "total_sales": "$ Sales",
                    "total_cost": "$ Cost",
                    "profit": "$ Profit",
                    "profit_margin_display": "Profit Margin",
                    "avg_sale_price": "Avg Sale",
                })

                st.dataframe(
                    product_display[["Product Type", "# Sold", "$ Sales", "$ Cost", "$ Profit", "Profit Margin", "Avg Sale"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "$ Sales": st.column_config.NumberColumn("$ Sales", format="$%.2f"),
                        "$ Cost": st.column_config.NumberColumn("$ Cost", format="$%.2f"),
                        "$ Profit": st.column_config.NumberColumn("$ Profit", format="$%.2f"),
                        "Avg Sale": st.column_config.NumberColumn("Avg Sale", format="$%.2f"),
                    },
                )

                chart_product = product_summary[["product_type", "total_sales"]].copy()
                chart_product = chart_product.set_index("product_type")
                st.bar_chart(chart_product)

            st.markdown("---")
            st.markdown("### Best-Selling Sets")
            set_display = set_summary.copy()
            set_display["profit_margin_display"] = set_display["profit_margin"].apply(_pct_display)
            set_display = set_display.rename(columns={
                "set_name": "Set",
                "brand_or_league": "Brand / League",
                "items_sold": "# Sold",
                "total_sales": "$ Sales",
                "total_cost": "$ Cost",
                "profit": "$ Profit",
                "profit_margin_display": "Profit Margin",
                "avg_sale_price": "Avg Sale",
            })

            st.dataframe(
                set_display[["Set", "Brand / League", "# Sold", "$ Sales", "$ Cost", "$ Profit", "Profit Margin", "Avg Sale"]],
                use_container_width=True,
                hide_index=True,
                height=420,
                column_config={
                    "$ Sales": st.column_config.NumberColumn("$ Sales", format="$%.2f"),
                    "$ Cost": st.column_config.NumberColumn("$ Cost", format="$%.2f"),
                    "$ Profit": st.column_config.NumberColumn("$ Profit", format="$%.2f"),
                    "Avg Sale": st.column_config.NumberColumn("Avg Sale", format="$%.2f"),
                },
            )

            st.markdown("---")
            st.markdown("### Sold Items Detail")
            detail_display = scoped_sales.copy()
            detail_display["profit_margin_display"] = detail_display["profit_margin"].apply(_pct_display)
            detail_display = detail_display.rename(columns={
                "show_date": "Show Date",
                "show_name": "Show",
                "card_name": "Card Name",
                "set_name": "Set",
                "price_bucket": "Price Bucket",
                "sold_price": "Sold Price",
                "all_in_cost": "All-In Cost",
                "profit": "Profit",
                "profit_margin_display": "Profit Margin",
            })

            detail_cols = [
                "Show Date",
                "Show",
                "Card Name",
                "Set",
                "Price Bucket",
                "Sold Price",
                "All-In Cost",
                "Profit",
                "Profit Margin",
                "inventory_id",
            ]

            st.dataframe(
                detail_display[detail_cols],
                use_container_width=True,
                hide_index=True,
                height=500,
                column_config={
                    "Sold Price": st.column_config.NumberColumn("Sold Price", format="$%.2f"),
                    "All-In Cost": st.column_config.NumberColumn("All-In Cost", format="$%.2f"),
                    "Profit": st.column_config.NumberColumn("Profit", format="$%.2f"),
                },
            )

            st.download_button(
                "Download Sold Items Detail CSV",
                data=scoped_sales.to_csv(index=False).encode("utf-8"),
                file_name="show_sold_items_detail.csv",
                mime="text/csv",
                use_container_width=True,
            )

