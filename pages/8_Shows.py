# pages/4_Shows.py
# ---------------------------------------------------------
# Shows page — inventory-first model (NO show_inventory_snapshots dependency)
#
# Core idea:
# - inventory is the item-level source of truth
# - shows only stores show metadata
# - show sales update inventory rows directly
# - show summaries are derived from inventory purchase_date, sold_date,
#   inventory_type, sale_channel, show_id, and show_name
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
st.title("Shows")

INVENTORY_WS_DEFAULT = "inventory"
SHOWS_WS_DEFAULT = "shows"

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
STATUS_TRADED = "TRADED"
STATUS_GRADING = "GRADING"

SHOW_STATUS_OPTIONS = ["Planned", "Completed", "Cancelled"]
SHOW_READY_STATUSES = {STATUS_ACTIVE, STATUS_LISTED}

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
    "sticker_price",
    "condition",
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
    # Sale / disposition fields stored on inventory.
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
    "sale_channel",
    "sale_notes",
    "show_id",
    "show_name",
    "sold_transaction_id",
    "sold_created_at",
    "sold_updated_at",
    "market_price",
    "market_value",
    "market_price_updated_at",
]

SHOW_COLUMNS = [
    "show_id",
    "show_name",
    "show_date",
    "location",
    "description",
    "status",
    "created_at",
    "updated_at",
]

NUMERIC_INV = [
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "grading_fee",
    "total_cost",
    "sticker_price",
    "market_price",
    "market_value",
    "list_price",
    "sold_price",
    "fees",
    "shipping_charged",
    "fees_total",
    "net_proceeds",
    "profit",
]

HEADER_ALIASES = {
    # IDs / statuses
    "inventory_id": ["inventory_id", "Inventory ID", "inv_id"],
    "show_id": ["show_id", "Show ID"],
    "inventory_status": ["inventory_status", "Inventory Status", "inventoryStatus"],
    "listed_transaction_id": ["listed_transaction_id", "Listed Transaction ID"],
    "status": ["status", "Status", "Show Status", "TX Status", "tx_status"],

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
    "sticker_price": ["sticker_price", "Sticker Price"],
    "grading_company": ["grading_company", "Grading Company"],
    "grade": ["grade", "Grade"],
    "market_price": ["market_price", "Market Price", "Market price", "market price"],
    "market_value": ["market_value", "Market Value", "market value"],
    "market_price_updated_at": ["market_price_updated_at", "Market Price Updated At"],

    # Inventory sale fields
    "transaction_type": ["transaction_type", "Transaction Type", "listing_type"],
    "platform": ["platform", "Platform"],
    "list_date": ["list_date", "List Date", "listed_date"],
    "list_price": ["list_price", "List Price", "listed_price"],
    "sold_date": ["sold_date", "Sold Date", "sale_date", "date"],
    "sold_price": ["sold_price", "Sold Price", "sale_price", "sell_price", "price"],
    "fees": ["fees", "Fees", "platform_fees", "fee"],
    "shipping_charged": ["shipping_charged", "Shipping Charged", "shipping_cost"],
    "fees_total": ["fees_total", "Fees Total", "total_fees", "total_fee"],
    "net_proceeds": ["net_proceeds", "Net Proceeds", "net"],
    "profit": ["profit", "Profit", "Profit/Loss", "profit_loss"],
    "sale_channel": ["sale_channel", "Sale Channel", "sales_channel"],
    "sale_notes": ["sale_notes", "Sale Notes", "notes"],
    "show_name": ["show_name", "Show Name"],
    "sold_transaction_id": ["sold_transaction_id", "Sold Transaction ID", "synced_transaction_id"],
    "sold_created_at": ["sold_created_at", "Sold Created At"],
    "sold_updated_at": ["sold_updated_at", "Sold Updated At", "synced_at"],

    # Shows metadata
    "show_date": ["show_date", "Show Date"],
}


# =========================================================
# SMALL HELPERS
# =========================================================


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
        "sticker_price": "Sticker Price",
        "fees_total": "Fees Total",
        "profit": "Profit",
        "grading_fee": "Grading Fee",
        "total_cost": "Total Cost",
        "grading_company": "Grading Company",
        "show_id": "show_id",
        "show_name": "show_name",
        "show_date": "show_date",
    }
    return defaults.get(internal, internal)


def _money_float(x) -> float:
    try:
        if x is None:
            return 0.0
        try:
            if pd.isna(x):
                return 0.0
        except Exception:
            pass
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


def _parse_date(x):
    parsed = pd.to_datetime(x, errors="coerce")
    if pd.isna(parsed):
        return pd.NaT
    return pd.Timestamp(parsed.date())


def _normalize_status(x) -> str:
    return _clean_text(x).upper()


def _normalize_inventory_type(x) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(x).lower())


def _normalize_channel(x) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean_text(x).lower())


def _normalize_show_key(x) -> str:
    """Stable comparison key for show names. Keeps words separated so similar names do not accidentally collide."""
    s = _clean_text(x).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


# =========================================================
# GOOGLE SHEETS CLIENT + READ/WRITE HELPERS
# =========================================================


def _is_retryable_gspread_error(e: Exception) -> bool:
    try:
        if not isinstance(e, gspread.exceptions.APIError):
            return False
        response = getattr(e, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code in {429, 500, 502, 503, 504}
    except Exception:
        return False


def _with_backoff(fn, tries: int = 6, base_sleep: float = 0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if _is_retryable_gspread_error(e):
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
    first_row = _with_backoff(lambda: ws.row_values(1))
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

    # IMPORTANT:
    # Do NOT overwrite the sheet's Total Cost when it already exists.
    # The Show Performance tab should use the inventory row's Total Cost as the cost basis.
    existing_total_price = _coerce_money_series(df["total_price"])
    calculated_total_price = (
        _coerce_money_series(df["purchase_price"])
        + _coerce_money_series(df["shipping"])
        + _coerce_money_series(df["tax"])
    ).round(2)
    df["total_price"] = existing_total_price.where(existing_total_price > 0, calculated_total_price).round(2)

    existing_total_cost = _coerce_money_series(df["total_cost"])
    calculated_total_cost = (
        _coerce_money_series(df["total_price"])
        + _coerce_money_series(df["grading_fee"])
    ).round(2)
    df["total_cost"] = existing_total_cost.where(existing_total_cost > 0, calculated_total_cost).round(2)

    df["market_value_resolved"] = _coerce_money_series(df["market_value"])
    fallback_market = _coerce_money_series(df["market_price"])
    df["market_value_resolved"] = df["market_value_resolved"].where(df["market_value_resolved"] > 0, fallback_market)

    return df


def load_shows_df() -> pd.DataFrame:
    ws_name = st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT)
    df = _load_sheet_df(ws_name, SHOW_COLUMNS, [])
    if df.empty:
        return df
    df["show_id"] = df["show_id"].astype(str).str.strip()
    df["show_date"] = df["show_date"].apply(_date_str)
    df["status"] = df["status"].replace("", "Planned").fillna("Planned")
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
            try:
                if not isinstance(v, str) and pd.isna(v):
                    v = ""
            except Exception:
                pass
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


def _row_from_sheet_values(values: list[list[str]], rownum: int) -> dict:
    if not values or rownum is None or rownum < 2:
        return {}
    headers = values[0]
    row_vals = values[rownum - 1] if len(values) >= rownum else []
    if len(row_vals) < len(headers):
        row_vals = row_vals + [""] * (len(headers) - len(row_vals))
    elif len(row_vals) > len(headers):
        row_vals = row_vals[:len(headers)]
    return {sheet_header_to_internal(h): v for h, v in zip(headers, row_vals)}


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
            try:
                if not isinstance(v, str) and pd.isna(v):
                    v = ""
            except Exception:
                pass
            values.append(v)
        data.append({"range": f"A{rownum}:{last_col_letter}{rownum}", "values": [values]})

    _with_backoff(lambda: ws.batch_update(data, value_input_option="USER_ENTERED"))
    _read_sheet_values_cached.clear()


def _refresh_all():
    _read_sheet_values_cached.clear()
    st.rerun()


# =========================================================
# BUSINESS LOGIC — INVENTORY-FIRST SHOW MODEL
# =========================================================


def get_current_show_inventory(inv_df: pd.DataFrame) -> pd.DataFrame:
    """Current unsold show inventory for prep / next show."""
    if inv_df.empty:
        return inv_df.copy()
    df = inv_df.copy()
    df["inventory_status_norm"] = df["inventory_status"].apply(_normalize_status)
    df["inventory_type_norm"] = df["inventory_type"].apply(_normalize_inventory_type)
    df["sold_price_num"] = _coerce_money_series(df["sold_price"])
    df["sold_date_clean"] = df["sold_date"].astype(str).str.strip()

    df = df[
        (df["inventory_type_norm"] == "showinventory")
        & (df["inventory_status_norm"].isin(SHOW_READY_STATUSES))
        & (df["sold_price_num"] <= 0)
        & (df["sold_date_clean"] == "")
    ].copy()
    return df


def get_inventory_at_show(show: pd.Series | dict, inv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive show-start inventory without snapshots:
    - Inventory Type must be Show Inventory
    - purchase_date must be on/before show_date
    - card must not have been sold before show_date

    Sold on the show date is included because it was available going into that show.
    """
    if inv_df.empty or show is None:
        return pd.DataFrame(columns=INV_COLUMNS)

    show_ts = _parse_date(show.get("show_date"))
    if pd.isna(show_ts):
        return get_current_show_inventory(inv_df)

    df = inv_df.copy()
    df["inventory_type_norm"] = df["inventory_type"].apply(_normalize_inventory_type)
    df["purchase_dt"] = pd.to_datetime(df["purchase_date"], errors="coerce").dt.normalize()
    df["sold_dt"] = pd.to_datetime(df["sold_date"], errors="coerce").dt.normalize()

    df = df[df["inventory_type_norm"] == "showinventory"].copy()
    df = df[(df["purchase_dt"].isna()) | (df["purchase_dt"] <= show_ts)].copy()
    df = df[(df["sold_dt"].isna()) | (df["sold_dt"] >= show_ts)].copy()
    return df


def get_unsold_inventory_for_show(show: pd.Series | dict, inv_df: pd.DataFrame) -> pd.DataFrame:
    """Rows eligible to sell through the Show Sales Sync page."""
    base = get_inventory_at_show(show, inv_df).copy()
    if base.empty:
        return base
    base["inventory_status_norm"] = base["inventory_status"].apply(_normalize_status)
    base["sold_price_num"] = _coerce_money_series(base["sold_price"])
    base["sold_date_clean"] = base["sold_date"].astype(str).str.strip()
    return base[
        (base["inventory_status_norm"].isin(SHOW_READY_STATUSES))
        & (base["sold_price_num"] <= 0)
        & (base["sold_date_clean"] == "")
    ].copy()


def _add_calculated_sale_metrics(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add report-only calculated sales fields.

    Show Performance should NOT trust the stored profit/net_proceeds columns because
    old migrated/synced rows can contain stale values. For show performance:
      total_sales = sum(sold_price)
      cost_sold   = sum(total_cost)
      profit      = total_sales - cost_sold
    """
    if sales_df.empty:
        return sales_df.copy()

    df = sales_df.copy()
    for c in ["sold_price", "total_cost"]:
        if c not in df.columns:
            df[c] = 0.0

    df["sold_price_num"] = _coerce_money_series(df["sold_price"])
    df["total_cost_num"] = _coerce_money_series(df["total_cost"])
    df["calculated_profit"] = (df["sold_price_num"] - df["total_cost_num"]).round(2)
    return df


def get_sales_for_show(show: pd.Series | dict, inv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return ONLY the inventory rows sold for the selected show.

    Matching rules are intentionally strict to prevent one show's sales from rolling into another:
    1) If the inventory sale row has show_id, it must equal this show's show_id.
    2) Older rows with blank show_id may match by exact show_name + sold_date = show_date.
    3) No date-only fallback. Date-only matching caused cross-show totals.
    """
    if inv_df.empty or show is None:
        return pd.DataFrame(columns=INV_COLUMNS)

    show_id = _clean_text(show.get("show_id"))
    show_name_key = _normalize_show_key(show.get("show_name"))
    show_date = _date_str(show.get("show_date"))

    df = inv_df.copy()
    df["sold_price_num"] = _coerce_money_series(df["sold_price"])
    df["sold_date_norm"] = df["sold_date"].apply(_date_str)
    df["show_id_norm"] = df["show_id"].astype(str).str.strip()
    df["show_name_key"] = df["show_name"].apply(_normalize_show_key)

    sold = df[df["sold_price_num"] > 0].copy()
    if sold.empty:
        return _add_calculated_sale_metrics(sold)

    match = pd.Series(False, index=sold.index)

    if show_id:
        match = match | sold["show_id_norm"].eq(show_id)

    # Fallback only for older/migrated sales rows where show_id was not written.
    # Require show_name AND sold_date to match so recurring shows do not get combined.
    if show_name_key and show_date:
        legacy_name_date_match = (
            sold["show_id_norm"].eq("")
            & sold["show_name_key"].eq(show_name_key)
            & sold["sold_date_norm"].eq(show_date)
        )
        match = match | legacy_name_date_match

    return _add_calculated_sale_metrics(sold[match].copy())


def _build_show_select_labels(shows_df: pd.DataFrame, *, exclude_cancelled: bool = True) -> tuple[list[str], dict[str, str]]:
    if shows_df.empty:
        return [], {}
    df = shows_df.copy()
    df["show_date"] = df["show_date"].apply(_date_str)
    df["status_norm"] = df["status"].astype(str).str.strip().str.lower() if "status" in df.columns else ""
    if exclude_cancelled:
        df = df[~df["status_norm"].isin(["cancelled", "canceled"])].copy()
    if df.empty:
        return [], {}

    df = df.sort_values(["show_date", "show_name"], ascending=[True, True], na_position="last")
    labels = []
    label_to_id = {}
    for _, r in df.iterrows():
        label = f"{_date_str(r.get('show_date'))} — {_clean_text(r.get('show_name'))} — {_clean_text(r.get('show_id'))}"
        labels.append(label)
        label_to_id[label] = _clean_text(r.get("show_id"))
    return labels, label_to_id


def _choose_next_show(shows_df: pd.DataFrame) -> pd.Series | None:
    if shows_df.empty:
        return None
    df = shows_df.copy()
    df["_date"] = pd.to_datetime(df["show_date"], errors="coerce")
    df["_status"] = df["status"].astype(str).str.strip().str.lower()
    today_ts = pd.Timestamp(date.today())
    future = df[(df["_date"].notna()) & (df["_date"] >= today_ts) & (~df["_status"].isin(["completed", "cancelled", "canceled"]))].copy()
    if not future.empty:
        return future.sort_values(["_date", "show_name"]).iloc[0]
    any_show = df[(df["_date"].notna()) & (~df["_status"].isin(["cancelled", "canceled"]))].copy()
    if not any_show.empty:
        return any_show.sort_values(["_date", "show_name"], ascending=[False, True]).iloc[0]
    return None


def _get_show_by_id(shows_df: pd.DataFrame, show_id: str) -> pd.Series | None:
    show_id = _clean_text(show_id)
    if shows_df.empty or not show_id:
        return None
    sub = shows_df[shows_df["show_id"].astype(str).str.strip() == show_id]
    if sub.empty:
        return None
    return sub.iloc[0]


def _inv_label(r: pd.Series) -> str:
    parts = [
        _clean_text(r.get("inventory_id")),
        _clean_text(r.get("year")),
        _clean_text(r.get("set_name")),
        _clean_text(r.get("card_name")),
        _clean_text(r.get("variant")),
        _clean_text(r.get("card_number")),
    ]
    return " — ".join([p for p in parts if p])


def _pricing_editor_df(show: pd.Series | dict, inv_df: pd.DataFrame) -> pd.DataFrame:
    base = get_unsold_inventory_for_show(show, inv_df).copy()
    if base.empty:
        return pd.DataFrame(columns=["inventory_id", "card_name", "set_name", "variant", "card_number", "total_cost", "market_value", "sticker_price"])
    base["market_value"] = _coerce_money_series(base.get("market_value_resolved", base.get("market_value", 0.0)))
    cols = ["inventory_id", "card_name", "set_name", "variant", "card_number", "total_cost", "market_value", "sticker_price"]
    for c in cols:
        if c not in base.columns:
            base[c] = 0.0 if c in ["total_cost", "market_value", "sticker_price"] else ""
    out = base[cols].copy()
    for c in ["total_cost", "market_value", "sticker_price"]:
        out[c] = _coerce_money_series(out[c])
    return out.sort_values(["card_name", "set_name", "inventory_id"], na_position="last").reset_index(drop=True)


def _sales_editor_df(show: pd.Series | dict, inv_df: pd.DataFrame) -> pd.DataFrame:
    base = get_unsold_inventory_for_show(show, inv_df).copy()
    if base.empty:
        return pd.DataFrame(columns=["inventory_id", "card_name", "purchase_price", "sell_price"])
    base["market_value"] = _coerce_money_series(base.get("market_value_resolved", base.get("market_value", 0.0)))
    base["sell_price"] = 0.0
    cols = ["inventory_id", "card_name", "set_name", "variant", "card_number", "sticker_price", "total_cost", "market_value", "sell_price"]
    for c in cols:
        if c not in base.columns:
            base[c] = 0.0 if c in ["sticker_price", "total_cost", "market_value", "sell_price"] else ""
    out = base[cols].copy()
    for c in ["sticker_price", "total_cost", "market_value", "sell_price"]:
        out[c] = _coerce_money_series(out[c])
    return out.sort_values(["card_name", "set_name", "inventory_id"], na_position="last").reset_index(drop=True)


def save_sticker_prices(pricing_df: pd.DataFrame) -> int:
    if pricing_df.empty:
        return 0
    pricing_df = pricing_df.copy()
    pricing_df["inventory_id"] = pricing_df["inventory_id"].astype(str).str.strip()
    pricing_df = pricing_df[pricing_df["inventory_id"] != ""].copy()
    if pricing_df.empty:
        return 0

    spreadsheet_id = st.secrets["spreadsheet_id"]
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    inv_ws = _get_or_create_ws(spreadsheet_id, inv_ws_name, INV_COLUMNS)
    _ensure_headers(inv_ws, INV_COLUMNS)
    values = _with_backoff(lambda: inv_ws.get_all_values())
    rownums = _find_rownums_by_id(values, "inventory_id", pricing_df["inventory_id"].tolist())

    updates = []
    for _, r in pricing_df.iterrows():
        inv_id = _clean_text(r.get("inventory_id"))
        rownum = rownums.get(inv_id)
        if not rownum:
            continue
        rec = _row_from_sheet_values(values, rownum)
        new_sticker = round(_money_float(r.get("sticker_price")), 2)
        if round(_money_float(rec.get("sticker_price")), 2) == new_sticker:
            continue
        rec["sticker_price"] = new_sticker
        updates.append((rownum, rec))

    _batch_update_full_rows(inv_ws_name, INV_COLUMNS, updates)
    return len(updates)


def sync_show_sales(show: pd.Series | dict, sales_df: pd.DataFrame, sale_date: date, default_notes: str = "") -> int:
    if sales_df.empty:
        return 0
    sales_df = sales_df.copy()
    sales_df["inventory_id"] = sales_df["inventory_id"].astype(str).str.strip()
    sales_df["sell_price"] = _coerce_money_series(sales_df["sell_price"])
    sales_df = sales_df[(sales_df["inventory_id"] != "") & (sales_df["sell_price"] > 0)].copy()
    if sales_df.empty:
        return 0

    spreadsheet_id = st.secrets["spreadsheet_id"]
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    inv_ws = _get_or_create_ws(spreadsheet_id, inv_ws_name, INV_COLUMNS)
    _ensure_headers(inv_ws, INV_COLUMNS)
    values = _with_backoff(lambda: inv_ws.get_all_values())
    rownums = _find_rownums_by_id(values, "inventory_id", sales_df["inventory_id"].tolist())

    now_iso = _utc_now_iso()
    show_id = _clean_text(show.get("show_id"))
    show_name = _clean_text(show.get("show_name"))
    sale_date_s = str(sale_date)

    updates = []
    for _, r in sales_df.iterrows():
        inv_id = _clean_text(r.get("inventory_id"))
        rownum = rownums.get(inv_id)
        if not rownum:
            continue

        rec = _row_from_sheet_values(values, rownum)
        sold_price = round(_money_float(r.get("sell_price")), 2)
        total_cost = _money_float(rec.get("total_cost")) or _money_float(rec.get("total_price"))
        fees = 0.0
        shipping_charged = 0.0
        fees_total = 0.0
        net = round(sold_price - fees_total, 2)
        profit = round(net - total_cost, 2)

        rec["inventory_status"] = STATUS_SOLD
        rec["transaction_type"] = ""
        rec["platform"] = ""
        rec["list_date"] = ""
        rec["list_price"] = ""
        rec["sold_date"] = sale_date_s
        rec["sold_price"] = sold_price
        rec["fees"] = fees
        rec["shipping_charged"] = shipping_charged
        rec["fees_total"] = fees_total
        rec["net_proceeds"] = net
        rec["profit"] = profit
        rec["sale_channel"] = "Card Show"
        rec["sale_notes"] = default_notes.strip()
        rec["show_id"] = show_id
        rec["show_name"] = show_name
        rec["sold_transaction_id"] = rec.get("sold_transaction_id") or str(uuid.uuid4())
        rec["sold_updated_at"] = now_iso
        if not _clean_text(rec.get("sold_created_at")):
            rec["sold_created_at"] = now_iso
        updates.append((rownum, rec))

    _batch_update_full_rows(inv_ws_name, INV_COLUMNS, updates)
    return len(updates)


def _show_sales_all(inv_df: pd.DataFrame) -> pd.DataFrame:
    if inv_df.empty:
        return inv_df.copy()
    df = inv_df.copy()
    df["sold_price_num"] = _coerce_money_series(df["sold_price"])
    df["channel_norm"] = df["sale_channel"].apply(_normalize_channel)
    df["show_id_clean"] = df["show_id"].astype(str).str.strip()
    df["show_name_clean"] = df["show_name"].astype(str).str.strip()
    return df[
        (df["sold_price_num"] > 0)
        & (
            (df["show_id_clean"] != "")
            | (df["show_name_clean"] != "")
            | (df["channel_norm"].str.contains("cardshow|show", regex=True))
        )
    ].copy()


def build_show_summary(shows_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build one summary row per show.

    Show sales math is intentionally simple:
      total_sales = sum(inventory.sold_price for that show)
      cost_sold   = sum(inventory.total_cost for those sold rows)
      profit      = total_sales - cost_sold
    """
    rows = []
    if shows_df.empty:
        return pd.DataFrame(rows)

    for _, show in shows_df.iterrows():
        inv_at_show = get_inventory_at_show(show, inv_df)
        sales = get_sales_for_show(show, inv_df)

        total_cost_start = (
            float(round(_coerce_money_series(inv_at_show.get("total_cost", pd.Series(dtype=float))).sum(), 2))
            if not inv_at_show.empty else 0.0
        )
        market_start = (
            float(round(_coerce_money_series(inv_at_show.get("market_value_resolved", inv_at_show.get("market_value", pd.Series(dtype=float)))).sum(), 2))
            if not inv_at_show.empty else 0.0
        )

        sales_total = float(round(sales["sold_price_num"].sum(), 2)) if not sales.empty else 0.0
        cost_sold = float(round(sales["total_cost_num"].sum(), 2)) if not sales.empty else 0.0
        profit = float(round(sales_total - cost_sold, 2))

        count_start = int(len(inv_at_show))
        count_sold = int(len(sales))

        rows.append({
            "show_id": _clean_text(show.get("show_id")),
            "show_name": _clean_text(show.get("show_name")),
            "show_date": _date_str(show.get("show_date")),
            "location": _clean_text(show.get("location")),
            "status": _clean_text(show.get("status")),
            "items_at_show": count_start,
            "cost_at_show": total_cost_start,
            "market_value_at_show": market_start,
            "cards_sold": count_sold,
            "total_sales": sales_total,
            "cost_sold": cost_sold,
            "profit": profit,
            "profit_margin": (profit / sales_total) if sales_total else 0.0,
            "sell_through_count": (count_sold / count_start) if count_start else 0.0,
        })

    return pd.DataFrame(rows)


# =========================================================
# LOAD DATA
# =========================================================


top = st.columns([3, 1])
with top[1]:
    if st.button("🔄 Refresh", use_container_width=True):
        _refresh_all()

inv_df = load_inventory_df()
shows_df = load_shows_df()

# =========================================================
# UI
# =========================================================

tab_summary, tab_manage, tab_pricing, tab_sales, tab_performance = st.tabs([
    "Show Inventory Summary",
    "Manage Shows",
    "Pricing for Show",
    "Sync Show Sales",
    "Show Performance",
])


with tab_summary:
    st.subheader("Current Show Inventory")
    st.caption("This is live inventory only: Inventory Type = Show Inventory, not sold, and status ACTIVE/LISTED. No snapshot tab is used.")
    show_inv = get_current_show_inventory(inv_df)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Items", f"{len(show_inv):,}")
    k2.metric("Total Cost", _money_display(show_inv["total_cost"].sum() if not show_inv.empty else 0))
    market_col = "market_value_resolved" if "market_value_resolved" in show_inv.columns else "market_value"
    k3.metric("Market Value", _money_display(show_inv[market_col].sum() if not show_inv.empty else 0))
    k4.metric("Sticker Value", _money_display(show_inv["sticker_price"].sum() if not show_inv.empty and "sticker_price" in show_inv.columns else 0))

    if show_inv.empty:
        st.info("No current show inventory found.")
    else:
        view_cols = [
            "inventory_id", "card_name", "set_name", "variant", "card_number", "inventory_status",
            "purchase_date", "total_cost", "market_value_resolved", "sticker_price",
        ]
        view_cols = [c for c in view_cols if c in show_inv.columns]
        view = show_inv[view_cols].copy()
        for c in ["total_cost", "market_value_resolved", "sticker_price"]:
            if c in view.columns:
                view[c] = view[c].apply(_money_display)
        st.dataframe(view, use_container_width=True, hide_index=True)


with tab_manage:
    st.subheader("Manage Shows")

    with st.form("create_show_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1.2, 1.6, 2.2])
        with c1:
            show_date = st.date_input("Show date*", value=date.today())
        with c2:
            show_name = st.text_input("Show name*", placeholder="e.g., Sand Mountain Card Show")
        with c3:
            location = st.text_input("Location", placeholder="City / venue")
        description = st.text_area("Description / notes", height=80)
        submitted = st.form_submit_button("Create Show", type="primary", use_container_width=True)
        if submitted:
            if not show_name.strip():
                st.error("Show name is required.")
            else:
                now = _utc_now_iso()
                row = {
                    "show_id": str(uuid.uuid4())[:8],
                    "show_name": show_name.strip(),
                    "show_date": str(show_date),
                    "location": location.strip(),
                    "description": description.strip(),
                    "status": "Planned",
                    "created_at": now,
                    "updated_at": now,
                }
                _append_rows(st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT), SHOW_COLUMNS, [row])
                st.success("Show created.")
                _refresh_all()

    st.markdown("---")
    st.subheader("Existing Shows")
    if shows_df.empty:
        st.info("No shows created yet.")
    else:
        show_table = shows_df.copy().sort_values("show_date", ascending=False)
        st.dataframe(show_table, use_container_width=True, hide_index=True)

        labels, label_to_id = _build_show_select_labels(shows_df, exclude_cancelled=False)
        if labels:
            sel = st.selectbox("Update a show", options=[""] + labels, index=0)
            if sel:
                show = _get_show_by_id(shows_df, label_to_id[sel])
                if show is not None:
                    with st.form("update_show_form"):
                        uc1, uc2, uc3 = st.columns([1.2, 1.6, 1.2])
                        with uc1:
                            new_date = st.date_input("Show date", value=pd.to_datetime(show.get("show_date"), errors="coerce").date() if pd.notna(pd.to_datetime(show.get("show_date"), errors="coerce")) else date.today())
                        with uc2:
                            new_name = st.text_input("Show name", value=_clean_text(show.get("show_name")))
                        with uc3:
                            status_val = _clean_text(show.get("status")) or "Planned"
                            idx = SHOW_STATUS_OPTIONS.index(status_val) if status_val in SHOW_STATUS_OPTIONS else 0
                            new_status = st.selectbox("Status", SHOW_STATUS_OPTIONS, index=idx)
                        new_location = st.text_input("Location", value=_clean_text(show.get("location")))
                        new_description = st.text_area("Description", value=_clean_text(show.get("description")), height=80)
                        update_submit = st.form_submit_button("Update Show", type="primary", use_container_width=True)

                    if update_submit:
                        spreadsheet_id = st.secrets["spreadsheet_id"]
                        shows_ws_name = st.secrets.get("shows_worksheet", SHOWS_WS_DEFAULT)
                        ws = _get_or_create_ws(spreadsheet_id, shows_ws_name, SHOW_COLUMNS)
                        _ensure_headers(ws, SHOW_COLUMNS)
                        values = _with_backoff(lambda: ws.get_all_values())
                        rownum = _find_rownums_by_id(values, "show_id", [_clean_text(show.get("show_id"))]).get(_clean_text(show.get("show_id")))
                        if not rownum:
                            st.error("Could not find that show row in Google Sheets.")
                        else:
                            rec = _row_from_sheet_values(values, rownum)
                            rec.update({
                                "show_name": new_name.strip(),
                                "show_date": str(new_date),
                                "location": new_location.strip(),
                                "description": new_description.strip(),
                                "status": new_status,
                                "updated_at": _utc_now_iso(),
                            })
                            _batch_update_full_rows(shows_ws_name, SHOW_COLUMNS, [(rownum, rec)])
                            st.success("Show updated.")
                            _refresh_all()


with tab_pricing:
    st.subheader("Pricing for Show")
    st.caption("Sticker prices are now stored directly on inventory rows in the Sticker Price column. No snapshot tab is used.")

    if shows_df.empty:
        st.info("Create a show first.")
    else:
        labels, label_to_id = _build_show_select_labels(shows_df)
        next_show = _choose_next_show(shows_df)
        default_idx = 0
        if next_show is not None:
            default_id = _clean_text(next_show.get("show_id"))
            for i, lab in enumerate(labels):
                if label_to_id.get(lab) == default_id:
                    default_idx = i
                    break
        label = st.selectbox("Show", labels, index=default_idx, key="pricing_show_select")
        show = _get_show_by_id(shows_df, label_to_id[label])
        editor_df = _pricing_editor_df(show, inv_df)

        if editor_df.empty:
            st.info("No eligible unsold show inventory for this show.")
        else:
            st.caption(f"{len(editor_df):,} item(s) available to price.")
            edited = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "inventory_id": st.column_config.TextColumn("Inventory ID", disabled=True),
                    "card_name": st.column_config.TextColumn("Card", disabled=True),
                    "set_name": st.column_config.TextColumn("Set", disabled=True),
                    "variant": st.column_config.TextColumn("Variant", disabled=True),
                    "card_number": st.column_config.TextColumn("#", disabled=True),
                    "total_cost": st.column_config.NumberColumn("Total Cost", format="$%.2f", disabled=True),
                    "market_value": st.column_config.NumberColumn("Market", format="$%.2f", disabled=True),
                    "sticker_price": st.column_config.NumberColumn("Sticker Price", min_value=0.0, step=1.0, format="$%.2f"),
                },
                key="pricing_editor",
            )
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                if st.button("Save Sticker Prices", type="primary", use_container_width=True):
                    n = save_sticker_prices(pd.DataFrame(edited))
                    st.success(f"Saved sticker prices for {n:,} row(s).")
                    _refresh_all()
            with c2:
                csv = pd.DataFrame(edited).to_csv(index=False).encode("utf-8")
                st.download_button("Download CSV", data=csv, file_name="show_pricing_template.csv", mime="text/csv", use_container_width=True)

            uploaded = st.file_uploader("Upload completed pricing file", type=["csv", "xlsx"], key="pricing_upload")
            if uploaded is not None:
                try:
                    if uploaded.name.lower().endswith(".csv"):
                        up_df = pd.read_csv(uploaded, dtype=str)
                    else:
                        up_df = pd.read_excel(uploaded, dtype=str)
                    if "inventory_id" not in up_df.columns or "sticker_price" not in up_df.columns:
                        st.error("Upload must include inventory_id and sticker_price columns.")
                    else:
                        up_df["sticker_price"] = up_df["sticker_price"].apply(_money_float)
                        st.dataframe(up_df[["inventory_id", "sticker_price"]].head(100), use_container_width=True, hide_index=True)
                        if st.button("Save Uploaded Sticker Prices", type="primary", use_container_width=True):
                            n = save_sticker_prices(up_df[["inventory_id", "sticker_price"]])
                            st.success(f"Saved uploaded sticker prices for {n:,} row(s).")
                            _refresh_all()
                except Exception as e:
                    st.error(f"Could not read upload: {e}")


with tab_sales:
    st.subheader("Sync Show Sales")
    st.caption("This writes sales directly to inventory: sold_date, sold_price, sale_channel='Card Show', show_id, show_name, net_proceeds, and profit.")

    if shows_df.empty:
        st.info("Create a show first.")
    else:
        labels, label_to_id = _build_show_select_labels(shows_df)
        next_show = _choose_next_show(shows_df)
        default_idx = 0
        if next_show is not None:
            default_id = _clean_text(next_show.get("show_id"))
            for i, lab in enumerate(labels):
                if label_to_id.get(lab) == default_id:
                    default_idx = i
                    break
        label = st.selectbox("Show", labels, index=default_idx, key="sales_show_select")
        show = _get_show_by_id(shows_df, label_to_id[label])
        default_sale_date = pd.to_datetime(show.get("show_date"), errors="coerce")
        if pd.isna(default_sale_date):
            default_sale_date = pd.Timestamp(date.today())
        sale_date = st.date_input("Sale date to write to sold_date", value=default_sale_date.date())
        sale_notes = st.text_input("Sale notes for these rows (optional)", placeholder="e.g., cash/Venmo batch, table sale")

        editor_df = _sales_editor_df(show, inv_df)
        if editor_df.empty:
            st.info("No eligible unsold show inventory for this show.")
        else:
            st.caption("Enter sell prices only for cards that sold. Leave all others at 0.")
            edited_sales = st.data_editor(
                editor_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config={
                    "inventory_id": st.column_config.TextColumn("Inventory ID", disabled=True),
                    "card_name": st.column_config.TextColumn("Card", disabled=True),
                    "set_name": st.column_config.TextColumn("Set", disabled=True),
                    "variant": st.column_config.TextColumn("Variant", disabled=True),
                    "card_number": st.column_config.TextColumn("#", disabled=True),
                    "sticker_price": st.column_config.NumberColumn("Sticker", format="$%.2f", disabled=True),
                    "total_cost": st.column_config.NumberColumn("Total Cost", format="$%.2f", disabled=True),
                    "market_value": st.column_config.NumberColumn("Market", format="$%.2f", disabled=True),
                    "sell_price": st.column_config.NumberColumn("Sell Price", min_value=0.0, step=1.0, format="$%.2f"),
                },
                key="sales_editor",
            )

            sales_preview = pd.DataFrame(edited_sales).copy()
            sales_preview["sell_price"] = _coerce_money_series(sales_preview["sell_price"])
            sales_to_sync = sales_preview[sales_preview["sell_price"] > 0].copy()
            st.metric("Rows ready to sync", f"{len(sales_to_sync):,}")

            c1, c2 = st.columns([1, 2])
            with c1:
                if st.button("Sync Sales to Inventory", type="primary", use_container_width=True, disabled=sales_to_sync.empty):
                    n = sync_show_sales(show, sales_to_sync, sale_date=sale_date, default_notes=sale_notes)
                    st.success(f"Synced {n:,} sale(s) to inventory.")
                    _refresh_all()
            with c2:
                csv = sales_preview.to_csv(index=False).encode("utf-8")
                st.download_button("Download Sales Entry CSV", data=csv, file_name="show_sales_entry.csv", mime="text/csv", use_container_width=True)

            uploaded_sales = st.file_uploader("Upload completed sales file", type=["csv", "xlsx"], key="sales_upload")
            if uploaded_sales is not None:
                try:
                    if uploaded_sales.name.lower().endswith(".csv"):
                        up_sales = pd.read_csv(uploaded_sales, dtype=str)
                    else:
                        up_sales = pd.read_excel(uploaded_sales, dtype=str)
                    if "inventory_id" not in up_sales.columns:
                        st.error("Upload must include inventory_id.")
                    else:
                        sell_col = "sell_price" if "sell_price" in up_sales.columns else "sold_price" if "sold_price" in up_sales.columns else None
                        if not sell_col:
                            st.error("Upload must include sell_price or sold_price.")
                        else:
                            up_sales["sell_price"] = up_sales[sell_col].apply(_money_float)
                            up_sales = up_sales[up_sales["sell_price"] > 0].copy()
                            st.caption(f"{len(up_sales):,} uploaded sale row(s) with sell_price > 0.")
                            st.dataframe(up_sales[["inventory_id", "sell_price"]].head(100), use_container_width=True, hide_index=True)
                            if st.button("Sync Uploaded Sales", type="primary", use_container_width=True, disabled=up_sales.empty):
                                n = sync_show_sales(show, up_sales[["inventory_id", "sell_price"]], sale_date=sale_date, default_notes=sale_notes)
                                st.success(f"Synced {n:,} uploaded sale(s) to inventory.")
                                _refresh_all()
                except Exception as e:
                    st.error(f"Could not read upload: {e}")


with tab_performance:
    st.subheader("Show Performance")
    st.caption("All numbers are derived from inventory rows. No show_inventory_snapshots sheet is used.")

    summary = build_show_summary(shows_df, inv_df)
    if summary.empty:
        st.info("No show data yet.")
    else:
        summary_view = summary.copy().sort_values("show_date", ascending=False)
        for c in ["cost_at_show", "market_value_at_show", "total_sales", "cost_sold", "profit"]:
            summary_view[c] = summary_view[c].apply(_money_display)
        for c in ["profit_margin", "sell_through_count"]:
            summary_view[c] = summary_view[c].apply(lambda x: f"{float(x or 0):.1%}")
        st.dataframe(summary_view, use_container_width=True, hide_index=True)

        labels, label_to_id = _build_show_select_labels(shows_df, exclude_cancelled=False)
        if labels:
            label = st.selectbox("Detail show", labels, key="perf_show_select")
            show = _get_show_by_id(shows_df, label_to_id[label])
            sales = get_sales_for_show(show, inv_df)

            st.markdown("### Sales Detail")
            if sales.empty:
                st.info("No sales recorded for this show yet.")
            else:
                sales_calc = _add_calculated_sale_metrics(sales)
                detail_cols = ["inventory_id", "card_name", "set_name", "variant", "card_number", "sold_date", "sold_price", "total_cost", "calculated_profit"]
                detail_cols = [c for c in detail_cols if c in sales_calc.columns]
                detail = sales_calc[detail_cols].copy()
                detail = detail.rename(columns={"calculated_profit": "profit"})
                for c in ["sold_price", "total_cost", "profit"]:
                    if c in detail.columns:
                        detail[c] = detail[c].apply(_money_display)
                st.dataframe(detail, use_container_width=True, hide_index=True)

                bucket_edges = [0, 10, 25, 50, 100, 250, 500, 1000, float("inf")]
                bucket_labels = ["$1-$10", "$10-$25", "$25-$50", "$50-$100", "$100-$250", "$250-$500", "$500-$1000", "$1000+"]
                sales2 = _add_calculated_sale_metrics(sales)
                sales2["profit_num"] = sales2["calculated_profit"]
                sales2["cost_num"] = sales2["total_cost_num"]
                sales2["bucket"] = pd.cut(sales2["sold_price_num"], bins=bucket_edges, labels=bucket_labels, include_lowest=False, right=True)
                buckets = sales2.groupby("bucket", observed=False).agg(
                    cards_sold=("inventory_id", "count"),
                    total_sales=("sold_price_num", "sum"),
                    total_cost=("cost_num", "sum"),
                    profit=("profit_num", "sum"),
                ).reset_index()
                buckets["profit_margin"] = buckets.apply(lambda r: (r["profit"] / r["total_sales"]) if r["total_sales"] else 0.0, axis=1)
                for c in ["total_sales", "total_cost", "profit"]:
                    buckets[c] = buckets[c].apply(_money_display)
                buckets["profit_margin"] = buckets["profit_margin"].apply(lambda x: f"{float(x or 0):.1%}")
                st.markdown("### Sales by Price Bucket")
                st.dataframe(buckets, use_container_width=True, hide_index=True)

                set_summary = sales2.groupby("set_name", dropna=False).agg(
                    cards_sold=("inventory_id", "count"),
                    total_sales=("sold_price_num", "sum"),
                    profit=("profit_num", "sum"),
                ).reset_index().sort_values("total_sales", ascending=False)
                for c in ["total_sales", "profit"]:
                    set_summary[c] = set_summary[c].apply(_money_display)
                st.markdown("### Sales by Set")
                st.dataframe(set_summary, use_container_width=True, hide_index=True)
