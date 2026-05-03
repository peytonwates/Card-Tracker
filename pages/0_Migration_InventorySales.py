# pages/0_Migration_InventorySales.py
# ---------------------------------------------------------
# One-time migration page for the inventory-first sales model.
#
# It can migrate:
# 1) historical SOLD rows from transactions -> inventory
# 2) historical show_inventory_snapshots rows with sold_price > 0 -> inventory
#
# After snapshot sales are migrated, you can optionally backup and delete the
# show_inventory_snapshots worksheet from the Google Sheet.
# ---------------------------------------------------------

import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Migration - Inventory Sales", layout="wide")
st.title("One-Time Migration — Move Sales to Inventory")

INVENTORY_WS_DEFAULT = "inventory"
TRANSACTIONS_WS_DEFAULT = "transactions"
SHOW_SNAPSHOTS_WS_DEFAULT = "show_inventory_snapshots"
STATUS_SOLD = "SOLD"

SALE_COLUMNS = [
    "sticker_price",
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
]

HEADER_ALIASES = {
    "inventory_id": ["inventory_id", "Inventory ID", "inv_id"],
    "inventory_status": ["inventory_status", "Inventory Status", "status"],
    "transaction_id": ["transaction_id", "Transaction ID"],
    "transaction_type": ["transaction_type", "Transaction Type", "listing_type"],
    "platform": ["platform", "Platform"],
    "list_date": ["list_date", "List Date", "listed_date"],
    "list_price": ["list_price", "List Price", "listed_price", "asking_price"],
    "sold_date": ["sold_date", "Sold Date", "sale_date", "date"],
    "sold_price": ["sold_price", "Sold Price", "sale_price", "sell_price", "price"],
    "fees": ["fees", "Fees", "platform_fees", "fee"],
    "shipping_charged": ["shipping_charged", "Shipping Charged", "shipping", "shipping_cost"],
    "fees_total": ["fees_total", "Fees Total", "total_fees", "total_fee"],
    "net_proceeds": ["net_proceeds", "Net Proceeds", "net"],
    "profit": ["profit", "Profit", "Profit/Loss", "profit_loss"],
    "notes": ["notes", "Notes", "sale_notes", "Sale Notes"],
    "status": ["status", "TX Status", "tx_status", "Status"],
    "purchase_total": ["purchase_total", "Purchase Total", "total_price", "cost_basis"],
    "grading_fee_total": ["grading_fee_total", "Grading Fee", "grading_fee", "total_grading_cost"],
    "all_in_cost": ["all_in_cost", "All In Cost", "total_cost", "all_in"],
    "total_cost": ["total_cost", "Total Cost", "All In Cost", "all_in_cost"],
    "total_price": ["total_price", "Total Price", "Purchase Total", "purchase_total"],
    "show_id": ["show_id", "Show ID"],
    "show_name": ["show_name", "Show Name"],
    "show_date": ["show_date", "Show Date"],
    "synced_at": ["synced_at", "Synced At", "sold_updated_at"],
    "synced_transaction_id": ["synced_transaction_id", "Synced Transaction ID", "sold_transaction_id"],
    "snapshot_id": ["snapshot_id", "Snapshot ID"],
    "sticker_price": ["sticker_price", "Sticker Price"],
}


def _norm_header(s: str) -> str:
    s = str(s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def sheet_header_to_internal(header: str) -> str:
    h = _norm_header(header)
    for internal, aliases in HEADER_ALIASES.items():
        if h in {_norm_header(a) for a in aliases}:
            return internal
    return h


def _money(x) -> float:
    try:
        if x is None:
            return 0.0
        try:
            if pd.isna(x):
                return 0.0
        except Exception:
            pass
        s = str(x).strip()
        if not s:
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


def _clean(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def _date_str(x) -> str:
    d = pd.to_datetime(x, errors="coerce")
    if pd.isna(d):
        return ""
    return str(d.date())


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
        p = Path(st.secrets["service_account_json_path"])
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets: add "gcp_service_account" or "service_account_json_path".')


@st.cache_resource
def get_sheet():
    client = get_gspread_client()
    return _with_backoff(lambda: client.open_by_key(st.secrets["spreadsheet_id"]))


def get_ws(ws_name: str):
    return _with_backoff(lambda: get_sheet().worksheet(ws_name))


def get_ws_optional(ws_name: str):
    try:
        return get_ws(ws_name)
    except gspread.exceptions.WorksheetNotFound:
        return None


def _values_to_df(values: list[list[str]]) -> tuple[pd.DataFrame, list[str]]:
    if not values:
        return pd.DataFrame(), []
    headers = [str(h or "").strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []
    norm_rows = []
    for r in rows:
        if len(r) < len(headers):
            r = r + [""] * (len(headers) - len(r))
        elif len(r) > len(headers):
            r = r[:len(headers)]
        norm_rows.append(r)
    raw = pd.DataFrame(norm_rows, columns=headers)
    raw = raw.rename(columns={h: sheet_header_to_internal(h) for h in raw.columns})

    if raw.columns.duplicated().any():
        out = pd.DataFrame(index=raw.index)
        for col in pd.unique(raw.columns):
            cols = raw.loc[:, raw.columns == col]
            if cols.shape[1] == 1:
                out[col] = cols.iloc[:, 0]
            else:
                out[col] = cols.astype(str).apply(lambda r: next((v for v in r.tolist() if str(v).strip()), ""), axis=1)
        raw = out
    return raw, headers


def ensure_inventory_sale_headers(inv_ws) -> list[str]:
    headers = _with_backoff(lambda: inv_ws.row_values(1))
    if not headers:
        headers = ["inventory_id", "inventory_status"] + SALE_COLUMNS
        _with_backoff(lambda: inv_ws.update("1:1", [headers], value_input_option="USER_ENTERED"))
        return headers

    internal_existing = [sheet_header_to_internal(h) for h in headers]
    missing = [c for c in SALE_COLUMNS if c not in internal_existing]
    if "inventory_status" not in internal_existing:
        missing = ["inventory_status"] + missing
    if missing:
        headers = headers + missing
        _with_backoff(lambda: inv_ws.update("1:1", [headers], value_input_option="USER_ENTERED"))
    return headers


def backup_ws(ws_name: str, prefix: str) -> str:
    sh = get_sheet()
    ws = get_ws(ws_name)
    values = _with_backoff(lambda: ws.get_all_values())
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    title = f"{prefix}_{stamp}"[:95]
    rows = max(len(values) + 10, 100)
    cols = max(len(values[0]) + 5 if values else 20, 20)
    backup = _with_backoff(lambda: sh.add_worksheet(title=title, rows=rows, cols=cols))
    if values:
        _with_backoff(lambda: backup.update("A1", values, value_input_option="RAW"))
    return title


def _row_to_internal(headers: list[str], row_vals: list[str]) -> dict:
    if len(row_vals) < len(headers):
        row_vals = row_vals + [""] * (len(headers) - len(row_vals))
    elif len(row_vals) > len(headers):
        row_vals = row_vals[:len(headers)]
    return {sheet_header_to_internal(h): v for h, v in zip(headers, row_vals)}


def _row_values_from_internal(headers: list[str], record: dict) -> list:
    vals = []
    for h in headers:
        internal = sheet_header_to_internal(h)
        vals.append(record.get(internal, ""))
    return vals


def clean_blank_duplicate_transaction_headers(tx_ws) -> tuple[int, list[str]]:
    values = _with_backoff(lambda: tx_ws.get_all_values())
    if not values or not values[0]:
        return 0, []

    headers = values[0]
    internals = [sheet_header_to_internal(h) for h in headers]
    seen = {}
    delete_cols = []
    for idx, internal in enumerate(internals, start=1):
        if not internal:
            continue
        if internal in seen:
            has_data = any(str(row[idx - 1]).strip() for row in values[1:] if len(row) >= idx)
            if not has_data:
                delete_cols.append(idx)
        else:
            seen[internal] = idx

    deleted = 0
    skipped = []
    for col in sorted(delete_cols, reverse=True):
        try:
            _with_backoff(lambda c=col: tx_ws.delete_columns(c))
            deleted += 1
        except Exception as e:
            header_name = headers[col - 1] if col - 1 < len(headers) else f"Column {col}"
            skipped.append(f"Skipped deleting duplicate blank column {col} ({header_name}): {type(e).__name__}")
            continue
    return deleted, skipped


def _inventory_lookup(inv_df: pd.DataFrame) -> dict[str, int]:
    return {str(r.get("inventory_id", "")).strip(): i for i, r in inv_df.iterrows() if str(r.get("inventory_id", "")).strip()}


def _already_has_sale(inv_row: pd.Series) -> bool:
    return _money(inv_row.get("sold_price")) > 0 or _clean(inv_row.get("sold_date")) != ""


def build_transaction_migration_plan(overwrite_existing: bool = False) -> tuple[pd.DataFrame, list[str]]:
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)

    inv_ws = get_ws(inv_ws_name)
    tx_ws = get_ws(tx_ws_name)
    ensure_inventory_sale_headers(inv_ws)

    inv_df, _ = _values_to_df(_with_backoff(lambda: inv_ws.get_all_values()))
    tx_df, _ = _values_to_df(_with_backoff(lambda: tx_ws.get_all_values()))

    warnings = []
    if inv_df.empty:
        return pd.DataFrame(), ["Inventory sheet is empty."]
    if tx_df.empty:
        return pd.DataFrame(), ["Transactions sheet is empty."]
    if "inventory_id" not in inv_df.columns:
        return pd.DataFrame(), ["Inventory sheet does not have inventory_id."]
    if "inventory_id" not in tx_df.columns:
        return pd.DataFrame(), ["Transactions sheet does not have inventory_id."]

    tx_df["inventory_id"] = tx_df["inventory_id"].astype(str).str.strip()
    tx_df = tx_df[tx_df["inventory_id"] != ""].copy()

    status = tx_df["status"].astype(str).str.upper().str.strip() if "status" in tx_df.columns else pd.Series("", index=tx_df.index)
    sold_price = tx_df["sold_price"].apply(_money) if "sold_price" in tx_df.columns else pd.Series(0.0, index=tx_df.index)
    tx_df = tx_df[(status.eq("SOLD")) | (sold_price > 0)].copy()
    if tx_df.empty:
        return pd.DataFrame(), ["No SOLD transaction rows found."]

    tx_df["__sold_dt"] = pd.to_datetime(tx_df["sold_date"], errors="coerce") if "sold_date" in tx_df.columns else pd.NaT
    tx_df["__row_order"] = range(len(tx_df))
    tx_df = tx_df.sort_values(["inventory_id", "__sold_dt", "__row_order"], na_position="last").drop_duplicates("inventory_id", keep="last")

    inv_by_id = _inventory_lookup(inv_df)
    plan_rows = []
    for _, tx in tx_df.iterrows():
        inv_id = _clean(tx.get("inventory_id"))
        if inv_id not in inv_by_id:
            warnings.append(f"Skipped transaction sale for {inv_id}: no matching inventory row.")
            continue
        inv_row = inv_df.loc[inv_by_id[inv_id]]
        if _already_has_sale(inv_row) and not overwrite_existing:
            warnings.append(f"Skipped transaction sale for {inv_id}: inventory already has sold data.")
            continue

        sp = _money(tx.get("sold_price"))
        fees = _money(tx.get("fees"))
        shipping_charged = _money(tx.get("shipping_charged"))
        fees_total = _money(tx.get("fees_total")) or (fees + shipping_charged)
        net = _money(tx.get("net_proceeds")) or (sp - fees_total if sp > 0 else 0.0)
        all_in = _money(tx.get("all_in_cost")) or _money(inv_row.get("total_cost")) or _money(inv_row.get("total_price"))
        profit = _money(tx.get("profit"))
        if profit == 0 and (sp > 0 or net != 0):
            profit = net - all_in

        platform = _clean(tx.get("platform"))
        tx_type = _clean(tx.get("transaction_type"))
        sale_channel = "Card Show" if "show" in platform.lower() or "show" in tx_type.lower() else ("Trade In" if "trade" in tx_type.lower() else "Online")

        plan_rows.append({
            "source": "transactions",
            "inventory_id": inv_id,
            "sheet_row": int(inv_by_id[inv_id] + 2),
            "transaction_id": _clean(tx.get("transaction_id")),
            "transaction_type": tx_type,
            "platform": platform,
            "list_date": _date_str(tx.get("list_date")),
            "list_price": _money(tx.get("list_price")),
            "sold_date": _date_str(tx.get("sold_date")),
            "sold_price": sp,
            "fees": fees,
            "shipping_charged": shipping_charged,
            "fees_total": fees_total,
            "net_proceeds": net,
            "profit": profit,
            "sale_channel": sale_channel,
            "sale_notes": _clean(tx.get("notes")),
            "show_id": "",
            "show_name": "",
            "sticker_price": "",
        })

    return pd.DataFrame(plan_rows), warnings


def build_snapshot_sales_migration_plan(overwrite_existing: bool = False, sold_date_source: str = "synced_at") -> tuple[pd.DataFrame, list[str]]:
    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    snap_ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)

    inv_ws = get_ws(inv_ws_name)
    snap_ws = get_ws_optional(snap_ws_name)
    ensure_inventory_sale_headers(inv_ws)

    warnings = []
    if snap_ws is None:
        return pd.DataFrame(), [f"Snapshot worksheet '{snap_ws_name}' was not found."]

    inv_df, _ = _values_to_df(_with_backoff(lambda: inv_ws.get_all_values()))
    snap_df, _ = _values_to_df(_with_backoff(lambda: snap_ws.get_all_values()))

    if inv_df.empty:
        return pd.DataFrame(), ["Inventory sheet is empty."]
    if snap_df.empty:
        return pd.DataFrame(), ["Snapshot sheet is empty."]
    if "inventory_id" not in inv_df.columns:
        return pd.DataFrame(), ["Inventory sheet does not have inventory_id."]
    if "inventory_id" not in snap_df.columns:
        return pd.DataFrame(), ["Snapshot sheet does not have inventory_id."]
    if "sold_price" not in snap_df.columns:
        return pd.DataFrame(), ["Snapshot sheet does not have sold_price."]

    snap_df["inventory_id"] = snap_df["inventory_id"].astype(str).str.strip()
    snap_df["sold_price_num"] = snap_df["sold_price"].apply(_money)
    snap_df = snap_df[(snap_df["inventory_id"] != "") & (snap_df["sold_price_num"] > 0)].copy()
    if snap_df.empty:
        return pd.DataFrame(), ["No snapshot rows with sold_price > 0 were found."]

    # If the same inventory_id appears multiple times, keep the latest synced/show row.
    snap_df["__synced_dt"] = pd.to_datetime(snap_df["synced_at"], errors="coerce") if "synced_at" in snap_df.columns else pd.NaT
    snap_df["__show_dt"] = pd.to_datetime(snap_df["show_date"], errors="coerce") if "show_date" in snap_df.columns else pd.NaT
    snap_df["__row_order"] = range(len(snap_df))
    snap_df = snap_df.sort_values(["inventory_id", "__synced_dt", "__show_dt", "__row_order"], na_position="last").drop_duplicates("inventory_id", keep="last")

    inv_by_id = _inventory_lookup(inv_df)
    plan_rows = []
    for _, snap in snap_df.iterrows():
        inv_id = _clean(snap.get("inventory_id"))
        if inv_id not in inv_by_id:
            warnings.append(f"Skipped snapshot sale for {inv_id}: no matching inventory row.")
            continue
        inv_row = inv_df.loc[inv_by_id[inv_id]]
        if _already_has_sale(inv_row) and not overwrite_existing:
            warnings.append(f"Skipped snapshot sale for {inv_id}: inventory already has sold data.")
            continue

        sp = _money(snap.get("sold_price"))
        total_cost = _money(inv_row.get("total_cost")) or _money(snap.get("total_cost")) or _money(inv_row.get("total_price")) or _money(snap.get("total_price"))
        net = sp
        profit = net - total_cost

        sold_date = _date_str(snap.get("synced_at")) if sold_date_source == "synced_at" else _date_str(snap.get("show_date"))
        if not sold_date:
            sold_date = _date_str(snap.get("show_date")) or _date_str(snap.get("synced_at"))

        plan_rows.append({
            "source": "show_inventory_snapshots",
            "inventory_id": inv_id,
            "sheet_row": int(inv_by_id[inv_id] + 2),
            "transaction_id": _clean(snap.get("synced_transaction_id")) or _clean(snap.get("snapshot_id")),
            "transaction_type": "",
            "platform": "",
            "list_date": "",
            "list_price": "",
            "sold_date": sold_date,
            "sold_price": sp,
            "fees": 0.0,
            "shipping_charged": 0.0,
            "fees_total": 0.0,
            "net_proceeds": net,
            "profit": profit,
            "sale_channel": "Card Show",
            "sale_notes": "Migrated from show_inventory_snapshots",
            "show_id": _clean(snap.get("show_id")),
            "show_name": _clean(snap.get("show_name")),
            "sticker_price": _money(snap.get("sticker_price")) if "sticker_price" in snap.index else "",
        })

    return pd.DataFrame(plan_rows), warnings


def build_combined_plan(*, include_transactions: bool, include_snapshots: bool, overwrite_existing: bool, snapshot_sold_date_source: str) -> tuple[pd.DataFrame, list[str]]:
    plans = []
    warnings = []

    if include_transactions:
        p, w = build_transaction_migration_plan(overwrite_existing=overwrite_existing)
        if not p.empty:
            plans.append(p)
        warnings.extend(w)

    if include_snapshots:
        p, w = build_snapshot_sales_migration_plan(overwrite_existing=overwrite_existing, sold_date_source=snapshot_sold_date_source)
        if not p.empty:
            plans.append(p)
        warnings.extend(w)

    if not plans:
        return pd.DataFrame(), warnings

    combined = pd.concat(plans, ignore_index=True)

    # Avoid writing two sources to the same inventory row in one run.
    # Prefer snapshot rows for card-show sales, then transactions.
    priority = {"show_inventory_snapshots": 1, "transactions": 2}
    combined["__priority"] = combined["source"].map(priority).fillna(9)
    combined = combined.sort_values(["inventory_id", "__priority"]).drop_duplicates("inventory_id", keep="first")
    combined = combined.drop(columns=["__priority"])
    return combined, warnings


def run_migration(plan: pd.DataFrame, *, backup_transactions: bool = False, backup_snapshots: bool = True, clean_tx_headers: bool = False):
    if plan.empty:
        return {"updated": 0, "backups": [], "deleted_duplicate_tx_cols": 0, "cleanup_warnings": []}

    inv_ws_name = st.secrets.get("inventory_worksheet", INVENTORY_WS_DEFAULT)
    tx_ws_name = st.secrets.get("transactions_worksheet", TRANSACTIONS_WS_DEFAULT)
    snap_ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)

    inv_ws = get_ws(inv_ws_name)
    backups = [backup_ws(inv_ws_name, "backup_inventory_before_sales_migration")]

    if backup_transactions and "transactions" in set(plan["source"]):
        backups.append(backup_ws(tx_ws_name, "backup_transactions_before_sales_migration"))
    if backup_snapshots and "show_inventory_snapshots" in set(plan["source"]):
        snap_ws = get_ws_optional(snap_ws_name)
        if snap_ws is not None:
            backups.append(backup_ws(snap_ws_name, "backup_snapshots_before_inventory_migration"))

    deleted_dups = 0
    cleanup_warnings = []
    if clean_tx_headers:
        tx_ws = get_ws(tx_ws_name)
        deleted_dups, cleanup_warnings = clean_blank_duplicate_transaction_headers(tx_ws)

    inv_headers = ensure_inventory_sale_headers(inv_ws)
    inv_values = _with_backoff(lambda: inv_ws.get_all_values())
    last_col = gspread.utils.rowcol_to_a1(1, len(inv_headers)).split("1")[0]
    now_iso = datetime.utcnow().isoformat()

    batch = []
    for _, r in plan.iterrows():
        rownum = int(r["sheet_row"])
        raw_vals = inv_values[rownum - 1] if len(inv_values) >= rownum else []
        rec = _row_to_internal(inv_headers, raw_vals)
        rec["inventory_status"] = STATUS_SOLD
        rec["sold_transaction_id"] = _clean(r.get("transaction_id"))
        for col in [
            "transaction_type", "platform", "list_date", "list_price", "sold_date", "sold_price",
            "fees", "shipping_charged", "fees_total", "net_proceeds", "profit", "sale_channel",
            "sale_notes", "show_id", "show_name", "sticker_price",
        ]:
            val = r.get(col, "")
            if col == "sticker_price" and (val == "" or _money(val) <= 0):
                continue
            rec[col] = val
        rec["sold_updated_at"] = now_iso
        if not _clean(rec.get("sold_created_at")):
            rec["sold_created_at"] = now_iso

        batch.append({
            "range": f"A{rownum}:{last_col}{rownum}",
            "values": [_row_values_from_internal(inv_headers, rec)],
        })

    if batch:
        _with_backoff(lambda: inv_ws.batch_update(batch, value_input_option="USER_ENTERED"))

    return {
        "updated": len(batch),
        "backups": backups,
        "deleted_duplicate_tx_cols": deleted_dups,
        "cleanup_warnings": cleanup_warnings,
    }


def backup_and_delete_snapshot_worksheet() -> tuple[bool, str]:
    snap_ws_name = st.secrets.get("show_snapshots_worksheet", SHOW_SNAPSHOTS_WS_DEFAULT)
    sh = get_sheet()
    snap_ws = get_ws_optional(snap_ws_name)
    if snap_ws is None:
        return False, f"Worksheet '{snap_ws_name}' was not found. Nothing to delete."
    backup_title = backup_ws(snap_ws_name, "backup_deleted_show_inventory_snapshots")
    _with_backoff(lambda: sh.del_worksheet(snap_ws))
    return True, f"Backed up '{snap_ws_name}' to '{backup_title}' and deleted the original worksheet."


st.markdown(
    """
This page is meant to cleanly finish the move to the new model where **inventory is the only item-level source of truth**.

It can copy show sales out of `show_inventory_snapshots` into matching `inventory` rows, then you can remove the snapshot worksheet. The old snapshot sheet is not needed by the new Shows page.
"""
)

with st.expander("Migration options", expanded=True):
    c1, c2 = st.columns(2)
    with c1:
        include_snapshots = st.checkbox("Migrate show sales from show_inventory_snapshots", value=True)
        include_transactions = st.checkbox("Also migrate SOLD rows from transactions", value=False)
        overwrite = st.checkbox("Overwrite inventory rows that already have sold data", value=False)
    with c2:
        sold_date_choice = st.selectbox(
            "For snapshot show sales, write sold_date from",
            options=[
                "synced_at date (recommended if the old show_date was wrong)",
                "show_date",
            ],
            index=0,
        )
        snapshot_sold_date_source = "synced_at" if sold_date_choice.startswith("synced_at") else "show_date"
        clean_dupes = st.checkbox("Also remove blank duplicate transaction header columns (optional)", value=False)

if st.button("Preview Migration", use_container_width=True):
    plan, warnings = build_combined_plan(
        include_transactions=include_transactions,
        include_snapshots=include_snapshots,
        overwrite_existing=overwrite,
        snapshot_sold_date_source=snapshot_sold_date_source,
    )
    st.session_state["migration_plan"] = plan
    st.session_state["migration_warnings"] = warnings

plan = st.session_state.get("migration_plan", pd.DataFrame())
warnings = st.session_state.get("migration_warnings", [])

if warnings:
    with st.expander(f"Warnings / skipped rows ({len(warnings):,})", expanded=False):
        for w in warnings[:500]:
            st.write("- " + w)
        if len(warnings) > 500:
            st.write(f"...and {len(warnings)-500:,} more")

if isinstance(plan, pd.DataFrame) and not plan.empty:
    st.success(f"Preview ready: {len(plan):,} inventory rows will be updated.")
    preview = plan.copy()
    for c in ["list_price", "sold_price", "fees", "shipping_charged", "fees_total", "net_proceeds", "profit", "sticker_price"]:
        if c in preview.columns:
            preview[c] = preview[c].apply(lambda x: f"${float(_money(x)):,.2f}" if _clean(x) != "" else "")
    display_cols = [
        "source", "inventory_id", "sold_date", "sold_price", "sale_channel", "show_name",
        "show_id", "profit", "sticker_price", "sheet_row", "transaction_id",
    ]
    display_cols = [c for c in display_cols if c in preview.columns]
    st.dataframe(preview[display_cols], use_container_width=True, hide_index=True)

    st.warning("This creates backup worksheets before writing. Run it once, then confirm the Inventory and Shows pages look right.")
    if st.button("Run Migration Now", type="primary", use_container_width=True):
        result = run_migration(
            plan,
            backup_transactions=include_transactions,
            backup_snapshots=include_snapshots,
            clean_tx_headers=clean_dupes,
        )
        st.success(f"Migration complete. Updated {result['updated']:,} inventory rows.")
        if result["backups"]:
            st.write("Backup worksheets created:")
            for b in result["backups"]:
                st.write(f"- {b}")
        if clean_dupes:
            st.write(f"Blank duplicate transaction columns removed: {result['deleted_duplicate_tx_cols']:,}")
            if result.get("cleanup_warnings"):
                with st.expander("Duplicate-column cleanup warnings", expanded=False):
                    for msg in result.get("cleanup_warnings", []):
                        st.write("- " + msg)
elif isinstance(plan, pd.DataFrame):
    st.info("Click Preview Migration to see what will change.")

st.markdown("---")
st.subheader("After confirming migration: remove the snapshot worksheet")
st.caption("This backs up show_inventory_snapshots first, then deletes the original tab from the Google Sheet.")
confirm_delete = st.checkbox("I have confirmed the snapshot sales are now in inventory and I want to delete show_inventory_snapshots.")
if st.button("Backup and Delete show_inventory_snapshots", type="secondary", use_container_width=True, disabled=not confirm_delete):
    ok, msg = backup_and_delete_snapshot_worksheet()
    if ok:
        st.success(msg)
    else:
        st.info(msg)
