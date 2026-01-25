import json
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

INVENTORY_STATE_KEY = "inventory_df_v8"         # must match Inventory page state key load function (we will load fresh anyway)
TRANSACTIONS_STATE_KEY = "transactions_df_v1"

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"

TX_STATUS_OPEN = "OPEN"
TX_STATUS_SOLD = "SOLD"
TX_STATUS_DELETED = "DELETED"

LISTING_TYPE_OPTIONS = ["Auction", "Buy It Now", "Trade-In"]
PLATFORM_OPTIONS = ["eBay", "Whatnot", "Facebook Marketplace", "Card Show", "Trade-In", "Other"]

DEFAULT_TX_COLUMNS = [
    "transaction_id",
    "inventory_id",
    "image_url",
    "listing_type",
    "platform",
    "list_date",
    "list_price",
    "sold_date",
    "sold_price",
    "fees",
    "net_proceeds",
    "cost_basis",
    "profit",
    "notes",
    "status",
    "created_at",
    "updated_at",
]

NUMERIC_TX_COLS = ["list_price", "sold_price", "fees", "net_proceeds", "cost_basis", "profit"]

# ---- UI sizing for image tables (your request) ----
TABLE_ROW_HEIGHT_PX = 200
TABLE_IMAGE_WIDTH_PX = 190


# =========================================================
# GLOBAL CSS: force bigger rows + bigger images in Streamlit tables
# =========================================================
def inject_table_css():
    st.markdown(
        f"""
        <style>
        div[data-testid="stDataFrame"] .ag-row,
        div[data-testid="stDataEditor"] .ag-row {{
            height: {TABLE_ROW_HEIGHT_PX}px !important;
        }}

        div[data-testid="stDataFrame"] .ag-cell,
        div[data-testid="stDataEditor"] .ag-cell {{
            line-height: normal !important;
            display: flex;
            align-items: center;
        }}

        div[data-testid="stDataFrame"] .ag-cell img,
        div[data-testid="stDataEditor"] .ag-cell img {{
            width: {TABLE_IMAGE_WIDTH_PX}px !important;
            height: auto !important;
            border-radius: 6px;
        }}

        div[data-testid="stDataFrame"] .ag-header-cell[col-id="image_url"],
        div[data-testid="stDataEditor"] .ag-header-cell[col-id="image_url"] {{
            min-width: {TABLE_IMAGE_WIDTH_PX + 40}px !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# GOOGLE SHEETS
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
        rel = st.secrets["service_account_json_path"]
        p = Path(rel)
        if not p.is_absolute():
            p = Path.cwd() / rel
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets. Add "gcp_service_account" (Cloud) OR "service_account_json_path" (local).')


def _canonicalize_header(h: str) -> str:
    if h is None:
        return ""
    raw = str(h).strip()
    low = raw.lower().strip()
    mapping = {"image url": "image_url"}
    if low in mapping:
        return mapping[low]
    return raw


def migrate_and_fix_headers(ws):
    headers = ws.row_values(1)
    if not headers:
        ws.append_row(DEFAULT_TX_COLUMNS)
        return DEFAULT_TX_COLUMNS

    canon = [_canonicalize_header(h) for h in headers]
    if canon != headers:
        ws.update("1:1", [canon])
        headers = canon

    # delete duplicates in SHEET
    seen = {}
    dup_idxs = []
    for idx, h in enumerate(headers, start=1):
        key = str(h).strip()
        if key == "":
            continue
        if key in seen:
            dup_idxs.append(idx)
        else:
            seen[key] = idx
    for col_idx in sorted(dup_idxs, reverse=True):
        ws.delete_columns(col_idx)

    headers = ws.row_values(1)

    existing = set(headers)
    missing = [c for c in DEFAULT_TX_COLUMNS if c not in existing]
    if missing:
        ws.update("1:1", [headers + missing])
        headers = ws.row_values(1)

    return headers


def get_inventory_ws(sh):
    name = st.secrets.get("inventory_worksheet", "inventory")
    return sh.worksheet(name)


def get_transactions_ws(sh):
    name = st.secrets.get("transactions_worksheet", "transactions")
    try:
        return sh.worksheet(name)
    except Exception:
        # Create if missing
        ws = sh.add_worksheet(title=name, rows=2000, cols=max(20, len(DEFAULT_TX_COLUMNS) + 2))
        ws.append_row(DEFAULT_TX_COLUMNS)
        return ws


def load_inventory_df() -> pd.DataFrame:
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = get_inventory_ws(sh)

    headers = ws.row_values(1)
    if not headers:
        return pd.DataFrame()

    records = ws.get_all_records()
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=headers)

    for needed in [
        "inventory_id", "image_url", "product_type", "card_type", "brand_or_league", "set_name",
        "year", "card_name", "purchase_date", "purchased_from", "purchase_price", "shipping", "tax",
        "total_price", "inventory_status", "listed_transaction_id", "reference_link"
    ]:
        if needed not in df.columns:
            df[needed] = ""

    for c in ["purchase_price", "shipping", "tax", "total_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["inventory_id"] = df["inventory_id"].astype(str)
    df["inventory_status"] = df["inventory_status"].replace("", STATUS_ACTIVE).fillna(STATUS_ACTIVE)
    df["listed_transaction_id"] = df["listed_transaction_id"].astype(str).replace("nan", "").fillna("")
    df["image_url"] = df["image_url"].astype(str).replace("nan", "").fillna("")

    return df


def load_transactions_df() -> pd.DataFrame:
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = get_transactions_ws(sh)
    headers = migrate_and_fix_headers(ws)

    records = ws.get_all_records()
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=headers)

    for col in DEFAULT_TX_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[DEFAULT_TX_COLUMNS].copy()

    for c in NUMERIC_TX_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["transaction_id"] = df["transaction_id"].astype(str)
    df["inventory_id"] = df["inventory_id"].astype(str)
    df["status"] = df["status"].replace("", TX_STATUS_OPEN).fillna(TX_STATUS_OPEN)
    df["image_url"] = df["image_url"].astype(str).replace("nan", "").fillna("")
    return df


def _find_row_numbers_by_id(ws, id_col_idx_1based: int, ids: list[str]) -> dict:
    col_vals = ws.col_values(id_col_idx_1based)
    mapping = {}
    for rownum, val in enumerate(col_vals[1:], start=2):
        if val is not None and str(val).strip() != "":
            mapping[str(val)] = rownum
    return {str(i): mapping.get(str(i)) for i in ids}


def append_transaction_row(row: dict):
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = get_transactions_ws(sh)
    headers = migrate_and_fix_headers(ws)
    ordered = [row.get(h, "") for h in headers]
    ws.append_row(ordered, value_input_option="USER_ENTERED")


def update_transaction_rows(df_rows: pd.DataFrame):
    if df_rows.empty:
        return

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = get_transactions_ws(sh)
    headers = migrate_and_fix_headers(ws)

    ids = df_rows["transaction_id"].astype(str).tolist()
    id_to_row = _find_row_numbers_by_id(ws, 1, ids)  # transaction_id is col A

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]

    for _, r in df_rows.iterrows():
        tid = str(r["transaction_id"])
        rownum = id_to_row.get(tid)
        if not rownum:
            continue

        values = []
        for h in headers:
            v = r.get(h, "")
            if pd.isna(v):
                v = ""
            values.append(v)

        ws.update(f"A{rownum}:{last_col_letter}{rownum}", [values], value_input_option="USER_ENTERED")


def update_inventory_rows(df_rows: pd.DataFrame):
    if df_rows.empty:
        return

    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    ws = get_inventory_ws(sh)

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Inventory sheet is missing headers. Load Inventory tab once to initialize it.")

    if "inventory_id" not in headers:
        raise RuntimeError('Inventory sheet missing "inventory_id" column.')

    id_col_idx = headers.index("inventory_id") + 1
    ids = df_rows["inventory_id"].astype(str).tolist()
    id_to_row = _find_row_numbers_by_id(ws, id_col_idx, ids)

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]

    for _, r in df_rows.iterrows():
        inv_id = str(r["inventory_id"])
        rownum = id_to_row.get(inv_id)
        if not rownum:
            continue

        values = []
        for h in headers:
            v = r.get(h, "")
            if pd.isna(v):
                v = ""
            values.append(v)

        ws.update(f"A{rownum}:{last_col_letter}{rownum}", [values], value_input_option="USER_ENTERED")


def _inv_label(r: pd.Series) -> str:
    inv = str(r.get("inventory_id", "")).strip()
    name = str(r.get("card_name", "")).strip()
    setn = str(r.get("set_name", "")).strip()
    yr = str(r.get("year", "")).strip()
    pt = str(r.get("product_type", "")).strip()
    return f"{inv} — {name} ({setn}{' ' + yr if yr else ''}) [{pt}]"


def _fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return ""


# =========================================================
# UI
# =========================================================

st.set_page_config(page_title="Transactions", layout="wide")
inject_table_css()
st.title("Transactions")

inv_df = load_inventory_df()
tx_df = load_transactions_df()

tab_create, tab_update, tab_history = st.tabs(["Create Listing", "Mark Sold / Update", "Transactions History"])

# ---------------------------
# TAB 1: Create Listing
# ---------------------------
with tab_create:
    st.subheader("Create a listing (marks inventory as LISTED)")

    selectable = inv_df[inv_df["inventory_status"] == STATUS_ACTIVE].copy()

    if selectable.empty:
        st.info("No ACTIVE inventory items available to list.")
    else:
        labels = selectable.apply(_inv_label, axis=1).tolist()
        inv_id_by_label = dict(zip(labels, selectable["inventory_id"].astype(str).tolist()))

        left, right = st.columns([1, 3])

        with right:
            selected_label = st.selectbox("Select item to list*", options=labels)

        selected_inv_id = inv_id_by_label[selected_label]
        selected_row = selectable[selectable["inventory_id"].astype(str) == str(selected_inv_id)].iloc[0]

        with left:
            img = str(selected_row.get("image_url", "")).strip()
            if img:
                st.image(img, width=180)
            else:
                st.caption("No image available.")

        st.markdown("#### Purchase info")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Purchase date", str(selected_row.get("purchase_date", "")))
        c2.metric("Purchased from", str(selected_row.get("purchased_from", "")))
        c3.metric("Purchase price", _fmt_money(selected_row.get("purchase_price", 0)))
        c4.metric("Total cost", _fmt_money(selected_row.get("total_price", 0)))

        st.markdown("---")

        with st.form("create_listing_form", clear_on_submit=True):
            a1, a2, a3, a4 = st.columns([1.1, 1.3, 1.0, 1.0])

            with a1:
                listing_type = st.selectbox("Listing type*", LISTING_TYPE_OPTIONS, index=0)
            with a2:
                platform = st.selectbox("Platform*", PLATFORM_OPTIONS, index=0)
            with a3:
                list_date = st.date_input("List date*", value=date.today())
            with a4:
                list_price = st.number_input("List price", min_value=0.0, step=1.0, format="%.2f")

            notes = st.text_area("Notes (optional)", height=80)

            submitted = st.form_submit_button("Create Listing", type="primary", use_container_width=True)

            if submitted:
                tid = str(uuid.uuid4())[:8]
                cost_basis = float(selected_row.get("total_price", 0.0))

                new_tx = {
                    "transaction_id": tid,
                    "inventory_id": str(selected_inv_id),
                    "image_url": str(selected_row.get("image_url", "")).strip(),
                    "listing_type": listing_type,
                    "platform": platform,
                    "list_date": str(list_date),
                    "list_price": float(list_price),
                    "sold_date": "",
                    "sold_price": 0.0,
                    "fees": 0.0,
                    "net_proceeds": 0.0,
                    "cost_basis": cost_basis,
                    "profit": 0.0,
                    "notes": notes.strip() if notes else "",
                    "status": TX_STATUS_OPEN,
                    "created_at": pd.Timestamp.utcnow().isoformat(),
                    "updated_at": pd.Timestamp.utcnow().isoformat(),
                }
                append_transaction_row(new_tx)

                inv_update = inv_df.copy()
                inv_update.loc[inv_update["inventory_id"].astype(str) == str(selected_inv_id), "inventory_status"] = STATUS_LISTED
                inv_update.loc[inv_update["inventory_id"].astype(str) == str(selected_inv_id), "listed_transaction_id"] = tid

                update_inventory_rows(inv_update[inv_update["inventory_id"].astype(str) == str(selected_inv_id)])

                st.success(f"Listing created. Transaction ID: {tid}")
                st.rerun()

# ---------------------------
# TAB 2: Mark Sold / Update
# ---------------------------
with tab_update:
    st.subheader("Update an open listing")

    open_tx = tx_df[tx_df["status"] == TX_STATUS_OPEN].copy()
    if open_tx.empty:
        st.info("No OPEN listings.")
    else:
        inv_lookup = inv_df.set_index("inventory_id")
        open_tx["name"] = open_tx["inventory_id"].apply(lambda x: inv_lookup.loc[x, "card_name"] if x in inv_lookup.index else "")
        open_tx["set_name"] = open_tx["inventory_id"].apply(lambda x: inv_lookup.loc[x, "set_name"] if x in inv_lookup.index else "")
        open_tx["product_type"] = open_tx["inventory_id"].apply(lambda x: inv_lookup.loc[x, "product_type"] if x in inv_lookup.index else "")
        open_tx["img"] = open_tx["inventory_id"].apply(lambda x: inv_lookup.loc[x, "image_url"] if x in inv_lookup.index else "")
        open_tx["label"] = open_tx.apply(lambda r: f'{r["transaction_id"]} — {r["name"]} ({r["set_name"]}) [{r["product_type"]}]', axis=1)

        labels = open_tx["label"].tolist()
        label_to_tid = dict(zip(labels, open_tx["transaction_id"].astype(str).tolist()))

        selected_label = st.selectbox("Select open listing*", options=labels)
        tid = label_to_tid[selected_label]
        tx_row = open_tx[open_tx["transaction_id"].astype(str) == str(tid)].iloc[0]

        inv_id = str(tx_row.get("inventory_id", "")).strip()

        # ✅ FIX #3: Purchase details in the highlighted top area
        top_left, top_right = st.columns([1, 3], vertical_alignment="top")

        with top_left:
            img = str(tx_row.get("img", "")).strip()
            if img:
                st.image(img, width=180)
            else:
                st.caption("No image available.")

        with top_right:
            purchase_date = ""
            purchased_from = ""
            purchase_price = 0.0
            total_price = 0.0

            if inv_id in inv_lookup.index:
                purchase_date = str(inv_lookup.loc[inv_id, "purchase_date"])
                purchased_from = str(inv_lookup.loc[inv_id, "purchased_from"])
                purchase_price = float(inv_lookup.loc[inv_id, "purchase_price"])
                total_price = float(inv_lookup.loc[inv_id, "total_price"])

            st.markdown("#### Purchase details")
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Purchase date", purchase_date)
            p2.metric("Purchased from", purchased_from)
            p3.metric("Purchase price", _fmt_money(purchase_price))
            p4.metric("Total cost", _fmt_money(total_price))

        st.markdown("#### Listing details")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Platform", str(tx_row.get("platform", "")))
        c2.metric("Listing type", str(tx_row.get("listing_type", "")))
        c3.metric("List date", str(tx_row.get("list_date", "")))
        c4.metric("List price", _fmt_money(tx_row.get("list_price", 0)))

        st.markdown("---")

        left, right = st.columns(2)

        with left:
            st.markdown("### Mark Sold")
            with st.form("mark_sold_form", clear_on_submit=True):
                sold_date = st.date_input("Sold date*", value=date.today(), key=f"sold_date_{tid}")
                sold_price = st.number_input("Sold price*", min_value=0.0, step=1.0, format="%.2f", key=f"sold_price_{tid}")
                fees = st.number_input("Fees (eBay/processing)*", min_value=0.0, step=1.0, format="%.2f", key=f"fees_{tid}")
                notes = st.text_area("Notes (optional)", height=70, key=f"sold_notes_{tid}")

                mark_btn = st.form_submit_button("Mark Sold", type="primary", use_container_width=True)

                if mark_btn:
                    cost_basis = float(tx_row.get("cost_basis", 0.0))
                    net = float(sold_price) - float(fees)
                    profit = net - cost_basis

                    tx_update = tx_df.copy()
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "sold_date"] = str(sold_date)
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "sold_price"] = float(sold_price)
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "fees"] = float(fees)
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "net_proceeds"] = float(net)
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "profit"] = float(profit)
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "notes"] = notes.strip() if notes else ""
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "status"] = TX_STATUS_SOLD
                    tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "updated_at"] = pd.Timestamp.utcnow().isoformat()

                    update_transaction_rows(tx_update[tx_update["transaction_id"].astype(str) == str(tid)])

                    inv_update = inv_df.copy()
                    inv_update.loc[inv_update["inventory_id"].astype(str) == inv_id, "inventory_status"] = STATUS_SOLD
                    update_inventory_rows(inv_update[inv_update["inventory_id"].astype(str) == inv_id])

                    st.success(f"Marked SOLD. Profit: {_fmt_money(profit)}")
                    st.rerun()

        with right:
            st.markdown("### Delete Listing (restore inventory)")
            st.caption("Use this if you created a listing by mistake or decided not to list. Inventory returns to ACTIVE.")
            if st.button("Delete Listing", use_container_width=True):
                # Update transaction status -> DELETED
                tx_update = tx_df.copy()
                tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "status"] = TX_STATUS_DELETED
                tx_update.loc[tx_update["transaction_id"].astype(str) == str(tid), "updated_at"] = pd.Timestamp.utcnow().isoformat()
                update_transaction_rows(tx_update[tx_update["transaction_id"].astype(str) == str(tid)])

                # Restore inventory -> ACTIVE and clear listed_transaction_id
                inv_update = inv_df.copy()
                inv_update.loc[inv_update["inventory_id"].astype(str) == inv_id, "inventory_status"] = STATUS_ACTIVE
                inv_update.loc[inv_update["inventory_id"].astype(str) == inv_id, "listed_transaction_id"] = ""
                update_inventory_rows(inv_update[inv_update["inventory_id"].astype(str) == inv_id])

                st.success("Listing deleted and inventory restored to ACTIVE.")
                st.rerun()

# ---------------------------
# TAB 3: Transactions History
# ---------------------------
with tab_history:
    st.subheader("Transactions History")

    if tx_df.empty:
        st.info("No transactions yet.")
    else:
        inv_lookup = inv_df.set_index("inventory_id") if not inv_df.empty else None

        hist = tx_df.copy()
        if inv_lookup is not None:
            hist["item_name"] = hist["inventory_id"].apply(lambda x: inv_lookup.loc[x, "card_name"] if x in inv_lookup.index else "")
            hist["set_name"] = hist["inventory_id"].apply(lambda x: inv_lookup.loc[x, "set_name"] if x in inv_lookup.index else "")
            hist["product_type"] = hist["inventory_id"].apply(lambda x: inv_lookup.loc[x, "product_type"] if x in inv_lookup.index else "")

        display = hist.copy()
        display = display.sort_values(["status", "list_date"], ascending=[True, False])

        # ✅ FIX #2: image column renders + CSS makes rows ~2 inches
        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "image_url": st.column_config.ImageColumn("Image", width="large"),
            },
        )
