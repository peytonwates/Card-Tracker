# pages/7_Breaks.py
import json
import uuid
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials


# =========================
# Page config
# =========================
st.set_page_config(page_title="Breaks", layout="wide")
st.title("Breaks")


# =========================
# Google Sheets client (same pattern as your other pages)
# =========================
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit Cloud: TOML table
    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Streamlit Cloud: JSON string
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_json_str = st.secrets["gcp_service_account"]
        sa_info = json.loads(sa_json_str)
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: JSON file path stored in secrets.toml
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


def _open_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(st.secrets["spreadsheet_id"])


def _ensure_ws(ws_name: str, rows: int = 2000, cols: int = 40):
    sh = _open_spreadsheet()
    try:
        return sh.worksheet(ws_name)
    except WorksheetNotFound:
        return sh.add_worksheet(title=ws_name, rows=str(rows), cols=str(cols))


# =========================
# Helpers
# =========================
def _safe_str(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x)


def _to_dt(s):
    return pd.to_datetime(s, errors="coerce")


def _to_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        b = _safe_str(c)
        if b not in seen:
            seen[b] = 0
            new_cols.append(b)
        else:
            seen[b] += 1
            new_cols.append(f"{b}__dup{seen[b]}")
    out = df.copy()
    out.columns = new_cols
    return out


def _normalize_card_type(val: str) -> str:
    s = _safe_str(val).strip().lower()
    if s == "sports" or "sport" in s:
        return "Sports"
    # default anything else -> Pokemon
    return "Pokemon"


def _now_iso_local():
    return datetime.now().isoformat(timespec="seconds")


def _a1_col_letter(n: int) -> str:
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters


def _col_index(header, name: str):
    name = str(name).strip()
    for i, h in enumerate(header):
        if str(h).strip() == name:
            return i
    return None


# =========================
# Inventory schema (MATCH 2_Inventory.py DEFAULT_COLUMNS)
# =========================
STATUS_ACTIVE = "ACTIVE"

INV_DEFAULT_COLUMNS = [
    "inventory_id",
    "image_url",
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
    "grading_company",
    "grade",
    "reference_link",
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
    "market_price",
    "market_value",
    "market_price_updated_at",
]

# match your Inventory header aliasing pattern (Product Type / Sealed Product Type)
HEADER_ALIASES = {
    "product_type": ["product_type", "Product Type"],
    "sealed_product_type": ["sealed_product_type", "Sealed Product Type"],
}

def sheet_header_to_internal(header: str) -> str:
    for internal, aliases in HEADER_ALIASES.items():
        if header in aliases:
            return internal
    return header

def internal_to_sheet_header(internal: str, existing_headers: list[str]) -> str:
    aliases = HEADER_ALIASES.get(internal, [internal])
    for a in aliases:
        if a in existing_headers:
            return a
    if internal == "product_type":
        return "Product Type"
    if internal == "sealed_product_type":
        return "Sealed Product Type"
    return internal


# =========================
# Breaks sheet schemas
# =========================
BREAKS_HEADERS = [
    "break_id",
    "purchase_date",
    "purchased_from",
    "reference_link",     # PriceCharting link for the box
    "card_type",          # Pokemon / Sports
    "brand_or_league",    # Pokemon TCG / Football / etc (optional)
    "set_name",
    "year",
    "box_name",
    "box_type",           # Hobby / Blaster / ETB / etc (optional)
    "qty_boxes",
    "purchase_price",     # per box
    "shipping",
    "tax",
    "total_price",        # qty * (purchase_price+shipping+tax) OR explicit
    "notes",
    "status",             # OPEN / FINALIZED
    "created_at",
    "finalized_at",
    "cards_count",        # populated on finalize
    "cost_per_card",      # populated on finalize
]

BREAK_CARDS_HEADERS = [
    "break_card_id",
    "break_id",
    "card_type",          # Pokemon / Sports
    "product_type",       # Card / Graded Card / Sealed (but for break cards normally Card)
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "grading_company",
    "grade",
    "condition",
    "reference_link",
    "quantity",
    "notes",
    "created_at",
    "exported_to_inventory",  # YES/NO
    "inventory_ids",          # comma list once exported
]


@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_sheet_df(worksheet_name: str) -> pd.DataFrame:
    ws = _ensure_ws(worksheet_name)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    header = [str(h or "").strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []

    # make unique
    fixed = []
    seen = {}
    for i, h in enumerate(header):
        name = h if h else f"col_{i+1}"
        if name not in seen:
            seen[name] = 0
            fixed.append(name)
        else:
            seen[name] += 1
            fixed.append(f"{name}__dup{seen[name]}")

    width = len(fixed)
    norm_rows = []
    for r in rows:
        r = list(r)
        if len(r) < width:
            r = r + [""] * (width - len(r))
        elif len(r) > width:
            r = r[:width]
        norm_rows.append(r)

    df = pd.DataFrame(norm_rows, columns=fixed)
    df = _ensure_unique_columns(df)
    return df


def _ensure_headers(ws, required_headers):
    values = ws.get_all_values()
    header = []
    if values and len(values) >= 1:
        header = [str(h or "").strip() for h in values[0]]
    if not header:
        header = []

    existing = {h.strip(): i for i, h in enumerate(header) if str(h or "").strip() != ""}
    changed = False
    for h in required_headers:
        if h not in existing:
            header.append(h)
            changed = True

    if changed:
        ws.update("1:1", [header], value_input_option="USER_ENTERED")

    return header


def _append_row(ws, header, row_dict: dict):
    row = []
    for h in header:
        row.append(row_dict.get(h, ""))
    ws.append_row(row, value_input_option="USER_ENTERED")


def _find_row_by_id(ws, id_col_name: str, id_value: str):
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None
    header = [str(h or "").strip() for h in values[0]]
    idx = _col_index(header, id_col_name)
    if idx is None:
        return None
    for r_i, row in enumerate(values[1:], start=2):
        v = row[idx] if idx < len(row) else ""
        if str(v).strip() == str(id_value).strip():
            return r_i
    return None


# =========================
# Worksheet names
# =========================
INV_WS = st.secrets.get("inventory_worksheet", "inventory")
BRK_WS = st.secrets.get("breaks_worksheet", "breaks")
BRK_CARDS_WS = st.secrets.get("break_cards_worksheet", "break_cards")

ws_inv = _ensure_ws(INV_WS)
ws_breaks = _ensure_ws(BRK_WS)
ws_break_cards = _ensure_ws(BRK_CARDS_WS)

# ensure headers
inv_sheet_headers = _ensure_headers(ws_inv, [internal_to_sheet_header(c, ws_inv.row_values(1) or []) for c in INV_DEFAULT_COLUMNS])
breaks_headers = _ensure_headers(ws_breaks, BREAKS_HEADERS)
break_cards_headers = _ensure_headers(ws_break_cards, BREAK_CARDS_HEADERS)


def _coerce_breaks(df: pd.DataFrame) -> pd.DataFrame:
    # ALWAYS return with purchase_date_dt present (fixes your KeyError)
    if df is None or df.empty:
        out = pd.DataFrame(columns=BREAKS_HEADERS + ["purchase_date_dt"])
        return out

    out = df.copy()
    for c in BREAKS_HEADERS:
        if c not in out.columns:
            out[c] = ""

    out["qty_boxes"] = _to_num(out["qty_boxes"])
    out["purchase_price"] = _to_num(out["purchase_price"])
    out["shipping"] = _to_num(out["shipping"])
    out["tax"] = _to_num(out["tax"])
    out["total_price"] = _to_num(out["total_price"])
    out["cards_count"] = _to_num(out["cards_count"])
    out["cost_per_card"] = _to_num(out["cost_per_card"])

    out["status"] = out["status"].astype(str).str.upper().replace("", "OPEN").fillna("OPEN")
    out["purchase_date_dt"] = _to_dt(out["purchase_date"])  # ✅ always created when non-empty
    return out


def _coerce_break_cards(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=BREAK_CARDS_HEADERS)

    out = df.copy()
    for c in BREAK_CARDS_HEADERS:
        if c not in out.columns:
            out[c] = ""

    out["quantity"] = _to_num(out["quantity"]).replace(0, 1)
    out["exported_to_inventory"] = out["exported_to_inventory"].astype(str).str.upper().replace("", "NO").fillna("NO")
    out["card_type"] = out["card_type"].apply(_normalize_card_type)
    return out


def _append_inventory_row(row_internal: dict):
    """
    Append row to inventory using the *existing sheet headers* and alias mapping
    so we don't create mismatched columns.
    """
    ws = ws_inv
    sheet_headers = ws.row_values(1) or []
    if not sheet_headers:
        # create headers if sheet is empty
        sheet_headers = [internal_to_sheet_header(c, []) for c in INV_DEFAULT_COLUMNS]
        ws.append_row(sheet_headers)

    # Map sheet header -> internal field name
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}

    ordered = []
    for sheet_h in sheet_headers:
        internal = header_to_internal.get(sheet_h, sheet_h)
        ordered.append(row_internal.get(internal, ""))

    ws.append_row(ordered, value_input_option="USER_ENTERED")


def _export_break_to_inventory(break_id: str):
    """
    Export all NON-exported break cards for this break_id to inventory,
    allocating break total cost evenly across total card units (sum of quantities).
    """
    # reload fresh to avoid stale cached state during export
    try:
        st.cache_data.clear()
    except Exception:
        pass

    bdf = _coerce_breaks(load_sheet_df(BRK_WS))
    cdf = _coerce_break_cards(load_sheet_df(BRK_CARDS_WS))

    b = bdf[bdf["break_id"].astype(str).str.strip().eq(str(break_id).strip())]
    if b.empty:
        raise ValueError("Break not found.")
    b = b.iloc[0].to_dict()

    if str(b.get("status", "")).upper().strip() != "OPEN":
        raise ValueError("This break is not OPEN (already finalized).")

    cards = cdf[
        cdf["break_id"].astype(str).str.strip().eq(str(break_id).strip())
        & cdf["exported_to_inventory"].astype(str).str.upper().ne("YES")
    ].copy()

    if cards.empty:
        raise ValueError("No un-exported cards found for this break.")

    cards["quantity"] = _to_num(cards["quantity"]).replace(0, 1)
    total_units = int(cards["quantity"].sum())
    if total_units <= 0:
        raise ValueError("Total card quantity is 0.")

    total_cost = float(_to_num(pd.Series([b.get("total_price", 0.0)])).iloc[0])
    if total_cost <= 0:
        raise ValueError("Break total_price must be > 0.")

    cost_per_card = total_cost / float(total_units)

    purchase_date = _safe_str(b.get("purchase_date", "")).strip()
    purchased_from = _safe_str(b.get("purchased_from", "")).strip() or "Break"
    box_name = _safe_str(b.get("box_name", "")).strip()
    box_link = _safe_str(b.get("reference_link", "")).strip()

    created_inventory_ids_by_break_card_id = {}

    for _, r in cards.iterrows():
        break_card_id = _safe_str(r.get("break_card_id", "")).strip()
        qty = int(_to_num(pd.Series([r.get("quantity", 1)])).iloc[0] or 1)
        qty = max(qty, 1)

        inv_ids = []
        for _k in range(qty):
            inventory_id = str(uuid.uuid4())[:8]
            inv_ids.append(inventory_id)

            card_type = _normalize_card_type(r.get("card_type", "Pokemon"))
            brand = _safe_str(r.get("brand_or_league", "")).strip()
            if not brand and card_type == "Pokemon":
                brand = "Pokemon TCG"

            product_type = _safe_str(r.get("product_type", "Card")).strip() or "Card"
            if product_type not in ["Card", "Sealed", "Graded Card"]:
                product_type = "Card"

            sealed_product_type = ""
            grading_company = ""
            grade = ""
            condition = _safe_str(r.get("condition", "")).strip()

            if product_type == "Sealed":
                # If you ever use sealed here
                sealed_product_type = _safe_str(r.get("card_name", "")).strip()
                condition = "Sealed"
            elif product_type == "Graded Card":
                grading_company = _safe_str(r.get("grading_company", "")).strip()
                grade = _safe_str(r.get("grade", "")).strip()
                condition = "Graded"
            else:
                # raw Card
                if not condition:
                    condition = "Near Mint"

            notes = _safe_str(r.get("notes", "")).strip()
            notes_prefix = f"Break {break_id}"
            if box_name:
                notes_prefix += f" | {box_name}"
            if notes:
                notes_prefix += f" | {notes}"

            inv_row = {
                "inventory_id": inventory_id,
                "image_url": "",  # Inventory page can fill later; keeping schema clean
                "product_type": product_type,
                "sealed_product_type": sealed_product_type,
                "card_type": card_type,
                "brand_or_league": brand,
                "set_name": _safe_str(r.get("set_name", "")).strip(),
                "year": _safe_str(r.get("year", "")).strip(),
                "card_name": _safe_str(r.get("card_name", "")).strip(),
                "card_number": _safe_str(r.get("card_number", "")).strip(),
                "variant": _safe_str(r.get("variant", "")).strip(),
                "card_subtype": _safe_str(r.get("card_subtype", "")).strip(),
                "grading_company": grading_company,
                "grade": grade,
                "reference_link": _safe_str(r.get("reference_link", "")).strip(),
                "purchase_date": purchase_date,
                "purchased_from": purchased_from,
                "purchase_price": round(cost_per_card, 2),
                "shipping": 0.0,
                "tax": 0.0,
                "total_price": round(cost_per_card, 2),
                "condition": condition,
                "notes": notes_prefix,
                "created_at": pd.Timestamp.utcnow().isoformat(),
                "inventory_status": STATUS_ACTIVE,
                "listed_transaction_id": "",
                "market_price": 0.0,
                "market_value": 0.0,
                "market_price_updated_at": "",
            }

            # If the break has a box link and the card doesn't, keep the card link blank;
            # inventory rows represent the cards, not the sealed box.
            _append_inventory_row(inv_row)

        if break_card_id:
            created_inventory_ids_by_break_card_id[break_card_id] = inv_ids

    # Update break_cards exported_to_inventory + inventory_ids
    values = ws_break_cards.get_all_values()
    if values and len(values) >= 2:
        header = [str(h or "").strip() for h in values[0]]
        idx_break_card_id = _col_index(header, "break_card_id")
        idx_exported = _col_index(header, "exported_to_inventory")
        idx_inv_ids = _col_index(header, "inventory_ids")

        updates = []
        for sheet_row_num, row in enumerate(values[1:], start=2):
            if idx_break_card_id is None or idx_break_card_id >= len(row):
                continue
            bc_id = str(row[idx_break_card_id]).strip()
            if bc_id in created_inventory_ids_by_break_card_id:
                inv_ids_str = ", ".join(created_inventory_ids_by_break_card_id[bc_id])

                if idx_exported is not None:
                    col_letter = _a1_col_letter(idx_exported + 1)
                    updates.append({"range": f"{col_letter}{sheet_row_num}", "values": [["YES"]]})
                if idx_inv_ids is not None:
                    col_letter = _a1_col_letter(idx_inv_ids + 1)
                    updates.append({"range": f"{col_letter}{sheet_row_num}", "values": [[inv_ids_str]]})

        if updates:
            ws_break_cards.batch_update(updates, value_input_option="USER_ENTERED")

    # Update breaks row: finalize
    br_row = _find_row_by_id(ws_breaks, "break_id", break_id)
    if br_row is not None:
        values = ws_breaks.get_all_values()
        header = [str(h or "").strip() for h in values[0]] if values else breaks_headers

        def _upd(col_name, val):
            idx = _col_index(header, col_name)
            if idx is None:
                return None
            col_letter = _a1_col_letter(idx + 1)
            return {"range": f"{col_letter}{br_row}", "values": [[val]]}

        payload = []
        payload.append(_upd("status", "FINALIZED"))
        payload.append(_upd("finalized_at", _now_iso_local()))
        payload.append(_upd("cards_count", total_units))
        payload.append(_upd("cost_per_card", round(cost_per_card, 4)))
        payload = [p for p in payload if p is not None]
        if payload:
            ws_breaks.batch_update(payload, value_input_option="USER_ENTERED")

    return {
        "total_units": total_units,
        "total_cost": total_cost,
        "cost_per_card": cost_per_card,
        "rows_added_to_inventory": total_units,
        "box_link": box_link,
    }


# =========================
# Load + normalize
# =========================
breaks_df = _coerce_breaks(load_sheet_df(BRK_WS))
break_cards_df = _coerce_break_cards(load_sheet_df(BRK_CARDS_WS))

open_breaks = breaks_df[breaks_df["status"].astype(str).str.upper().eq("OPEN")].copy()

# ✅ safe sort (purchase_date_dt always exists now, but keep this defensive)
sort_cols = [c for c in ["purchase_date_dt", "created_at"] if c in open_breaks.columns]
if sort_cols:
    open_breaks = open_breaks.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")


# =========================
# UI
# =========================
tab1, tab2 = st.tabs(["Breaks", "Break Cards → Inventory"])

with tab1:
    st.subheader("Create a Break (Box Opening)")

    with st.form("create_break_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            purchase_date = st.date_input("Purchase Date", value=date.today())
        with c2:
            qty_boxes = st.number_input("# of Boxes", min_value=1, max_value=999, value=1, step=1)
        with c3:
            purchase_price = st.number_input("Purchase Price (per box)", min_value=0.0, value=0.0, step=1.0, format="%.2f")

        c4, c5, c6 = st.columns([1.3, 1.0, 1.0])
        with c4:
            box_name = st.text_input("Box Name (e.g., 2024 Prizm Blaster)")
        with c5:
            set_name = st.text_input("Set Name (optional)")
        with c6:
            year = st.text_input("Year (optional)")

        c7, c8, c9 = st.columns([1, 1, 2])
        with c7:
            card_type = st.selectbox("Card Type", options=["Pokemon", "Sports"], index=0)
        with c8:
            brand_or_league = st.text_input("Brand / League (optional)", value=("Pokemon TCG" if card_type == "Pokemon" else ""))
        with c9:
            purchased_from = st.text_input("Purchased From*", placeholder="Walmart, Target, LCS, Whatnot, etc.")

        c10, c11 = st.columns([1, 2])
        with c10:
            shipping = st.number_input("Shipping (total)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            tax = st.number_input("Tax (total)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        with c11:
            reference_link = st.text_input("PriceCharting Link for Box (optional)")
            box_type = st.text_input("Box Type (optional)", placeholder="Hobby / Blaster / ETB / Booster Box / etc.")
            notes = st.text_area("Notes (optional)", height=92)

        submitted = st.form_submit_button("Add Break", use_container_width=True)
        if submitted:
            if not purchased_from.strip():
                st.error("Purchased From is required.")
            else:
                break_id = str(uuid.uuid4())[:8]
                total_price = float(qty_boxes) * float(purchase_price) + float(shipping) + float(tax)

                row = {
                    "break_id": break_id,
                    "purchase_date": purchase_date.isoformat(),
                    "purchased_from": purchased_from.strip(),
                    "reference_link": reference_link.strip(),
                    "card_type": _normalize_card_type(card_type),
                    "brand_or_league": brand_or_league.strip(),
                    "set_name": set_name.strip(),
                    "year": year.strip(),
                    "box_name": box_name.strip(),
                    "box_type": box_type.strip(),
                    "qty_boxes": int(qty_boxes),
                    "purchase_price": float(purchase_price),
                    "shipping": float(shipping),
                    "tax": float(tax),
                    "total_price": round(total_price, 2),
                    "notes": notes.strip(),
                    "status": "OPEN",
                    "created_at": _now_iso_local(),
                    "finalized_at": "",
                    "cards_count": "",
                    "cost_per_card": "",
                }
                _append_row(ws_breaks, breaks_headers, row)
                st.success(f"Break created: {break_id}")
                st.rerun()

    st.markdown("---")
    st.subheader("Your Breaks")

    if breaks_df.empty:
        st.info("No breaks yet. Create one above.")
    else:
        view = breaks_df.copy()
        if "purchase_date_dt" in view.columns:
            view = view.sort_values(["purchase_date_dt", "created_at"], ascending=[False, False], na_position="last")

        show_cols = [
            "break_id", "status", "purchase_date", "box_name", "qty_boxes",
            "purchase_price", "shipping", "tax", "total_price",
            "cards_count", "cost_per_card", "finalized_at"
        ]
        show_cols = [c for c in show_cols if c in view.columns]
        st.dataframe(view[show_cols], use_container_width=True, hide_index=True)


with tab2:
    st.subheader("Add Cards from a Break")

    if open_breaks.empty:
        st.info("No OPEN breaks found. Create a break first.")
    else:
        options = []
        for _, r in open_breaks.iterrows():
            bid = _safe_str(r.get("break_id", "")).strip()
            label = f"{bid} — {r.get('box_name','')}".strip()
            options.append((label, bid))

        labels = [o[0] for o in options]
        label_choice = st.selectbox("Select an OPEN Break", options=labels, index=0)
        break_id = dict(options).get(label_choice)

        brow = open_breaks[open_breaks["break_id"].astype(str).str.strip().eq(str(break_id).strip())]
        b = brow.iloc[0].to_dict() if len(brow) else {}

        total_cost = float(_to_num(pd.Series([b.get("total_price", 0.0)])).iloc[0])
        st.caption(f"Break Total Cost: ${total_cost:,.2f}")

        cards_for_break = break_cards_df[
            break_cards_df["break_id"].astype(str).str.strip().eq(str(break_id).strip())
        ].copy()

        # estimate cost per card based on un-exported units
        units_unexported = 0
        if not cards_for_break.empty:
            tmp = cards_for_break.copy()
            tmp["quantity"] = _to_num(tmp["quantity"]).replace(0, 1)
            tmp = tmp[tmp["exported_to_inventory"].astype(str).str.upper().ne("YES")]
            units_unexported = int(tmp["quantity"].sum())

        if units_unexported > 0 and total_cost > 0:
            st.caption(f"Un-exported card units entered: {units_unexported} → Estimated cost per card: ${total_cost/units_unexported:,.2f}")
        else:
            st.caption("Un-exported card units entered: 0")

        st.markdown("### Add Card(s) from this Break")

        with st.form("add_break_card_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
            with c1:
                card_type = st.selectbox("Card Type*", options=["Pokemon", "Sports"], index=0)
            with c2:
                product_type = st.selectbox("Product Type*", options=["Card", "Graded Card", "Sealed"], index=0)
            with c3:
                quantity = st.number_input("Quantity*", min_value=1, max_value=999, value=1, step=1)
            with c4:
                condition = st.text_input("Condition (raw cards)", value="Near Mint")

            c5, c6, c7 = st.columns([1, 1, 1])
            with c5:
                brand_or_league = st.text_input("Brand / League", value=("Pokemon TCG" if card_type == "Pokemon" else _safe_str(b.get("brand_or_league","")).strip()))
            with c6:
                set_name = st.text_input("Set Name", value=_safe_str(b.get("set_name", "")).strip())
            with c7:
                year = st.text_input("Year", value=_safe_str(b.get("year", "")).strip())

            c8, c9, c10 = st.columns([1.6, 1.0, 1.4])
            with c8:
                card_name = st.text_input("Card Name*")
            with c9:
                card_number = st.text_input("Card # (optional)")
            with c10:
                variant = st.text_input("Variant (optional)")

            c11, c12, c13 = st.columns([1, 1, 2])
            with c11:
                card_subtype = st.text_input("Card Subtype (optional)", placeholder="Rookie, Insert, Parallel, etc.")
            with c12:
                grading_company = st.text_input("Grading Company (if graded)", placeholder="PSA / CGC / Beckett")
                grade = st.text_input("Grade (if graded)", placeholder="10 / 9 / etc.")
            with c13:
                reference_link = st.text_input("PriceCharting Link for CARD (optional)")

            notes = st.text_area("Notes (optional)", height=70)

            submitted = st.form_submit_button("Add Card Line", use_container_width=True)
            if submitted:
                if not card_name.strip():
                    st.error("Card Name is required.")
                else:
                    bc_id = str(uuid.uuid4())[:8]
                    row = {
                        "break_card_id": bc_id,
                        "break_id": str(break_id).strip(),
                        "card_type": _normalize_card_type(card_type),
                        "product_type": product_type,
                        "brand_or_league": brand_or_league.strip(),
                        "set_name": set_name.strip(),
                        "year": year.strip(),
                        "card_name": card_name.strip(),
                        "card_number": card_number.strip(),
                        "variant": variant.strip(),
                        "card_subtype": card_subtype.strip(),
                        "grading_company": grading_company.strip(),
                        "grade": grade.strip(),
                        "condition": condition.strip(),
                        "reference_link": reference_link.strip(),
                        "quantity": int(quantity),
                        "notes": notes.strip(),
                        "created_at": _now_iso_local(),
                        "exported_to_inventory": "NO",
                        "inventory_ids": "",
                    }
                    _append_row(ws_break_cards, break_cards_headers, row)
                    st.success("Added.")
                    st.rerun()

        st.markdown("---")
        st.markdown("### Cards Entered for this Break")

        if cards_for_break.empty:
            st.info("No cards entered yet.")
        else:
            show_cols = [
                "break_card_id", "card_type", "set_name", "year",
                "card_name", "card_number", "variant", "quantity",
                "exported_to_inventory", "inventory_ids"
            ]
            show_cols = [c for c in show_cols if c in cards_for_break.columns]
            st.dataframe(cards_for_break[show_cols].copy(), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("Finalize: Add these cards to Inventory")

        colA, colB = st.columns([1, 2])
        with colA:
            do_export = st.button("✅ Finalize Break → Add to Inventory", use_container_width=True)
        with colB:
            st.caption(
                "Allocates the break’s total cost evenly across ALL un-exported card units (sum of Quantity). "
                "Then creates one inventory row per unit and marks the break as FINALIZED."
            )

        if do_export:
            try:
                result = _export_break_to_inventory(str(break_id).strip())
                st.success(
                    f"Export complete. Added {result['rows_added_to_inventory']} inventory row(s). "
                    f"Cost/card: ${result['cost_per_card']:,.2f}"
                )
                st.rerun()
            except Exception as e:
                st.error(f"Export failed: {e}")
