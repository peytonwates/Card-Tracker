# pages/7_Breaks.py
import json
import re
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
# Google Sheets client
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


def _open_ws(ws_name: str):
    sh = _open_spreadsheet()
    return sh.worksheet(ws_name)


def _ensure_ws(ws_name: str, rows: int = 2000, cols: int = 40):
    sh = _open_spreadsheet()
    try:
        return sh.worksheet(ws_name)
    except WorksheetNotFound:
        return sh.add_worksheet(title=ws_name, rows=str(rows), cols=str(cols))


# =========================
# Helpers (robust)
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
    if s == "sports":
        return "Sports"
    if s == "pokemon":
        return "Pokemon"
    if "sport" in s:
        return "Sports"
    if "pok" in s or "pokemon" in s:
        return "Pokemon"
    return "Pokemon"


@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_sheet_df(worksheet_name: str) -> pd.DataFrame:
    ws = _open_ws(worksheet_name)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    header = [str(h or "").strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []

    # Fill blank headers and make unique
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
    """
    Ensure the sheet has at least these headers (adds missing at end).
    Only updates row 1.
    """
    values = ws.get_all_values()
    header = []
    if values and len(values) >= 1:
        header = [str(h or "").strip() for h in values[0]]
    # If completely empty, start fresh
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

    return header  # current header order


def _col_index(header, name: str):
    # returns 0-based index or None
    name = str(name).strip()
    for i, h in enumerate(header):
        if str(h).strip() == name:
            return i
    return None


def _find_row_by_id(ws, id_col_name: str, id_value: str):
    """
    Find the sheet row number (1-based) where id_col == id_value.
    Returns None if not found.
    """
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None
    header = [str(h or "").strip() for h in values[0]]
    idx = _col_index(header, id_col_name)
    if idx is None:
        return None
    for r_i, row in enumerate(values[1:], start=2):  # row number in sheet
        v = row[idx] if idx < len(row) else ""
        if str(v).strip() == str(id_value).strip():
            return r_i
    return None


def _a1_col_letter(n: int) -> str:
    # 1-based col number to letters
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters


def _now_iso_local():
    # lightweight, good enough for tracking
    return datetime.now().isoformat(timespec="seconds")


# =========================
# Worksheet names
# =========================
INV_WS = st.secrets.get("inventory_worksheet", "inventory")
BRK_WS = st.secrets.get("breaks_worksheet", "breaks")
BRK_CARDS_WS = st.secrets.get("break_cards_worksheet", "break_cards")

# Ensure worksheets exist
ws_breaks = _ensure_ws(BRK_WS)
ws_break_cards = _ensure_ws(BRK_CARDS_WS)
ws_inv = _ensure_ws(INV_WS)

# Ensure headers exist (breaks, break_cards, inventory)
breaks_headers_req = [
    "break_id",
    "purchase_date",
    "box_name",
    "set_name",
    "year",
    "box_type",
    "reference_link",
    "qty_boxes",
    "price_per_box",
    "total_price",
    "notes",
    "status",
    "created_at",
    "finalized_at",
    "cards_count",
    "cost_per_card",
]
break_cards_headers_req = [
    "break_card_id",
    "break_id",
    "card_type",
    "product_type",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "condition",
    "grading_company",
    "grade",
    "reference_link",
    "quantity",
    "created_at",
    "exported_to_inventory",
    "inventory_ids",
]
inventory_headers_req = [
    "inventory_id",
    "inventory_status",
    "product_type",
    "card_type",
    "purchase_date",
    "total_price",
    "purchased_from",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "condition",
    "grading_company",
    "grade",
    "reference_link",
    "break_id",
    "break_box_name",
]

breaks_header = _ensure_headers(ws_breaks, breaks_headers_req)
break_cards_header = _ensure_headers(ws_break_cards, break_cards_headers_req)
inv_header = _ensure_headers(ws_inv, inventory_headers_req)

# Load data
breaks_df = load_sheet_df(BRK_WS)
break_cards_df = load_sheet_df(BRK_CARDS_WS)


def _coerce_breaks(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=breaks_headers_req)
    df = df.copy()
    for c in breaks_headers_req:
        if c not in df.columns:
            df[c] = ""
    df["qty_boxes"] = _to_num(df["qty_boxes"])
    df["price_per_box"] = _to_num(df["price_per_box"])
    df["total_price"] = _to_num(df["total_price"])
    df["cards_count"] = _to_num(df["cards_count"])
    df["cost_per_card"] = _to_num(df["cost_per_card"])
    df["status"] = df["status"].astype(str).str.upper().replace("", "OPEN")
    df["purchase_date_dt"] = _to_dt(df["purchase_date"])
    return df


def _coerce_break_cards(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=break_cards_headers_req)
    df = df.copy()
    for c in break_cards_headers_req:
        if c not in df.columns:
            df[c] = ""
    df["quantity"] = _to_num(df["quantity"]).replace(0, 1)
    df["exported_to_inventory"] = df["exported_to_inventory"].astype(str).str.upper().replace("", "NO")
    return df


breaks_df = _coerce_breaks(breaks_df)
break_cards_df = _coerce_break_cards(break_cards_df)

# Convenience views
open_breaks = breaks_df[breaks_df["status"].astype(str).str.upper().eq("OPEN")].copy()
open_breaks = open_breaks.sort_values(["purchase_date_dt", "created_at"], ascending=[False, False], na_position="last")


def _append_row(ws, header, row_dict: dict):
    row = []
    for h in header:
        row.append(row_dict.get(h, ""))
    ws.append_row(row, value_input_option="USER_ENTERED")


def _export_break_to_inventory(break_id: str):
    """
    Export all NON-exported break cards for this break_id to inventory, allocating break total cost evenly
    across total card units (sum of quantities).
    """
    # Reload fresh from Sheets (avoid stale cache during export)
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

    # Total card units (sum of quantities)
    cards["quantity"] = _to_num(cards["quantity"]).replace(0, 1)
    total_units = int(cards["quantity"].sum())
    if total_units <= 0:
        raise ValueError("Total card quantity is 0.")

    total_cost = float(_to_num(pd.Series([b.get("total_price", 0.0)])).iloc[0])
    if total_cost <= 0:
        raise ValueError("Break total_price must be > 0.")

    cost_per_card = total_cost / float(total_units)

    purchase_date = _safe_str(b.get("purchase_date", "")).strip()
    box_name = _safe_str(b.get("box_name", "")).strip()

    # Append inventory rows
    created_inventory_ids_by_break_card_id = {}

    for _, r in cards.iterrows():
        break_card_id = _safe_str(r.get("break_card_id", "")).strip()
        qty = int(_to_num(pd.Series([r.get("quantity", 1)])).iloc[0] or 1)
        qty = max(qty, 1)

        inv_ids = []
        for _k in range(qty):
            inventory_id = str(uuid.uuid4())[:8]
            inv_ids.append(inventory_id)

            inv_row = {
                "inventory_id": inventory_id,
                "inventory_status": "ACTIVE",
                "product_type": _safe_str(r.get("product_type", "Card")).strip() or "Card",
                "card_type": _normalize_card_type(r.get("card_type", "")),
                "purchase_date": purchase_date,
                "total_price": round(cost_per_card, 2),
                "purchased_from": "Break",
                "set_name": _safe_str(r.get("set_name", "")).strip(),
                "year": _safe_str(r.get("year", "")).strip(),
                "card_name": _safe_str(r.get("card_name", "")).strip(),
                "card_number": _safe_str(r.get("card_number", "")).strip(),
                "variant": _safe_str(r.get("variant", "")).strip(),
                "condition": _safe_str(r.get("condition", "")).strip(),
                "grading_company": _safe_str(r.get("grading_company", "")).strip(),
                "grade": _safe_str(r.get("grade", "")).strip(),
                "reference_link": _safe_str(r.get("reference_link", "")).strip(),
                "break_id": str(break_id).strip(),
                "break_box_name": box_name,
            }
            _append_row(ws_inv, inv_header, inv_row)

        if break_card_id:
            created_inventory_ids_by_break_card_id[break_card_id] = inv_ids

    # Update break_cards exported_to_inventory + inventory_ids
    # (batch update only the relevant rows/cols)
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

    # Update breaks sheet: status, finalized_at, cards_count, cost_per_card
    br_row = _find_row_by_id(ws_breaks, "break_id", break_id)
    if br_row is not None:
        values = ws_breaks.get_all_values()
        header = [str(h or "").strip() for h in values[0]] if values else breaks_header

        def _upd(col_name, val):
            idx = _col_index(header, col_name)
            if idx is None:
                return None
            col_letter = _a1_col_letter(idx + 1)
            return {"range": f"{col_letter}{br_row}", "values": [[val]]}

        upd_payload = []
        upd_payload.append(_upd("status", "FINALIZED"))
        upd_payload.append(_upd("finalized_at", _now_iso_local()))
        upd_payload.append(_upd("cards_count", total_units))
        upd_payload.append(_upd("cost_per_card", round(cost_per_card, 4)))
        upd_payload = [u for u in upd_payload if u is not None]

        if upd_payload:
            ws_breaks.batch_update(upd_payload, value_input_option="USER_ENTERED")

    return {
        "total_units": total_units,
        "total_cost": total_cost,
        "cost_per_card": cost_per_card,
        "rows_added_to_inventory": total_units,
    }


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
            price_per_box = st.number_input("Price per Box ($)", min_value=0.0, value=0.0, step=1.0)

        c4, c5, c6 = st.columns([1.5, 1, 1])
        with c4:
            box_name = st.text_input("Box Name (e.g., 2024 Prizm Blaster)")
        with c5:
            set_name = st.text_input("Set Name (optional)")
        with c6:
            year = st.text_input("Year (optional)")

        c7, c8 = st.columns([1, 2])
        with c7:
            box_type = st.text_input("Box Type (optional)", placeholder="Blaster / Hobby / ETB / Booster Box / etc.")
        with c8:
            reference_link = st.text_input("PriceCharting Link for Box (optional)")

        notes = st.text_area("Notes (optional)")

        submitted = st.form_submit_button("Add Break", use_container_width=True)
        if submitted:
            break_id = str(uuid.uuid4())[:8]
            total_price = float(qty_boxes) * float(price_per_box)

            row = {
                "break_id": break_id,
                "purchase_date": purchase_date.isoformat(),
                "box_name": box_name.strip(),
                "set_name": set_name.strip(),
                "year": year.strip(),
                "box_type": box_type.strip(),
                "reference_link": reference_link.strip(),
                "qty_boxes": int(qty_boxes),
                "price_per_box": float(price_per_box),
                "total_price": float(total_price),
                "notes": notes.strip(),
                "status": "OPEN",
                "created_at": _now_iso_local(),
                "finalized_at": "",
                "cards_count": "",
                "cost_per_card": "",
            }

            _append_row(ws_breaks, breaks_header, row)
            st.success(f"Break created: {break_id}")
            st.rerun()

    st.markdown("---")
    st.subheader("Your Breaks")

    if breaks_df.empty:
        st.info("No breaks yet. Create one above.")
    else:
        view = breaks_df.copy()
        view = view.sort_values(["purchase_date_dt", "created_at"], ascending=[False, False], na_position="last")
        show_cols = [
            "break_id",
            "status",
            "purchase_date",
            "box_name",
            "qty_boxes",
            "price_per_box",
            "total_price",
            "cards_count",
            "cost_per_card",
            "finalized_at",
        ]
        show_cols = [c for c in show_cols if c in view.columns]
        st.dataframe(view[show_cols], use_container_width=True, hide_index=True)


with tab2:
    st.subheader("Add Cards from a Break")
    if open_breaks.empty:
        st.info("No OPEN breaks found. Create a break first.")
    else:
        # Build select options
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

        # Summary
        total_cost = float(_to_num(pd.Series([b.get("total_price", 0.0)])).iloc[0])
        st.caption(f"Break Total Cost: ${total_cost:,.2f}")

        # Show existing cards for this break
        cards_for_break = break_cards_df[
            break_cards_df["break_id"].astype(str).str.strip().eq(str(break_id).strip())
        ].copy()

        if not cards_for_break.empty:
            cards_for_break["quantity"] = _to_num(cards_for_break["quantity"]).replace(0, 1)
            units = int(cards_for_break.loc[cards_for_break["exported_to_inventory"].astype(str).str.upper().ne("YES"), "quantity"].sum())
        else:
            units = 0

        if units > 0 and total_cost > 0:
            st.caption(f"Un-exported card units entered: {units} → Estimated cost per card: ${total_cost/units:,.2f}")
        else:
            st.caption("Un-exported card units entered: 0")

        st.markdown("### Add Card(s) from this Break")

        with st.form("add_break_card_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                card_type = st.selectbox("Card Type", options=["Pokemon", "Sports"], index=0)
            with c2:
                product_type = st.selectbox("Product Type", options=["Card"], index=0)
            with c3:
                quantity = st.number_input("Quantity", min_value=1, max_value=999, value=1, step=1)

            c4, c5, c6 = st.columns([1, 1, 1])
            with c4:
                set_name = st.text_input("Set Name", value=_safe_str(b.get("set_name", "")).strip())
            with c5:
                year = st.text_input("Year", value=_safe_str(b.get("year", "")).strip())
            with c6:
                condition = st.text_input("Condition (optional)", placeholder="Near Mint / LP / etc.")

            c7, c8, c9 = st.columns([1.5, 1, 1.5])
            with c7:
                card_name = st.text_input("Card Name")
            with c8:
                card_number = st.text_input("Card # (optional)")
            with c9:
                variant = st.text_input("Variant (optional)", placeholder="Silver / Holo / Parallel / etc.")

            c10, c11, c12 = st.columns([1, 1, 2])
            with c10:
                grading_company = st.text_input("Grading Company (optional)", placeholder="PSA / BGS / CGC")
            with c11:
                grade = st.text_input("Grade (optional)", placeholder="10 / 9 / etc.")
            with c12:
                ref_link = st.text_input("PriceCharting Link for CARD (optional)")

            submitted = st.form_submit_button("Add Card Line", use_container_width=True)
            if submitted:
                bc_id = str(uuid.uuid4())[:8]
                row = {
                    "break_card_id": bc_id,
                    "break_id": str(break_id).strip(),
                    "card_type": _normalize_card_type(card_type),
                    "product_type": product_type,
                    "set_name": set_name.strip(),
                    "year": year.strip(),
                    "card_name": card_name.strip(),
                    "card_number": card_number.strip(),
                    "variant": variant.strip(),
                    "condition": condition.strip(),
                    "grading_company": grading_company.strip(),
                    "grade": grade.strip(),
                    "reference_link": ref_link.strip(),
                    "quantity": int(quantity),
                    "created_at": _now_iso_local(),
                    "exported_to_inventory": "NO",
                    "inventory_ids": "",
                }
                _append_row(ws_break_cards, break_cards_header, row)
                st.success("Added.")
                st.rerun()

        st.markdown("---")
        st.markdown("### Cards Entered for this Break")

        if cards_for_break.empty:
            st.info("No cards entered yet.")
        else:
            show_cols = [
                "break_card_id",
                "card_type",
                "set_name",
                "year",
                "card_name",
                "card_number",
                "variant",
                "quantity",
                "exported_to_inventory",
                "inventory_ids",
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
                "This will allocate the break’s total cost evenly across ALL un-exported card units "
                "(sum of Quantity). Then it creates one inventory row per card unit, and marks the break as FINALIZED."
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
