import json
import re
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

INVENTORY_STATE_KEY = "inventory_df_v3"

DEFAULT_COLUMNS = [
    "inventory_id",
    "card_type",          # Pokemon / Sports / Other
    "brand_or_league",    # Pokemon TCG / Football / Basketball / etc.
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",       # optional: Rookie / Insert / Parallel / EX / etc.
    "reference_link",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",        # computed
    "condition",
    "notes",
    "created_at",
]

CARD_TYPE_OPTIONS = ["Pokemon", "Sports", "Other"]

CONDITION_OPTIONS = [
    "Near Mint",
    "Lightly Played",
    "Moderately Played",
    "Heavily Played",
    "Damaged",
]

SPORT_TOKENS = {
    "football": "Football",
    "basketball": "Basketball",
    "baseball": "Baseball",
    "hockey": "Hockey",
    "soccer": "Soccer",
    "golf": "Golf",
    "ufc": "UFC",
    "wrestling": "Wrestling",
}

NUMERIC_COLS = ["purchase_price", "shipping", "tax", "total_price"]


# =========================================================
# GOOGLE SHEETS
# =========================================================

@st.cache_resource
def get_gspread_client():
    """
    Reads service account JSON from file path specified in st.secrets:
      service_account_json_path = "secrets/gpc_service_account.json"
    """
    try:
        sa_rel = st.secrets["service_account_json_path"]
        sa_path = Path(sa_rel)
        if not sa_path.is_absolute():
            # resolve relative to project root (current working directory)
            sa_path = Path.cwd() / sa_rel

        if not sa_path.exists():
            st.error(f"Service account JSON not found at: {sa_path}")
            st.stop()

        sa_info = json.loads(sa_path.read_text(encoding="utf-8"))

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    except Exception as e:
        st.error(f"Failed to initialize Google Sheets client: {e}")
        st.stop()


def get_worksheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets["spreadsheet_id"]
    worksheet_name = st.secrets.get("inventory_worksheet", "inventory")
    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


def ensure_headers(ws, headers):
    """
    If sheet is empty, write header row.
    If header exists but missing columns, extend it (append missing at end).
    """
    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(headers)
        return headers

    existing = first_row
    missing = [h for h in headers if h not in existing]
    if missing:
        new_headers = existing + missing
        ws.update("1:1", [new_headers])
        return new_headers

    return existing


def sheets_load_inventory() -> pd.DataFrame:
    ws = get_worksheet()
    headers = ensure_headers(ws, DEFAULT_COLUMNS)

    records = ws.get_all_records()  # uses row 1 as headers
    df = pd.DataFrame(records)

    # Ensure all columns exist
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[DEFAULT_COLUMNS].copy()

    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["inventory_id"] = df["inventory_id"].astype(str)
    return df


def _find_row_numbers_by_inventory_id(ws, inventory_ids):
    """
    Returns dict: inventory_id -> sheet_row_number
    Assumes inventory_id is column A (first column).
    """
    col_a = ws.col_values(1)  # includes header
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_row[str(val)] = idx

    return {str(inv_id): id_to_row.get(str(inv_id)) for inv_id in inventory_ids}


def sheets_append_inventory_row(row: dict):
    ws = get_worksheet()
    headers = ensure_headers(ws, DEFAULT_COLUMNS)
    ordered = [row.get(h, "") for h in headers]
    ws.append_row(ordered, value_input_option="USER_ENTERED")


def sheets_update_rows(rows: pd.DataFrame):
    if rows.empty:
        return

    ws = get_worksheet()
    headers = ensure_headers(ws, DEFAULT_COLUMNS)

    inv_ids = rows["inventory_id"].astype(str).tolist()
    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inv_ids)

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]

    for _, r in rows.iterrows():
        inv_id = str(r["inventory_id"])
        rownum = id_to_rownum.get(inv_id)
        if not rownum:
            continue

        values = []
        for h in headers:
            v = r.get(h, "")
            if pd.isna(v):
                v = ""
            values.append(v)

        rng = f"A{rownum}:{last_col_letter}{rownum}"
        ws.update(rng, [values], value_input_option="USER_ENTERED")


def sheets_delete_rows_by_ids(inventory_ids):
    if not inventory_ids:
        return

    ws = get_worksheet()
    ensure_headers(ws, DEFAULT_COLUMNS)

    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inventory_ids)
    rownums = [rn for rn in id_to_rownum.values() if rn]

    # delete bottom-up to avoid shifting issues
    for rn in sorted(rownums, reverse=True):
        ws.delete_rows(rn)


# =========================================================
# SCRAPE HELPERS (PriceCharting + Sportscardspro)
# =========================================================

def _money(x) -> float:
    try:
        if x is None or x == "":
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def _compute_total(purchase_price, shipping, tax):
    return round(_money(purchase_price) + _money(shipping) + _money(tax), 2)


def _title_case_from_slug(slug: str) -> str:
    return " ".join([w for w in slug.replace("-", " ").split() if w]).title()


def _parse_set_slug_generic(set_slug: str):
    tokens = [t for t in set_slug.split("-") if t]
    year = ""
    for t in tokens:
        if re.fullmatch(r"(19|20)\d{2}", t):
            year = t
            break

    if tokens and tokens[0].lower() == "pokemon":
        set_name = _title_case_from_slug("-".join(tokens[1:])) if len(tokens) > 1 else ""
        return {"card_type": "Pokemon", "brand_or_league": "Pokemon TCG", "year": year, "set_name": set_name}

    sport_token = tokens[0].lower() if tokens else ""
    if sport_token in SPORT_TOKENS:
        brand_or_league = SPORT_TOKENS[sport_token]

        cleaned = tokens[:]
        if len(cleaned) > 1 and cleaned[1].lower() == "cards":
            cleaned = [cleaned[0]] + cleaned[2:]

        cleaned_no_year = [t for t in cleaned[1:] if t != year]
        set_name = _title_case_from_slug("-".join(cleaned_no_year)) if cleaned_no_year else ""

        return {"card_type": "Sports", "brand_or_league": brand_or_league, "year": year, "set_name": set_name}

    return {"card_type": "", "brand_or_league": "", "year": year, "set_name": _title_case_from_slug(set_slug)}


def _find_best_title(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()

    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(" ", strip=True)

    if soup.title and soup.title.string:
        return soup.title.string.strip()

    return ""


def _parse_header_like_sportscardspro(header: str):
    out = {"card_name": "", "variant": "", "card_number": "", "year": "", "set_name": ""}
    if not header:
        return out

    header = header.strip()

    m_num = re.search(r"#\s*([A-Za-z0-9]+)", header)
    if m_num:
        out["card_number"] = m_num.group(1).strip()

    if "#" in header and m_num:
        left = header[:m_num.start()].strip()
        right = header[m_num.end():].strip()
    else:
        left, right = header, ""

    m_var = re.search(r"\[(.+?)\]", left)
    if m_var:
        out["variant"] = m_var.group(1).strip()
        left = re.sub(r"\s*\[.+?\]\s*", " ", left).strip()

    out["card_name"] = left.strip()

    m_year = re.match(r"((19|20)\d{2})\s+(.+)$", right)
    if m_year:
        out["year"] = m_year.group(1).strip()
        out["set_name"] = m_year.group(3).strip()
    else:
        out["set_name"] = right.strip()

    return out


def _parse_pricecharting_title(title: str):
    if not title:
        return {"card_name": "", "card_number": "", "variant": ""}

    num = ""
    m = re.search(r"#\s*([A-Za-z0-9]+)", title)
    if m:
        num = m.group(1).strip()

    name_part = title.split("#")[0].strip() if "#" in title else title.strip()
    for sep in [" - ", " – "]:
        if sep in name_part:
            name_part = name_part.split(sep)[0].strip()

    variant = ""
    tokens = name_part.split()
    if tokens and tokens[-1].lower() in {"ex", "gx", "v", "vmax", "silver", "holo"}:
        variant = tokens[-1]
        name_part = " ".join(tokens[:-1]).strip()

    return {"card_name": name_part, "card_number": num, "variant": variant}


def fetch_card_details_from_link(url: str):
    result = {
        "card_type": "",
        "brand_or_league": "",
        "set_name": "",
        "year": "",
        "card_name": "",
        "card_number": "",
        "variant": "",
        "card_subtype": "",
        "reference_link": url,
    }

    if not url or not url.strip():
        return result

    url = url.strip()
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    soup = None
    page_title = ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTrackerPrototype; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        page_title = _find_best_title(soup)
    except Exception:
        soup = None
        page_title = ""

    def _extract_game_parts(p: str):
        parts = [x for x in p.split("/") if x]
        if len(parts) >= 3 and parts[0].lower() == "game":
            return parts[1], parts[2]
        return None, None

    set_slug, card_slug = _extract_game_parts(path)

    if "sportscardspro.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))

        header_text = ""
        if soup:
            h1 = soup.find("h1")
            if h1 and h1.get_text(strip=True):
                header_text = h1.get_text(" ", strip=True)
        header_text = header_text or page_title

        header_info = _parse_header_like_sportscardspro(header_text)
        for k in ["card_name", "variant", "card_number", "year", "set_name"]:
            if header_info.get(k):
                result[k] = header_info[k] or result.get(k, "")

        if not result["card_name"] and card_slug:
            result["card_name"] = _title_case_from_slug(re.sub(r"-(\d+[A-Za-z0-9]*)$", "", card_slug))
        if not result["card_number"] and card_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug)
            if m:
                result["card_number"] = m.group(1)

        return result

    if "pricecharting.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))
        result.update(_parse_pricecharting_title(page_title))

        if not result["card_number"] and card_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug)
            if m:
                result["card_number"] = m.group(1)
        if not result["card_name"] and card_slug:
            cleaned = re.sub(r"-(\d+[A-Za-z0-9]*)$", "", card_slug)
            result["card_name"] = _title_case_from_slug(cleaned)

        return result

    header_info = _parse_header_like_sportscardspro(page_title)
    for k in ["card_name", "variant", "card_number", "year", "set_name"]:
        if header_info.get(k):
            result[k] = header_info[k]

    lowered = (url + " " + page_title).lower()
    if "pokemon" in lowered:
        result["card_type"] = "Pokemon"
        result["brand_or_league"] = "Pokemon TCG"
    elif any(tok in lowered for tok in ["prizm", "optic", "select", "donruss", "panini", "topps"]):
        result["card_type"] = "Sports"

    return result


# =========================================================
# STATE
# =========================================================

def init_inventory_from_sheets():
    if INVENTORY_STATE_KEY not in st.session_state:
        st.session_state[INVENTORY_STATE_KEY] = sheets_load_inventory()


def refresh_inventory_from_sheets():
    st.session_state[INVENTORY_STATE_KEY] = sheets_load_inventory()


def _safe_money_display(x):
    if pd.isna(x) or x is None or x == "":
        return ""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)


# =========================================================
# UI
# =========================================================

st.set_page_config(page_title="Inventory", layout="wide")
init_inventory_from_sheets()

st.title("Inventory")

top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Refresh from Sheets", use_container_width=True):
        refresh_inventory_from_sheets()
        st.success("Reloaded from Google Sheets.")

tab_new, tab_list, tab_summary = st.tabs(["New Inventory", "Inventory List", "Inventory Summary"])

# ---------------------------
# TAB 1: New Inventory
# ---------------------------
with tab_new:
    st.subheader("Add new inventory (intake)")
    st.caption("Paste a PriceCharting or Sportscardspro link and click Pull details to auto-fill fields.")

    link_col1, link_col2 = st.columns([4, 1])
    with link_col1:
        reference_link = st.text_input(
            "Reference link (recommended)",
            key="ref_link_input",
            placeholder="https://www.sportscardspro.com/game/football-cards-2025-panini-prizm/jaxson-dart-white-disco-332",
        )
    with link_col2:
        pull = st.button("Pull details", use_container_width=True)

    if pull:
        details = fetch_card_details_from_link(reference_link)
        st.session_state["prefill_details"] = details
        if any(details.get(k) for k in ["card_name", "set_name", "card_number", "variant", "card_type", "year"]):
            st.success("Pulled details. Review/adjust below, then add to inventory.")
        else:
            st.warning("Could not pull much from that link. You can still enter details manually.")

    prefill = st.session_state.get("prefill_details", {}) or {}

    with st.form("new_inventory_form_v3", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            card_type = st.selectbox(
                "Card type*",
                CARD_TYPE_OPTIONS,
                index=(CARD_TYPE_OPTIONS.index(prefill.get("card_type")) if prefill.get("card_type") in CARD_TYPE_OPTIONS else 0),
            )
            brand_or_league = st.text_input(
                "Brand / League*",
                value=prefill.get("brand_or_league", ""),
                placeholder="Pokemon TCG / Football / NBA / MLB / Soccer / etc.",
            )
            year = st.text_input("Year (optional)", value=prefill.get("year", ""), placeholder="2024, 2025, ...")

        with c2:
            set_name = st.text_input("Set (optional)", value=prefill.get("set_name", ""), placeholder="Destined Rivals, Panini Prizm, Optic, ...")
            card_name = st.text_input("Card name*", value=prefill.get("card_name", ""), placeholder="Pikachu / Jaxson Dart / etc.")
            card_number = st.text_input("Card # (optional)", value=prefill.get("card_number", ""), placeholder="332")

        with c3:
            variant = st.text_input("Variant (optional)", value=prefill.get("variant", ""), placeholder="White Disco, Silver, Holo, Parallel, EX, ...")
            card_subtype = st.text_input("Card subtype (optional)", value=prefill.get("card_subtype", ""), placeholder="Rookie, Insert, Parallel, etc.")
            condition = st.selectbox("Condition*", CONDITION_OPTIONS, index=0)

        st.markdown("---")

        c4, c5, c6 = st.columns(3)
        with c4:
            purchase_date = st.date_input("Purchase date*", value=date.today())
            purchased_from = st.text_input("Purchased from*", placeholder="eBay, Opened, Whatnot, Card show, LCS, etc.")

        with c5:
            purchase_price = st.number_input("Purchase price*", min_value=0.0, step=1.0, format="%.2f")
            shipping = st.number_input("Shipping", min_value=0.0, step=1.0, format="%.2f")

        with c6:
            tax = st.number_input("Tax", min_value=0.0, step=1.0, format="%.2f")
            notes = st.text_area("Notes (optional)", height=92, placeholder="Anything you want to remember…")

        submitted = st.form_submit_button("Add to Inventory", type="primary", use_container_width=True)

        if submitted:
            missing = []
            if not card_name.strip():
                missing.append("Card name")
            if not brand_or_league.strip():
                missing.append("Brand / League")
            if not purchased_from.strip():
                missing.append("Purchased from")

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                total_price = _compute_total(purchase_price, shipping, tax)

                new_row = {
                    "inventory_id": str(uuid.uuid4())[:8],
                    "card_type": card_type.strip(),
                    "brand_or_league": brand_or_league.strip(),
                    "set_name": set_name.strip() if set_name else "",
                    "year": year.strip() if year else "",
                    "card_name": card_name.strip(),
                    "card_number": card_number.strip() if card_number else "",
                    "variant": variant.strip() if variant else "",
                    "card_subtype": card_subtype.strip() if card_subtype else "",
                    "reference_link": reference_link.strip() if reference_link else "",
                    "purchase_date": str(purchase_date),
                    "purchased_from": purchased_from.strip(),
                    "purchase_price": float(purchase_price),
                    "shipping": float(shipping),
                    "tax": float(tax),
                    "total_price": float(total_price),
                    "condition": condition,
                    "notes": notes.strip() if notes else "",
                    "created_at": pd.Timestamp.utcnow().isoformat(),
                }

                sheets_append_inventory_row(new_row)
                st.session_state["prefill_details"] = {}
                refresh_inventory_from_sheets()

                st.success(f"Added: {new_row['inventory_id']} — {new_row['card_name']} ({new_row['set_name']})")

    df = st.session_state[INVENTORY_STATE_KEY]
    if len(df) > 0:
        st.markdown("#### Recently added")
        show = df.tail(10).copy()
        for col in ["purchase_price", "shipping", "tax", "total_price"]:
            show[col] = show[col].apply(_safe_money_display)
        st.dataframe(show, use_container_width=True, hide_index=True)
    else:
        st.info("No inventory yet — add your first card above.")

# ---------------------------
# TAB 2: Inventory List (per-row delete checkbox)
# ---------------------------
with tab_list:
    st.subheader("Inventory List")

    df = st.session_state[INVENTORY_STATE_KEY].copy()
    if df.empty:
        st.info("No inventory yet. Add cards in the New Inventory tab.")
    else:
        f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.2, 2])
        with f1:
            type_filter = st.multiselect("Card type", sorted(df["card_type"].dropna().unique().tolist()), default=[])
        with f2:
            league_filter = st.multiselect("Brand/League", sorted(df["brand_or_league"].dropna().unique().tolist()), default=[])
        with f3:
            set_filter = st.multiselect("Set", sorted(df["set_name"].dropna().unique().tolist()), default=[])
        with f4:
            search = st.text_input("Search (name/set/notes/id)", placeholder="Type to filter…")

        filtered = df.copy()
        if type_filter:
            filtered = filtered[filtered["card_type"].isin(type_filter)]
        if league_filter:
            filtered = filtered[filtered["brand_or_league"].isin(league_filter)]
        if set_filter:
            filtered = filtered[filtered["set_name"].isin(set_filter)]
        if search.strip():
            s = search.strip().lower()
            filtered = filtered[
                filtered.apply(
                    lambda r: (
                        s in str(r.get("inventory_id", "")).lower()
                        or s in str(r.get("card_name", "")).lower()
                        or s in str(r.get("set_name", "")).lower()
                        or s in str(r.get("notes", "")).lower()
                        or s in str(r.get("brand_or_league", "")).lower()
                    ),
                    axis=1,
                )
            ]

        st.caption(f"Showing {len(filtered):,} of {len(df):,} items")

        display = filtered.copy()
        display.insert(0, "delete", False)

        edited = st.data_editor(
            display,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "delete": st.column_config.CheckboxColumn("Delete", help="Check to delete this row"),
                "reference_link": st.column_config.LinkColumn("Reference link"),
            },
        )

        c1, c2 = st.columns([1, 3])
        with c1:
            apply_btn = st.button("Apply changes", type="primary", use_container_width=True)

        with c2:
            csv = filtered.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download filtered CSV",
                data=csv,
                file_name="inventory_filtered.csv",
                mime="text/csv",
                use_container_width=True,
            )

        if apply_btn:
            edited_rows = edited.drop(columns=["delete"], errors="ignore").copy()

            for c in ["purchase_price", "shipping", "tax"]:
                if c in edited_rows.columns:
                    edited_rows[c] = pd.to_numeric(edited_rows[c], errors="coerce").fillna(0.0)

            if set(["purchase_price", "shipping", "tax"]).issubset(edited_rows.columns):
                edited_rows["total_price"] = (edited_rows["purchase_price"] + edited_rows["shipping"] + edited_rows["tax"]).round(2)

            delete_ids = edited.loc[edited["delete"] == True, "inventory_id"].astype(str).tolist()

            sheets_update_rows(edited_rows)

            if delete_ids:
                sheets_delete_rows_by_ids(delete_ids)
                st.success(f"Deleted {len(delete_ids)} item(s).")

            refresh_inventory_from_sheets()
            if not delete_ids:
                st.success("Changes saved.")

# ---------------------------
# TAB 3: Inventory Summary
# ---------------------------
with tab_summary:
    st.subheader("Inventory Summary")

    df = st.session_state[INVENTORY_STATE_KEY].copy()
    if df.empty:
        st.info("No inventory yet. Add cards in the New Inventory tab.")
    else:
        total_cards = len(df)
        total_invested = df["total_price"].fillna(0).sum()

        k1, k2 = st.columns(2)
        k1.metric("Cards", f"{total_cards:,}")
        k2.metric("Total Invested", f"${total_invested:,.2f}")

        st.markdown("---")
        st.markdown("### Breakdown by Card Type")
        type_summary = (
            df.groupby("card_type", dropna=False)
            .agg(cards=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(type_summary, use_container_width=True, hide_index=True)
        st.bar_chart(type_summary.set_index("card_type")[["invested"]])

        st.markdown("---")
        st.markdown("### Top Sets by Invested")
        set_summary = (
            df.groupby(["card_type", "brand_or_league", "set_name"], dropna=False)
            .agg(cards=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(set_summary.head(30), use_container_width=True, hide_index=True)
