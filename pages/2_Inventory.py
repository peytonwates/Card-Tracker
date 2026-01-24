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

INVENTORY_STATE_KEY = "inventory_df_v5"

# New: Product Type + Sealed Product Type
DEFAULT_COLUMNS = [
    "inventory_id",
    "product_type",        # Card / Sealed
    "sealed_product_type", # Booster Box / ETB / etc (only for sealed)
    "card_type",           # Pokemon / Sports / Other
    "brand_or_league",     # Pokemon TCG / Football / Basketball / etc.
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",        # optional: Rookie / Insert / Parallel / EX / etc.
    "reference_link",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",         # computed
    "condition",
    "notes",
    "created_at",
]

PRODUCT_TYPE_OPTIONS = ["Card", "Sealed"]
CARD_TYPE_OPTIONS = ["Pokemon", "Sports", "Other"]

POKEMON_SEALED_TYPE_OPTIONS = [
    "Booster Box",
    "Booster Bundle",
    "Premium Collection Box",
    "Blister Pack",
    "Elite Trainer Box",
    "Tech Sticker Collection",
    "Collection Box",
]

# Common sealed types we can infer from PriceCharting slug/title
SEALED_TYPE_KEYWORDS = {
    "elite-trainer-box": "Elite Trainer Box",
    "etb": "Elite Trainer Box",
    "booster-box": "Booster Box",
    "booster-display": "Booster Box",
    "booster-bundle": "Booster Bundle",
    "booster-pack": "Booster Pack",
    "sleeved-booster": "Sleeved Booster",
    "tin": "Tin",
    "collection-box": "Collection Box",
    "premium-collection": "Premium Collection",
    "battle-box": "Battle Box",
    "theme-deck": "Theme Deck",
    "starter-deck": "Starter Deck",
    "trainer-toolkit": "Trainer Toolkit",
    "bundle": "Bundle",
}

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

# IMPORTANT:
# You manually added "Product Type" in Google Sheets (with a space).
# We support BOTH styles using aliases.
HEADER_ALIASES = {
    "product_type": ["product_type", "Product Type"],
    "sealed_product_type": ["sealed_product_type", "Sealed Product Type"],
}

def internal_to_sheet_header(internal: str, existing_headers: list[str]) -> str:
    """Return the actual header name in the sheet for an internal column key."""
    aliases = HEADER_ALIASES.get(internal, [internal])
    for a in aliases:
        if a in existing_headers:
            return a
    if internal == "product_type":
        return "Product Type"
    if internal == "sealed_product_type":
        return "Sealed Product Type"
    return internal

def sheet_header_to_internal(header: str) -> str:
    """Map a sheet header name to the internal column key, if known."""
    for internal, aliases in HEADER_ALIASES.items():
        if header in aliases:
            return internal
    return header


# =========================================================
# GOOGLE SHEETS
# =========================================================

@st.cache_resource
def get_gspread_client():
    """
    Supports:
    - Streamlit Cloud: gcp_service_account stored as TOML table OR JSON string
    - Local: service_account_json_path points to a JSON file
    """
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

    # Local dev: JSON file
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


def get_worksheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets["spreadsheet_id"]
    worksheet_name = st.secrets.get("inventory_worksheet", "inventory")
    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


def ensure_headers(ws, internal_headers):
    """
    - If sheet empty, write header row using preferred sheet header names
    - If header exists, ensure it includes columns for all internal headers
      while respecting aliases like "Product Type".
    """
    first_row = ws.row_values(1)
    if not first_row:
        sheet_headers = []
        for internal in internal_headers:
            if internal == "product_type":
                sheet_headers.append("Product Type")
            elif internal == "sealed_product_type":
                sheet_headers.append("Sealed Product Type")
            else:
                sheet_headers.append(internal)
        ws.append_row(sheet_headers)
        return sheet_headers

    existing = first_row
    existing_internal = set(sheet_header_to_internal(h) for h in existing)

    missing_internal = [h for h in internal_headers if h not in existing_internal]
    if missing_internal:
        additions = []
        for internal in missing_internal:
            additions.append(internal_to_sheet_header(internal, existing))
        new_headers = existing + additions
        ws.update("1:1", [new_headers])
        return new_headers

    return existing


def sheets_load_inventory() -> pd.DataFrame:
    ws = get_worksheet()
    sheet_headers = ensure_headers(ws, DEFAULT_COLUMNS)

    records = ws.get_all_records()
    df = pd.DataFrame(records)

    if not df.empty:
        df = df.rename(columns={c: sheet_header_to_internal(c) for c in df.columns})

    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[DEFAULT_COLUMNS].copy()

    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["inventory_id"] = df["inventory_id"].astype(str)

    if "product_type" in df.columns:
        df["product_type"] = df["product_type"].replace("", "Card").fillna("Card")

    return df


def _find_row_numbers_by_inventory_id(ws, inventory_ids):
    col_a = ws.col_values(1)
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_row[str(val)] = idx
    return {str(inv_id): id_to_row.get(str(inv_id)) for inv_id in inventory_ids}


def sheets_append_inventory_row(row_internal: dict):
    ws = get_worksheet()
    sheet_headers = ensure_headers(ws, DEFAULT_COLUMNS)

    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}

    ordered = []
    for sheet_h in sheet_headers:
        internal = header_to_internal.get(sheet_h, sheet_h)
        ordered.append(row_internal.get(internal, ""))

    ws.append_row(ordered, value_input_option="USER_ENTERED")


def sheets_update_rows(rows_internal: pd.DataFrame):
    if rows_internal.empty:
        return

    ws = get_worksheet()
    sheet_headers = ensure_headers(ws, DEFAULT_COLUMNS)
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}

    inv_ids = rows_internal["inventory_id"].astype(str).tolist()
    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inv_ids)

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(sheet_headers)).split("1")[0]

    for _, r in rows_internal.iterrows():
        inv_id = str(r["inventory_id"])
        rownum = id_to_rownum.get(inv_id)
        if not rownum:
            continue

        values = []
        for sheet_h in sheet_headers:
            internal = header_to_internal.get(sheet_h, sheet_h)
            v = r.get(internal, "")
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


def _infer_sealed_type_from_slug_or_title(slug: str, title: str) -> str:
    text = (slug or "") + " " + (title or "")
    t = text.lower()
    for k, v in SEALED_TYPE_KEYWORDS.items():
        if k in t:
            return v
    if "box" in t:
        return "Box"
    if "bundle" in t:
        return "Bundle"
    if "tin" in t:
        return "Tin"
    if "pack" in t:
        return "Pack"
    return ""


def _looks_like_single_card_slug(card_slug: str) -> bool:
    if not card_slug:
        return False
    return bool(re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug))


def fetch_item_details_from_link(url: str):
    """
    Returns details for both Cards and Sealed items.
    """
    result = {
        "product_type": "Card",
        "sealed_product_type": "",
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

    # Sportscardspro = singles (Card)
    if "sportscardspro.com" in host and set_slug:
        result["product_type"] = "Card"
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

    # PriceCharting = could be card or sealed
    if "pricecharting.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))

        if card_slug and not _looks_like_single_card_slug(card_slug):
            # Sealed item
            result["product_type"] = "Sealed"
            result["sealed_product_type"] = _infer_sealed_type_from_slug_or_title(card_slug, page_title)
            if result["sealed_product_type"]:
                result["card_name"] = result["sealed_product_type"]
            else:
                result["card_name"] = page_title or _title_case_from_slug(card_slug)
            result["card_number"] = ""
            result["variant"] = ""
            return result

        # Single card
        result["product_type"] = "Card"
        result.update(_parse_pricecharting_title(page_title))

        if not result["card_number"] and card_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug)
            if m:
                result["card_number"] = m.group(1)
        if not result["card_name"] and card_slug:
            cleaned = re.sub(r"-(\d+[A-Za-z0-9]*)$", "", card_slug)
            result["card_name"] = _title_case_from_slug(cleaned)

        return result

    # Generic fallback
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
            placeholder="https://www.pricecharting.com/game/pokemon-surging-sparks/elite-trainer-box",
        )
    with link_col2:
        pull = st.button("Pull details", use_container_width=True)

    if pull:
        details = fetch_item_details_from_link(reference_link)
        st.session_state["prefill_details"] = details
        if any(details.get(k) for k in ["card_name", "set_name", "card_number", "variant", "card_type", "year", "sealed_product_type"]):
            st.success("Pulled details. Review/adjust below, then add to inventory.")
        else:
            st.warning("Could not pull much from that link. You can still enter details manually.")

    prefill = st.session_state.get("prefill_details", {}) or {}

    with st.form("new_inventory_form_v5", clear_on_submit=True):
        # First choose Product Type and Card Type (so we can decide sealed type behavior)
        a1, a2, a3 = st.columns([1.2, 1.2, 2.2])

        with a1:
            product_type = st.selectbox(
                "Product Type*",
                PRODUCT_TYPE_OPTIONS,
                index=(PRODUCT_TYPE_OPTIONS.index(prefill.get("product_type")) if prefill.get("product_type") in PRODUCT_TYPE_OPTIONS else 0),
            )
        with a2:
            card_type = st.selectbox(
                "Card Type*",
                CARD_TYPE_OPTIONS,
                index=(CARD_TYPE_OPTIONS.index(prefill.get("card_type")) if prefill.get("card_type") in CARD_TYPE_OPTIONS else 0),
            )
        with a3:
            sealed_product_type = ""
            if product_type == "Sealed":
                # Pokemon sealed -> dropdown. Anything else -> free text.
                if card_type == "Pokemon":
                    pre = (prefill.get("sealed_product_type") or "").strip()
                    options = [""] + POKEMON_SEALED_TYPE_OPTIONS
                    idx = options.index(pre) if pre in options else 0
                    sealed_product_type = st.selectbox(
                        "Sealed Product Type*",
                        options=options,
                        index=idx,
                        help="For Pokemon sealed products, choose a standardized type.",
                    )
                else:
                    sealed_product_type = st.text_input(
                        "Sealed Product Type*",
                        value=(prefill.get("sealed_product_type") or ""),
                        placeholder="Hobby box, blaster, mega box, fat pack, etc.",
                    )
            else:
                st.caption("Sealed Product Type appears when Product Type = Sealed")

        c1, c2, c3 = st.columns(3)

        with c1:
            brand_or_league = st.text_input(
                "Brand / League*",
                value=prefill.get("brand_or_league", ""),
                placeholder="Pokemon TCG / Football / NBA / MLB / Soccer / etc.",
            )
            year = st.text_input("Year (optional)", value=prefill.get("year", ""), placeholder="2024, 2025, ...")

        with c2:
            set_name = st.text_input("Set (optional)", value=prefill.get("set_name", ""), placeholder="Surging Sparks, Panini Prizm, Optic, ...")

            card_name_label = "Item name*" if product_type == "Sealed" else "Card name*"
            card_name = st.text_input(card_name_label, value=prefill.get("card_name", ""), placeholder="Elite Trainer Box / Pikachu / Jaxson Dart / etc.")

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
                missing.append("Item/Card name")
            if not brand_or_league.strip():
                missing.append("Brand / League")
            if not purchased_from.strip():
                missing.append("Purchased from")
            if product_type == "Sealed" and not sealed_product_type.strip():
                missing.append("Sealed Product Type")

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                total_price = _compute_total(purchase_price, shipping, tax)

                new_row = {
                    "inventory_id": str(uuid.uuid4())[:8],
                    "product_type": product_type.strip(),
                    "sealed_product_type": sealed_product_type.strip() if product_type == "Sealed" else "",
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
        st.info("No inventory yet — add your first item above.")

# ---------------------------
# TAB 2: Inventory List
# ---------------------------
with tab_list:
    st.subheader("Inventory List")

    df = st.session_state[INVENTORY_STATE_KEY].copy()
    if df.empty:
        st.info("No inventory yet. Add items in the New Inventory tab.")
    else:
        f1, f2, f3, f4, f5 = st.columns([1.1, 1.1, 1.1, 1.1, 2])

        with f1:
            product_filter = st.multiselect("Product Type", sorted(df["product_type"].dropna().unique().tolist()), default=[])
        with f2:
            type_filter = st.multiselect("Card Type", sorted(df["card_type"].dropna().unique().tolist()), default=[])
        with f3:
            league_filter = st.multiselect("Brand/League", sorted(df["brand_or_league"].dropna().unique().tolist()), default=[])
        with f4:
            set_filter = st.multiselect("Set", sorted(df["set_name"].dropna().unique().tolist()), default=[])
        with f5:
            search = st.text_input("Search (name/set/notes/id)", placeholder="Type to filter…")

        filtered = df.copy()
        if product_filter:
            filtered = filtered[filtered["product_type"].isin(product_filter)]
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
                        or s in str(r.get("product_type", "")).lower()
                        or s in str(r.get("sealed_product_type", "")).lower()
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
        st.info("No inventory yet. Add items in the New Inventory tab.")
    else:
        total_items = len(df)
        total_invested = df["total_price"].fillna(0).sum()

        k1, k2 = st.columns(2)
        k1.metric("Items", f"{total_items:,}")
        k2.metric("Total Invested", f"${total_invested:,.2f}")

        st.markdown("---")
        st.markdown("### Breakdown by Product Type")
        p_summary = (
            df.groupby("product_type", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(p_summary, use_container_width=True, hide_index=True)
        st.bar_chart(p_summary.set_index("product_type")[["invested"]])

        st.markdown("---")
        st.markdown("### Breakdown by Card Type")
        type_summary = (
            df.groupby("card_type", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(type_summary, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("### Top Sets by Invested")
        set_summary = (
            df.groupby(["product_type", "card_type", "brand_or_league", "set_name"], dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(set_summary.head(30), use_container_width=True, hide_index=True)
