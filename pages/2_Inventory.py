# pages/2_Inventory.py
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

INVENTORY_STATE_KEY = "inventory_df_v8"

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
STATUS_TRADED = "TRADED"

PRODUCT_TYPE_OPTIONS = ["Card", "Sealed", "Graded Card"]
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

GRADING_COMPANY_OPTIONS = ["PSA", "CGC", "Beckett"]

# PSA: 1-10 with half grades (must include 10)
PSA_GRADE_OPTIONS = ["10", "9.5", "9", "8.5", "8", "7.5", "7", "6.5", "6", "5.5", "5", "4.5", "4", "3.5", "3", "2.5", "2", "1.5", "1"]
CGC_GRADE_OPTIONS = ["Pristine 10"] + PSA_GRADE_OPTIONS
BECKETT_GRADE_OPTIONS = ["Black Label 10"] + PSA_GRADE_OPTIONS

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

# Columns stored in Google Sheets (internal names)
DEFAULT_COLUMNS = [
    "inventory_id",
    "image_url",
    "product_type",            # Card / Sealed / Graded Card
    "sealed_product_type",     # only for sealed
    "card_type",               # Pokemon / Sports / Other
    "brand_or_league",         # Pokemon TCG / Football / etc.
    "set_name",
    "year",
    "card_name",               # for sealed: item name
    "card_number",
    "variant",
    "card_subtype",
    "grading_company",         # only for graded
    "grade",                   # only for graded
    "reference_link",
    "purchase_date",
    "purchased_from",
    "purchase_price",
    "shipping",
    "tax",
    "total_price",
    "condition",               # raw card condition; sealed="Sealed"; graded="Graded"
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
    # ---- cached market pricing (populated elsewhere, e.g. Transactions refresh) ----
    "market_price",
    "market_price_updated_at",
]

NUMERIC_COLS = ["purchase_price", "shipping", "tax", "total_price", "market_price"]

# Support both header styles if sheet was edited manually
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

# =========================================================
# GOOGLE SHEETS CLIENT
# =========================================================

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

def get_worksheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets["spreadsheet_id"]
    worksheet_name = st.secrets.get("inventory_worksheet", "inventory")
    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)

def ensure_headers(ws):
    first_row = ws.row_values(1)
    if not first_row:
        sheet_headers = []
        for internal in DEFAULT_COLUMNS:
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

    missing_internal = [h for h in DEFAULT_COLUMNS if h not in existing_internal]
    if missing_internal:
        additions = [internal_to_sheet_header(h, existing) for h in missing_internal]
        new_headers = existing + additions
        ws.update("1:1", [new_headers])
        return new_headers

    return existing

# =========================================================
# SCRAPING (DETAILS + IMAGE)
# =========================================================

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

def _find_best_image(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:image"})
    if tw and tw.get("content"):
        return tw["content"].strip()
    img = soup.find("img")
    if img and img.get("src"):
        return img["src"].strip()
    return ""

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

def _looks_like_single_card_slug(card_slug: str) -> bool:
    if not card_slug:
        return False
    return bool(re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug))

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

SEALED_TYPE_KEYWORDS = {
    "elite-trainer-box": "Elite Trainer Box",
    "etb": "Elite Trainer Box",
    "booster-box": "Booster Box",
    "booster-display": "Booster Box",
    "booster-bundle": "Booster Bundle",
    "blister": "Blister Pack",
    "tech-sticker-collection": "Tech Sticker Collection",
    "collection-box": "Collection Box",
    "premium-collection": "Premium Collection Box",
}

def _infer_sealed_type_from_slug_or_title(slug: str, title: str) -> str:
    t = ((slug or "") + " " + (title or "")).lower()
    for k, v in SEALED_TYPE_KEYWORDS.items():
        if k in t:
            return v
    if "elite trainer box" in t:
        return "Elite Trainer Box"
    if "booster box" in t:
        return "Booster Box"
    if "booster bundle" in t:
        return "Booster Bundle"
    if "tech sticker collection" in t:
        return "Tech Sticker Collection"
    if "premium collection" in t:
        return "Premium Collection Box"
    if "collection box" in t:
        return "Collection Box"
    if "blister" in t:
        return "Blister Pack"
    return ""

@st.cache_data(show_spinner=False, ttl=60 * 60 * 24)
def fetch_details_and_image(url: str):
    result = {
        "image_url": "",
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
    image_url = ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTrackerPrototype; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        page_title = _find_best_title(soup)
        image_url = _find_best_image(soup)
        if image_url and image_url.startswith("//"):
            image_url = "https:" + image_url
    except Exception:
        soup = None
        page_title = ""
        image_url = ""

    result["image_url"] = image_url

    parts = [x for x in path.split("/") if x]
    set_slug, item_slug = None, None
    if len(parts) >= 3 and parts[0].lower() == "game":
        set_slug, item_slug = parts[1], parts[2]

    if "pricecharting.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))

        if item_slug and not _looks_like_single_card_slug(item_slug):
            result["product_type"] = "Sealed"
            result["sealed_product_type"] = _infer_sealed_type_from_slug_or_title(item_slug, page_title)
            result["card_name"] = result["sealed_product_type"] or (page_title or _title_case_from_slug(item_slug))
            result["card_number"] = ""
            result["variant"] = ""
            result["card_subtype"] = ""
            return result

        result["product_type"] = "Card"
        result.update(_parse_pricecharting_title(page_title))
        if not result["card_number"] and item_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", item_slug)
            if m:
                result["card_number"] = m.group(1)
        if not result["card_name"] and item_slug:
            cleaned = re.sub(r"-(\d+[A-Za-z0-9]*)$", "", item_slug)
            result["card_name"] = _title_case_from_slug(cleaned)
        return result

    lowered = (url + " " + page_title).lower()
    if "pokemon" in lowered:
        result["card_type"] = "Pokemon"
        result["brand_or_league"] = "Pokemon TCG"
    elif any(tok in lowered for tok in ["prizm", "optic", "select", "donruss", "panini", "topps"]):
        result["card_type"] = "Sports"

    if page_title:
        result["card_name"] = page_title

    return result

# =========================================================
# DATA HELPERS
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

def _safe_money_display(x):
    if pd.isna(x) or x is None or x == "":
        return ""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return str(x)

# =========================================================
# SHEETS LOAD/SAVE
# =========================================================

def sheets_load_inventory() -> pd.DataFrame:
    ws = get_worksheet()
    ensure_headers(ws)

    records = ws.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        df = pd.DataFrame(columns=DEFAULT_COLUMNS)

    # rename sheet headers -> internal names (may create duplicates!)
    df = df.rename(columns={c: sheet_header_to_internal(c) for c in df.columns})

    # ensure expected columns exist
    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # coalesce duplicates safely
    coalesce_cols = [
        "product_type",
        "sealed_product_type",
        "inventory_status",
        "listed_transaction_id",
        "image_url",
        "market_price",
        "market_price_updated_at",
    ]
    for col in coalesce_cols:
        if col in df.columns:
            obj = df.loc[:, col]
            if isinstance(obj, pd.DataFrame):
                combined = None
                for i in range(obj.shape[1]):
                    s = obj.iloc[:, i]
                    s = s.astype(str)
                    s = s.where(s.str.strip() != "", "")
                    if combined is None:
                        combined = s
                    else:
                        combined = combined.where(combined != "", s)
                df[col] = combined
            else:
                s = obj.astype(str)
                df[col] = s.where(s.str.strip() != "", "")

    # drop duplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # enforce ordering
    df = df[[c for c in DEFAULT_COLUMNS if c in df.columns]].copy()

    # numeric cleanup
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    # normalize defaults
    df["inventory_id"] = df["inventory_id"].astype(str)
    df["product_type"] = df["product_type"].replace("", "Card").fillna("Card")
    df["inventory_status"] = df["inventory_status"].replace("", STATUS_ACTIVE).fillna(STATUS_ACTIVE)

    # enforce condition invariants
    df["condition"] = df["condition"].astype(str)
    df.loc[df["product_type"] == "Sealed", "condition"] = "Sealed"
    df.loc[df["product_type"] == "Graded Card", "condition"] = "Graded"

    return df

def sheets_append_inventory_row(row_internal: dict):
    ws = get_worksheet()
    sheet_headers = ensure_headers(ws)
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
    sheet_headers = ensure_headers(ws)
    header_to_internal = {h: sheet_header_to_internal(h) for h in sheet_headers}

    # map inventory_id -> row number by reading col A once
    col_a = ws.col_values(1)
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_row[str(val)] = idx

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(sheet_headers)).split("1")[0]

    for _, r in rows_internal.iterrows():
        inv_id = str(r.get("inventory_id", ""))
        rownum = id_to_row.get(inv_id)
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
    ensure_headers(ws)

    col_a = ws.col_values(1)
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_row[str(val)] = idx

    rownums = [id_to_row.get(str(i)) for i in inventory_ids]
    rownums = [r for r in rownums if r]

    for rn in sorted(rownums, reverse=True):
        ws.delete_rows(rn)

# =========================================================
# STATE
# =========================================================

def init_inventory_from_sheets():
    if INVENTORY_STATE_KEY not in st.session_state:
        st.session_state[INVENTORY_STATE_KEY] = sheets_load_inventory()

def refresh_inventory_from_sheets():
    st.session_state[INVENTORY_STATE_KEY] = sheets_load_inventory()

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
    st.caption("Paste a PriceCharting link and click Pull details to auto-fill fields (and image).")

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
        details = fetch_details_and_image(reference_link)
        st.session_state["prefill_details"] = details
        st.success("Pulled details. Review/adjust below, then add to inventory.")

    prefill = st.session_state.get("prefill_details", {}) or {}

    # show image
    if prefill.get("image_url"):
        st.image(prefill["image_url"], width=160)

    with st.form("new_inventory_form_v8", clear_on_submit=True):
        a1, a2, a3, a4 = st.columns([1.4, 1.2, 2.4, 1.0])

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
            grading_company = ""
            grade = ""

            if product_type == "Sealed":
                if card_type == "Pokemon":
                    pre = (prefill.get("sealed_product_type") or "").strip()
                    options = [""] + POKEMON_SEALED_TYPE_OPTIONS
                    idx = options.index(pre) if pre in options else 0
                    sealed_product_type = st.selectbox("Sealed Product Type*", options=options, index=idx)
                else:
                    sealed_product_type = st.text_input("Sealed Product Type*", value=(prefill.get("sealed_product_type") or ""))

            elif product_type == "Graded Card":
                grading_company = st.selectbox("Grading Company*", GRADING_COMPANY_OPTIONS, index=0)
                if grading_company == "PSA":
                    grade = st.selectbox("Grade*", PSA_GRADE_OPTIONS, index=0)
                elif grading_company == "CGC":
                    grade = st.selectbox("Grade*", CGC_GRADE_OPTIONS, index=0)
                else:
                    grade = st.selectbox("Grade*", BECKETT_GRADE_OPTIONS, index=0)
            else:
                st.caption("Sealed/Grading fields show when applicable.")

        with a4:
            quantity = st.number_input(
                "Quantity*",
                min_value=1,
                step=1,
                value=1,
                help="Creates one inventory row per item. Prices/tax/shipping are per-item.",
            )

        c1, c2, c3 = st.columns(3)

        with c1:
            brand_or_league = st.text_input(
                "Brand / League*",
                value=prefill.get("brand_or_league", ""),
                placeholder="Pokemon TCG / Football / NBA / MLB / Soccer / etc.",
            )
            year = st.text_input("Year (optional)", value=prefill.get("year", ""), placeholder="2024, 2025, ...")

        with c2:
            set_name = st.text_input("Set (optional)", value=prefill.get("set_name", ""), placeholder="Surging Sparks, Prizm, Optic, ...")
            name_label = "Item name*" if product_type == "Sealed" else "Card name*"
            card_name = st.text_input(name_label, value=prefill.get("card_name", ""), placeholder="Elite Trainer Box / Pikachu / etc.")

            if product_type in ["Card", "Graded Card"]:
                card_number = st.text_input("Card # (optional)", value=prefill.get("card_number", ""), placeholder="332")
            else:
                card_number = ""

        with c3:
            if product_type in ["Card", "Graded Card"]:
                variant = st.text_input("Variant (optional)", value=prefill.get("variant", ""), placeholder="Silver, Holo, Parallel, EX, ...")
                card_subtype = st.text_input("Card subtype (optional)", value=prefill.get("card_subtype", ""), placeholder="Rookie, Insert, Parallel, etc.")

                if product_type == "Card":
                    condition = st.selectbox("Condition*", CONDITION_OPTIONS, index=0)
                else:
                    condition = "Graded"
                    st.caption("Condition is not used for graded cards (grade is stored instead).")
            else:
                variant = ""
                card_subtype = ""
                condition = "Sealed"
                st.caption("Variant / Card subtype / Card # / Condition are not applicable for sealed.")

        st.markdown("---")

        c4, c5, c6 = st.columns(3)
        with c4:
            purchase_date = st.date_input("Purchase date*", value=date.today())
            purchased_from = st.text_input("Purchased from*", placeholder="eBay, Whatnot, card show, LCS, etc.")

        with c5:
            purchase_price = st.number_input("Purchase price (per item)*", min_value=0.0, step=1.0, format="%.2f")
            shipping = st.number_input("Shipping (per item)", min_value=0.0, step=1.0, format="%.2f")

        with c6:
            tax = st.number_input("Tax (per item)", min_value=0.0, step=1.0, format="%.2f")
            notes = st.text_area("Notes (optional)", height=92)

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

            if product_type == "Card" and not str(condition).strip():
                missing.append("Condition")

            if product_type == "Graded Card":
                if not str(grading_company).strip():
                    missing.append("Grading Company")
                if not str(grade).strip():
                    missing.append("Grade")

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                total_price = _compute_total(purchase_price, shipping, tax)
                created_ids = []

                for _ in range(int(quantity)):
                    new_row = {
                        "inventory_id": str(uuid.uuid4())[:8],
                        "image_url": (prefill.get("image_url") or ""),
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
                        "grading_company": grading_company.strip() if product_type == "Graded Card" else "",
                        "grade": grade.strip() if product_type == "Graded Card" else "",
                        "reference_link": reference_link.strip() if reference_link else "",
                        "purchase_date": str(purchase_date),
                        "purchased_from": purchased_from.strip(),
                        "purchase_price": float(purchase_price),
                        "shipping": float(shipping),
                        "tax": float(tax),
                        "total_price": float(total_price),
                        "condition": "Sealed" if product_type == "Sealed" else ("Graded" if product_type == "Graded Card" else condition),
                        "notes": notes.strip() if notes else "",
                        "created_at": pd.Timestamp.utcnow().isoformat(),
                        "inventory_status": STATUS_ACTIVE,
                        "listed_transaction_id": "",
                        "market_price": 0.0,
                        "market_price_updated_at": "",
                    }

                    sheets_append_inventory_row(new_row)
                    created_ids.append(new_row["inventory_id"])

                st.session_state["prefill_details"] = {}
                refresh_inventory_from_sheets()

                st.success(f"Added {len(created_ids)} item(s).")

    df = st.session_state[INVENTORY_STATE_KEY].copy()
    df = df[df["inventory_status"].isin([STATUS_ACTIVE, STATUS_LISTED])]

    if len(df) > 0:
        st.markdown("#### Recently added")
        show = df.tail(10).copy()
        for col in ["purchase_price", "shipping", "tax", "total_price", "market_price"]:
            show[col] = show[col].apply(_safe_money_display)

        st.dataframe(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "image_url": st.column_config.ImageColumn("Image", width="small"),
                "reference_link": st.column_config.LinkColumn("Reference link"),
            },
        )
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
        status_options = sorted(df["inventory_status"].dropna().unique().tolist())
        default_status = [s for s in [STATUS_ACTIVE, STATUS_LISTED] if s in status_options]
        if not default_status and status_options:
            default_status = [status_options[0]]

        f1, f2, f3, f4, f5, f6 = st.columns([1.1, 1.1, 1.1, 1.1, 1.4, 2.2])

        with f1:
            status_filter = st.multiselect("Status", status_options, default=default_status)
        with f2:
            product_filter = st.multiselect("Product Type", sorted(df["product_type"].dropna().unique().tolist()), default=[])
        with f3:
            type_filter = st.multiselect("Card Type", sorted(df["card_type"].dropna().unique().tolist()), default=[])
        with f4:
            league_filter = st.multiselect("Brand/League", sorted(df["brand_or_league"].dropna().unique().tolist()), default=[])
        with f5:
            set_filter = st.multiselect("Set", sorted(df["set_name"].dropna().unique().tolist()), default=[])
        with f6:
            search = st.text_input("Search (name/set/notes/id)", placeholder="Type to filter…")

        filtered = df.copy()
        if status_filter:
            filtered = filtered[filtered["inventory_status"].isin(status_filter)]
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
                        or s in str(r.get("grading_company", "")).lower()
                        or s in str(r.get("grade", "")).lower()
                    ),
                    axis=1,
                )
            ]

        st.caption(f"Showing {len(filtered):,} of {len(df):,} items")

        display = filtered.copy()
        display.insert(0, "delete", False)

        display.loc[display["product_type"] == "Sealed", "condition"] = "Sealed"
        display.loc[display["product_type"] == "Graded Card", "condition"] = "Graded"

        edited = st.data_editor(
            display,
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "delete": st.column_config.CheckboxColumn("Delete", help="Check to delete this row"),
                "image_url": st.column_config.ImageColumn("Image", width="small"),
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

            for c in ["purchase_price", "shipping", "tax", "market_price"]:
                if c in edited_rows.columns:
                    edited_rows[c] = pd.to_numeric(edited_rows[c], errors="coerce").fillna(0.0)

            if set(["purchase_price", "shipping", "tax"]).issubset(edited_rows.columns):
                edited_rows["total_price"] = (edited_rows["purchase_price"] + edited_rows["shipping"] + edited_rows["tax"]).round(2)

            edited_rows.loc[edited_rows["product_type"] == "Sealed", "condition"] = "Sealed"
            edited_rows.loc[edited_rows["product_type"] == "Sealed", ["variant", "card_subtype", "card_number", "grading_company", "grade"]] = ""

            edited_rows.loc[edited_rows["product_type"] == "Graded Card", "condition"] = "Graded"
            edited_rows.loc[edited_rows["product_type"] == "Graded Card", "sealed_product_type"] = ""

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
        total_market = df["market_price"].fillna(0).sum() if "market_price" in df.columns else 0.0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Items", f"{total_items:,}")
        k2.metric("Total Invested", f"${total_invested:,.2f}")
        k3.metric("Active Items", f"{(df['inventory_status'] == STATUS_ACTIVE).sum():,}")
        k4.metric("Cached Market Total", f"${total_market:,.2f}")

        st.markdown("---")
        st.markdown("### Breakdown by Product Type")
        p_summary = (
            df.groupby("product_type", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"), market=("market_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(p_summary, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("### Breakdown by Status")
        s_summary = (
            df.groupby("inventory_status", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"), market=("market_price", "sum"))
            .reset_index()
            .sort_values("items", ascending=False)
        )
        st.dataframe(s_summary, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("### Top Sets by Invested")
        set_summary = (
            df.groupby(["product_type", "card_type", "brand_or_league", "set_name"], dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"), market=("market_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
        )
        st.dataframe(set_summary.head(30), use_container_width=True, hide_index=True)
