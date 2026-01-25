import json
import re
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import urlparse, urljoin

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

# Canonical sheet columns (snake_case only)
DEFAULT_COLUMNS = [
    "inventory_id",
    "image_url",            # scraped from reference_link
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
]

SEALED_TYPE_KEYWORDS = {
    "elite-trainer-box": "Elite Trainer Box",
    "etb": "Elite Trainer Box",
    "booster-box": "Booster Box",
    "booster-display": "Booster Box",
    "booster-bundle": "Booster Bundle",
    "premium-collection": "Premium Collection Box",
    "collection-box": "Collection Box",
    "blister": "Blister Pack",
    "tech-sticker-collection": "Tech Sticker Collection",
}

# ---- UI sizing for image tables (your request) ----
# ~2 inches at typical 96dpi ~= 192px. We’ll use 200px for “actually visible”.
TABLE_ROW_HEIGHT_PX = 200
TABLE_IMAGE_WIDTH_PX = 190


# =========================================================
# GLOBAL CSS: force bigger rows + bigger images in Streamlit tables
# =========================================================
def inject_table_css():
    st.markdown(
        f"""
        <style>
        /* Applies to st.dataframe + st.data_editor (AgGrid) */
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

        /* Make images big and readable */
        div[data-testid="stDataFrame"] .ag-cell img,
        div[data-testid="stDataEditor"] .ag-cell img {{
            width: {TABLE_IMAGE_WIDTH_PX}px !important;
            height: auto !important;
            border-radius: 6px;
        }}

        /* Give the image column enough room so it doesn't clip */
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


def get_worksheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets["spreadsheet_id"]
    worksheet_name = st.secrets.get("inventory_worksheet", "inventory")
    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


def _canonicalize_header(h: str) -> str:
    if h is None:
        return ""
    raw = str(h).strip()
    low = raw.lower().strip()

    # Normalize a few known “human headers” to canonical snake_case
    mapping = {
        "product type": "product_type",
        "sealed product type": "sealed_product_type",
        "image url": "image_url",
    }
    if low in mapping:
        return mapping[low]

    # If it’s already snake_case we keep it
    return raw


def migrate_and_fix_headers(ws):
    """
    - Canonicalize headers into snake_case for known fields
    - Delete duplicate columns in the SHEET (keep first occurrence)
    - Ensure all DEFAULT_COLUMNS exist (append missing)
    """
    headers = ws.row_values(1)

    if not headers:
        ws.append_row(DEFAULT_COLUMNS)
        return DEFAULT_COLUMNS

    canon = [_canonicalize_header(h) for h in headers]

    if canon != headers:
        ws.update("1:1", [canon])
        headers = canon

    # Delete duplicates (in the sheet)
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
    missing = [c for c in DEFAULT_COLUMNS if c not in existing]
    if missing:
        ws.update("1:1", [headers + missing])
        headers = ws.row_values(1)

    return headers


def sheets_load_inventory() -> pd.DataFrame:
    ws = get_worksheet()
    headers = migrate_and_fix_headers(ws)

    records = ws.get_all_records()
    df = pd.DataFrame(records) if records else pd.DataFrame(columns=headers)

    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[DEFAULT_COLUMNS].copy()

    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["inventory_id"] = df["inventory_id"].astype(str)
    df["product_type"] = df["product_type"].replace("", "Card").fillna("Card")
    df["inventory_status"] = df["inventory_status"].replace("", STATUS_ACTIVE).fillna(STATUS_ACTIVE)
    df["listed_transaction_id"] = df["listed_transaction_id"].astype(str).replace("nan", "").fillna("")
    df["image_url"] = df["image_url"].astype(str).replace("nan", "").fillna("")

    sealed_mask = (df["product_type"] == "Sealed") & (df["condition"].astype(str).str.strip() == "")
    df.loc[sealed_mask, "condition"] = "Sealed"

    return df


def _find_row_numbers_by_inventory_id(ws, inventory_ids):
    col_a = ws.col_values(1)
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val is not None and str(val).strip() != "":
            id_to_row[str(val)] = idx
    return {str(inv_id): id_to_row.get(str(inv_id)) for inv_id in inventory_ids}


def sheets_append_inventory_row(row_internal: dict):
    ws = get_worksheet()
    headers = migrate_and_fix_headers(ws)
    ordered = [row_internal.get(h, "") for h in headers]
    ws.append_row(ordered, value_input_option="USER_ENTERED")


def sheets_update_rows(rows_internal: pd.DataFrame):
    if rows_internal.empty:
        return

    ws = get_worksheet()
    headers = migrate_and_fix_headers(ws)

    inv_ids = rows_internal["inventory_id"].astype(str).tolist()
    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inv_ids)

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]

    for _, r in rows_internal.iterrows():
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
    migrate_and_fix_headers(ws)

    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inventory_ids)
    rownums = [rn for rn in id_to_rownum.values() if rn]

    for rn in sorted(rownums, reverse=True):
        ws.delete_rows(rn)


# =========================================================
# SCRAPE HELPERS
# =========================================================

@st.cache_data(show_spinner=False, ttl=60 * 60 * 6)
def scrape_image_url(reference_link: str) -> str:
    """
    Tries, in order:
    - og:image
    - twitter:image
    - first <img> tag
    Returns absolute URL when possible.
    """
    if not reference_link or not str(reference_link).strip():
        return ""

    url = str(reference_link).strip()
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            return urljoin(url, og["content"].strip())

        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            return urljoin(url, tw["content"].strip())

        img = soup.find("img")
        if img and img.get("src"):
            return urljoin(url, img["src"].strip())
    except Exception:
        return ""

    return ""


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
    return ""


def _looks_like_single_card_slug(card_slug: str) -> bool:
    if not card_slug:
        return False
    return bool(re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug))


def fetch_item_details_from_link(url: str):
    """
    Returns card/sealed details + image_url.
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
        "image_url": "",
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
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        page_title = _find_best_title(soup)
    except Exception:
        soup = None
        page_title = ""

    # image
    result["image_url"] = scrape_image_url(url)

    def _extract_game_parts(p: str):
        parts = [x for x in p.split("/") if x]
        if len(parts) >= 3 and parts[0].lower() == "game":
            return parts[1], parts[2]
        return None, None

    set_slug, card_slug = _extract_game_parts(path)

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

        return result

    if "pricecharting.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))

        if card_slug and not _looks_like_single_card_slug(card_slug):
            result["product_type"] = "Sealed"
            result["sealed_product_type"] = _infer_sealed_type_from_slug_or_title(card_slug, page_title)
            result["card_name"] = result["sealed_product_type"] or (page_title or _title_case_from_slug(card_slug))
            result["card_number"] = ""
            result["variant"] = ""
            result["card_subtype"] = ""
            return result

        result["product_type"] = "Card"
        result.update(_parse_pricecharting_title(page_title))
        return result

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


def _label_for_row(r: pd.Series) -> str:
    inv = str(r.get("inventory_id", "")).strip()
    name = str(r.get("card_name", "")).strip()
    setn = str(r.get("set_name", "")).strip()
    yr = str(r.get("year", "")).strip()
    pt = str(r.get("product_type", "")).strip()
    return f"{inv} — {name} ({setn}{' ' + yr if yr else ''}) [{pt}]"


# =========================================================
# UI
# =========================================================

st.set_page_config(page_title="Inventory", layout="wide")
inject_table_css()
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
    st.caption("Paste a PriceCharting or Sportscardspro link and click Pull details to auto-fill fields (and image).")

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
        st.success("Pulled details. Review/adjust below, then add to inventory.")

    prefill = st.session_state.get("prefill_details", {}) or {}

    # Image: upper-left after pulling details
    img_url = (prefill.get("image_url") or "").strip()
    if img_url:
        st.image(img_url, width=180)

    with st.form("new_inventory_form_v8", clear_on_submit=True):
        a1, a2, a3, a4 = st.columns([1.2, 1.2, 2.2, 1.0])

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
                if card_type == "Pokemon":
                    pre = (prefill.get("sealed_product_type") or "").strip()
                    options = [""] + POKEMON_SEALED_TYPE_OPTIONS
                    idx = options.index(pre) if pre in options else 0
                    sealed_product_type = st.selectbox("Sealed Product Type*", options=options, index=idx)
                else:
                    sealed_product_type = st.text_input(
                        "Sealed Product Type*",
                        value=(prefill.get("sealed_product_type") or ""),
                        placeholder="Hobby box, blaster, mega box, fat pack, etc.",
                    )
            else:
                st.caption("Sealed Product Type appears when Product Type = Sealed")

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
            set_name = st.text_input("Set (optional)", value=prefill.get("set_name", ""))
            name_label = "Item name*" if product_type == "Sealed" else "Card name*"
            card_name = st.text_input(name_label, value=prefill.get("card_name", ""))

            if product_type == "Card":
                card_number = st.text_input("Card # (optional)", value=prefill.get("card_number", ""))
            else:
                card_number = ""

        with c3:
            if product_type == "Card":
                variant = st.text_input("Variant (optional)", value=prefill.get("variant", ""))
                card_subtype = st.text_input("Card subtype (optional)", value=prefill.get("card_subtype", ""))
                condition = st.selectbox("Condition*", CONDITION_OPTIONS, index=0)
            else:
                variant = ""
                card_subtype = ""
                condition = "Sealed"
                st.caption("Variant / subtype / card # / condition not applicable for sealed.")

        st.markdown("---")
        c4, c5, c6 = st.columns(3)
        with c4:
            purchase_date = st.date_input("Purchase date*", value=date.today())
            purchased_from = st.text_input("Purchased from*", placeholder="eBay, Opened, Whatnot, Card show, LCS, etc.")
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

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                total_price = _compute_total(purchase_price, shipping, tax)
                created_ids = []

                # If no image_url was scraped, try once at submit time (in case user didn't click Pull)
                final_image_url = img_url.strip()
                if not final_image_url and reference_link:
                    final_image_url = scrape_image_url(reference_link)

                for _ in range(int(quantity)):
                    new_row = {
                        "inventory_id": str(uuid.uuid4())[:8],
                        "image_url": final_image_url,
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
                        "condition": condition if product_type == "Card" else "Sealed",
                        "notes": notes.strip() if notes else "",
                        "created_at": pd.Timestamp.utcnow().isoformat(),
                        "inventory_status": STATUS_ACTIVE,
                        "listed_transaction_id": "",
                    }
                    sheets_append_inventory_row(new_row)
                    created_ids.append(new_row["inventory_id"])

                st.session_state["prefill_details"] = {}
                refresh_inventory_from_sheets()
                st.success(f"Added {len(created_ids)} item(s).")

    # ---- Recently added table (FIX #1 + FIX #2)
    df_recent = st.session_state[INVENTORY_STATE_KEY].copy()
    df_recent = df_recent[df_recent["inventory_status"].isin([STATUS_ACTIVE, STATUS_LISTED])]
    if len(df_recent) > 0:
        st.markdown("#### Recently added")
        show = df_recent.tail(10).copy()

        for col in ["purchase_price", "shipping", "tax", "total_price"]:
            show[col] = show[col].apply(_safe_money_display)

        st.dataframe(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                # ✅ show the image instead of URL
                "image_url": st.column_config.ImageColumn("Image", width="large"),
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
        f1, f2, f3, f4, f5, f6 = st.columns([1.1, 1.1, 1.1, 1.1, 1.2, 2])
        with f1:
            status_filter = st.multiselect("Status", sorted(df["inventory_status"].dropna().unique().tolist()), default=[STATUS_ACTIVE, STATUS_LISTED])
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
                "delete": st.column_config.CheckboxColumn("Delete"),
                "reference_link": st.column_config.LinkColumn("Reference link"),
                # ✅ image shown as image + bigger column width (row height handled by CSS)
                "image_url": st.column_config.ImageColumn("Image", width="large"),
            },
            disabled=["inventory_id"],
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

            edited_rows["total_price"] = (edited_rows["purchase_price"] + edited_rows["shipping"] + edited_rows["tax"]).round(2)

            sealed_mask = edited_rows["product_type"] == "Sealed"
            edited_rows.loc[sealed_mask, "condition"] = "Sealed"
            for col in ["variant", "card_subtype", "card_number"]:
                edited_rows.loc[sealed_mask, col] = ""

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

        k1, k2, k3 = st.columns(3)
        k1.metric("Items", f"{total_items:,}")
        k2.metric("Total Invested", f"${total_invested:,.2f}")
        k3.metric("Listed Items", f"{(df['inventory_status'] == STATUS_LISTED).sum():,}")

        st.markdown("---")
        st.markdown("### Breakdown by Status")
        s_summary = (
            df.groupby("inventory_status", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("items", ascending=False)
        )
        st.dataframe(s_summary, use_container_width=True, hide_index=True)

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
