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

# PSA: 10..1 plus half grades in-between (9.5..1.5), in DESCENDING order
PSA_GRADE_OPTIONS = [""] + (
    [str(i) for i in range(10, 0, -1)] + [f"{i}.5" for i in range(9, 0, -1)]
)
# (If you prefer interleaved: 10, 9.5, 9, 8.5, ... you can change it—this keeps it simple + guaranteed includes 10.)

CGC_GRADE_OPTIONS = [""] + [str(i) for i in range(1, 11)] + [f"{i}.5" for i in range(1, 10)] + ["Pristine 10"]
BECKETT_GRADE_OPTIONS = [""] + [str(i) for i in range(1, 11)] + [f"{i}.5" for i in range(1, 10)] + ["Black Label 10"]

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

SEALED_TYPE_KEYWORDS = {
    "elite-trainer-box": "Elite Trainer Box",
    "etb": "Elite Trainer Box",
    "booster-box": "Booster Box",
    "booster-display": "Booster Box",
    "booster-bundle": "Booster Bundle",
    "booster-pack": "Blister Pack",
    "sleeved-booster": "Blister Pack",
    "tin": "Collection Box",
    "collection-box": "Collection Box",
    "premium-collection": "Premium Collection Box",
    "tech-sticker-collection": "Tech Sticker Collection",
    "blister": "Blister Pack",
}

# Internal columns (canonical)
DEFAULT_COLUMNS = [
    "inventory_id",
    "image_url",
    "product_type",          # Card / Sealed / Graded Card
    "sealed_product_type",   # only sealed
    "grading_company",       # only graded
    "grade",                 # only graded
    "card_type",             # Pokemon / Sports / Other
    "brand_or_league",       # Pokemon TCG / Football / etc
    "set_name",
    "year",
    "card_name",             # for sealed: item name
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
    "condition",             # for sealed: "Sealed"; for graded: blank
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
]

NUMERIC_COLS = ["purchase_price", "shipping", "tax", "total_price"]

# Aliases to prevent duplicate columns like "Product Type" + "product_type"
HEADER_ALIASES = {
    "product_type": ["Product Type", "product_type"],
    "sealed_product_type": ["Sealed Product Type", "sealed_product_type"],
    "grading_company": ["Grading Company", "grading_company"],
    "grade": ["Grade", "grade"],
    "image_url": ["Image URL", "image_url", "image"],
}

PREFERRED_SHEET_HEADERS = {
    "product_type": "Product Type",
    "sealed_product_type": "Sealed Product Type",
    "grading_company": "Grading Company",
    "grade": "Grade",
    "image_url": "Image URL",
}

USER_AGENT = "Mozilla/5.0 (CardTrackerPrototype; +Streamlit)"


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
        sa_json_str = st.secrets["gcp_service_account"]
        sa_info = json.loads(sa_json_str)
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

    raise KeyError('Missing secrets: add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')


def get_worksheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets["spreadsheet_id"]
    worksheet_name = st.secrets.get("inventory_worksheet", "inventory")
    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)


def ensure_headers(ws):
    existing = ws.row_values(1)
    if not existing:
        sheet_headers = []
        for internal in DEFAULT_COLUMNS:
            sheet_headers.append(PREFERRED_SHEET_HEADERS.get(internal, internal))
        ws.append_row(sheet_headers)
        return sheet_headers

    existing_internal = set()
    for h in existing:
        mapped = None
        for internal, aliases in HEADER_ALIASES.items():
            if h in aliases:
                mapped = internal
                break
        existing_internal.add(mapped or h)

    missing = [c for c in DEFAULT_COLUMNS if c not in existing_internal]
    if missing:
        new_headers = existing[:]
        for internal in missing:
            new_headers.append(PREFERRED_SHEET_HEADERS.get(internal, internal))
        ws.update("1:1", [new_headers])
        return new_headers

    return existing


def _find_row_numbers_by_inventory_id(ws, inventory_ids):
    col_a = ws.col_values(1)
    id_to_row = {}
    for idx, val in enumerate(col_a[1:], start=2):
        if val:
            id_to_row[str(val)] = idx
    return {str(inv_id): id_to_row.get(str(inv_id)) for inv_id in inventory_ids}


def sheets_append_inventory_row(row_internal: dict):
    ws = get_worksheet()
    sheet_headers = ensure_headers(ws)

    header_to_internal = {}
    for h in sheet_headers:
        mapped = None
        for internal, aliases in HEADER_ALIASES.items():
            if h in aliases:
                mapped = internal
                break
        header_to_internal[h] = mapped or h

    ordered = []
    for sheet_h in sheet_headers:
        internal = header_to_internal[sheet_h]
        ordered.append(row_internal.get(internal, ""))

    ws.append_row(ordered, value_input_option="USER_ENTERED")


def sheets_update_rows(rows_internal: pd.DataFrame):
    if rows_internal.empty:
        return

    ws = get_worksheet()
    sheet_headers = ensure_headers(ws)

    header_to_internal = {}
    for h in sheet_headers:
        mapped = None
        for internal, aliases in HEADER_ALIASES.items():
            if h in aliases:
                mapped = internal
                break
        header_to_internal[h] = mapped or h

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
            internal = header_to_internal[sheet_h]
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
    id_to_rownum = _find_row_numbers_by_inventory_id(ws, inventory_ids)
    rownums = [rn for rn in id_to_rownum.values() if rn]
    for rn in sorted(rownums, reverse=True):
        ws.delete_rows(rn)


def sheets_load_inventory() -> pd.DataFrame:
    ws = get_worksheet()
    ensure_headers(ws)

    records = ws.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

    rename_map = {}
    for c in df.columns:
        mapped = None
        for internal, aliases in HEADER_ALIASES.items():
            if c in aliases:
                mapped = internal
                break
        rename_map[c] = mapped or c
    df = df.rename(columns=rename_map)

    # coalesce duplicate columns if they exist
    if df.columns.duplicated().any():
        new_df = df.copy()
        dup_names = pd.Index(new_df.columns)[pd.Index(new_df.columns).duplicated()].unique().tolist()
        for col in dup_names:
            cols = [c for c in df.columns if c == col]
            # take first non-empty across duplicates
            combined = None
            for i, c in enumerate(cols):
                s = new_df[c].astype(str).replace({"nan": "", "None": ""})
                if i == 0:
                    combined = s
                else:
                    combined = combined.where(combined.str.strip() != "", s)
            new_df = new_df.loc[:, [c for c in new_df.columns if c != col]]
            new_df[col] = combined
        df = new_df

    for col in DEFAULT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[DEFAULT_COLUMNS].copy()

    for c in NUMERIC_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["inventory_id"] = df["inventory_id"].astype(str)
    df["product_type"] = df["product_type"].astype(str).replace("", "Card")
    df["inventory_status"] = df["inventory_status"].astype(str).replace("", STATUS_ACTIVE)

    is_sealed = df["product_type"].astype(str).str.strip().eq("Sealed")
    df.loc[is_sealed, "condition"] = "Sealed"

    is_graded = df["product_type"].astype(str).str.strip().eq("Graded Card")
    df.loc[is_graded, "condition"] = ""

    return df


# =========================================================
# SCRAPE (details + image)
# =========================================================

def _compute_total(purchase_price, shipping, tax):
    try:
        return round(float(purchase_price) + float(shipping) + float(tax), 2)
    except Exception:
        return 0.0


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


def _infer_sealed_type_from_slug_or_title(slug: str, title: str) -> str:
    text = (slug or "") + " " + (title or "")
    t = text.lower()
    for k, v in SEALED_TYPE_KEYWORDS.items():
        if k in t:
            return v
    if "elite trainer box" in t:
        return "Elite Trainer Box"
    if "booster box" in t:
        return "Booster Box"
    if "booster bundle" in t:
        return "Booster Bundle"
    if "premium collection" in t:
        return "Premium Collection Box"
    if "tech sticker" in t:
        return "Tech Sticker Collection"
    if "blister" in t:
        return "Blister Pack"
    if "collection box" in t:
        return "Collection Box"
    return ""


def _find_best_title_and_image(soup: BeautifulSoup):
    title = ""
    img = ""

    ogt = soup.find("meta", property="og:title")
    if ogt and ogt.get("content"):
        title = ogt["content"].strip()

    ogi = soup.find("meta", property="og:image")
    if ogi and ogi.get("content"):
        img = ogi["content"].strip()

    if not title:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            title = h1.get_text(" ", strip=True)

    if not title and soup.title and soup.title.string:
        title = soup.title.string.strip()

    if not img:
        imgtag = soup.find("img")
        if imgtag and imgtag.get("src"):
            img = imgtag["src"].strip()

    return title, img


def fetch_item_details_from_link(url: str):
    result = {
        "image_url": "",
        "product_type": "Card",
        "sealed_product_type": "",
        "grading_company": "",
        "grade": "",
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
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        page_title, image_url = _find_best_title_and_image(soup)
    except Exception:
        soup = None
        page_title = ""
        image_url = ""

    result["image_url"] = image_url

    parts = [x for x in path.split("/") if x]
    set_slug = None
    card_slug = None
    if len(parts) >= 3 and parts[0].lower() == "game":
        set_slug, card_slug = parts[1], parts[2]

    if "sportscardspro.com" in host and set_slug:
        result["product_type"] = "Card"
        result.update(_parse_set_slug_generic(set_slug))

        header_text = page_title
        if soup:
            h1 = soup.find("h1")
            if h1 and h1.get_text(strip=True):
                header_text = h1.get_text(" ", strip=True)

        if header_text:
            m_num = re.search(r"#\s*([A-Za-z0-9]+)", header_text)
            if m_num:
                result["card_number"] = m_num.group(1).strip()

            left = header_text
            right = ""
            if "#" in header_text and m_num:
                left = header_text[:m_num.start()].strip()
                right = header_text[m_num.end():].strip()

            m_var = re.search(r"\[(.+?)\]", left)
            if m_var:
                result["variant"] = m_var.group(1).strip()
                left = re.sub(r"\s*\[.+?\]\s*", " ", left).strip()

            result["card_name"] = left.strip()

            m_year = re.match(r"((19|20)\d{2})\s+(.+)$", right)
            if m_year:
                result["year"] = m_year.group(1).strip()
                result["set_name"] = m_year.group(3).strip()

        if not result["card_name"] and card_slug:
            result["card_name"] = _title_case_from_slug(re.sub(r"-(\d+[A-Za-z0-9]*)$", "", card_slug))

        if not result["card_number"] and card_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug)
            if m:
                result["card_number"] = m.group(1)

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

        if page_title:
            m = re.search(r"#\s*([A-Za-z0-9]+)", page_title)
            if m:
                result["card_number"] = m.group(1).strip()

            name_part = page_title.split("#")[0].strip() if "#" in page_title else page_title.strip()
            for sep in [" - ", " – "]:
                if sep in name_part:
                    name_part = name_part.split(sep)[0].strip()

            variant = ""
            tokens = name_part.split()
            if tokens and tokens[-1].lower() in {"ex", "gx", "v", "vmax", "silver", "holo"}:
                variant = tokens[-1]
                name_part = " ".join(tokens[:-1]).strip()

            result["card_name"] = name_part
            result["variant"] = variant

        if not result["card_number"] and card_slug:
            m = re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug)
            if m:
                result["card_number"] = m.group(1)

        if not result["card_name"] and card_slug:
            cleaned = re.sub(r"-(\d+[A-Za-z0-9]*)$", "", card_slug)
            result["card_name"] = _title_case_from_slug(cleaned)

        return result

    lowered = (url + " " + page_title).lower()
    if "pokemon" in lowered:
        result["card_type"] = "Pokemon"
        result["brand_or_league"] = "Pokemon TCG"
    elif any(tok in lowered for tok in ["prizm", "optic", "select", "donruss", "panini", "topps"]):
        result["card_type"] = "Sports"

    result["card_name"] = page_title or result["card_name"]
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
    st.caption("Paste a PriceCharting or Sportscardspro link and click Pull details to auto-fill fields (and image).")

    link_col1, link_col2 = st.columns([4, 1])
    with link_col1:
        reference_link = st.text_input(
            "Reference link (recommended)",
            key="inv_ref_link_input",
            placeholder="https://www.pricecharting.com/game/pokemon-surging-sparks/elite-trainer-box",
        )
    with link_col2:
        pull = st.button("Pull details", use_container_width=True)

    if pull:
        details = fetch_item_details_from_link(reference_link)
        st.session_state["inv_prefill_details"] = details
        st.session_state["inv_prefill_image_url"] = details.get("image_url", "")
        st.success("Pulled details. Review/adjust below, then add to inventory.")

    prefill = st.session_state.get("inv_prefill_details", {}) or {}
    prefill_image_url = st.session_state.get("inv_prefill_image_url", "") or ""

    img_col, _ = st.columns([1, 3])
    with img_col:
        if prefill_image_url:
            st.image(prefill_image_url, caption="Pulled image", use_container_width=True)

    # These must be OUTSIDE the form so conditionals update immediately
    sel1, sel2, sel3, sel4 = st.columns([1.2, 1.2, 2.2, 1.0])

    with sel1:
        st.selectbox(
            "Product Type*",
            PRODUCT_TYPE_OPTIONS,
            index=(PRODUCT_TYPE_OPTIONS.index(prefill.get("product_type")) if prefill.get("product_type") in PRODUCT_TYPE_OPTIONS else 0),
            key="ui_product_type",
        )

    with sel2:
        st.selectbox(
            "Card Type*",
            CARD_TYPE_OPTIONS,
            index=(CARD_TYPE_OPTIONS.index(prefill.get("card_type")) if prefill.get("card_type") in CARD_TYPE_OPTIONS else 0),
            key="ui_card_type",
        )

    with sel3:
        if st.session_state["ui_product_type"] == "Sealed":
            if st.session_state["ui_card_type"] == "Pokemon":
                options = [""] + POKEMON_SEALED_TYPE_OPTIONS
                pre = (prefill.get("sealed_product_type") or "").strip()
                idx = options.index(pre) if pre in options else 0
                st.selectbox("Sealed Product Type*", options, index=idx, key="ui_sealed_product_type")
            else:
                st.text_input(
                    "Sealed Product Type*",
                    value=(prefill.get("sealed_product_type") or ""),
                    placeholder="Hobby box, blaster, mega box, fat pack, etc.",
                    key="ui_sealed_product_type_text",
                )
        else:
            st.caption("Sealed Product Type appears when Product Type = Sealed")

    with sel4:
        st.number_input(
            "Quantity*",
            min_value=1,
            step=1,
            value=1,
            help="Creates one inventory row per item. Prices/tax/shipping are per-item.",
            key="ui_quantity",
        )

    # Graded card controls (outside form)
    gc1, gc2 = st.columns([1, 2])
    with gc1:
        if st.session_state["ui_product_type"] == "Graded Card":
            st.selectbox("Grading Company*", [""] + GRADING_COMPANY_OPTIONS, key="ui_grading_company")
        else:
            st.session_state["ui_grading_company"] = ""

    with gc2:
        if st.session_state["ui_product_type"] == "Graded Card":
            company = st.session_state.get("ui_grading_company", "")
            if company == "PSA":
                options = PSA_GRADE_OPTIONS
            elif company == "CGC":
                options = CGC_GRADE_OPTIONS
            elif company == "Beckett":
                options = BECKETT_GRADE_OPTIONS
            else:
                options = [""]

            # keep current selection valid
            if st.session_state.get("ui_grade", "") not in options:
                st.session_state["ui_grade"] = ""

            st.selectbox("Grade*", options, key="ui_grade")
        else:
            st.session_state["ui_grade"] = ""

    with st.form("new_inventory_form_v8", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)

        with c1:
            brand_or_league = st.text_input(
                "Brand / League*",
                value=prefill.get("brand_or_league", ""),
                placeholder="Pokemon TCG / Football / NBA / MLB / Soccer / etc.",
            )
            year = st.text_input("Year (optional)", value=prefill.get("year", ""), placeholder="2024, 2025, ...")

        with c2:
            set_name = st.text_input(
                "Set (optional)",
                value=prefill.get("set_name", ""),
                placeholder="Surging Sparks, Panini Prizm, Optic, ...",
            )

            name_label = "Item name*" if st.session_state["ui_product_type"] == "Sealed" else "Card name*"
            card_name = st.text_input(
                name_label,
                value=prefill.get("card_name", ""),
                placeholder="Elite Trainer Box / Pikachu / Jaxson Dart / etc.",
            )

            if st.session_state["ui_product_type"] in ["Card", "Graded Card"]:
                card_number = st.text_input("Card # (optional)", value=prefill.get("card_number", ""), placeholder="184")
            else:
                card_number = ""

        with c3:
            if st.session_state["ui_product_type"] in ["Card", "Graded Card"]:
                variant = st.text_input(
                    "Variant (optional)",
                    value=prefill.get("variant", ""),
                    placeholder="White Disco, Silver, Holo, Parallel, EX, ...",
                )
                card_subtype = st.text_input("Card subtype (optional)", value=prefill.get("card_subtype", ""), placeholder="Rookie, Insert, Parallel, etc.")

                if st.session_state["ui_product_type"] == "Card":
                    condition = st.selectbox("Condition*", CONDITION_OPTIONS, index=0)
                else:
                    condition = ""
                    st.caption("Condition is not used for graded cards (grade is used instead).")
            else:
                variant = ""
                card_subtype = ""
                condition = "Sealed"
                st.caption("Variant / Card subtype / Card # / Condition are not applicable for sealed products.")

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
            notes = st.text_area("Notes (optional)", height=92, placeholder="Anything you want to remember…")

        submitted = st.form_submit_button("Add to Inventory", type="primary", use_container_width=True)

        if submitted:
            product_type = st.session_state.get("ui_product_type", "Card")
            card_type = st.session_state.get("ui_card_type", "Pokemon")
            quantity = int(st.session_state.get("ui_quantity", 1))

            sealed_product_type = ""
            if product_type == "Sealed":
                if card_type == "Pokemon":
                    sealed_product_type = (st.session_state.get("ui_sealed_product_type", "") or "").strip()
                else:
                    sealed_product_type = (st.session_state.get("ui_sealed_product_type_text", "") or "").strip()

            grading_company = (st.session_state.get("ui_grading_company", "") or "").strip()
            grade = (st.session_state.get("ui_grade", "") or "").strip()

            missing = []
            if not card_name.strip():
                missing.append("Item/Card name")
            if not brand_or_league.strip():
                missing.append("Brand / League")
            if not purchased_from.strip():
                missing.append("Purchased from")

            if product_type == "Sealed" and not sealed_product_type:
                missing.append("Sealed Product Type")

            if product_type == "Card" and not condition.strip():
                missing.append("Condition")

            if product_type == "Graded Card":
                if not grading_company:
                    missing.append("Grading Company")
                if not grade:
                    missing.append("Grade")

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                total_price = _compute_total(purchase_price, shipping, tax)

                created_ids = []
                for _ in range(quantity):
                    new_row = {
                        "inventory_id": str(uuid.uuid4())[:8],
                        "image_url": (prefill_image_url or "").strip(),
                        "product_type": product_type,
                        "sealed_product_type": sealed_product_type if product_type == "Sealed" else "",
                        "grading_company": grading_company if product_type == "Graded Card" else "",
                        "grade": grade if product_type == "Graded Card" else "",
                        "card_type": card_type,
                        "brand_or_league": brand_or_league.strip(),
                        "set_name": set_name.strip() if set_name else "",
                        "year": year.strip() if year else "",
                        "card_name": card_name.strip(),
                        "card_number": card_number.strip() if card_number else "",
                        "variant": variant.strip() if variant else "",
                        "card_subtype": card_subtype.strip() if card_subtype else "",
                        "reference_link": (reference_link or "").strip(),
                        "purchase_date": str(purchase_date),
                        "purchased_from": purchased_from.strip(),
                        "purchase_price": float(purchase_price),
                        "shipping": float(shipping),
                        "tax": float(tax),
                        "total_price": float(total_price),
                        "condition": "Sealed" if product_type == "Sealed" else (condition if product_type == "Card" else ""),
                        "notes": notes.strip() if notes else "",
                        "created_at": pd.Timestamp.utcnow().isoformat(),
                        "inventory_status": STATUS_ACTIVE,
                        "listed_transaction_id": "",
                    }

                    if product_type == "Sealed":
                        new_row["card_number"] = ""
                        new_row["variant"] = ""
                        new_row["card_subtype"] = ""
                    if product_type == "Graded Card":
                        new_row["condition"] = ""

                    sheets_append_inventory_row(new_row)
                    created_ids.append(new_row["inventory_id"])

                st.session_state["inv_prefill_details"] = {}
                st.session_state["inv_prefill_image_url"] = ""
                refresh_inventory_from_sheets()

                if len(created_ids) == 1:
                    st.success(f"Added: {created_ids[0]} — {card_name}")
                else:
                    st.success(f"Added {len(created_ids)} items. Example IDs: {', '.join(created_ids[:5])}{'…' if len(created_ids) > 5 else ''}")

    df = st.session_state[INVENTORY_STATE_KEY].copy()
    df = df[df["inventory_status"].fillna(STATUS_ACTIVE).isin([STATUS_ACTIVE, STATUS_LISTED])]

    if len(df) > 0:
        st.markdown("#### Recently added")
        show = df.tail(10).copy()
        for col in ["purchase_price", "shipping", "tax", "total_price"]:
            show[col] = show[col].apply(_safe_money_display)

        st.dataframe(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "image_url": st.column_config.ImageColumn("Image"),
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
        f1, f2, f3, f4, f5, f6 = st.columns([1.1, 1.1, 1.3, 1.3, 1.2, 2])

        with f1:
            status_options = sorted([x for x in df["inventory_status"].dropna().unique().tolist() if str(x).strip() != ""])
            default_status = [s for s in [STATUS_ACTIVE, STATUS_LISTED] if s in status_options]
            if not default_status and status_options:
                default_status = status_options[:]
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
                        or s in str(r.get("grading_company", "")).lower()
                        or s in str(r.get("grade", "")).lower()
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
                "image_url": st.column_config.ImageColumn("Image"),
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
                edited_rows[c] = pd.to_numeric(edited_rows[c], errors="coerce").fillna(0.0)

            edited_rows["total_price"] = (edited_rows["purchase_price"] + edited_rows["shipping"] + edited_rows["tax"]).round(2)

            is_sealed = edited_rows["product_type"].astype(str).str.strip().eq("Sealed")
            edited_rows.loc[is_sealed, "condition"] = "Sealed"
            for col in ["variant", "card_subtype", "card_number", "grading_company", "grade"]:
                edited_rows.loc[is_sealed, col] = ""

            is_graded = edited_rows["product_type"].astype(str).str.strip().eq("Graded Card")
            edited_rows.loc[is_graded, "condition"] = ""

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
        k2.metric("Total Invested", f"${total_invested:,.2f}"

        )

        st.markdown("---")
        st.markdown("### Breakdown by Status")
        s_summary = (
            df.groupby("inventory_status", dropna=False)
            .agg(items=("inventory_id", "count"), invested=("total_price", "sum"))
            .reset_index()
            .sort_values("invested", ascending=False)
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
