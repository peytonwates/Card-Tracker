# pages/7_Breaks.py
import json
import re
import time
import uuid
from datetime import date
from pathlib import Path
from urllib.parse import urlparse, urljoin

import pandas as pd
import numpy as np
import streamlit as st

import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# Page config
# =========================================================
st.set_page_config(page_title="Breaks", layout="wide")
st.title("Breaks")


# =========================================================
# CONFIG (worksheets)
# =========================================================
INV_WS = st.secrets.get("inventory_worksheet", "inventory")
BREAKS_WS = st.secrets.get("breaks_worksheet", "breaks")                  # you already created this tab
BREAK_CARDS_WS = st.secrets.get("break_cards_worksheet", "break_cards")   # CREATE THIS TAB in the sheet


# =========================================================
# Quota-safe helpers (BACKOFF like Transactions page)
# =========================================================
def _is_quota_429(e: Exception) -> bool:
    try:
        return (
            isinstance(e, gspread.exceptions.APIError)
            and getattr(e, "response", None) is not None
            and e.response.status_code == 429
        )
    except Exception:
        return False


def _with_backoff(fn, tries: int = 6, base_sleep: float = 0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if _is_quota_429(e):
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last


# =========================================================
# Google Sheets client (same pattern as other pages)
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


@st.cache_resource
def _get_spreadsheet(spreadsheet_id: str):
    client = get_gspread_client()
    return _with_backoff(lambda: client.open_by_key(spreadsheet_id))


@st.cache_resource
def _get_ws(spreadsheet_id: str, ws_name: str):
    sh = _get_spreadsheet(spreadsheet_id)
    return _with_backoff(lambda: sh.worksheet(ws_name))


def _open_ws(ws_name: str):
    return _get_ws(st.secrets["spreadsheet_id"], ws_name)


@st.cache_data(show_spinner=False, ttl=45)
def _read_sheet_values_cached(spreadsheet_id: str, ws_name: str) -> list[list[str]]:
    ws = _get_ws(spreadsheet_id, ws_name)
    return _with_backoff(lambda: ws.get_all_values())


# =========================================================
# Small helpers
# =========================================================
def _safe_str(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x)


def _to_dt(x):
    return pd.to_datetime(x, errors="coerce")


def _now_iso_utc():
    return pd.Timestamp.utcnow().isoformat()


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
    df = df.copy()
    df.columns = new_cols
    return df


@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_sheet_df(worksheet_name: str) -> pd.DataFrame:
    """
    Robust sheet read that tolerates:
    - duplicate headers
    - blank headers
    - ragged rows
    And is quota-safe via cached reads + backoff.
    """
    spreadsheet_id = st.secrets["spreadsheet_id"]
    values = _read_sheet_values_cached(spreadsheet_id, worksheet_name)

    if not values:
        return pd.DataFrame()

    header = [str(h or "").strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []

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

    return pd.DataFrame(norm_rows, columns=fixed)


def _append_row(ws, header: list[str], row_dict: dict):
    ordered = []
    for h in header:
        key = h.split("__dup")[0] if "__dup" in h else h
        v = row_dict.get(key, row_dict.get(h, ""))
        if pd.isna(v):
            v = ""
        ordered.append(v)
    _with_backoff(lambda: ws.append_row(ordered, value_input_option="USER_ENTERED"))


def _update_row_by_id(ws, id_col_name: str, row_id: str, updates: dict):
    """
    Updates a single row where id_col_name == row_id.
    Uses quota-safe get_all_values.
    """
    values = _with_backoff(lambda: ws.get_all_values())
    if not values or len(values) < 2:
        return False

    header = [h.strip() for h in values[0]]
    try:
        id_idx = header.index(id_col_name)
    except ValueError:
        return False

    target_rownum = None
    for i, r in enumerate(values[1:], start=2):
        if id_idx < len(r) and str(r[id_idx]).strip() == str(row_id).strip():
            target_rownum = i
            break

    if not target_rownum:
        return False

    row = values[target_rownum - 1]
    if len(row) < len(header):
        row = row + [""] * (len(header) - len(row))

    for k, v in updates.items():
        if k in header:
            row[header.index(k)] = v

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(header)).split("1")[0]
    _with_backoff(lambda: ws.update(f"A{target_rownum}:{last_col_letter}{target_rownum}", [row], value_input_option="USER_ENTERED"))
    return True


# =========================================================
# Inventory header rules (match 2_Inventory.py)
# =========================================================
PRODUCT_TYPE_OPTIONS = ["Card", "Sealed", "Graded Card"]
CARD_TYPE_OPTIONS = ["Pokemon", "Sports"]

CONDITION_OPTIONS = [
    "Near Mint",
    "Lightly Played",
    "Moderately Played",
    "Heavily Played",
    "Damaged",
]

DEFAULT_INV_COLUMNS = [
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

INV_HEADER_ALIASES = {
    "product_type": ["product_type", "Product Type"],
    "sealed_product_type": ["sealed_product_type", "Sealed Product Type"],
}


def _sheet_header_to_internal(header: str) -> str:
    for internal, aliases in INV_HEADER_ALIASES.items():
        if header in aliases:
            return internal
    return header


def _internal_to_sheet_header(internal: str, existing_headers: list[str]) -> str:
    aliases = INV_HEADER_ALIASES.get(internal, [internal])
    for a in aliases:
        if a in existing_headers:
            return a
    if internal == "product_type":
        return "Product Type"
    if internal == "sealed_product_type":
        return "Sealed Product Type"
    return internal


def _ensure_inventory_headers(ws_inv) -> list[str]:
    first_row = _with_backoff(lambda: ws_inv.row_values(1))
    if not first_row:
        sheet_headers = []
        for internal in DEFAULT_INV_COLUMNS:
            if internal == "product_type":
                sheet_headers.append("Product Type")
            elif internal == "sealed_product_type":
                sheet_headers.append("Sealed Product Type")
            else:
                sheet_headers.append(internal)
        _with_backoff(lambda: ws_inv.append_row(sheet_headers, value_input_option="USER_ENTERED"))
        return sheet_headers

    existing = first_row
    existing_internal = set(_sheet_header_to_internal(h) for h in existing)
    missing_internal = [h for h in DEFAULT_INV_COLUMNS if h not in existing_internal]
    if missing_internal:
        additions = [_internal_to_sheet_header(h, existing) for h in missing_internal]
        new_headers = existing + additions
        _with_backoff(lambda: ws_inv.update("1:1", [new_headers], value_input_option="USER_ENTERED"))
        return new_headers

    return existing


def _normalize_card_type(val: str) -> str:
    s = _safe_str(val).strip().lower()
    if s == "sports" or "sport" in s:
        return "Sports"
    if s == "pokemon" or "pok" in s:
        return "Pokemon"
    return "Pokemon"


def _append_inventory_row(row_internal: dict):
    ws = _open_ws(INV_WS)
    sheet_headers = _ensure_inventory_headers(ws)
    header_to_internal = {h: _sheet_header_to_internal(h) for h in sheet_headers}

    ordered = []
    for sheet_h in sheet_headers:
        internal = header_to_internal.get(sheet_h, sheet_h)
        ordered.append(row_internal.get(internal, ""))

    _with_backoff(lambda: ws.append_row(ordered, value_input_option="USER_ENTERED"))


# =========================================================
# Breaks + Break Cards sheet headers
# =========================================================
BREAKS_COLUMNS = [
    "break_id",
    "purchase_date",
    "purchased_from",
    "reference_link",
    "image_url",
    "card_type",
    "brand_or_league",
    "set_name",
    "year",
    "box_name",
    "box_type",
    "qty_boxes",
    "purchase_price",  # per box
    "shipping",        # total
    "tax",             # total
    "total_price",     # all-in total for the break
    "notes",
    "status",          # OPEN / FINALIZED
    "cards_count",
    "cost_per_card",
    "created_at",
    "finalized_at",
]

BREAK_CARDS_COLUMNS = [
    "break_card_id",
    "break_id",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "condition",
    "reference_link",
    "image_url",
    "notes",
    "created_at",
    "pushed_to_inventory",  # YES/blank
    "inventory_id",         # populated when pushed
]


def _ensure_headers(ws, needed_cols: list[str]) -> list[str]:
    values = _with_backoff(lambda: ws.get_all_values())
    if not values:
        _with_backoff(lambda: ws.append_row(needed_cols, value_input_option="USER_ENTERED"))
        return needed_cols

    header = [h.strip() for h in values[0]]
    existing_set = set(header)

    missing = [c for c in needed_cols if c not in existing_set]
    if missing:
        new_header = header + missing
        _with_backoff(lambda: ws.update("1:1", [new_header], value_input_option="USER_ENTERED"))
        return new_header

    return header


# =========================================================
# Shared scraping helpers (Inventory-style)
# =========================================================
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
    "mega-box": "Mega Box",
    "blaster": "Blaster",
    "hobby-box": "Hobby Box",
    "hobby": "Hobby Box",
    "fat-pack": "Fat Pack",
    "value-pack": "Value Pack",
    "tin": "Tin",
    "booster-box": "Booster Box",
    "booster-bundle": "Booster Bundle",
    "elite-trainer-box": "Elite Trainer Box",
    "etb": "Elite Trainer Box",
}


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
    """
    Inventory-style: prefer real product images when present.
    """
    if soup is None:
        return ""

    for meta in [
        soup.find("meta", property="og:image"),
        soup.find("meta", attrs={"name": "twitter:image"}),
    ]:
        if meta and meta.get("content"):
            return meta["content"].strip()

    img = soup.find("img")
    if img and img.get("src"):
        return img["src"].strip()

    return ""


def _find_pricecharting_main_image(soup: BeautifulSoup) -> str:
    """
    PriceCharting often exposes true photo under 'More Photos' as storage.googleapis.com.
    Prefer that if present (same as Inventory page approach).
    """
    if soup is None:
        return ""

    candidates = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href

        if "storage.googleapis.com" in href and "images.pricecharting.com" in href:
            label = ""
            if a.parent:
                label = " ".join(a.parent.stripped_strings)
            if "main image" in (label or "").lower():
                return href
            candidates.append(href)

    # if no explicit "Main Image", use first storage image
    return candidates[0] if candidates else ""


def _title_case_from_slug(slug: str) -> str:
    return " ".join([w for w in (slug or "").replace("-", " ").split() if w]).title()


def _infer_sealed_type_from_slug_or_title(slug: str, title: str) -> str:
    t = ((slug or "") + " " + (title or "")).lower()
    for k, v in SEALED_TYPE_KEYWORDS.items():
        if k in t:
            return v
    return ""


def _parse_set_slug_generic(set_slug: str) -> dict:
    """
    Example:
      football-cards-2025-panini-donruss
      pokemon-surging-sparks
    """
    tokens = [t for t in (set_slug or "").split("-") if t]
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

    return {"card_type": "", "brand_or_league": "", "year": year, "set_name": _title_case_from_slug(set_slug or "")}


def _clean_box_title(title: str) -> str:
    if not title:
        return ""
    cleaned = title
    cleaned = cleaned.replace("| Sports Cards Pro", "").replace("| Sportscardspro", "")
    cleaned = cleaned.replace("| PriceCharting", "").replace("| Pricecharting", "")
    cleaned = cleaned.replace(" - Sports Cards Pro", "").replace(" - Sportscardspro", "")
    cleaned = cleaned.replace(" - PriceCharting", "").replace(" - Pricecharting", "")
    return cleaned.strip()


def _looks_like_single_card_slug(card_slug: str) -> bool:
    if not card_slug:
        return False
    return bool(re.search(r"-(\d+[A-Za-z0-9]*)$", card_slug))


def _parse_pricecharting_title(title: str):
    """
    Inventory-style: parse "Name #123" and a simple variant heuristic.
    """
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


# =========================================================
# Box pulling (SportscardsPro / PriceCharting)
# =========================================================
@st.cache_data(show_spinner=False, ttl=60 * 60 * 24)
def fetch_box_details(url: str) -> dict:
    out = {
        "image_url": "",
        "reference_link": (url or "").strip(),
        "card_type": "",
        "brand_or_league": "",
        "set_name": "",
        "year": "",
        "box_name": "",
        "box_type": "",
    }
    if not url or not str(url).strip():
        return out

    url = str(url).strip()
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    soup = None
    page_title = ""
    image_url = ""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        page_title = _find_best_title(soup)

        if "pricecharting.com" in host:
            image_url = _find_pricecharting_main_image(soup) or _find_best_image(soup)
        else:
            image_url = _find_best_image(soup)

        if image_url:
            image_url = urljoin(url, image_url)

    except Exception:
        soup = None
        page_title = ""
        image_url = ""

    out["image_url"] = image_url

    parts = [p for p in (path or "").split("/") if p]
    set_slug, item_slug = None, None
    if len(parts) >= 3 and parts[0].lower() == "game":
        set_slug, item_slug = parts[1], parts[2]

    if set_slug:
        out.update(_parse_set_slug_generic(set_slug))

    if page_title:
        out["box_name"] = _clean_box_title(page_title)

    out["box_type"] = _infer_sealed_type_from_slug_or_title(item_slug or "", out["box_name"])

    lowered = (url + " " + page_title).lower()
    if not out["card_type"]:
        if "pokemon" in lowered:
            out["card_type"] = "Pokemon"
            out["brand_or_league"] = out["brand_or_league"] or "Pokemon TCG"
        elif "sportscardspro.com" in host:
            out["card_type"] = "Sports"

    out["card_type"] = _normalize_card_type(out["card_type"])
    return out


# =========================================================
# Card pulling (Inventory-style Pull details for a single card link)
# =========================================================
@st.cache_data(show_spinner=False, ttl=60 * 60 * 24)
def fetch_card_details_and_image(url: str) -> dict:
    """
    Mirrors Inventory behavior:
    - best title
    - best image (prefer PriceCharting main image storage.googleapis.com)
    - parse PriceCharting slug/title for card name/number/variant + set/year when possible
    """
    result = {
        "image_url": "",
        "card_name": "",
        "card_number": "",
        "variant": "",
        "card_subtype": "",
        "reference_link": (url or "").strip(),
        "card_type": "",
        "brand_or_league": "",
        "set_name": "",
        "year": "",
    }

    if not url or not str(url).strip():
        return result

    url = str(url).strip()
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    soup = None
    page_title = ""
    image_url = ""

    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        page_title = _find_best_title(soup)

        if "pricecharting.com" in host:
            image_url = _find_pricecharting_main_image(soup) or _find_best_image(soup)
        else:
            image_url = _find_best_image(soup)

        if image_url:
            image_url = urljoin(url, image_url)

    except Exception:
        soup = None
        page_title = ""
        image_url = ""

    result["image_url"] = image_url

    # Try PriceCharting parse for set + item
    parts = [p for p in (path or "").split("/") if p]
    set_slug, item_slug = None, None
    if len(parts) >= 3 and parts[0].lower() == "game":
        set_slug, item_slug = parts[1], parts[2]

    if "pricecharting.com" in host and set_slug:
        result.update(_parse_set_slug_generic(set_slug))

        # If it "looks like a single card slug", treat as a card and parse title
        if item_slug and _looks_like_single_card_slug(item_slug):
            result.update(_parse_pricecharting_title(page_title))

            # fallback card_number from slug tail
            if not result["card_number"] and item_slug:
                m = re.search(r"-(\d+[A-Za-z0-9]*)$", item_slug)
                if m:
                    result["card_number"] = m.group(1)

            # fallback card_name from slug
            if not result["card_name"] and item_slug:
                cleaned = re.sub(r"-(\d+[A-Za-z0-9]*)$", "", item_slug)
                result["card_name"] = _title_case_from_slug(cleaned)

        else:
            # Not clearly a single card; still return title as name
            if page_title and not result["card_name"]:
                result["card_name"] = page_title

        return result

    # Non-PriceCharting fallback:
    if page_title:
        result["card_name"] = page_title

    lowered = (url + " " + page_title).lower()
    if "pokemon" in lowered:
        result["card_type"] = "Pokemon"
        result["brand_or_league"] = "Pokemon TCG"
    elif any(tok in lowered for tok in ["prizm", "optic", "select", "donruss", "panini", "topps"]):
        result["card_type"] = "Sports"

    return result


# =========================================================
# Load sheets
# =========================================================
ws_breaks = _open_ws(BREAKS_WS)
ws_break_cards = _open_ws(BREAK_CARDS_WS)  # NOTE: you must create this tab in Sheets

breaks_headers = _ensure_headers(ws_breaks, BREAKS_COLUMNS)
break_cards_headers = _ensure_headers(ws_break_cards, BREAK_CARDS_COLUMNS)

breaks_df = load_sheet_df(BREAKS_WS)
break_cards_df = load_sheet_df(BREAK_CARDS_WS)

breaks_df = _ensure_unique_columns(breaks_df)
break_cards_df = _ensure_unique_columns(break_cards_df)

# Normalize breaks df
if breaks_df.empty:
    breaks_df = pd.DataFrame(columns=BREAKS_COLUMNS)

for c in BREAKS_COLUMNS:
    if c not in breaks_df.columns:
        breaks_df[c] = ""

breaks_df["purchase_date_dt"] = _to_dt(breaks_df["purchase_date"])
breaks_df["created_at_dt"] = _to_dt(breaks_df["created_at"])
breaks_df["status"] = breaks_df["status"].replace("", "OPEN").fillna("OPEN").astype(str)

# Normalize break cards df
if break_cards_df.empty:
    break_cards_df = pd.DataFrame(columns=BREAK_CARDS_COLUMNS)

for c in BREAK_CARDS_COLUMNS:
    if c not in break_cards_df.columns:
        break_cards_df[c] = ""

break_cards_df["created_at_dt"] = _to_dt(break_cards_df["created_at"])
break_cards_df["pushed_to_inventory"] = break_cards_df["pushed_to_inventory"].astype(str).fillna("")

open_breaks = breaks_df[breaks_df["status"].astype(str).str.upper().eq("OPEN")].copy()
open_breaks = open_breaks.sort_values(
    by=[c for c in ["purchase_date_dt", "created_at_dt"] if c in open_breaks.columns],
    ascending=[False, False][: len([c for c in ["purchase_date_dt", "created_at_dt"] if c in open_breaks.columns])],
    na_position="last",
)

finalized_breaks = breaks_df[breaks_df["status"].astype(str).str.upper().eq("FINALIZED")].copy()
finalized_breaks = finalized_breaks.sort_values(
    by=[c for c in ["finalized_at", "purchase_date_dt", "created_at_dt"] if c in finalized_breaks.columns],
    ascending=False,
    na_position="last",
)


# =========================================================
# Refresh button
# =========================================================
top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Refresh from Sheets", use_container_width=True):
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.rerun()


# =========================================================
# UI Tabs
# =========================================================
tab_breaks, tab_cards = st.tabs(["Breaks (Boxes)", "Cards in Break"])


# ---------------------------------------------------------
# TAB 1: Breaks
# ---------------------------------------------------------
with tab_breaks:
    st.subheader("Create a Break (Box Opening)")
    st.caption("Paste a SportsCardsPro or PriceCharting box link and click Pull details to auto-fill the box fields.")

    link_c1, link_c2 = st.columns([4, 1])
    with link_c1:
        break_reference_link = st.text_input(
            "Box reference link (recommended)",
            key="break_ref_link_input",
            placeholder="https://www.sportscardspro.com/game/football-cards-2025-panini-donruss/mega-box",
        )
    with link_c2:
        pull_box = st.button("Pull details", use_container_width=True)

    if pull_box:
        details = fetch_box_details(break_reference_link)
        st.session_state["break_prefill"] = details

        st.session_state["break_box_name"] = details.get("box_name", "")
        st.session_state["break_card_type"] = details.get("card_type", "Pokemon") or "Pokemon"
        st.session_state["break_brand_or_league"] = details.get("brand_or_league", "")
        st.session_state["break_set_name"] = details.get("set_name", "")
        st.session_state["break_year"] = details.get("year", "")
        st.session_state["break_box_type"] = details.get("box_type", "")
        st.session_state["break_image_url"] = details.get("image_url", "")

        st.success("Pulled details. Review/edit below, then Add Break.")
        st.rerun()

    prefill = st.session_state.get("break_prefill", {}) or {}
    img = st.session_state.get("break_image_url", "") or prefill.get("image_url", "")
    if img:
        try:
            st.image(img, width=160)
        except Exception:
            st.caption("Image unavailable")

    with st.form("create_break_form", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([1.1, 1.0, 1.0, 1.2])
        with c1:
            purchase_date = st.date_input("Purchase Date*", value=date.today())
        with c2:
            qty_boxes = st.number_input("# of Boxes*", min_value=1, max_value=999, value=1, step=1)
        with c3:
            purchase_price = st.number_input("Purchase Price (per box)*", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        with c4:
            purchased_from = st.text_input("Purchased From*", placeholder="Walmart, Target, LCS, Whatnot, etc.")

        c5, c6, c7 = st.columns([2.0, 1.2, 1.0])
        with c5:
            box_name = st.text_input("Box Name*", value=st.session_state.get("break_box_name", ""))
        with c6:
            set_name = st.text_input("Set Name (optional)", value=st.session_state.get("break_set_name", ""))
        with c7:
            year = st.text_input("Year (optional)", value=st.session_state.get("break_year", ""))

        c8, c9, c10 = st.columns([1.0, 1.4, 1.6])
        with c8:
            ct_default = st.session_state.get("break_card_type", "Pokemon")
            ct_default = "Sports" if str(ct_default).strip().lower() == "sports" else "Pokemon"
            card_type = st.selectbox("Card Type*", options=["Pokemon", "Sports"], index=(1 if ct_default == "Sports" else 0))
        with c9:
            default_brand = st.session_state.get("break_brand_or_league", "")
            if not default_brand and card_type == "Pokemon":
                default_brand = "Pokemon TCG"
            brand_or_league = st.text_input("Brand / League (optional)", value=default_brand)
        with c10:
            box_type = st.text_input("Box Type (optional)", value=st.session_state.get("break_box_type", ""), placeholder="Mega Box / Blaster / Hobby / ETB / etc.")

        c11, c12 = st.columns([1.0, 2.0])
        with c11:
            shipping = st.number_input("Shipping (total)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
            tax = st.number_input("Tax (total)", min_value=0.0, value=0.0, step=1.0, format="%.2f")
        with c12:
            reference_link = st.text_input("Reference link (auto)", value=(break_reference_link or "").strip())
            notes = st.text_area("Notes (optional)", height=92)

        submitted = st.form_submit_button("Add Break", type="primary", use_container_width=True)
        if submitted:
            missing = []
            if not purchased_from.strip():
                missing.append("Purchased From")
            if not box_name.strip():
                missing.append("Box Name")

            if missing:
                st.error("Missing required fields: " + ", ".join(missing))
            else:
                break_id = str(uuid.uuid4())[:8]
                total_price = float(qty_boxes) * float(purchase_price) + float(shipping) + float(tax)

                row = {
                    "break_id": break_id,
                    "purchase_date": purchase_date.isoformat(),
                    "purchased_from": purchased_from.strip(),
                    "reference_link": reference_link.strip(),
                    "image_url": img or "",
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
                    "total_price": round(float(total_price), 2),
                    "notes": notes.strip(),
                    "status": "OPEN",
                    "cards_count": "",
                    "cost_per_card": "",
                    "created_at": _now_iso_utc(),
                    "finalized_at": "",
                }

                _append_row(ws_breaks, breaks_headers, row)

                st.session_state["break_prefill"] = {}
                for k in ["break_box_name", "break_card_type", "break_brand_or_league", "break_set_name", "break_year", "break_box_type", "break_image_url"]:
                    st.session_state.pop(k, None)

                st.success(f"Break created: {break_id}")
                st.rerun()

    st.markdown("---")
    st.markdown("### Open Breaks")
    if open_breaks.empty:
        st.info("No OPEN breaks yet.")
    else:
        view = open_breaks[[
            "break_id", "purchase_date", "box_name", "box_type", "card_type", "set_name", "year",
            "qty_boxes", "total_price", "purchased_from", "reference_link"
        ]].copy()
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.markdown("### Finalized Breaks")
    if finalized_breaks.empty:
        st.caption("None yet.")
    else:
        view = finalized_breaks[[
            "break_id", "purchase_date", "box_name", "card_type", "cards_count", "cost_per_card", "total_price", "finalized_at"
        ]].copy()
        st.dataframe(view, use_container_width=True, hide_index=True)


# ---------------------------------------------------------
# TAB 2: Cards in Break
# ---------------------------------------------------------
with tab_cards:
    st.subheader("Add Cards from a Break")
    st.caption("Select an OPEN break, enter each card you pulled, then finalize to push them into Inventory.")

    if open_breaks.empty:
        st.info("Create an OPEN break first (Breaks tab).")
    else:
        options = []
        open_map = {}
        for _, r in open_breaks.iterrows():
            bid = _safe_str(r.get("break_id", "")).strip()
            label = f"{bid} — {_safe_str(r.get('box_name','')).strip()} ({_safe_str(r.get('purchase_date','')).strip()})"
            options.append(label)
            open_map[label] = r.to_dict()

        choice = st.selectbox("Select OPEN break", options=options, index=0)
        br = open_map.get(choice, {}) or {}

        break_id = _safe_str(br.get("break_id", "")).strip()
        break_card_type = _normalize_card_type(br.get("card_type", "Pokemon"))
        break_brand = _safe_str(br.get("brand_or_league", "")).strip()
        break_set = _safe_str(br.get("set_name", "")).strip()
        break_year = _safe_str(br.get("year", "")).strip()
        break_purchase_date = _safe_str(br.get("purchase_date", "")).strip()
        break_purchased_from = _safe_str(br.get("purchased_from", "")).strip()
        break_box_name = _safe_str(br.get("box_name", "")).strip()
        break_total_price = float(pd.to_numeric(br.get("total_price", 0), errors="coerce") or 0.0)

        bc = break_cards_df[break_cards_df["break_id"].astype(str).str.strip().eq(break_id)].copy()
        bc = bc.sort_values("created_at_dt", na_position="last")

        # =====================================================
        # NEW: Inventory-style "Pull details" for CARD link
        # =====================================================
        st.markdown("#### Pull card details (optional)")
        st.caption("Paste a PriceCharting link for the single card and click Pull details to auto-fill fields (and image), like the Inventory page.")

        card_link_c1, card_link_c2 = st.columns([4, 1])
        with card_link_c1:
            card_reference_link = st.text_input(
                "Card reference link",
                key=f"break_card_ref_link_input__{break_id}",
                placeholder="https://www.pricecharting.com/game/pokemon-surging-sparks/pikachu-123",
            )
        with card_link_c2:
            pull_card = st.button("Pull details", use_container_width=True, key=f"pull_break_card__{break_id}")

        if pull_card:
            details = fetch_card_details_and_image(card_reference_link)
            st.session_state["break_card_prefill"] = details
            st.success("Pulled card details. Review/adjust below, then Add Card.")
            st.rerun()

        card_prefill = st.session_state.get("break_card_prefill", {}) or {}
        card_img = (card_prefill.get("image_url") or "").strip()
        if card_img:
            try:
                st.image(card_img, width=160)
            except Exception:
                st.caption("Image unavailable")

        st.markdown("#### Add a card")
        with st.form("add_break_card_form", clear_on_submit=True):
            c1, c2, c3 = st.columns([2.0, 1.0, 1.0])
            with c1:
                card_name = st.text_input(
                    "Card name*",
                    value=(card_prefill.get("card_name", "") or "").strip(),
                    placeholder="Pikachu / Jayden Daniels / etc."
                )
            with c2:
                card_number = st.text_input(
                    "Card # (optional)",
                    value=(card_prefill.get("card_number", "") or "").strip(),
                    placeholder="332"
                )
            with c3:
                condition = st.selectbox("Condition*", CONDITION_OPTIONS, index=0)

            c4, c5, c6 = st.columns([1.0, 1.2, 2.0])
            with c4:
                variant = st.text_input(
                    "Variant (optional)",
                    value=(card_prefill.get("variant", "") or "").strip(),
                    placeholder="Silver, Holo, Parallel, Insert…"
                )
            with c5:
                card_subtype = st.text_input(
                    "Card subtype (optional)",
                    value=(card_prefill.get("card_subtype", "") or "").strip(),
                    placeholder="Rookie, Insert, Parallel…"
                )
            with c6:
                # default to pulled link if available; else whatever they typed above
                default_ref = (card_prefill.get("reference_link") or card_reference_link or "").strip()
                card_ref = st.text_input(
                    "Card reference link (optional)",
                    value=default_ref,
                    placeholder="(optional) PriceCharting/SCP link for the single card"
                )

            c7, c8 = st.columns([1.0, 2.0])
            with c7:
                image_url = st.text_input(
                    "Image URL (optional)",
                    value=(card_img or "").strip(),
                    placeholder="(optional)"
                )
            with c8:
                notes = st.text_area("Notes (optional)", height=80)

            add_btn = st.form_submit_button("Add Card", type="primary", use_container_width=True)
            if add_btn:
                if not card_name.strip():
                    st.error("Card name is required.")
                else:
                    row = {
                        "break_card_id": str(uuid.uuid4())[:10],
                        "break_id": break_id,
                        "card_name": card_name.strip(),
                        "card_number": card_number.strip(),
                        "variant": variant.strip(),
                        "card_subtype": card_subtype.strip(),
                        "condition": condition.strip(),
                        "reference_link": (card_ref or "").strip(),
                        "image_url": (image_url or "").strip(),
                        "notes": (notes or "").strip(),
                        "created_at": _now_iso_utc(),
                        "pushed_to_inventory": "",
                        "inventory_id": "",
                    }
                    _append_row(ws_break_cards, break_cards_headers, row)

                    # clear the prefill after successful add (so next entry is fresh)
                    st.session_state["break_card_prefill"] = {}
                    st.success("Card added to break.")
                    st.rerun()

        st.markdown("---")
        st.markdown("#### Cards entered for this break")
        if bc.empty:
            st.info("No cards entered yet.")
        else:
            show = bc[[
                "break_card_id", "card_name", "card_number", "variant", "card_subtype",
                "condition", "reference_link", "pushed_to_inventory", "inventory_id"
            ]].copy()
            st.dataframe(show, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### Finalize Break → Push to Inventory")
        st.caption("Finalize will evenly distribute the Break total cost across all cards (that have not been pushed yet).")

        not_pushed = bc[bc["pushed_to_inventory"].astype(str).str.upper().ne("YES")].copy()
        n_cards = int(len(not_pushed))
        est_cpp = (break_total_price / n_cards) if n_cards > 0 else 0.0

        k1, k2, k3 = st.columns(3)
        k1.metric("Break Total Cost", f"${break_total_price:,.2f}")
        k2.metric("Cards not pushed", f"{n_cards:,}")
        k3.metric("Est. cost per card", f"${est_cpp:,.2f}" if n_cards else "$0.00")

        finalize = st.button("✅ Finalize + Add Cards to Inventory", type="primary", use_container_width=True, disabled=(n_cards == 0))
        if finalize:
            if n_cards == 0:
                st.warning("No cards to push (all cards already pushed, or none entered).")
            else:
                cost_per_card = round(float(break_total_price) / float(n_cards), 2)

                pushed_ids = []
                for _, row in not_pushed.iterrows():
                    inv_id = str(uuid.uuid4())[:8]

                    inv_row = {
                        "inventory_id": inv_id,
                        "image_url": _safe_str(row.get("image_url", "")).strip(),
                        "product_type": "Card",
                        "sealed_product_type": "",
                        "card_type": break_card_type,
                        "brand_or_league": break_brand if break_brand else ("Pokemon TCG" if break_card_type == "Pokemon" else ""),
                        "set_name": break_set,
                        "year": break_year,
                        "card_name": _safe_str(row.get("card_name", "")).strip(),
                        "card_number": _safe_str(row.get("card_number", "")).strip(),
                        "variant": _safe_str(row.get("variant", "")).strip(),
                        "card_subtype": _safe_str(row.get("card_subtype", "")).strip(),
                        "grading_company": "",
                        "grade": "",
                        "reference_link": _safe_str(row.get("reference_link", "")).strip(),
                        "purchase_date": break_purchase_date or str(date.today()),
                        "purchased_from": break_purchased_from or f"Break {break_id}",
                        "purchase_price": float(cost_per_card),
                        "shipping": 0.0,
                        "tax": 0.0,
                        "total_price": float(cost_per_card),
                        "condition": _safe_str(row.get("condition", "Near Mint")).strip(),
                        "notes": ("From break "
                                  f"{break_id} — {break_box_name}. "
                                  + _safe_str(row.get("notes", "")).strip()).strip(),
                        "created_at": _now_iso_utc(),
                        "inventory_status": "ACTIVE",
                        "listed_transaction_id": "",
                        "market_price": 0.0,
                        "market_value": 0.0,
                        "market_price_updated_at": "",
                    }

                    _append_inventory_row(inv_row)

                    _update_row_by_id(
                        ws_break_cards,
                        id_col_name="break_card_id",
                        row_id=_safe_str(row.get("break_card_id", "")).strip(),
                        updates={
                            "pushed_to_inventory": "YES",
                            "inventory_id": inv_id,
                        },
                    )

                    pushed_ids.append(inv_id)

                _update_row_by_id(
                    ws_breaks,
                    id_col_name="break_id",
                    row_id=break_id,
                    updates={
                        "status": "FINALIZED",
                        "cards_count": str(n_cards),
                        "cost_per_card": str(cost_per_card),
                        "finalized_at": _now_iso_utc(),
                    },
                )

                st.success(f"Finalized break {break_id}. Added {len(pushed_ids)} card(s) to Inventory.")
                st.rerun()
