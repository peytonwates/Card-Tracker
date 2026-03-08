# pages/5_Grading.py
# ---------------------------------------------------------
# COMBINED VERSION (Watchlist + Create Submission screens)
#
# Includes:
# 1) Your current Watchlist Pricing + Summary (PriceCharting sales history + TCGplayer market price via Playwright)
#    - Refresh mode: TCGplayer only / PriceCharting only / Both
#    - Stores TCGplayer prices back into watchlist sheet (batch write) for speed
#
# 2) Restores your old Grading Submission UI:
#    - Analysis (single-card PSA9/PSA10 calc)
#    - Create Submission (select inventory items -> write grading rows + set inventory status GRADING)
#    - Update Returns (edit returned date / grade / costs, mark RETURNED, sync back to inventory)
#    - Submission Summary (rollups by submission_id)
#
# IMPORTANT:
# - PriceCharting sold-sales scraping logic for watchlist remains untouched.
# - Grading submission logic uses the canonical grading columns you provided.
# ---------------------------------------------------------

import json
import re
import time
import uuid
from datetime import datetime, date, timedelta
from pathlib import Path
from math import exp

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials


# =========================
# Page config
# =========================
st.set_page_config(page_title="Grading", layout="wide")
st.title("Grading — Analysis")


# =========================================================
# CONFIG (Watchlist / Market)
# =========================================================
WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")
GEMRATES_WS_NAME = st.secrets.get("gemrates_worksheet", "gemrates")

WATCHLIST_HEADERS_EXPECTED = ["Generation", "Set", "Card Name", "Card No", "Link", "Image"]

TCG_LINK_COL = "tcgplayer_link"
TCG_PRICE_COL = "tcgplayer_price"
TCG_UPDATED_COL = "tcgplayer_price_updated_utc"

SALES_HISTORY_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "grade_bucket",
    "sale_date",
    "price",
    "title",
    "sale_key",
    "updated_utc",
]

PER_ITEM_N = 5
PSA10_PER_ITEM = 5
PSA9_PER_ITEM = 5
EBAY_ONLY = True

GRADING_ALL_IN_COST = float(st.secrets.get("grading_all_in_cost", 25.0))
CONF_K = float(st.secrets.get("gemrate_conf_k", 250.0))
TCG_PRICE_TTL_HOURS = float(st.secrets.get("tcgplayer_price_ttl_hours", 6.0))


# =========================================================
# CONFIG (Grading Submission screens)
# =========================================================
INVENTORY_WS_NAME = st.secrets.get("inventory_worksheet", "inventory")
GRADING_WS_NAME = st.secrets.get("grading_worksheet", "grading")

STATUS_ACTIVE = "ACTIVE"
STATUS_LISTED = "LISTED"
STATUS_SOLD = "SOLD"
STATUS_TRADED = "TRADED"
STATUS_GRADING = "GRADING"

ELIGIBLE_INV_STATUSES = {STATUS_ACTIVE}

DEFAULT_GRADING_FEE_PER_CARD = float(st.secrets.get("default_grading_fee_per_card", 28.0))
DEFAULT_RETURN_BUSINESS_DAYS = int(st.secrets.get("default_business_days_return", 75))

GRADING_CANON_COLS = [
    "grading_row_id",
    "submission_id",
    "submission_date",
    "estimated_return_date",
    "inventory_id",
    "reference_link",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "purchased_from",
    "purchase_date",
    "purchase_total",
    "grading_company",
    "grading_fee_initial",   # <-- use THIS
    "additional_costs",      # <-- use THIS
    "psa9_price",
    "psa10_price",
    "status",
    "returned_date",
    "received_grade",
    "notes",
    "created_at",
    "updated_at",
    "synced_to_inventory",
]


# =========================
# Shared small helpers
# =========================
def safe_str(x) -> str:
    return "" if x is None else str(x)

def is_blank(x) -> bool:
    s = safe_str(x).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}

def safe_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = safe_str(x).strip().replace("$", "").replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, int):
            return int(x)
        s = safe_str(x).strip().replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def a1_col_letter(n: int) -> str:
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters

def clamp(v, lo, hi):
    return lo if v < lo else hi if v > hi else v

def add_business_days(start_d: date, n: int) -> date:
    d = start_d
    added = 0
    while added < n:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d

def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()

def _parse_utc_iso(s: str):
    try:
        if not s:
            return None
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None

def _bs_parser():
    try:
        import lxml  # noqa: F401
        return "lxml"
    except Exception:
        return "html.parser"

def _parse_any_date(text: str):
    if not text:
        return None
    d = pd.to_datetime(text, errors="coerce")
    if pd.isna(d):
        return None
    if d.year < 2000:
        return None
    return d.date()

def _price_from_cell_text(text: str) -> float:
    if not text:
        return 0.0
    m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", text)
    if not m:
        return 0.0
    return safe_float(m.group(1), 0.0)

def _normalize_set(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def _normalize_cardno(v: str) -> str:
    digits = re.sub(r"[^\d]", "", safe_str(v))
    if digits == "":
        return ""
    return str(int(digits))

def _unique_sale_key(link: str, base_sale_key: str) -> str:
    lk = (link or "").strip()
    bk = (base_sale_key or "").strip()
    if not bk:
        return lk
    return f"{lk}|{bk}"


# =========================
# Google Sheets auth
# =========================
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
        sa_rel = st.secrets["service_account_json_path"]
        p = Path(sa_rel)
        if not p.is_absolute():
            p = Path.cwd() / sa_rel
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets: add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')

def _gs_write_retry(fn, *args, **kwargs):
    max_tries = 8
    base_sleep = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            raise
    raise RuntimeError("Google Sheets API quota exceeded (retries exhausted).")

def get_sheet():
    client = get_gspread_client()
    return client.open_by_key(st.secrets["spreadsheet_id"])

def get_ws(sheet, ws_name: str):
    return sheet.worksheet(ws_name)

def read_ws_df(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values or not values[0]:
        return pd.DataFrame()
    header = [safe_str(x).strip() for x in values[0]]
    rows = values[1:]
    out = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        elif len(r) > len(header):
            r = r[:len(header)]
        out.append(r)
    df = pd.DataFrame(out, columns=header)
    df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


# =========================================================
# Header management
#   - simple headers for watchlist/sales_history/gemrates
#   - canonical header repair for grading/inventory (from your old section)
# =========================================================
def ensure_headers_simple(ws, headers: list[str]):
    values = ws.get_all_values()
    if not values:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
        return
    current = [safe_str(x).strip() for x in (values[0] or [])]
    if current != headers:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")

def ensure_headers_canon(ws, needed_headers: list[str]):
    """
    Idempotent header repair (quota-safe):
    - strips whitespace
    - strips ANY stacked __dup suffixes
    - rebuilds stable unique header set: foo, foo__dup2, foo__dup3...
    - appends missing canonical columns by base-name
    - optionally deletes duplicate columns that are completely blank (safe prune)
    """
    def strip_dups(h: str) -> str:
        return re.sub(r"(?:__dup\d+)+$", "", str(h or "").strip())

    values = ws.get_all_values()
    if not values:
        _gs_write_retry(ws.update, values=[needed_headers], range_name="1:1", value_input_option="USER_ENTERED")
        return needed_headers

    raw = values[0] if values else []
    if not raw:
        _gs_write_retry(ws.update, values=[needed_headers], range_name="1:1", value_input_option="USER_ENTERED")
        return needed_headers

    cleaned = []
    for i, h in enumerate(raw, start=1):
        hh = str(h or "").strip()
        if hh == "":
            hh = f"unnamed__col{i}"
        cleaned.append(hh)

    bases = [strip_dups(h) for h in cleaned]

    base_set = set(bases)
    for h in needed_headers:
        b = strip_dups(h)
        if b not in base_set:
            bases.append(b)
            cleaned.append(b)
            base_set.add(b)

    counts = {}
    new_header = []
    for b in bases:
        counts[b] = counts.get(b, 0) + 1
        if counts[b] == 1:
            new_header.append(b)
        else:
            new_header.append(f"{b}__dup{counts[b]}")

    # safe prune blank duplicate cols
    if len(values) > 1:
        data_rows = []
        for r in values[1:]:
            if len(r) < len(raw):
                r = r + [""] * (len(raw) - len(r))
            data_rows.append(r)

        base_to_cols = {}
        for j, h in enumerate(raw):
            b = strip_dups(h)
            base_to_cols.setdefault(b, []).append(j)

        delete_col_idxs_1based = []
        for b, idxs in base_to_cols.items():
            if len(idxs) <= 1:
                continue
            for j in idxs[1:]:
                all_blank = True
                for r in data_rows:
                    v = str(r[j] if j < len(r) else "").strip()
                    if v != "":
                        all_blank = False
                        break
                if all_blank:
                    delete_col_idxs_1based.append(j + 1)

        deleted_any = False
        for col in sorted(delete_col_idxs_1based, reverse=True):
            try:
                ws.delete_columns(col)
                deleted_any = True
            except Exception:
                pass

        if deleted_any:
            values = ws.get_all_values()
            raw = values[0] if values else []
            if not raw:
                _gs_write_retry(ws.update, values=[needed_headers], range_name="1:1", value_input_option="USER_ENTERED")
                return needed_headers

            cleaned = []
            for i, h in enumerate(raw, start=1):
                hh = str(h or "").strip()
                if hh == "":
                    hh = f"unnamed__col{i}"
                cleaned.append(hh)

            bases = [strip_dups(h) for h in cleaned]
            base_set = set(bases)
            for h in needed_headers:
                b = strip_dups(h)
                if b not in base_set:
                    bases.append(b)
                    base_set.add(b)

            counts = {}
            new_header = []
            for b in bases:
                counts[b] = counts.get(b, 0) + 1
                new_header.append(b if counts[b] == 1 else f"{b}__dup{counts[b]}")

    if raw != new_header:
        _gs_write_retry(ws.update, values=[new_header], range_name="1:1", value_input_option="USER_ENTERED")

    return new_header


def ensure_watchlist_col(ws, col_name: str):
    values = ws.get_all_values()
    if not values or not values[0]:
        headers = WATCHLIST_HEADERS_EXPECTED.copy()
        if col_name not in headers:
            headers.append(col_name)
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
        return headers, headers.index(col_name) + 1

    headers = [safe_str(x).strip() for x in values[0]]
    if col_name not in headers:
        headers.append(col_name)
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
    return headers, headers.index(col_name) + 1

def ensure_watchlist_image_col(watch_ws):
    values = watch_ws.get_all_values()
    if not values or not values[0]:
        _gs_write_retry(watch_ws.update, values=[WATCHLIST_HEADERS_EXPECTED], range_name="1:1", value_input_option="RAW")
        return WATCHLIST_HEADERS_EXPECTED, WATCHLIST_HEADERS_EXPECTED.index("Image") + 1

    headers = [safe_str(x).strip() for x in values[0]]
    if "Image" not in headers:
        headers.append("Image")
        _gs_write_retry(watch_ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
    return headers, headers.index("Image") + 1

def batch_update_column_values(ws, col_idx_1based: int, start_row_1based: int, values_list: list[str]):
    if not values_list:
        return
    col_letter = a1_col_letter(col_idx_1based)
    end_row = start_row_1based + len(values_list) - 1
    rng = f"{col_letter}{start_row_1based}:{col_letter}{end_row}"
    payload = [[safe_str(v)] for v in values_list]
    _gs_write_retry(ws.update, range_name=rng, values=payload, value_input_option="RAW")


# =========================
# HTTP (basic backoff)
# =========================
@st.cache_resource
def get_http_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s

def http_get_with_backoff(url: str, *, timeout=25, max_tries=6):
    sess = get_http_session()
    sleep_s = 1.0
    last_exc = None

    for _ in range(max_tries):
        try:
            r = sess.get(url, timeout=timeout)
        except Exception as e:
            last_exc = e
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.7, 20.0)
            continue

        if r.status_code == 200:
            return r

        if r.status_code == 429:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.8, 25.0)
            continue

        if r.status_code in {500, 502, 503, 504}:
            time.sleep(sleep_s)
            sleep_s = min(sleep_s * 1.6, 15.0)
            continue

        r.raise_for_status()

    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"HTTPError: retries exhausted for {url}")


# =========================
# TCGplayer Market Price (JS-rendered -> Playwright)
# =========================
def _extract_tcgplayer_market_price_from_dom_text(dom_text: str) -> float:
    if not dom_text:
        return 0.0
    v = _price_from_cell_text(dom_text)
    if v > 0:
        return float(v)
    m = re.search(r"([0-9][0-9,]*\.?[0-9]{0,2})", dom_text)
    if m:
        vv = safe_float(m.group(1), 0.0)
        return float(vv) if vv > 0 else 0.0
    return 0.0

@st.cache_resource
def _get_playwright_browser():
    from playwright.sync_api import sync_playwright  # lazy import
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
    )
    return {"p": p, "browser": browser}

def _tcgplayer_price_via_playwright(url: str) -> float:
    url = (url or "").strip()
    if not url or "tcgplayer.com" not in url.lower():
        return 0.0

    holder = _get_playwright_browser()
    browser = holder["browser"]

    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale="en-US",
        viewport={"width": 1280, "height": 800},
    )
    page = context.new_page()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(1200)

        sel = "span.price-points__upper__price"

        try:
            page.wait_for_selector(sel, timeout=10000)
        except Exception:
            txt = page.inner_text("body")
            m = re.search(r"Market Price\s*\$?\s*([0-9][0-9,]*\.?[0-9]{0,2})", txt, flags=re.IGNORECASE)
            if m:
                vv = safe_float(m.group(1), 0.0)
                return float(vv) if vv > 0 else 0.0
            return 0.0

        price_text = page.locator(sel).first.inner_text().strip()
        price = _extract_tcgplayer_market_price_from_dom_text(price_text)
        if price > 0:
            return float(price)

        body_txt = page.inner_text("body")
        m = re.search(r"Market Price\s*\$?\s*([0-9][0-9,]*\.?[0-9]{0,2})", body_txt, flags=re.IGNORECASE)
        if m:
            vv = safe_float(m.group(1), 0.0)
            return float(vv) if vv > 0 else 0.0

        return 0.0

    finally:
        try:
            page.close()
        except Exception:
            pass
        try:
            context.close()
        except Exception:
            pass

@st.cache_data(ttl=60 * 60 * 6)
def fetch_tcgplayer_market_price(tcg_url: str) -> float:
    try:
        return float(_tcgplayer_price_via_playwright(tcg_url))
    except Exception:
        return 0.0


def refresh_watchlist_tcg_prices_batched(watch_ws, wdf: pd.DataFrame):
    if wdf is None or wdf.empty:
        return

    _, _ = ensure_watchlist_col(watch_ws, TCG_LINK_COL)
    _, price_idx = ensure_watchlist_col(watch_ws, TCG_PRICE_COL)
    _, upd_idx = ensure_watchlist_col(watch_ws, TCG_UPDATED_COL)

    wdf2 = read_ws_df(watch_ws)
    if wdf2 is None or wdf2.empty:
        return
    if TCG_LINK_COL not in wdf2.columns:
        return
    if TCG_PRICE_COL not in wdf2.columns:
        wdf2[TCG_PRICE_COL] = ""
    if TCG_UPDATED_COL not in wdf2.columns:
        wdf2[TCG_UPDATED_COL] = ""

    tcg_links = [safe_str(v).strip() for v in wdf2[TCG_LINK_COL].tolist()]
    prices = [safe_str(v).strip() for v in wdf2[TCG_PRICE_COL].tolist()]
    updated = [safe_str(v).strip() for v in wdf2[TCG_UPDATED_COL].tolist()]

    now = datetime.utcnow()
    max_age = timedelta(hours=float(TCG_PRICE_TTL_HOURS))

    for i in range(len(tcg_links)):
        url = tcg_links[i]
        if not url:
            continue

        ts = _parse_utc_iso(updated[i])
        is_stale = (ts is None) or ((now - ts) > max_age)

        cur_price = safe_float(prices[i], 0.0)
        if cur_price > 0 and not is_stale:
            continue

        p = fetch_tcgplayer_market_price(url)
        if p > 0:
            prices[i] = f"{p:.2f}"
        updated[i] = _utc_now_iso()

        time.sleep(0.15)

    start_row = 2
    batch_update_column_values(watch_ws, price_idx, start_row, prices)
    batch_update_column_values(watch_ws, upd_idx, start_row, updated)


# =========================
# Image scraping (PriceCharting image for watchlist) - unchanged
# =========================
def _find_best_image(soup: BeautifulSoup) -> str:
    if soup is None:
        return ""

    for meta in [
        soup.find("meta", property="og:image"),
        soup.find("meta", attrs={"name": "twitter:image"}),
    ]:
        if meta and meta.get("content"):
            url = meta["content"].strip()
            if "storage.googleapis.com/images.pricecharting.com" in url:
                return url
            if "/images/pokemon-sets/" not in url:
                return url

    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if "storage.googleapis.com/images.pricecharting.com" in href:
            return href

    for img in soup.find_all("img", src=True):
        src = (img.get("src") or "").strip()
        if "storage.googleapis.com/images.pricecharting.com" in src:
            return src

    for img in soup.find_all("img", src=True):
        src = (img.get("src") or "").strip()
        if not src:
            continue
        if "/images/pokemon-sets/" in src:
            continue
        return src

    return ""

def _find_pricecharting_main_image(soup: BeautifulSoup) -> str:
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
    return candidates[0] if candidates else ""

@st.cache_data(ttl=60 * 60 * 24)
def fetch_pricecharting_image_url(reference_link: str) -> str:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return ""
    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())
    img = _find_best_image(soup)
    if img:
        return img
    return _find_pricecharting_main_image(soup) or ""


def refresh_watchlist_images_batched(watch_ws, wdf: pd.DataFrame):
    if wdf is None or wdf.empty or "Link" not in wdf.columns:
        return

    _, img_col_idx = ensure_watchlist_image_col(watch_ws)

    wdf2 = wdf.copy()
    if "Image" not in wdf2.columns:
        wdf2["Image"] = ""

    image_values = [safe_str(v).strip() for v in wdf2["Image"].tolist()]

    for i, row in wdf2.reset_index(drop=True).iterrows():
        link = safe_str(row.get("Link", "")).strip()
        if not link or "pricecharting.com" not in link.lower():
            continue

        cur = safe_str(image_values[i]).strip()
        if cur and (cur.startswith("http://") or cur.startswith("https://")):
            continue

        try:
            img = fetch_pricecharting_image_url(link)
        except Exception:
            img = ""

        if img:
            image_values[i] = img

        time.sleep(0.2)

    start_row = 2
    batch_update_column_values(watch_ws, img_col_idx, start_row, image_values)


# =========================
# Watchlist Sold sales scrapers (DO NOT CHANGE)
# =========================
def _classify_grade_from_title(title: str) -> str:
    t = (title or "").upper()
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*(MINT|MT)\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"

@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sold_sales(reference_link: str, limit: int = 80) -> list[dict]:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    target_table = None
    for tbl in soup.find_all("table"):
        ths = [th.get_text(" ", strip=True) for th in tbl.find_all("th")]
        ths_norm = [t.lower() for t in ths if t]
        if any("sale date" in t for t in ths_norm) and any(t.strip() == "price" or "price" in t for t in ths_norm):
            target_table = tbl
            break

    if target_table is None:
        return []

    header_cells = [th.get_text(" ", strip=True) for th in target_table.find_all("th")]
    header_norm = [h.strip().lower() for h in header_cells]

    def _find_col_idx(needle: str):
        for i, h in enumerate(header_norm):
            if needle in h:
                return i
        return None

    sale_date_idx = _find_col_idx("sale date")
    title_idx = _find_col_idx("title")
    price_idx = _find_col_idx("price")

    if sale_date_idx is None:
        sale_date_idx = 0
    if title_idx is None:
        title_idx = 2
    if price_idx is None:
        price_idx = 3

    sales = []
    for tr in target_table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        def cell(i: int) -> str:
            if i < 0 or i >= len(tds):
                return ""
            return tds[i].get_text(" ", strip=True)

        sale_date_txt = cell(sale_date_idx)
        title_txt = cell(title_idx)
        price_txt = cell(price_idx)

        d = _parse_any_date(sale_date_txt)
        if not d:
            continue

        price = _price_from_cell_text(price_txt)
        if price <= 0:
            continue

        title = title_txt.strip()
        bucket = _classify_grade_from_title(title)

        sale_key = f"{d.isoformat()}|{price:.2f}|{bucket}|{title[:90].strip().lower()}"
        sales.append({"sale_date": d, "price": float(price), "title": title, "grade_bucket": bucket, "sale_key": sale_key})

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


def _find_manual_only_sales_table(soup: BeautifulSoup):
    tables = soup.select("div.completed-auctions-manual-only table")
    if not tables:
        return None

    def looks_like_sales_table(tbl):
        ths = [th.get_text(" ", strip=True).lower() for th in tbl.find_all("th")]
        return any("sale date" in t for t in ths) and any("price" == t or "price" in t for t in ths)

    for t in tables:
        if looks_like_sales_table(t):
            return t
    return tables[0]

def fetch_pricecharting_psa10_manual_only(reference_link: str, limit: int = 20) -> list[dict]:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    table = _find_manual_only_sales_table(soup)
    if table is None:
        return []

    trs = table.find_all("tr")
    sales = []

    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        date_td = tr.find("td", class_="date")
        title_td = tr.find("td", class_="title")

        price_td = tr.select_one("td.numeric:not(.listed-price)")
        if price_td is None:
            price_td = tr.select_one("td.numeric")

        sale_date_txt = (date_td.get_text(" ", strip=True) if date_td else "")
        title_txt = (title_td.get_text(" ", strip=True) if title_td else "")
        price_cell_text = (price_td.get_text(" ", strip=True) if price_td else "")

        d = _parse_any_date(sale_date_txt)
        if not d:
            continue

        title = (title_txt or "").strip()
        if _classify_grade_from_title(title) != "psa10":
            continue

        if price_td is None:
            continue

        price = 0.0
        spans = price_td.find_all("span", class_=re.compile(r"\bjs-price\b"))
        for sp in spans:
            p = _price_from_cell_text(sp.get_text(" ", strip=True))
            if p > 0:
                price = p
                break

        if price <= 0:
            price = _price_from_cell_text(price_cell_text)
        if price <= 0:
            continue

        sale_key = f"{d.isoformat()}|{price:.2f}|psa10|{title[:90].strip().lower()}"
        sales.append({"sale_date": d, "price": float(price), "title": title, "grade_bucket": "psa10", "sale_key": sale_key})

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


def _find_graded_sales_table(soup: BeautifulSoup):
    tables = soup.select("div.completed-auctions-graded table")
    if not tables:
        return None

    def looks_like_sales_table(tbl):
        ths = [th.get_text(" ", strip=True).lower() for th in tbl.find_all("th")]
        return any("sale date" in t for t in ths) and any("price" == t or "price" in t for t in ths)

    for t in tables:
        if looks_like_sales_table(t):
            return t
    return tables[0]

def fetch_pricecharting_psa9_graded(reference_link: str, limit: int = 20) -> list[dict]:
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    table = _find_graded_sales_table(soup)
    if table is None:
        return []

    trs = table.find_all("tr")
    sales = []

    for tr in trs:
        tds = tr.find_all("td")
        if not tds:
            continue

        date_td = tr.find("td", class_="date")
        title_td = tr.find("td", class_="title")

        price_td = tr.select_one("td.numeric:not(.listed-price)")
        if price_td is None:
            price_td = tr.select_one("td.numeric")

        sale_date_txt = (date_td.get_text(" ", strip=True) if date_td else "")
        title_txt = (title_td.get_text(" ", strip=True) if title_td else "")
        price_cell_text = (price_td.get_text(" ", strip=True) if price_td else "")

        d = _parse_any_date(sale_date_txt)
        if not d:
            continue

        title = (title_txt or "").strip()
        if _classify_grade_from_title(title) != "psa9":
            continue

        if price_td is None:
            continue

        price = 0.0
        spans = price_td.find_all("span", class_=re.compile(r"\bjs-price\b"))
        for sp in spans:
            p = _price_from_cell_text(sp.get_text(" ", strip=True))
            if p > 0:
                price = p
                break

        if price <= 0:
            price = _price_from_cell_text(price_cell_text)
        if price <= 0:
            continue

        sale_key = f"{d.isoformat()}|{price:.2f}|psa9|{title[:90].strip().lower()}"
        sales.append({"sale_date": d, "price": float(price), "title": title, "grade_bucket": "psa9", "sale_key": sale_key})

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# Build sales history rows (watchlist) - unchanged
# =========================
def build_sales_history_rows_from_watchlist(wdf: pd.DataFrame) -> pd.DataFrame:
    if wdf is None or wdf.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    for h in WATCHLIST_HEADERS_EXPECTED:
        if h not in wdf.columns:
            wdf[h] = ""

    rows_out = []
    now_utc = _utc_now_iso()

    wdf2 = wdf.copy()
    wdf2["Link"] = wdf2["Link"].astype(str).str.strip()
    wdf2 = wdf2[wdf2["Link"] != ""].copy()

    for _, r in wdf2.iterrows():
        link = safe_str(r.get("Link", "")).strip()
        if "pricecharting.com" not in link.lower():
            continue

        if rows_out:
            time.sleep(0.75)

        sales = fetch_pricecharting_sold_sales(link, limit=120)
        if sales:
            ungraded = [s for s in sales if (s.get("grade_bucket") or "").lower() == "ungraded"]
            if EBAY_ONLY:
                ungraded = [s for s in ungraded if "[ebay]" in (s.get("title", "").lower())]
            ungraded = ungraded[:PER_ITEM_N]

            for s in ungraded:
                base_key = safe_str(s.get("sale_key", "")).strip()
                rows_out.append(
                    {
                        "Generation": safe_str(r.get("Generation", "")).strip(),
                        "Set": safe_str(r.get("Set", "")).strip(),
                        "Card Name": safe_str(r.get("Card Name", "")).strip(),
                        "Card No": safe_str(r.get("Card No", "")).strip(),
                        "Link": link,
                        "grade_bucket": "ungraded",
                        "sale_date": s["sale_date"].isoformat() if isinstance(s.get("sale_date"), date) else safe_str(s.get("sale_date", "")).strip(),
                        "price": float(safe_float(s.get("price", 0.0), 0.0)),
                        "title": safe_str(s.get("title", "")).strip(),
                        "sale_key": _unique_sale_key(link, base_key),
                        "updated_utc": now_utc,
                    }
                )

        psa10_sales = fetch_pricecharting_psa10_manual_only(link, limit=50)
        if psa10_sales:
            if EBAY_ONLY:
                psa10_sales = [s for s in psa10_sales if "[ebay]" in (s.get("title", "").lower())]
            psa10_sales = psa10_sales[:PSA10_PER_ITEM]
            for s in psa10_sales:
                base_key = safe_str(s.get("sale_key", "")).strip()
                rows_out.append(
                    {
                        "Generation": safe_str(r.get("Generation", "")).strip(),
                        "Set": safe_str(r.get("Set", "")).strip(),
                        "Card Name": safe_str(r.get("Card Name", "")).strip(),
                        "Card No": safe_str(r.get("Card No", "")).strip(),
                        "Link": link,
                        "grade_bucket": "psa10",
                        "sale_date": s["sale_date"].isoformat() if isinstance(s.get("sale_date"), date) else safe_str(s.get("sale_date", "")).strip(),
                        "price": float(safe_float(s.get("price", 0.0), 0.0)),
                        "title": safe_str(s.get("title", "")).strip(),
                        "sale_key": _unique_sale_key(link, base_key),
                        "updated_utc": now_utc,
                    }
                )

        psa9_sales = fetch_pricecharting_psa9_graded(link, limit=50)
        if psa9_sales:
            if EBAY_ONLY:
                psa9_sales = [s for s in psa9_sales if "[ebay]" in (s.get("title", "").lower())]
            psa9_sales = psa9_sales[:PSA9_PER_ITEM]
            for s in psa9_sales:
                base_key = safe_str(s.get("sale_key", "")).strip()
                rows_out.append(
                    {
                        "Generation": safe_str(r.get("Generation", "")).strip(),
                        "Set": safe_str(r.get("Set", "")).strip(),
                        "Card Name": safe_str(r.get("Card Name", "")).strip(),
                        "Card No": safe_str(r.get("Card No", "")).strip(),
                        "Link": link,
                        "grade_bucket": "psa9",
                        "sale_date": s["sale_date"].isoformat() if isinstance(s.get("sale_date"), date) else safe_str(s.get("sale_date", "")).strip(),
                        "price": float(safe_float(s.get("price", 0.0), 0.0)),
                        "title": safe_str(s.get("title", "")).strip(),
                        "sale_key": _unique_sale_key(link, base_key),
                        "updated_utc": now_utc,
                    }
                )

    df_out = pd.DataFrame(rows_out)
    if df_out.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    if "sale_key" in df_out.columns:
        df_out = df_out.drop_duplicates(subset=["sale_key"], keep="first")

    df_out["price"] = df_out["price"].apply(lambda v: safe_float(v, 0.0))
    df_out["sale_date_dt"] = pd.to_datetime(df_out["sale_date"], errors="coerce")
    df_out = df_out.sort_values(
        ["Card Name", "Card No", "grade_bucket", "sale_date_dt"],
        ascending=[True, True, True, False]
    ).drop(columns=["sale_date_dt"])
    return df_out[SALES_HISTORY_HEADERS].copy()


def write_ws_df(ws, df: pd.DataFrame, headers: list[str]):
    df2 = df.copy()
    for h in headers:
        if h not in df2.columns:
            df2[h] = ""
    df2 = df2[headers].copy()

    values = [headers] + df2.astype(str).fillna("").values.tolist()
    last_col = a1_col_letter(len(headers))
    rng = f"A1:{last_col}{len(values)}"
    _gs_write_retry(ws.update, range_name=rng, values=values, value_input_option="RAW")


# =========================
# Gemrates loader
# =========================
def load_gemrates_lookup(gdf: pd.DataFrame) -> dict[tuple[str, str], dict]:
    if gdf is None or gdf.empty:
        return {}

    col_set = next((c for c in ["Set Name", "Set", "SetName"] if c in gdf.columns), None)
    col_card = next((c for c in ["Card #", "Card#", "Card No", "CardNo"] if c in gdf.columns), None)
    col_total = next((c for c in ["Total", "TOTAL", "Total Graded"] if c in gdf.columns), None)
    col_rate = next((c for c in ["Gem rate - All time", "Gem Rate - All time", "Gem rate", "Gem Rate"] if c in gdf.columns), None)

    if not col_set or not col_card:
        return {}

    out = {}
    for _, row in gdf.iterrows():
        set_norm = _normalize_set(safe_str(row.get(col_set, "")))
        card_norm = _normalize_cardno(safe_str(row.get(col_card, "")))
        if not set_norm or not card_norm:
            continue

        total = safe_int(row.get(col_total, 0), 0) if col_total else 0
        rate_raw = safe_str(row.get(col_rate, "")).strip() if col_rate else ""
        rate = 0.0
        if rate_raw.endswith("%"):
            rate = safe_float(rate_raw.replace("%", ""), 0.0) / 100.0
        else:
            rate = safe_float(rate_raw, 0.0)
            if rate > 1.0:
                rate = rate / 100.0

        out[(set_norm, card_norm)] = {"gem_rate": float(rate), "total": int(total)}
    return out


# =========================
# Scoring (watchlist)
# =========================
def add_prospect_scoring(summary: pd.DataFrame) -> pd.DataFrame:
    if summary is None or summary.empty:
        return summary

    df = summary.copy()

    for col in ["UNGRADED Avg", "PSA9 Avg", "PSA10 Avg"]:
        if col not in df.columns:
            df[col] = 0.0

    df["UNGRADED Avg"] = df["UNGRADED Avg"].apply(lambda v: safe_float(v, 0.0))
    df["PSA9 Avg"] = df["PSA9 Avg"].apply(lambda v: safe_float(v, 0.0))
    df["PSA10 Avg"] = df["PSA10 Avg"].apply(lambda v: safe_float(v, 0.0))

    df["Gem rate (all time)"] = df.get("Gem rate (all time)", 0.0).apply(lambda v: safe_float(v, 0.0))
    df["Total graded"] = df.get("Total graded", 0).apply(lambda v: safe_int(v, 0))

    C = float(GRADING_ALL_IN_COST)

    df["Gem conf"] = df["Total graded"].apply(
        lambda n: round(1.0 - exp(-max(0, safe_int(n, 0)) / max(1.0, CONF_K)), 4)
    )
    df["P10 adj"] = (df["Gem rate (all time)"] * df["Gem conf"]).apply(
        lambda v: round(clamp(safe_float(v, 0.0), 0.0, 1.0), 4)
    )

    df["Net 9"] = (df["PSA9 Avg"] - (df["UNGRADED Avg"] + C)).apply(lambda v: round(safe_float(v, 0.0), 2))
    df["Net 10"] = (df["PSA10 Avg"] - (df["UNGRADED Avg"] + C)).apply(lambda v: round(safe_float(v, 0.0), 2))

    def s9(net9: float) -> float:
        return clamp((net9 + 15.0) / 20.0, 0.0, 1.0)

    def s10(net10: float) -> float:
        return clamp(net10 / 50.0, 0.0, 1.0)

    def sg(p10_adj: float) -> float:
        return clamp((p10_adj - 0.10) / 0.40, 0.0, 1.0)

    score = 100.0 * (
        0.45 * df["Net 10"].apply(lambda v: s10(safe_float(v, 0.0))) +
        0.35 * df["P10 adj"].apply(lambda v: sg(safe_float(v, 0.0))) +
        0.20 * df["Net 9"].apply(lambda v: s9(safe_float(v, 0.0)))
    )
    df["Prospect Score"] = score.apply(lambda v: round(clamp(safe_float(v, 0.0), 0.0, 100.0), 1))

    df["EV (vs ungraded)"] = (
        df["P10 adj"] * df["PSA10 Avg"] + (1.0 - df["P10 adj"]) * df["PSA9 Avg"] - (df["UNGRADED Avg"] + C)
    ).apply(lambda v: round(safe_float(v, 0.0), 2))

    return df


def build_summary_from_sales_history(sdf: pd.DataFrame, wdf: pd.DataFrame, gem_lookup: dict) -> pd.DataFrame:
    if sdf is None or sdf.empty:
        return pd.DataFrame()

    needed = ["Generation", "Set", "Card Name", "Card No", "Link", "grade_bucket", "price"]
    for c in needed:
        if c not in sdf.columns:
            sdf[c] = ""

    df = sdf.copy()
    df["price"] = df["price"].apply(lambda v: safe_float(v, 0.0))
    df["grade_bucket"] = df["grade_bucket"].astype(str).str.strip().str.lower()

    keys = ["Generation", "Set", "Card Name", "Card No", "Link"]

    stats = (
        df.groupby(keys + ["grade_bucket"], dropna=False)["price"]
          .agg(["mean", "min", "max"])
          .reset_index()
    )

    def _bucket_cols(bucket: str):
        return {"mean": f"{bucket.upper()} Avg", "min": f"{bucket.upper()} Min", "max": f"{bucket.upper()} Max"}

    out = None
    for bucket in ["ungraded", "psa9", "psa10"]:
        sub = stats[stats["grade_bucket"] == bucket].copy()
        sub = sub[keys + ["mean", "min", "max"]].copy() if not sub.empty else pd.DataFrame(columns=keys + ["mean", "min", "max"])
        sub = sub.rename(columns={"mean": _bucket_cols(bucket)["mean"], "min": _bucket_cols(bucket)["min"], "max": _bucket_cols(bucket)["max"]})
        out = sub if out is None else out.merge(sub, on=keys, how="outer")

    if out is None or out.empty:
        return pd.DataFrame()

    img_map = {}
    tcg_price_map = {}

    if wdf is not None and not wdf.empty and "Link" in wdf.columns:
        for _, r in wdf.iterrows():
            lk = safe_str(r.get("Link", "")).strip()
            if not lk:
                continue
            if "Image" in wdf.columns:
                img_map[lk] = safe_str(r.get("Image", "")).strip()
            if TCG_PRICE_COL in wdf.columns:
                tcg_price_map[lk] = safe_float(r.get(TCG_PRICE_COL, 0.0), 0.0)

    out["Image"] = out["Link"].map(lambda x: img_map.get(safe_str(x).strip(), ""))

    for c in out.columns:
        if c.endswith(" Avg") or c.endswith(" Min") or c.endswith(" Max"):
            out[c] = out[c].apply(lambda v: round(safe_float(v, 0.0), 2))

    out["Gem rate (all time)"] = 0.0
    out["Total graded"] = 0

    for i, row in out.iterrows():
        set_norm = _normalize_set(row.get("Set", ""))
        card_norm = _normalize_cardno(row.get("Card No", ""))
        rec = gem_lookup.get((set_norm, card_norm))
        if rec:
            out.at[i, "Gem rate (all time)"] = round(float(rec.get("gem_rate", 0.0)), 4)
            out.at[i, "Total graded"] = int(rec.get("total", 0))

    out["TCGplayer Price"] = out["Link"].map(lambda x: round(safe_float(tcg_price_map.get(safe_str(x).strip(), 0.0), 0.0), 2))
    out["TCG - Ungraded"] = (
        out["TCGplayer Price"].apply(lambda v: safe_float(v, 0.0)) -
        out["UNGRADED Avg"].apply(lambda v: safe_float(v, 0.0))
    ).apply(lambda v: round(v, 2))

    out = add_prospect_scoring(out)
    out = out.sort_values(["Set", "Card Name", "Card No"], ascending=[True, True, True]).reset_index(drop=True)
    return out


# =========================================================
# PriceCharting PSA9 / PSA10 (for submission screens)
# =========================================================
@st.cache_data(ttl=60 * 60 * 12)
def fetch_pricecharting_prices(reference_link: str) -> dict:
    out = {"raw": 0.0, "psa9": 0.0, "psa10": 0.0}
    if not reference_link or "pricecharting.com" not in reference_link.lower():
        return out

    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(reference_link.strip(), headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        nodes = soup.select(".price.js-price")
        prices = []
        for n in nodes:
            t = n.get_text(" ", strip=True)
            m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", t)
            prices.append(safe_float(m.group(1), 0.0) if m else 0.0)

        out["raw"] = float(prices[0] if len(prices) >= 1 else 0.0)
        out["psa9"] = float(prices[3] if len(prices) >= 4 else 0.0)
        out["psa10"] = float(prices[5] if len(prices) >= 6 else 0.0)
        return out
    except Exception:
        return out


# =========================================================
# LOADERS (Grading submission screens)
# =========================================================
@st.cache_data(ttl=30)
def load_inventory_df():
    sh = get_sheet()
    ws = get_ws(sh, INVENTORY_WS_NAME)
    records = ws.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return df

    if "inventory_status" not in df.columns:
        df["inventory_status"] = STATUS_ACTIVE
    df["inventory_status"] = df["inventory_status"].astype(str).replace("", STATUS_ACTIVE)

    for c in ["inventory_id", "reference_link", "card_name", "set_name", "year", "total_price", "purchase_date", "purchased_from", "product_type",
              "card_number", "variant", "card_subtype", "grading_company", "grade", "market_price", "market_value"]:
        if c not in df.columns:
            df[c] = ""

    df["inventory_id"] = df["inventory_id"].astype(str)

    df["year"] = (
        df["year"]
        .astype(str)
        .replace({"nan": "", "None": "", "<NA>": ""})
        .str.strip()
    )

    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce").fillna(0.0)
    df["product_type"] = df["product_type"].astype(str)

    return df


@st.cache_data(ttl=30)
def load_grading_df():
    sh = get_sheet()
    ws = get_ws(sh, GRADING_WS_NAME)

    _ = ensure_headers_canon(ws, GRADING_CANON_COLS)

    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame(columns=GRADING_CANON_COLS)

    header_row = values[0]
    if not header_row:
        header_row = GRADING_CANON_COLS

    data_rows = []
    for r in values[1:]:
        if len(r) < len(header_row):
            r = r + [""] * (len(header_row) - len(r))
        elif len(r) > len(header_row):
            r = r[: len(header_row)]
        data_rows.append(r)

    df = pd.DataFrame(data_rows, columns=header_row)
    if df.empty:
        return pd.DataFrame(columns=header_row)

    for c in GRADING_CANON_COLS:
        if c not in df.columns:
            df[c] = ""

    def _cols_named(base: str):
        cols = []
        for c in df.columns:
            if re.sub(r"__dup\d+$", "", str(c)) == base:
                cols.append(c)
        return cols

    def _coalesce_into(base: str, fallbacks: list[str]):
        candidates = _cols_named(base)
        for fb in fallbacks:
            candidates += _cols_named(fb)

        seen = set()
        ordered = []
        for c in candidates:
            if c not in seen:
                ordered.append(c)
                seen.add(c)

        if not ordered:
            return

        s = df[ordered[0]].astype(str)
        for c in ordered[1:]:
            t = df[c].astype(str)
            s = s.where(s.str.strip() != "", t)

        df[base] = s

    _coalesce_into("grading_fee_initial", ["grading_fee_per_card"])
    _coalesce_into("additional_costs", ["extra_costs"])
    _coalesce_into("received_grade", ["returned_grade"])

    for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
        df[c] = df[c].apply(lambda v: safe_float(v, 0.0))

    df["grading_row_id"] = df["grading_row_id"].astype(str)
    df["submission_id"] = df["submission_id"].astype(str)
    df["status"] = df["status"].astype(str).replace("", "SUBMITTED")

    return df


def refresh_all_grading():
    load_inventory_df.clear()
    load_grading_df.clear()
    st.rerun()


# =========================================================
# WRITES (Grading submission screens)
# =========================================================
def append_grading_rows(rows: list[dict]):
    if not rows:
        return

    sh = get_sheet()
    ws = get_ws(sh, GRADING_WS_NAME)
    headers = ensure_headers_canon(ws, GRADING_CANON_COLS)

    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    for row in rows:
        values = []
        for h in headers:
            base = re.sub(r"__dup\d+$", "", h)
            v = row.get(base, "")
            if base in NUM_COLS:
                v = _num_str(v)
            values.append(v)

        ws.append_row(values, value_input_option="RAW")


def update_grading_rows_quota_safe(df_rows: pd.DataFrame):
    if df_rows is None or df_rows.empty:
        return

    sh = get_sheet()
    ws = get_ws(sh, GRADING_WS_NAME)
    _ = ensure_headers_canon(ws, GRADING_CANON_COLS)

    values = ws.get_all_values()
    if not values:
        return
    sheet_header = values[0] if values else []
    if not sheet_header:
        return

    id_col_idx = None
    for j, h in enumerate(sheet_header):
        if re.sub(r"__dup\d+$", "", str(h)) == "grading_row_id":
            id_col_idx = j
            break
    if id_col_idx is None:
        raise ValueError("grading_row_id must exist in grading sheet header row.")

    id_to_rownum: dict[str, int] = {}
    for rownum, row in enumerate(values[1:], start=2):
        v = ""
        if len(row) > id_col_idx:
            v = str(row[id_col_idx] or "").strip()
        if v:
            id_to_rownum[v] = rownum

    headers = sheet_header
    last_col = a1_col_letter(len(headers))

    NUM_COLS = {"purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"}

    def _num_str(v):
        if v is None or (isinstance(v, str) and v.strip() == ""):
            return ""
        try:
            return str(float(str(v).replace("$", "").replace(",", "").strip()))
        except Exception:
            return ""

    for i, (_, r) in enumerate(df_rows.iterrows(), start=1):
        rid = str(r.get("grading_row_id", "")).strip()
        rownum = id_to_rownum.get(rid)
        if not rownum:
            continue

        out_row = []
        for h in headers:
            base = re.sub(r"__dup\d+$", "", str(h))
            v = r.get(base, "")
            if pd.isna(v):
                v = ""
            if base in NUM_COLS:
                v = _num_str(v)
            out_row.append(v)

        ws.update(f"A{rownum}:{last_col}{rownum}", [out_row], value_input_option="RAW")

        if i % 5 == 0:
            time.sleep(0.6)


def update_inventory_status(inventory_id: str, new_status: str):
    sh = get_sheet()
    inv_ws = get_ws(sh, INVENTORY_WS_NAME)

    _ = ensure_headers_canon(inv_ws, ["inventory_id", "inventory_status"])
    headers = inv_ws.row_values(1)
    if not headers:
        return

    def _header_for(base_name: str):
        for h in headers:
            if re.sub(r"__dup\d+$", "", h) == base_name:
                return h
        return None

    id_header = _header_for("inventory_id")
    st_header = _header_for("inventory_status")
    if not id_header or not st_header:
        return

    id_col = headers.index(id_header) + 1
    ids = inv_ws.col_values(id_col)

    rownum = None
    for i, v in enumerate(ids[1:], start=2):
        if str(v).strip() == str(inventory_id).strip():
            rownum = i
            break
    if not rownum:
        return

    c = headers.index(st_header) + 1
    inv_ws.update(f"{a1_col_letter(c)}{rownum}", [[new_status]], value_input_option="USER_ENTERED")


def mark_inventory_as_graded(inventory_id: str, grading_company: str, grade: str):
    sh = get_sheet()
    inv_ws = get_ws(sh, INVENTORY_WS_NAME)

    _ = ensure_headers_canon(inv_ws, ["inventory_id", "product_type", "grading_company", "grade", "reference_link", "market_price", "market_value"])
    headers = inv_ws.row_values(1)
    if not headers:
        return

    def _header_for(base_name: str):
        for h in headers:
            if re.sub(r"__dup\d+$", "", h) == base_name:
                return h
        return None

    id_header = _header_for("inventory_id")
    if not id_header:
        return

    id_col = headers.index(id_header) + 1
    ids = inv_ws.col_values(id_col)

    rownum = None
    for i, v in enumerate(ids[1:], start=2):
        if str(v).strip() == str(inventory_id).strip():
            rownum = i
            break
    if not rownum:
        return

    def _set(base_name: str, value):
        target = _header_for(base_name)
        if not target:
            return
        c = headers.index(target) + 1
        inv_ws.update(f"{a1_col_letter(c)}{rownum}", [[value]], value_input_option="USER_ENTERED")

    def _get(base_name: str) -> str:
        target = _header_for(base_name)
        if not target:
            return ""
        c = headers.index(target) + 1
        return str(inv_ws.cell(rownum, c).value or "").strip()

    _set("product_type", "Graded Card")
    _set("grading_company", grading_company)
    _set("grade", grade)

    link = _get("reference_link")
    if link and "pricecharting.com" in link.lower():
        prices = fetch_pricecharting_prices(link)
        g = safe_str(grade).strip().upper()

        mv = float(prices.get("raw", 0.0) or 0.0)
        if "10" in g:
            mv = float(prices.get("psa10", 0.0) or 0.0)
        elif "9" in g:
            mv = float(prices.get("psa9", 0.0) or 0.0)

        _set("market_price", mv)
        _set("market_value", mv)


# =========================
# Build watchlist + grading data sources
# =========================
sheet = get_sheet()
watch_ws = get_ws(sheet, WATCHLIST_WS_NAME)
sales_ws = get_ws(sheet, SALES_HISTORY_WS_NAME)
gem_ws = get_ws(sheet, GEMRATES_WS_NAME)

ensure_headers_simple(sales_ws, SALES_HISTORY_HEADERS)

wdf_watch = read_ws_df(watch_ws)
gdf = read_ws_df(gem_ws)
gem_lookup = load_gemrates_lookup(gdf)

# grading submission data
inv_df = load_inventory_df()
grading_df = load_grading_df()

eligible_inv = inv_df.copy()
if not eligible_inv.empty:
    eligible_inv = eligible_inv[eligible_inv["inventory_status"].isin(list(ELIGIBLE_INV_STATUSES))].copy()
    if "product_type" in eligible_inv.columns:
        eligible_inv = eligible_inv[~eligible_inv["product_type"].astype(str).str.lower().str.contains("sealed", na=False)]


# =========================================================
# UI TABS (combined)
# =========================================================
tab_watchlist, tab_analysis, tab_submit, tab_update, tab_summary = st.tabs(
    ["Watchlist / Market", "Analysis", "Create Submission", "Update Returns", "Submission Summary"]
)

# ---------------------------------------------------------
# TAB 1: Watchlist / Market
# ---------------------------------------------------------
with tab_watchlist:
    top = st.container()
    with top:
        c1, c2 = st.columns([1.2, 3])

        with c1:
            refresh_mode = st.radio(
                "Refresh mode",
                ["TCGplayer only", "PriceCharting only", "Both"],
                index=2,
            )

            if st.button("Run Refresh", type="primary", use_container_width=True):
                try:
                    with st.spinner("Refreshing..."):
                        wdf_watch = read_ws_df(watch_ws)

                        if refresh_mode in ("TCGplayer only", "Both"):
                            refresh_watchlist_tcg_prices_batched(watch_ws, wdf_watch)
                            wdf_watch = read_ws_df(watch_ws)

                        if refresh_mode in ("PriceCharting only", "Both"):
                            refresh_watchlist_images_batched(watch_ws, wdf_watch)
                            wdf_watch = read_ws_df(watch_ws)

                            out_df = build_sales_history_rows_from_watchlist(wdf_watch)
                            write_ws_df(sales_ws, out_df, SALES_HISTORY_HEADERS)

                    st.success("Refresh complete.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Refresh failed: {e}")
                    st.exception(e)

        with c2:
            st.caption(
                f"Tip: Use **TCGplayer only** for fast price updates. "
                f"TCG prices are stored in the watchlist columns `{TCG_PRICE_COL}` and `{TCG_UPDATED_COL}` "
                f"(TTL {TCG_PRICE_TTL_HOURS:g}h) so filtering does not re-scrape."
            )

    st.divider()

    sdf = read_ws_df(sales_ws)
    wdf_watch = read_ws_df(watch_ws)
    summary_df = build_summary_from_sales_history(sdf, wdf_watch, gem_lookup)

    if summary_df is None or summary_df.empty:
        st.info("No sales history yet. Run **PriceCharting only** (or **Both**) at least once.")
    else:
        with st.expander("Filters", expanded=True):
            f1, f2, f3, f4 = st.columns([1.2, 1.2, 1.2, 1.2])

            sets = sorted([s for s in summary_df["Set"].dropna().astype(str).unique() if s.strip() != ""])
            gens = sorted([g for g in summary_df["Generation"].dropna().astype(str).unique() if g.strip() != ""])

            with f1:
                sel_set = st.multiselect("Set", options=sets, default=sets)
            with f2:
                sel_gen = st.multiselect("Generation", options=gens, default=gens)

            score_min = float(summary_df["Prospect Score"].min()) if "Prospect Score" in summary_df.columns else 0.0
            score_max = float(summary_df["Prospect Score"].max()) if "Prospect Score" in summary_df.columns else 100.0
            with f3:
                score_rng = st.slider("Prospect Score", 0.0, 100.0, (max(0.0, score_min), min(100.0, score_max)), 0.5)

            with f4:
                total_min = st.number_input("Min Total graded", min_value=0, value=0, step=10)

            g1, g2, g3, g4 = st.columns([1.2, 1.2, 1.2, 1.2])

            def _rng(col: str):
                if col not in summary_df.columns:
                    return (0.0, 0.0)
                vals = summary_df[col].apply(lambda v: safe_float(v, 0.0))
                return (float(vals.min()), float(vals.max()))

            _, p9_max = _rng("PSA9 Avg")
            _, p10_max = _rng("PSA10 Avg")
            _, u_max = _rng("UNGRADED Avg")
            ev_min, ev_max = _rng("EV (vs ungraded)")

            with g1:
                psa9_rng = st.slider("PSA9 Avg ($)", 0.0, max(1.0, p9_max), (0.0, p9_max), 0.5)
            with g2:
                psa10_rng = st.slider("PSA10 Avg ($)", 0.0, max(1.0, p10_max), (0.0, p10_max), 0.5)
            with g3:
                ungraded_rng = st.slider("UNGRADED Avg ($)", 0.0, max(1.0, u_max), (0.0, u_max), 0.5)
            with g4:
                ev_rng = st.slider("EV (vs ungraded)", min(-200.0, ev_min), max(200.0, ev_max),
                                   (min(-200.0, ev_min), max(200.0, ev_max)), 0.5)

        fdf = summary_df.copy()

        if sel_set:
            fdf = fdf[fdf["Set"].astype(str).isin(sel_set)]
        if sel_gen:
            fdf = fdf[fdf["Generation"].astype(str).isin(sel_gen)]

        if "Prospect Score" in fdf.columns:
            fdf = fdf[(fdf["Prospect Score"].apply(lambda v: safe_float(v, 0.0)) >= score_rng[0]) &
                      (fdf["Prospect Score"].apply(lambda v: safe_float(v, 0.0)) <= score_rng[1])]

        if "Total graded" in fdf.columns:
            fdf = fdf[fdf["Total graded"].apply(lambda v: safe_int(v, 0)) >= int(total_min)]

        fdf = fdf[(fdf["PSA9 Avg"].apply(lambda v: safe_float(v, 0.0)) >= psa9_rng[0]) &
                  (fdf["PSA9 Avg"].apply(lambda v: safe_float(v, 0.0)) <= psa9_rng[1])]

        fdf = fdf[(fdf["PSA10 Avg"].apply(lambda v: safe_float(v, 0.0)) >= psa10_rng[0]) &
                  (fdf["PSA10 Avg"].apply(lambda v: safe_float(v, 0.0)) <= psa10_rng[1])]

        fdf = fdf[(fdf["UNGRADED Avg"].apply(lambda v: safe_float(v, 0.0)) >= ungraded_rng[0]) &
                  (fdf["UNGRADED Avg"].apply(lambda v: safe_float(v, 0.0)) <= ungraded_rng[1])]

        if "EV (vs ungraded)" in fdf.columns:
            fdf = fdf[(fdf["EV (vs ungraded)"].apply(lambda v: safe_float(v, 0.0)) >= ev_rng[0]) &
                      (fdf["EV (vs ungraded)"].apply(lambda v: safe_float(v, 0.0)) <= ev_rng[1])]

        preferred_cols = [
            "Image", "Link", "Generation", "Set", "Card Name", "Card No",
            "Prospect Score", "EV (vs ungraded)", "Gem rate (all time)", "Total graded", "Gem conf", "P10 adj",
            "UNGRADED Avg", "TCGplayer Price", "TCG - Ungraded", "UNGRADED Min", "UNGRADED Max",
            "PSA9 Avg", "PSA9 Min", "PSA9 Max",
            "PSA10 Avg", "PSA10 Min", "PSA10 Max",
            "Net 9", "Net 10",
        ]
        final_cols = [c for c in preferred_cols if c in fdf.columns] + [c for c in fdf.columns if c not in preferred_cols]
        fdf = fdf[final_cols].copy()

        st.markdown("### Watchlist Summary (filterable)")
        st.dataframe(
            fdf,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Image": st.column_config.ImageColumn("Image", width="small"),
                "Link": st.column_config.LinkColumn("Link", width="medium"),
            },
        )


# ---------------------------------------------------------
# TAB 2: Analysis (single-card PSA9/PSA10 calc)
# ---------------------------------------------------------
with tab_analysis:
    st.subheader("Analysis (pull PSA 9/10 from PriceCharting)")

    if eligible_inv.empty:
        st.info("No eligible ACTIVE inventory items to analyze.")
    else:
        records = eligible_inv.to_dict("records")

        def label(r):
            return f"{r.get('inventory_id','')} — {r.get('card_name','')} ({r.get('set_name','')} {r.get('year','')}) — Cost ${safe_float(r.get('total_price'),0):,.2f}"

        idx = st.selectbox("Select an item", options=list(range(len(records))), format_func=lambda i: label(records[i]))
        r = records[idx]

        link = safe_str(r.get("reference_link", "")).strip()
        purchase_total = safe_float(r.get("total_price", 0.0), 0.0)

        st.write("**Purchased from:**", safe_str(r.get("purchased_from", "")))
        st.write("**Purchase date:**", safe_str(r.get("purchase_date", "")))
        st.write("**Reference link:**", link if link else "(none)")

        fee = st.number_input(
            "Assumed grading fee (per card)",
            min_value=0.0,
            value=DEFAULT_GRADING_FEE_PER_CARD,
            step=1.0,
            format="%.2f",
        )

        psa9 = 0.0
        psa10 = 0.0
        if link and "pricecharting.com" in link.lower():
            prices = fetch_pricecharting_prices(link)
            psa9 = prices["psa9"]
            psa10 = prices["psa10"]

        profit9 = psa9 - (purchase_total + fee)
        profit10 = psa10 - (purchase_total + fee)

        c1, c2, c3 = st.columns(3)
        c1.metric("PSA 9", f"${psa9:,.2f}")
        c2.metric("PSA 10", f"${psa10:,.2f}")
        c3.metric("Purchase Total", f"${purchase_total:,.2f}")

        d1, d2 = st.columns(2)
        d1.metric("Profit if PSA 9", f"${profit9:,.2f}")
        d2.metric("Profit if PSA 10", f"${profit10:,.2f}")


# ---------------------------------------------------------
# TAB 3: Create Submission
# ---------------------------------------------------------
with tab_submit:
    st.subheader("Create Submission")

    if eligible_inv.empty:
        st.info("No eligible ACTIVE inventory items to submit.")
    else:
        inv_records = eligible_inv.to_dict("records")

        def short(r):
            return f"{r.get('inventory_id','')} — {r.get('card_name','')} ({r.get('set_name','')} {r.get('year','')}) — ${safe_float(r.get('total_price'),0):,.2f}"

        choices = list(range(len(inv_records)))
        selected = st.multiselect("Select inventory items", options=choices, format_func=lambda i: short(inv_records[i]))

        c1, c2, c3, c4 = st.columns([1.1, 1.1, 1.1, 1.2])
        with c1:
            submission_date = st.date_input("Submission date", value=date.today())
        with c2:
            company = st.selectbox("Grading company", ["PSA", "CGC", "Beckett"])
        with c3:
            fee_initial = st.number_input(
                "Initial grading fee (per card)",
                min_value=0.0,
                value=DEFAULT_GRADING_FEE_PER_CARD,
                step=1.0,
                format="%.2f",
            )
        with c4:
            business_days = st.number_input(
                "Estimated return (business days)",
                min_value=1,
                value=DEFAULT_RETURN_BUSINESS_DAYS,
                step=1,
            )

        notes = st.text_area("Notes (optional)", height=80)

        if st.button("Create submission", type="primary", use_container_width=True, disabled=(len(selected) == 0)):
            sub_id = str(int(datetime.utcnow().timestamp()))
            est_return = add_business_days(submission_date, int(business_days))

            rows = []
            for i in selected:
                r = inv_records[i]
                inv_id = safe_str(r.get("inventory_id", "")).strip()
                link = safe_str(r.get("reference_link", "")).strip()

                psa9 = 0.0
                psa10 = 0.0
                if link and "pricecharting.com" in link.lower():
                    prices = fetch_pricecharting_prices(link)
                    psa9 = prices["psa9"]
                    psa10 = prices["psa10"]

                row_id = str(uuid.uuid4())[:10]
                now = datetime.utcnow().isoformat()

                rows.append({
                    "grading_row_id": row_id,
                    "submission_id": sub_id,
                    "submission_date": str(submission_date),
                    "estimated_return_date": str(est_return),

                    "inventory_id": inv_id,
                    "reference_link": link,

                    "card_name": safe_str(r.get("card_name", "")),
                    "card_number": safe_str(r.get("card_number", "")),
                    "variant": safe_str(r.get("variant", "")),
                    "card_subtype": safe_str(r.get("card_subtype", "")),

                    "purchased_from": safe_str(r.get("purchased_from", "")),
                    "purchase_date": safe_str(r.get("purchase_date", "")),
                    "purchase_total": float(safe_float(r.get("total_price", 0.0), 0.0)),

                    "grading_company": company,
                    "grading_fee_initial": float(fee_initial),
                    "additional_costs": 0.0,

                    "psa9_price": float(psa9),
                    "psa10_price": float(psa10),

                    "status": "SUBMITTED",
                    "returned_date": "",
                    "received_grade": "",

                    "notes": notes,
                    "created_at": now,
                    "updated_at": now,
                })

                update_inventory_status(inv_id, STATUS_GRADING)

            append_grading_rows(rows)
            st.success(f"Created submission {sub_id} with {len(rows)} card(s).")
            refresh_all_grading()


# ---------------------------------------------------------
# TAB 4: Update Returns
# ---------------------------------------------------------
with tab_update:
    st.subheader("Update Returns / Add Costs")

    if grading_df.empty:
        st.info("No grading records yet.")
    else:
        df = grading_df.copy()
        df["submission_id"] = df["submission_id"].astype(str)

        open_df = df[df["status"].astype(str).str.upper().isin(["SUBMITTED", "IN_TRANSIT"])].copy()
        open_df = open_df[~open_df["submission_id"].apply(is_blank)].copy()

        if open_df.empty:
            st.info("No open submissions.")
        else:
            meta = (
                open_df.groupby("submission_id", dropna=False)
                .agg(submission_date=("submission_date", "first"), cards=("grading_row_id", "count"))
                .reset_index()
            )
            meta["label"] = meta.apply(lambda r: f"{r['submission_id']} — {r['submission_date']} — {int(r['cards'])} card(s)", axis=1)

            sub_ids = meta["submission_id"].astype(str).tolist()
            label_map = dict(zip(sub_ids, meta["label"].tolist()))

            pick = st.selectbox("Select open submission", options=sub_ids, format_func=lambda sid: label_map.get(sid, sid))
            sub_rows = open_df[open_df["submission_id"].astype(str) == str(pick)].copy()

            st.caption("Edit the table, then click Save updates. Rows with a returned date or grade will be marked RETURNED automatically.")
            edit_cols = [
                "grading_row_id",
                "inventory_id",
                "card_name",
                "purchase_total",
                "grading_company",
                "grading_fee_initial",
                "additional_costs",
                "psa9_price",
                "psa10_price",
                "returned_date",
                "received_grade",
                "status",
            ]
            show = sub_rows[edit_cols].copy()

            for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa9_price", "psa10_price"]:
                if c in show.columns:
                    show[c] = show[c].apply(lambda v: safe_float(v, 0.0))

            edited = st.data_editor(show, use_container_width=True, hide_index=True, num_rows="fixed")
            save = st.button("Save updates", type="primary", use_container_width=True)

            if save:
                updated = sub_rows.copy()
                ed_map = {str(r["grading_row_id"]): r for _, r in edited.iterrows()}

                for idx, r in updated.iterrows():
                    rid = str(r["grading_row_id"])
                    if rid not in ed_map:
                        continue
                    e = ed_map[rid]

                    updated.at[idx, "grading_fee_initial"] = safe_float(e.get("grading_fee_initial", 0.0), 0.0)
                    updated.at[idx, "additional_costs"] = safe_float(e.get("additional_costs", 0.0), 0.0)
                    updated.at[idx, "returned_date"] = safe_str(e.get("returned_date", "")).strip()
                    updated.at[idx, "received_grade"] = safe_str(e.get("received_grade", "")).strip()

                    if (not is_blank(updated.at[idx, "returned_date"])) or (not is_blank(updated.at[idx, "received_grade"])):
                        updated.at[idx, "status"] = "RETURNED"

                    updated.at[idx, "updated_at"] = datetime.utcnow().isoformat()

                    if str(updated.at[idx, "status"]).upper() == "RETURNED":
                        inv_id = safe_str(updated.at[idx, "inventory_id"])
                        update_inventory_status(inv_id, STATUS_ACTIVE)
                        mark_inventory_as_graded(
                            inv_id,
                            safe_str(updated.at[idx, "grading_company"]),
                            safe_str(updated.at[idx, "received_grade"]),
                        )

                update_grading_rows_quota_safe(updated)
                st.success("Saved.")
                refresh_all_grading()


# ---------------------------------------------------------
# TAB 5: Submission Summary
# ---------------------------------------------------------
with tab_summary:
    st.subheader("Summary (Submission totals)")

    if grading_df.empty:
        st.info("No grading data.")
    else:
        df = grading_df.copy()
        df["submission_id"] = df["submission_id"].astype(str)

        df["purchase_total"] = df["purchase_total"].apply(lambda v: safe_float(v, 0.0))
        df["grading_fee_initial"] = df["grading_fee_initial"].apply(lambda v: safe_float(v, 0.0))
        df["additional_costs"] = df["additional_costs"].apply(lambda v: safe_float(v, 0.0))
        df["psa9_price"] = df["psa9_price"].apply(lambda v: safe_float(v, 0.0))
        df["psa10_price"] = df["psa10_price"].apply(lambda v: safe_float(v, 0.0))

        need_mask = (
            ((df["psa9_price"] == 0.0) | (df["psa10_price"] == 0.0))
            & df["reference_link"].astype(str).str.lower().str.contains("pricecharting.com", na=False)
        )
        if need_mask.any():
            for idx, r in df[need_mask].iterrows():
                prices = fetch_pricecharting_prices(safe_str(r["reference_link"]))
                if df.at[idx, "psa9_price"] == 0.0:
                    df.at[idx, "psa9_price"] = prices["psa9"]
                if df.at[idx, "psa10_price"] == 0.0:
                    df.at[idx, "psa10_price"] = prices["psa10"]
            st.caption("Some PSA 9/10 were 0 — summary is using live PriceCharting values for those rows.")

        df["grading_total_cost"] = (df["grading_fee_initial"] + df["additional_costs"]).round(2)

        grp = (
            df.groupby(["submission_id", "submission_date", "estimated_return_date"], dropna=False)
            .agg(
                cards=("grading_row_id", "count"),
                purchase_cost=("purchase_total", "sum"),
                grading_cost=("grading_total_cost", "sum"),
                psa9_value=("psa9_price", "sum"),
                psa10_value=("psa10_price", "sum"),
            )
            .reset_index()
        )

        grp["profit_all_psa9"] = (grp["psa9_value"] - (grp["purchase_cost"] + grp["grading_cost"])).round(2)
        grp["profit_all_psa10"] = (grp["psa10_value"] - (grp["purchase_cost"] + grp["grading_cost"])).round(2)

        def money(x):
            return f"${float(x):,.2f}"

        show = grp.copy()
        for c in ["purchase_cost", "grading_cost", "psa9_value", "psa10_value", "profit_all_psa9", "profit_all_psa10"]:
            show[c] = show[c].apply(money)

        st.dataframe(show, use_container_width=True, hide_index=True)

        if st.button("🔄 Refresh (Grading)", use_container_width=True):
            refresh_all_grading()