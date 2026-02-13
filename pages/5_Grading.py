# pages/5_Grading.py
# ---------------------------------------------------------
# FIX: Google Sheets quota exceeded when refreshing images for many new watchlist rows
#
# Root cause: update_ws_column_by_header() was doing 1 API call PER ROW (cell-by-cell).
# Fix: batch update the Image column in ONE call (or a few large calls) using ws.update(range, values).
#
# NEW (THIS UPDATE):
# - Pull TCGplayer Market Price from a tcgplayer product URL stored in watchlist column: `tcgplayer_link`
# - TCGplayer is JS-rendered -> use Playwright headless browser to load + extract Market Price.
# - Show in Summary table:
#     - "TCGplayer Price" next to "UNGRADED Avg"
#     - "TCG - Ungraded" = TCGplayer Price - UNGRADED Avg
#
# IMPORTANT:
# - Do NOT change ungraded / PSA9 / PSA10 scraping logic (left untouched).
# - Keep the "unique sale_key per link" fix so new watchlist items don't get dropped.
# ---------------------------------------------------------

import json
import re
import time
from datetime import datetime, date
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


# =========================
# Sheet config
# =========================
WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")
GEMRATES_WS_NAME = st.secrets.get("gemrates_worksheet", "gemrates")

WATCHLIST_HEADERS_EXPECTED = ["Generation", "Set", "Card Name", "Card No", "Link", "Image"]

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


# =========================
# Small helpers
# =========================
def safe_str(x) -> str:
    return "" if x is None else str(x)

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

def _bs_parser():
    try:
        import lxml  # noqa: F401
        return "lxml"
    except Exception:
        return "html.parser"

def _classify_grade_from_title(title: str) -> str:
    t = (title or "").upper()
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*(MINT|MT)\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"

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

def ensure_headers(ws, headers: list[str]):
    values = ws.get_all_values()
    if not values:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")
        return
    current = [safe_str(x).strip() for x in (values[0] or [])]
    if current != headers:
        _gs_write_retry(ws.update, values=[headers], range_name="1:1", value_input_option="RAW")

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


def ensure_watchlist_image_col(watch_ws):
    """Guarantee 'Image' header exists (single header update, not per-row)."""
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
    """
    Write a whole contiguous block in one call.
    values_list is one value per row, corresponding to start_row_1based..start_row_1based+len(values_list)-1
    """
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
    """
    Parse a dollar value from a text snippet or DOM extracted string.
    """
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
    """
    Launch Chromium once per Streamlit process.
    NOTE: This requires Playwright installed + Chromium downloaded at build time.
    """
    from playwright.sync_api import sync_playwright  # lazy import

    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )
    return {"p": p, "browser": browser}


def _tcgplayer_price_via_playwright(url: str) -> float:
    """
    Load the page with a headless browser, wait for price element, extract.
    Returns 0.0 if not found.
    """
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

        # Give their JS a moment
        page.wait_for_timeout(1200)

        # Primary selector from your screenshot
        sel = "span.price-points__upper__price"

        # Wait up to ~10s for the element to appear (sometimes slower)
        try:
            page.wait_for_selector(sel, timeout=10000)
        except Exception:
            # If selector never appears, try text-based approach on full page
            txt = page.inner_text("body")
            # Pattern like: "Market Price $39.74"
            m = re.search(r"Market Price\s*\$?\s*([0-9][0-9,]*\.?[0-9]{0,2})", txt, flags=re.IGNORECASE)
            if m:
                vv = safe_float(m.group(1), 0.0)
                return float(vv) if vv > 0 else 0.0
            return 0.0

        # Pull the price text
        price_text = page.locator(sel).first.inner_text().strip()
        price = _extract_tcgplayer_market_price_from_dom_text(price_text)
        if price > 0:
            return float(price)

        # Fallback: read row containing "Market Price"
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
    """
    Cached wrapper (6 hours) around Playwright extraction.
    """
    try:
        return float(_tcgplayer_price_via_playwright(tcg_url))
    except Exception:
        return 0.0


# =========================
# Image scraping (working)
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


# =========================
# Sold sales scrapers (DO NOT CHANGE)
# =========================
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
        sales.append(
            {"sale_date": d, "price": float(price), "title": title, "grade_bucket": bucket, "sale_key": sale_key}
        )

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
# Build sales history rows
# =========================
def build_sales_history_rows_from_watchlist(wdf: pd.DataFrame) -> pd.DataFrame:
    if wdf is None or wdf.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    for h in WATCHLIST_HEADERS_EXPECTED:
        if h not in wdf.columns:
            wdf[h] = ""

    rows_out = []
    now_utc = datetime.utcnow().isoformat()

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
# Scoring
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


# =========================
# Summary table builder
# =========================
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

    # --- Image map (PriceCharting) ---
    img_map = {}
    # --- TCGplayer link map (keyed by PriceCharting Link) ---
    tcg_link_map = {}

    if wdf is not None and not wdf.empty and "Link" in wdf.columns:
        wdf2 = wdf.copy()
        wdf2["Link"] = wdf2["Link"].astype(str).str.strip()

        has_img = "Image" in wdf2.columns
        has_tcg = "tcgplayer_link" in wdf2.columns

        for _, r in wdf2.iterrows():
            lk = safe_str(r.get("Link", "")).strip()
            if not lk:
                continue

            if has_img:
                img_map[lk] = safe_str(r.get("Image", "")).strip()

            if has_tcg:
                tcg_link_map[lk] = safe_str(r.get("tcgplayer_link", "")).strip()

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

    # --- TCGplayer price pull (Playwright) ---
    out["TCGplayer Price"] = 0.0
    for i, row in out.iterrows():
        pc_link = safe_str(row.get("Link", "")).strip()
        tcg_url = safe_str(tcg_link_map.get(pc_link, "")).strip()
        if not tcg_url:
            continue
        price = fetch_tcgplayer_market_price(tcg_url)
        out.at[i, "TCGplayer Price"] = round(safe_float(price, 0.0), 2)

        # tiny delay so we don't look botty if many rows (cache helps too)
        time.sleep(0.15)

    if "UNGRADED Avg" not in out.columns:
        out["UNGRADED Avg"] = 0.0

    out["TCG - Ungraded"] = (
        out["TCGplayer Price"].apply(lambda v: safe_float(v, 0.0)) -
        out["UNGRADED Avg"].apply(lambda v: safe_float(v, 0.0))
    ).apply(lambda v: round(v, 2))

    out = add_prospect_scoring(out)
    out = out.sort_values(["Set", "Card Name", "Card No"], ascending=[True, True, True]).reset_index(drop=True)
    return out


# =========================
# REFRESH IMAGES (BATCH WRITE)  <-- FIXED
# =========================
def refresh_watchlist_images_batched(watch_ws, wdf: pd.DataFrame):
    """
    Only scrape images for rows missing Image, then batch-write the Image column
    in ONE update call (or a few contiguous block calls).

    This prevents quota blow-ups.
    """
    if wdf is None or wdf.empty or "Link" not in wdf.columns:
        return

    headers, img_col_idx = ensure_watchlist_image_col(watch_ws)

    # re-read after header change to ensure alignment
    wdf2 = wdf.copy()
    if "Image" not in wdf2.columns:
        wdf2["Image"] = ""

    # Build a full column vector (row 2..N) that we will write back
    image_values = [safe_str(v).strip() for v in wdf2["Image"].tolist()]

    # Fill missing
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

        # tiny delay so we don't hammer PriceCharting
        time.sleep(0.2)

    # Batch update the whole Image column range in ONE API call
    start_row = 2
    batch_update_column_values(watch_ws, img_col_idx, start_row, image_values)


# =========================
# UI
# =========================
sheet = get_sheet()
watch_ws = get_ws(sheet, WATCHLIST_WS_NAME)
sales_ws = get_ws(sheet, SALES_HISTORY_WS_NAME)
gem_ws = get_ws(sheet, GEMRATES_WS_NAME)

ensure_headers(sales_ws, SALES_HISTORY_HEADERS)

wdf = read_ws_df(watch_ws)
gdf = read_ws_df(gem_ws)
gem_lookup = load_gemrates_lookup(gdf)

top = st.container()
with top:
    c1, c2 = st.columns([1, 3])

    with c1:
        if st.button("Refresh", type="primary", use_container_width=True):
            if wdf is None or wdf.empty:
                st.warning(f"No rows found in `{WATCHLIST_WS_NAME}`.")
            else:
                try:
                    with st.spinner("Refreshing images + sales history..."):
                        # FIX: batch write images (1 call) instead of cell-by-cell
                        refresh_watchlist_images_batched(watch_ws, wdf)

                        # reload watchlist so Image column is current + includes new rows
                        wdf = read_ws_df(watch_ws)

                        out_df = build_sales_history_rows_from_watchlist(wdf)
                        write_ws_df(sales_ws, out_df, SALES_HISTORY_HEADERS)

                    st.success(f"Refreshed. Wrote {len(out_df):,} rows to `{SALES_HISTORY_WS_NAME}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Refresh failed: {e}")
                    st.exception(e)

    with c2:
        st.caption(
            f"Summary is calculated from `grading_sales_history` (last 5 sales per grade bucket). "
            f"Prospect Score uses all-in grading cost = ${GRADING_ALL_IN_COST:.2f} and gemrate confidence. "
            f"TCGplayer Price is loaded via headless browser (Playwright) from `tcgplayer_link` in the watchlist."
        )

st.divider()

sdf = read_ws_df(sales_ws)
summary_df = build_summary_from_sales_history(sdf, wdf, gem_lookup)

if summary_df is None or summary_df.empty:
    st.info("No sales history yet. Click **Refresh** to populate `grading_sales_history` and see the summary.")
    st.stop()

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

    p9_min, p9_max = _rng("PSA9 Avg")
    p10_min, p10_max = _rng("PSA10 Avg")
    u_min, u_max = _rng("UNGRADED Avg")
    ev_min, ev_max = _rng("EV (vs ungraded)")

    with g1:
        psa9_rng = st.slider("PSA9 Avg ($)", 0.0, max(1.0, p9_max), (0.0, p9_max), 0.5)
    with g2:
        psa10_rng = st.slider("PSA10 Avg ($)", 0.0, max(1.0, p10_max), (0.0, p10_max), 0.5)
    with g3:
        ungraded_rng = st.slider("UNGRADED Avg ($)", 0.0, max(1.0, u_max), (0.0, u_max), 0.5)
    with g4:
        ev_rng = st.slider("EV (vs ungraded)", min(-200.0, ev_min), max(200.0, ev_max), (min(-200.0, ev_min), max(200.0, ev_max)), 0.5)

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
    "Image",
    "Link",
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Prospect Score",
    "EV (vs ungraded)",
    "Gem rate (all time)",
    "Total graded",
    "Gem conf",
    "P10 adj",

    # Put TCG next to Ungraded Avg
    "UNGRADED Avg", "TCGplayer Price", "TCG - Ungraded", "UNGRADED Min", "UNGRADED Max",

    "PSA9 Avg", "PSA9 Min", "PSA9 Max",
    "PSA10 Avg", "PSA10 Min", "PSA10 Max",
    "Net 9",
    "Net 10",
]
final_cols = [c for c in preferred_cols if c in fdf.columns] + [c for c in fdf.columns if c not in preferred_cols]
fdf = fdf[final_cols].copy()

st.markdown("### Summary (filterable)")
st.dataframe(
    fdf,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Image": st.column_config.ImageColumn("Image", width="small"),
        "Link": st.column_config.LinkColumn("Link", width="medium"),
    },
)
