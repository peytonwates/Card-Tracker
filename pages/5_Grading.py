# pages/5_Grading.py
# ---------------------------------------------------------
# Grading > Analysis (CLEAN VERSION)
# Goal:
# - Read rows from Google Sheet tab: grading_watch_list (now includes Image column)
# - For each row, use the "Link" (PriceCharting) to pull the most recent SOLD sales:
#     - Ungraded (existing table scrape)  (DO NOT CHANGE)
#     - PSA10 (completed-auctions-manual-only) (DO NOT CHANGE)
#     - PSA9  (completed-auctions-graded) (DO NOT CHANGE)
# - Keep ONLY the latest 5 sales per grade bucket per watchlist item
# - Overwrite Google Sheet tab: grading_sales_history
#
# NEW:
# - Scrape card image URL from the reference link and write it into watchlist "Image" column
# - Summary table:
#     - 1 row per card with Avg/Min/Max price for Ungraded, PSA9, PSA10
#     - show Image next to Link
#     - join GemRates tab (by Set + Card #) to pull Gem rate - All time and Total
# ---------------------------------------------------------

import json
import re
import time
from datetime import datetime, date
from pathlib import Path

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

# Watchlist now includes Image
WATCHLIST_HEADERS_EXPECTED = ["Generation", "Set", "Card Name", "Card No", "Link", "Image"]

SALES_HISTORY_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "grade_bucket",
    "sale_date",     # YYYY-MM-DD
    "price",         # numeric
    "title",
    "sale_key",      # stable key to dedupe
    "updated_utc",   # ISO timestamp
]

# Fixed behavior (no UI controls)
PER_ITEM_N = 5
PSA10_PER_ITEM = 5
PSA9_PER_ITEM = 5
EBAY_ONLY = True  # fixed (no checkbox)


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

def a1_col_letter(n: int) -> str:
    letters = ""
    while n:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return letters

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

def _norm_set(x: str) -> str:
    return re.sub(r"\s+", " ", safe_str(x).strip().lower())

def _norm_cardno(x: str) -> str:
    """
    Normalize card numbers so '063', '63', ' 63 ' all match.
    If there are no digits, fallback to trimmed lower string.
    """
    s = safe_str(x).strip()
    m = re.search(r"(\d+)", s)
    if not m:
        return s.strip().lower()
    return str(int(m.group(1)))  # removes leading zeros


# =========================
# Google Sheets auth
# =========================
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit Cloud: secrets as TOML table
    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Streamlit Cloud: secrets as JSON string
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_info = json.loads(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: file path in secrets
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
    max_tries = 6
    base_sleep = 0.8
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

def update_watchlist_image_column(ws, wdf: pd.DataFrame, image_urls_by_row_idx: dict[int, str]):
    """
    Writes image URLs back into the watchlist "Image" column for specific row indices.
    Row indices are 0-based within dataframe (excluding header row).
    """
    if not image_urls_by_row_idx:
        return

    values = ws.get_all_values()
    if not values or not values[0]:
        return

    header = [safe_str(x).strip() for x in values[0]]
    if "Image" not in header:
        return

    img_col_idx = header.index("Image") + 1  # 1-based for A1
    updates = []

    for df_row_idx, url in image_urls_by_row_idx.items():
        sheet_row = 2 + int(df_row_idx)  # header row=1
        a1 = f"{a1_col_letter(img_col_idx)}{sheet_row}"
        updates.append({"range": a1, "values": [[safe_str(url)]]})

    if updates:
        _gs_write_retry(ws.batch_update, updates)


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
# Image scraper (PriceCharting) - UPDATED using your proven logic
# =========================
def _find_best_image(soup: BeautifulSoup) -> str:
    """
    Prefer PriceCharting's real card/product photos (storage.googleapis.com)
    over set icons like /images/pokemon-sets/*.png.
    """
    if soup is None:
        return ""

    # 1) If og/twitter image is already a real hosted photo, use it.
    for meta in [
        soup.find("meta", property="og:image"),
        soup.find("meta", attrs={"name": "twitter:image"}),
    ]:
        if meta and meta.get("content"):
            url = meta["content"].strip()
            if "storage.googleapis.com/images.pricecharting.com" in url:
                return url
            # If it's the set icon, ignore it
            if "/images/pokemon-sets/" not in url:
                return url

    # 2) Prefer any storage.googleapis.com PriceCharting image anywhere on the page
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if "storage.googleapis.com/images.pricecharting.com" in href:
            return href

    for img in soup.find_all("img", src=True):
        src = (img.get("src") or "").strip()
        if "storage.googleapis.com/images.pricecharting.com" in src:
            return src

    # 3) Otherwise, choose the first non-set-icon <img>
    for img in soup.find_all("img", src=True):
        src = (img.get("src") or "").strip()
        if not src:
            continue
        if "/images/pokemon-sets/" in src:
            continue
        return src

    return ""

def _find_pricecharting_main_image(soup: BeautifulSoup) -> str:
    """
    PriceCharting often shows the real product/card image under:
    'More Photos' -> 'Main Image' as a storage.googleapis.com link.
    We prefer that over set icons like /images/pokemon-sets/....
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

    return candidates[0] if candidates else ""

@st.cache_data(ttl=60 * 60 * 24)
def fetch_pricecharting_image_url(reference_link: str) -> str:
    """
    Uses proven logic:
      - _find_best_image(soup) first
      - then _find_pricecharting_main_image(soup)
    """
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return ""

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    url = _find_best_image(soup) or ""
    if not url:
        url = _find_pricecharting_main_image(soup) or ""

    if not url:
        return ""

    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    return url


# =========================
# PriceCharting sold sales scraper (UNGRADED)  (DO NOT CHANGE)
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
            {
                "sale_date": d,
                "price": float(price),
                "title": title,
                "grade_bucket": bucket,
                "sale_key": sale_key,
            }
        )

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# PSA10 scraper from ".completed-auctions-manual-only"  (WORKING - DO NOT CHANGE)
# =========================
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
        sales.append(
            {
                "sale_date": d,
                "price": float(price),
                "title": title,
                "grade_bucket": "psa10",
                "sale_key": sale_key,
            }
        )

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# PSA9 scraper from ".completed-auctions-graded" (WORKING - DO NOT CHANGE)
# =========================
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
        sales.append(
            {
                "sale_date": d,
                "price": float(price),
                "title": title,
                "grade_bucket": "psa9",
                "sale_key": sale_key,
            }
        )

    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# Core: build sales-history rows (ungraded + PSA10 + PSA9)
# =========================
def build_sales_history_rows_from_watchlist(wdf: pd.DataFrame) -> pd.DataFrame:
    if wdf is None or wdf.empty:
        return pd.DataFrame(columns=SALES_HISTORY_HEADERS)

    for h in ["Generation", "Set", "Card Name", "Card No", "Link"]:
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

        # ---------- UNGRADED ----------
        sales = fetch_pricecharting_sold_sales(link, limit=120)
        if sales:
            ungraded = [s for s in sales if (s.get("grade_bucket") or "").lower() == "ungraded"]
            if EBAY_ONLY:
                ungraded = [s for s in ungraded if "[ebay]" in (s.get("title", "").lower())]
            ungraded = ungraded[:PER_ITEM_N]

            for s in ungraded:
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
                        "sale_key": safe_str(s.get("sale_key", "")).strip(),
                        "updated_utc": now_utc,
                    }
                )

        # ---------- PSA10 (manual-only) ----------
        psa10_sales = fetch_pricecharting_psa10_manual_only(link, limit=50)
        if psa10_sales:
            if EBAY_ONLY:
                psa10_sales = [s for s in psa10_sales if "[ebay]" in (s.get("title", "").lower())]
            psa10_sales = psa10_sales[:PSA10_PER_ITEM]

            for s in psa10_sales:
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
                        "sale_key": safe_str(s.get("sale_key", "")).strip(),
                        "updated_utc": now_utc,
                    }
                )

        # ---------- PSA9 (graded) ----------
        psa9_sales = fetch_pricecharting_psa9_graded(link, limit=50)
        if psa9_sales:
            if EBAY_ONLY:
                psa9_sales = [s for s in psa9_sales if "[ebay]" in (s.get("title", "").lower())]
            psa9_sales = psa9_sales[:PSA9_PER_ITEM]

            for s in psa9_sales:
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
                        "sale_key": safe_str(s.get("sale_key", "")).strip(),
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
        ascending=[True, True, True, False],
    ).drop(columns=["sale_date_dt"])
    return df_out[SALES_HISTORY_HEADERS].copy()


# =========================
# GemRates loader + matcher
# =========================
def load_gemrates_df(sheet) -> pd.DataFrame:
    try:
        gr_ws = get_ws(sheet, GEMRATES_WS_NAME)
        gdf = read_ws_df(gr_ws)
        return gdf
    except Exception:
        return pd.DataFrame()

def build_gemrates_lookup(gdf: pd.DataFrame) -> dict[tuple[str, str], dict]:
    """
    Returns dict keyed by (set_norm, cardno_norm) => {total, gem_rate}
    If multiple rows exist for same key, chooses the row with largest Total.
    """
    if gdf is None or gdf.empty:
        return {}

    col_set = "Set Name" if "Set Name" in gdf.columns else ("Set" if "Set" in gdf.columns else None)
    col_card = "Card #" if "Card #" in gdf.columns else ("Card No" if "Card No" in gdf.columns else None)
    col_total = "Total" if "Total" in gdf.columns else None
    col_rate = "Gem rate - All time" if "Gem rate - All time" in gdf.columns else ("Gem rate" if "Gem rate" in gdf.columns else None)

    if not col_set or not col_card:
        return {}

    tmp = gdf.copy()
    tmp["_set_norm"] = tmp[col_set].apply(_norm_set)
    tmp["_card_norm"] = tmp[col_card].apply(_norm_cardno)

    if col_total:
        tmp["_total_num"] = tmp[col_total].apply(lambda v: safe_float(v, 0.0))
    else:
        tmp["_total_num"] = 0.0

    tmp = tmp.sort_values("_total_num", ascending=False)

    out = {}
    for _, r in tmp.iterrows():
        k = (safe_str(r["_set_norm"]), safe_str(r["_card_norm"]))
        if k in out:
            continue
        out[k] = {
            "Total": safe_str(r.get(col_total, "")).strip() if col_total else "",
            "Gem rate - All time": safe_str(r.get(col_rate, "")).strip() if col_rate else "",
        }
    return out


# =========================
# Summary table builder (from grading_sales_history + watchlist image + gemrates)
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
        return {
            "mean": f"{bucket.upper()} Avg",
            "min": f"{bucket.upper()} Min",
            "max": f"{bucket.upper()} Max",
        }

    out = None
    for bucket in ["ungraded", "psa9", "psa10"]:
        sub = stats[stats["grade_bucket"] == bucket].copy()
        sub = sub[keys + ["mean", "min", "max"]].copy() if not sub.empty else pd.DataFrame(columns=keys + ["mean", "min", "max"])
        rename = _bucket_cols(bucket)
        sub = sub.rename(columns={"mean": rename["mean"], "min": rename["min"], "max": rename["max"]})

        if out is None:
            out = sub
        else:
            out = out.merge(sub, on=keys, how="outer")

    out = out.fillna(0.0)

    for c in out.columns:
        if c.endswith(" Avg") or c.endswith(" Min") or c.endswith(" Max"):
            out[c] = out[c].apply(lambda v: round(safe_float(v, 0.0), 2))

    # bring in Image from watchlist (by the same keys)
    img_map = {}
    if wdf is not None and not wdf.empty:
        for col in ["Generation", "Set", "Card Name", "Card No", "Link", "Image"]:
            if col not in wdf.columns:
                wdf[col] = ""
        w2 = wdf.copy()
        w2["Link"] = w2["Link"].astype(str).str.strip()
        w2["Image"] = w2["Image"].astype(str).str.strip()
        for _, r in w2.iterrows():
            k = (
                safe_str(r.get("Generation", "")).strip(),
                safe_str(r.get("Set", "")).strip(),
                safe_str(r.get("Card Name", "")).strip(),
                safe_str(r.get("Card No", "")).strip(),
                safe_str(r.get("Link", "")).strip(),
            )
            img_map[k] = safe_str(r.get("Image", "")).strip()

    out["Image"] = out.apply(
        lambda rr: img_map.get(
            (
                safe_str(rr.get("Generation", "")).strip(),
                safe_str(rr.get("Set", "")).strip(),
                safe_str(rr.get("Card Name", "")).strip(),
                safe_str(rr.get("Card No", "")).strip(),
                safe_str(rr.get("Link", "")).strip(),
            ),
            "",
        ),
        axis=1,
    )

    # gemrates join by Set + Card No
    out["_set_norm"] = out["Set"].apply(_norm_set)
    out["_card_norm"] = out["Card No"].apply(_norm_cardno)

    out["Gem rate - All time"] = out.apply(
        lambda rr: (gem_lookup.get((rr["_set_norm"], rr["_card_norm"]), {}) or {}).get("Gem rate - All time", ""),
        axis=1,
    )
    out["Total graded"] = out.apply(
        lambda rr: (gem_lookup.get((rr["_set_norm"], rr["_card_norm"]), {}) or {}).get("Total", ""),
        axis=1,
    )

    out = out.drop(columns=["_set_norm", "_card_norm"], errors="ignore")

    ordered = [
        "Generation",
        "Set",
        "Card Name",
        "Card No",
        "Link",
        "Image",
        "Gem rate - All time",
        "Total graded",
        "UNGRADED Avg", "UNGRADED Min", "UNGRADED Max",
        "PSA9 Avg", "PSA9 Min", "PSA9 Max",
        "PSA10 Avg", "PSA10 Min", "PSA10 Max",
    ]
    final_cols = [c for c in ordered if c in out.columns] + [c for c in out.columns if c not in ordered]
    out = out[final_cols].copy()

    out = out.sort_values(["Card Name", "Card No"], ascending=[True, True]).reset_index(drop=True)
    return out


# =========================
# UI + workflow
# =========================
sheet = get_sheet()
watch_ws = get_ws(sheet, WATCHLIST_WS_NAME)
sales_ws = get_ws(sheet, SALES_HISTORY_WS_NAME)

ensure_headers(sales_ws, SALES_HISTORY_HEADERS)

wdf = read_ws_df(watch_ws)

gdf = load_gemrates_df(sheet)
gem_lookup = build_gemrates_lookup(gdf)

top = st.container()
with top:
    c1, c2 = st.columns([1, 3])
    with c1:
        if st.button("Refresh", type="primary", use_container_width=True):
            if wdf is None or wdf.empty:
                st.warning(f"No rows found in `{WATCHLIST_WS_NAME}`.")
            else:
                try:
                    with st.spinner("Refreshing images + sales history (Ungraded + PSA9 + PSA10)..."):
                        # ---------- (1) Fill Image column in watchlist (blank cells only) ----------
                        wdf2 = wdf.copy()
                        if "Image" not in wdf2.columns:
                            wdf2["Image"] = ""

                        to_write = {}
                        for idx, row in wdf2.iterrows():
                            link = safe_str(row.get("Link", "")).strip()
                            img = safe_str(row.get("Image", "")).strip()
                            if img:
                                continue
                            if "pricecharting.com" not in link.lower():
                                continue
                            if to_write:
                                time.sleep(0.35)
                            url = fetch_pricecharting_image_url(link)
                            if url:
                                to_write[idx] = url
                                wdf2.at[idx, "Image"] = url

                        if to_write:
                            update_watchlist_image_column(watch_ws, wdf2, to_write)

                        # refresh local watchlist df after updating
                        wdf = read_ws_df(watch_ws)

                        # ---------- (2) Pull sales + write sales history ----------
                        out_df = build_sales_history_rows_from_watchlist(wdf)
                        write_ws_df(sales_ws, out_df, SALES_HISTORY_HEADERS)

                    st.success(f"Refreshed. Wrote {len(out_df):,} rows to `{SALES_HISTORY_WS_NAME}`.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Refresh failed: {e}")
                    st.exception(e)

    with c2:
        st.caption("Summary is calculated from `grading_sales_history` + watchlist Image + gemrates (Set + Card #).")

st.divider()

sdf = read_ws_df(sales_ws)
summary_df = build_summary_from_sales_history(sdf, wdf, gem_lookup)

if summary_df is None or summary_df.empty:
    st.info("No sales history yet. Click **Refresh** to populate `grading_sales_history` and see the summary.")
else:
    col_config = {}
    if "Image" in summary_df.columns:
        col_config["Image"] = st.column_config.ImageColumn("Image", help="Scraped from PriceCharting ref link", width="small")
    if "Link" in summary_df.columns:
        col_config["Link"] = st.column_config.LinkColumn("Link")
    st.dataframe(
        summary_df,
        use_container_width=True,
        hide_index=True,
        column_config=col_config,
    )
