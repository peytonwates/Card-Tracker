# pages/5_Grading.py
# ---------------------------------------------------------
# Grading > Analysis (SIMPLE VERSION)
# Goal:
# - Read rows from Google Sheet tab: grading_watch_list
# - For each row, use the "Link" (PriceCharting) to pull the most recent SOLD sales
# - Keep ONLY the latest N sales per watchlist item:
#     - ungraded (from the default "used" sold table)  <-- DO NOT TOUCH / already works
#     - psa10 (from #completed-auctions-manual-only section)
#     - psa9  (from #completed-auctions-graded section)
# - (Optionally) filter to eBay-only rows
# - Write those rows to Google Sheet tab: grading_sales_history (overwrite)
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
st.title("Grading — Analysis (Sales History Loader)")


# =========================
# Sheet config
# =========================
WATCHLIST_WS_NAME = st.secrets.get("grading_watchlist_worksheet", "grading_watch_list")
SALES_HISTORY_WS_NAME = st.secrets.get("grading_sales_history_worksheet", "grading_sales_history")

WATCHLIST_HEADERS_EXPECTED = ["Generation", "Set", "Card Name", "Card No", "Link"]

SALES_HISTORY_HEADERS = [
    "Generation",
    "Set",
    "Card Name",
    "Card No",
    "Link",
    "grade_bucket",  # ungraded / psa9 / psa10
    "sale_date",     # YYYY-MM-DD
    "price",         # numeric
    "title",
    "sale_key",      # stable key to dedupe
    "updated_utc",   # ISO timestamp
]


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
    """
    Simple classifier:
    - PSA 9/10 => graded
    - else => ungraded
    """
    t = (title or "").upper()
    if re.search(r"\bPSA\s*10\b", t) or re.search(r"\bGEM\s*(MINT|MT)\s*10\b", t):
        return "psa10"
    if re.search(r"\bPSA\s*9\b", t) or re.search(r"\bMINT\s*9\b", t):
        return "psa9"
    return "ungraded"

def _parse_any_date(text: str):
    """
    PriceCharting sold listings typically show YYYY-MM-DD.
    We'll be flexible anyway.
    """
    if not text:
        return None
    d = pd.to_datetime(text, errors="coerce")
    if pd.isna(d):
        return None
    if d.year < 2000:
        return None
    return d.date()

def _price_from_cell_text(text: str) -> float:
    """
    Pull a single $X.XX from a *cell*, not from a whole row blob.
    """
    if not text:
        return 0.0
    m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", text)
    if not m:
        return 0.0
    return safe_float(m.group(1), 0.0)

def _extract_first_price(text: str) -> float:
    if not text:
        return 0.0
    m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", text)
    if not m:
        return 0.0
    return safe_float(m.group(1), 0.0)

def _find_first_yyyy_mm_dd(text: str):
    if not text:
        return None
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text)
    if not m:
        return None
    return _parse_any_date(m.group(1))

def _pick_title_from_container_text(container_text: str) -> str:
    """
    Heuristic: pick a clean-ish title from the container text.
    We'll strip dates and prices and keep something meaningful.
    """
    if not container_text:
        return ""
    t = container_text.strip()

    # remove dates
    t = re.sub(r"\b20\d{2}-\d{2}-\d{2}\b", " ", t)
    # remove prices
    t = re.sub(r"\$\s*[0-9][0-9,]*\.?[0-9]{0,2}", " ", t)
    # collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # If there is a bracket source marker, keep it (like [eBay])
    # Otherwise just return trimmed.
    return t[:240].strip()


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


# =========================
# HTTP (basic backoff)
# =========================
@st.cache_resource
def get_http_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (CardTracker; Streamlit)"})
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
# PriceCharting sold sales scraper (FIXED)  <-- UNGRADED (DO NOT CHANGE)
# =========================
@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sold_sales(reference_link: str, limit: int = 80) -> list[dict]:
    """
    Parse sold listings by reading the actual table cells, not row text.

    Output:
      { sale_date: date, price: float, title: str, grade_bucket: str, sale_key: str }
    """
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    # Find a table whose header contains "Sale Date" and "Price"
    target_table = None
    for tbl in soup.find_all("table"):
        ths = [th.get_text(" ", strip=True) for th in tbl.find_all("th")]
        ths_norm = [t.lower() for t in ths if t]
        if any("sale date" in t for t in ths_norm) and any(t.strip() == "price" or "price" in t for t in ths_norm):
            target_table = tbl
            break

    if target_table is None:
        return []

    # Map header names -> column index
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

    # If we can't find a clean mapping, fallback to common layout:
    # [Sale Date, TW, Title, Price]
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

        # Safe get
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

    # Dedup + sort newest first
    by_key = {s["sale_key"]: s for s in sales if s.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


# =========================
# PSA sections scraper (PSA10 + PSA9)
# - PSA10: #completed-auctions-manual-only
# - PSA9 : #completed-auctions-graded
# =========================
def _next_price_after_js_price(container, js_price_tag) -> float:
    """
    PriceCharting PSA sections often have:
      <span class="js-price"> ...maybe list price or "Best Offer"... </span>
      then the next "$X.XX" shown nearby is the SOLD price we want.

    We walk forward in document order within the container and grab the next $.
    """
    if container is None or js_price_tag is None:
        return 0.0

    # Traverse forward among all strings after js_price_tag within the same container
    # BeautifulSoup doesn't give perfect "next sibling in same container" traversal,
    # so we do a safe approach: collect all text chunks from container, but start
    # counting after we hit js_price_tag text.
    texts = []
    for el in container.descendants:
        try:
            if hasattr(el, "get_text"):
                continue
        except Exception:
            pass
        # only NavigableString
    # Better approach: use .stripped_strings in order, but start after js_price_tag content
    all_strings = list(container.stripped_strings)
    js_text = " ".join(list(js_price_tag.stripped_strings)).strip()
    if not all_strings:
        return 0.0

    # Find where js_text appears (best effort)
    start_idx = 0
    if js_text:
        for i, s in enumerate(all_strings):
            if js_text in s:
                start_idx = i
                break

    for s in all_strings[start_idx + 1 :]:
        p = _extract_first_price(s)
        if p > 0:
            return float(p)

    # fallback: if nothing found after, try any price in the container (last resort)
    blob = " ".join(all_strings)
    prices = re.findall(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", blob)
    if prices:
        return safe_float(prices[-1], 0.0)
    return 0.0

@st.cache_data(ttl=60 * 60 * 6)
def fetch_pricecharting_sales_from_section(reference_link: str, section_id: str, forced_bucket: str, limit: int = 80) -> list[dict]:
    """
    Scrape sales from a specific section by ID.
    We do NOT add '#...' to the URL for the request (fragments aren't sent);
    we locate the section inside the HTML by id.

    Returns rows:
      { sale_date, price, title, grade_bucket, sale_key }
    """
    link = (reference_link or "").strip()
    if not link or "pricecharting.com" not in link.lower():
        return []

    r = http_get_with_backoff(link, timeout=25, max_tries=6)
    soup = BeautifulSoup(r.text, _bs_parser())

    section = soup.find(id=section_id)
    if section is None:
        # some pages might use a class instead of id; try class fallback
        section = soup.select_one(f"#{section_id}, .{section_id}")
    if section is None:
        return []

    # Heuristic: each listing is usually a row-like block that contains a date (YYYY-MM-DD)
    # and has a js-price element within it.
    rows = []
    candidates = section.find_all(True)

    for node in candidates:
        if not getattr(node, "get_text", None):
            continue
        txt = node.get_text(" ", strip=True)
        if not txt:
            continue

        # require a date somewhere nearby
        d = _find_first_yyyy_mm_dd(txt)
        if not d:
            continue

        js_price = node.find(class_=re.compile(r"\bjs-price\b"))
        if js_price is None:
            continue

        sold_price = _next_price_after_js_price(node, js_price)
        if sold_price <= 0:
            continue

        title = ""
        # prefer any anchor text inside the node (often the title)
        a = node.find("a")
        if a and a.get_text(" ", strip=True):
            title = a.get_text(" ", strip=True).strip()
        if not title:
            title = _pick_title_from_container_text(txt)

        bucket = forced_bucket.strip().lower()
        sale_key = f"{d.isoformat()}|{sold_price:.2f}|{bucket}|{title[:90].strip().lower()}"

        rows.append(
            {
                "sale_date": d,
                "price": float(sold_price),
                "title": title,
                "grade_bucket": bucket,
                "sale_key": sale_key,
            }
        )

    # Dedup + sort newest
    by_key = {r["sale_key"]: r for r in rows if r.get("sale_key")}
    out = list(by_key.values())
    out.sort(key=lambda x: (x["sale_date"], x["price"]), reverse=True)
    return out[: max(1, int(limit))]


def fetch_pricecharting_psa10_sales(reference_link: str, limit: int = 80) -> list[dict]:
    return fetch_pricecharting_sales_from_section(
        reference_link=reference_link,
        section_id="completed-auctions-manual-only",
        forced_bucket="psa10",
        limit=limit,
    )

def fetch_pricecharting_psa9_sales(reference_link: str, limit: int = 80) -> list[dict]:
    return fetch_pricecharting_sales_from_section(
        reference_link=reference_link,
        section_id="completed-auctions-graded",
        forced_bucket="psa9",
        limit=limit,
    )


# =========================
# Core: build sales-history rows
# - Keep last N per bucket per item
# =========================
def build_sales_history_rows_from_watchlist(
    wdf: pd.DataFrame,
    per_item_n: int = 5,
    ebay_only: bool = True,
    include_psa10: bool = True,
    include_psa9: bool = True,
) -> pd.DataFrame:
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

        # Polite delay between items
        if rows_out:
            time.sleep(0.75)

        # -------- UNGRADED (existing logic; DO NOT CHANGE)
        sales = fetch_pricecharting_sold_sales(link, limit=120)
        ungraded = [s for s in sales if (s.get("grade_bucket") or "").lower() == "ungraded"]
        if ebay_only:
            ungraded = [s for s in ungraded if "[ebay]" in (s.get("title", "").lower())]
        ungraded = ungraded[: int(per_item_n)]

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

        # -------- PSA10 (manual-only section)
        if include_psa10:
            psa10_sales = fetch_pricecharting_psa10_sales(link, limit=200)
            if ebay_only:
                psa10_sales = [s for s in psa10_sales if "[ebay]" in (safe_str(s.get("title", "")).lower())]
            psa10_sales = psa10_sales[: int(per_item_n)]

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

        # -------- PSA9 (graded section)
        if include_psa9:
            psa9_sales = fetch_pricecharting_psa9_sales(link, limit=200)
            if ebay_only:
                psa9_sales = [s for s in psa9_sales if "[ebay]" in (safe_str(s.get("title", "")).lower())]
            psa9_sales = psa9_sales[: int(per_item_n)]

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

    df_out["price"] = df_out["price"].apply(lambda v: safe_float(v, 0.0))
    df_out["_sale_date_dt"] = pd.to_datetime(df_out["sale_date"], errors="coerce")

    # Sort: group by card + bucket, newest first
    df_out = df_out.sort_values(
        ["Card Name", "Card No", "grade_bucket", "_sale_date_dt"],
        ascending=[True, True, True, False],
    ).drop(columns=["_sale_date_dt"])

    return df_out[SALES_HISTORY_HEADERS].copy()


# =========================
# UI
# =========================
st.caption(
    "This page reads `grading_watch_list`, scrapes SOLD comps from PriceCharting, keeps the latest "
    "**ungraded** + (optional) **PSA10** + **PSA9** sales per item, and overwrites `grading_sales_history`."
)

sheet = get_sheet()
watch_ws = get_ws(sheet, WATCHLIST_WS_NAME)
sales_ws = get_ws(sheet, SALES_HISTORY_WS_NAME)

ensure_headers(sales_ws, SALES_HISTORY_HEADERS)

wdf = read_ws_df(watch_ws)

c1, c2, c3, c4, c5 = st.columns([1, 1, 1, 1, 2])

with c1:
    per_item_n = st.number_input("Sales per bucket per item", min_value=1, max_value=20, value=5, step=1)

with c2:
    ebay_only = st.checkbox("eBay only", value=True)

with c3:
    include_psa10 = st.checkbox("Include PSA 10 sales", value=True)

with c4:
    include_psa9 = st.checkbox("Include PSA 9 sales", value=True)

with c5:
    if st.button("Pull sales + overwrite grading_sales_history", type="primary", use_container_width=True):
        if wdf is None or wdf.empty:
            st.warning(f"No rows found in `{WATCHLIST_WS_NAME}`.")
        else:
            out_df = build_sales_history_rows_from_watchlist(
                wdf,
                per_item_n=int(per_item_n),
                ebay_only=bool(ebay_only),
                include_psa10=bool(include_psa10),
                include_psa9=bool(include_psa9),
            )
            write_ws_df(sales_ws, out_df, SALES_HISTORY_HEADERS)
            st.success(f"Wrote {len(out_df):,} row(s) to `{SALES_HISTORY_WS_NAME}`.")
            st.rerun()

st.divider()

st.markdown("#### Watchlist preview")
if wdf is None or wdf.empty:
    st.info(f"`{WATCHLIST_WS_NAME}` is empty.")
else:
    show_cols = [c for c in WATCHLIST_HEADERS_EXPECTED if c in wdf.columns]
    st.dataframe(wdf[show_cols], use_container_width=True, hide_index=True)

st.divider()

st.markdown("#### Sales history preview (sheet)")
sdf = read_ws_df(sales_ws)
if sdf is None or sdf.empty:
    st.info(f"`{SALES_HISTORY_WS_NAME}` is empty.")
else:
    if "price" in sdf.columns:
        sdf["price"] = sdf["price"].apply(lambda v: safe_float(v, 0.0))
    if "sale_date" in sdf.columns:
        sdf["_sale_date"] = pd.to_datetime(sdf["sale_date"], errors="coerce")
        sdf = sdf.sort_values("_sale_date", ascending=False).drop(columns=["_sale_date"])
    st.dataframe(sdf, use_container_width=True, hide_index=True)
