# pages/1_Dashboard.py
import json
import re
import time
import random
from pathlib import Path
from datetime import date
from urllib.parse import urlparse, urlunparse

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Page config
# =========================
st.set_page_config(page_title="Dashboard", layout="wide")
st.title("Dashboard")


# =========================
# Google Sheets client
# =========================
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


def _open_ws(ws_name: str):
    client = get_gspread_client()
    sh = client.open_by_key(st.secrets["spreadsheet_id"])
    return sh.worksheet(ws_name)


# =========================
# Helpers (robust + dedupe)
# =========================
def _safe_str(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x)

def _to_dt(s):
    return pd.to_datetime(s, errors="coerce")

def _to_num(s):
    """
    Robust numeric parser:
    - handles currency strings like "$1,234.56"
    - handles negatives like "(12.34)" or "-12.34"
    - leaves real numerics alone
    """
    if isinstance(s, pd.Series):
        x = s.copy()
        if pd.api.types.is_numeric_dtype(x):
            return pd.to_numeric(x, errors="coerce").fillna(0.0)

        x = x.astype(str).str.strip()
        x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        x = x.str.replace(r"[\$,]", "", regex=True)
        x = x.replace({"": "0", "nan": "0", "None": "0"})

        return pd.to_numeric(x, errors="coerce").fillna(0.0)

    try:
        if s is None:
            return 0.0
        if isinstance(s, (int, float, np.number)):
            return float(s) if not (isinstance(s, float) and np.isnan(s)) else 0.0
        t = str(s).strip()
        if t.startswith("(") and t.endswith(")"):
            t = "-" + t[1:-1]
        t = re.sub(r"[\$,]", "", t)
        if t in {"", "nan", "None"}:
            return 0.0
        v = pd.to_numeric(t, errors="coerce")
        return float(v) if pd.notna(v) else 0.0
    except Exception:
        return 0.0


def _month_start(dt_series):
    d = _to_dt(dt_series)
    return d.dt.to_period("M").dt.to_timestamp()

def _fmt_money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"

def _pct(a, b):
    try:
        b = float(b)
    except Exception:
        b = 0.0
    if b == 0:
        return 0.0
    return float(a) / b

def _style_red_green(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if v < 0:
        return "color: #b00020; font-weight: 700;"
    if v > 0:
        return "color: #0b6b2f; font-weight: 800;"
    return ""

def _base_col(c: str) -> str:
    s = _safe_str(c)
    if "__dup" in s:
        s = s.split("__dup")[0]
    return s

def _norm_key(s: str) -> str:
    s = _safe_str(s).strip().lower()
    s = re.sub(r"[\s\-\/]+", "_", s)
    s = re.sub(r"[^\w]+", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Google Sheets can end up with duplicate headers (or merges can create duplicates).
    Streamlit/Arrow will crash if df.columns are not unique.
    We rename duplicates with __dup{n} suffixes.
    """
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

def _col_lookup(df: pd.DataFrame) -> dict:
    """
    Map normalized(base_col_name) -> actual column name (first occurrence wins).
    """
    m = {}
    for c in df.columns:
        key = _norm_key(_base_col(c))
        if key and key not in m:
            m[key] = c
    return m

def _pick_col(df: pd.DataFrame, name: str, fallback: str = None):
    m = _col_lookup(df)
    return m.get(_norm_key(name), fallback)

def _apply_period_filter(df: pd.DataFrame, dt_col: str, year_choice: str, month_choice: str) -> pd.DataFrame:
    if df is None or df.empty or dt_col not in df.columns:
        return df

    d = _to_dt(df[dt_col])
    out = df.copy()
    out["__dt_filter"] = d

    if year_choice != "All":
        try:
            y = int(year_choice)
            out = out[out["__dt_filter"].dt.year == y]
        except Exception:
            pass

    if month_choice != "All":
        try:
            m = pd.to_datetime(month_choice + "-01", errors="coerce")
            if pd.notna(m):
                out = out[out["__dt_filter"].dt.to_period("M") == m.to_period("M")]
        except Exception:
            pass

    out = out.drop(columns=["__dt_filter"], errors="ignore")
    return out

def _bucket_product(product_type, grading_company, grade, condition, inv_status) -> str:
    pt = _safe_str(product_type).strip().lower()
    comp = _safe_str(grading_company).strip()
    grd = _safe_str(grade).strip()
    cond = _safe_str(condition).strip().lower()
    status = _safe_str(inv_status).strip().upper()

    if status == "GRADING":
        return "Grading In-Process"

    if "sealed" in pt:
        return "Sealed"

    if "graded" in pt or comp or grd or ("graded" in cond):
        return "Graded Cards"

    return "Cards"

def _normalize_card_type(val: str) -> str:
    """
    User requirement: ONLY Pokemon or Sports.
    Unknown / blank values should not default to Pokemon.
    """
    s = _safe_str(val).strip().lower()
    if s == "sports":
        return "Sports"
    if s == "pokemon":
        return "Pokemon"
    if "sport" in s:
        return "Sports"
    if "pok" in s or "pokemon" in s:
        return "Pokemon"
    return ""


# =========================
# Canonicalize reference links
# =========================
def _canonicalize_reference_link(url: str) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if not url:
        return ""

    if url.startswith("//"):
        url = "https:" + url
    elif not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        p = urlparse(url)
    except Exception:
        return url

    netloc = (p.netloc or "").lower()
    path = p.path or ""

    if "sportscardspro.com" in netloc:
        netloc = "www.sportscardspro.com"

    path = path.rstrip("/")
    canonical = urlunparse(("https", netloc, path, "", "", ""))
    return canonical


# =========================
# HTTP session + throttling/backoff
# =========================
@st.cache_resource
def _get_http_session() -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=0,
        connect=0,
        read=0,
        status=0,
        backoff_factor=0,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_DOMAIN_LAST_HIT = {}
_MIN_GAP_SECONDS = 1.2
_JITTER_SECONDS = 0.4

def _throttle(url: str):
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = "unknown"
    now = time.time()
    last = _DOMAIN_LAST_HIT.get(host, 0.0)
    wait = (_MIN_GAP_SECONDS - (now - last))
    if wait > 0:
        time.sleep(wait + random.random() * _JITTER_SECONDS)
    _DOMAIN_LAST_HIT[host] = time.time()

def _http_get_with_backoff(url: str, headers: dict, timeout: int = 12) -> requests.Response:
    sess = _get_http_session()
    max_attempts = 5
    base_sleep = 2.0

    last_resp = None
    for attempt in range(1, max_attempts + 1):
        _throttle(url)
        resp = sess.get(url, headers=headers, timeout=timeout)
        last_resp = resp

        if resp.status_code < 400:
            return resp

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    sleep_s = float(ra)
                except Exception:
                    sleep_s = base_sleep * (2 ** (attempt - 1))
            else:
                sleep_s = base_sleep * (2 ** (attempt - 1))

            sleep_s = sleep_s + random.random() * 0.75
            time.sleep(min(sleep_s, 60))
            continue

        if resp.status_code in (500, 502, 503, 504):
            sleep_s = base_sleep * (2 ** (attempt - 1)) + random.random() * 0.75
            time.sleep(min(sleep_s, 30))
            continue

        break

    return last_resp


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def _fetch_market_prices(link: str) -> dict:
    """
    Supports BOTH:
      - pricecharting.com
      - sportscardspro.com

    Returns dict with:
      raw  = ungraded
      psa9 = PSA 9 (or Grade 9)
      psa10 = PSA 10 (or Grade 10)

    Also includes:
      _debug = "success" or a reason string (why prices are 0)
    """
    out = {"raw": 0.0, "psa9": 0.0, "psa10": 0.0, "_debug": "unknown"}

    if not link:
        out["_debug"] = "no_link"
        return out

    url = _canonicalize_reference_link(link)
    u = url.lower()

    if ("pricecharting.com" not in u) and ("sportscardspro.com" not in u):
        out["_debug"] = "unsupported_domain"
        return out

    def _parse_money(s: str) -> float:
        if not s:
            return 0.0
        m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", s)
        if not m:
            return 0.0
        try:
            return float(m.group(1).replace(",", ""))
        except Exception:
            return 0.0

    def _pick_from_map(m: dict, labels) -> float:
        if not m:
            return 0.0
        for lab in labels:
            if lab in m:
                return float(m.get(lab, 0.0) or 0.0)
        lower_map = {k.lower(): k for k in m.keys()}
        for lab in labels:
            k = lower_map.get(lab.lower())
            if k:
                return float(m.get(k, 0.0) or 0.0)
        return 0.0

    def _looks_like_bot_or_block(text: str) -> bool:
        t = (text or "").lower()
        bad = [
            "access denied",
            "request blocked",
            "captcha",
            "unusual traffic",
            "verify you are a human",
            "cloudflare",
            "attention required",
        ]
        return any(x in t for x in bad)

    def _extract_price_map_from_price_cells(soup: BeautifulSoup) -> tuple[dict, set]:
        price_map = {}
        labels_seen = set()

        cells = soup.select(".price.js-price")
        if not cells:
            return price_map, labels_seen

        for cell in cells:
            price_val = _parse_money(cell.get_text(" ", strip=True))

            label = ""
            tr = cell.find_parent("tr")
            if tr:
                tds = tr.find_all(["td", "th"])
                if len(tds) >= 1:
                    label = tds[0].get_text(" ", strip=True)

            if not label:
                prev = cell.find_previous(["td", "th"])
                if prev:
                    label = prev.get_text(" ", strip=True)

            label = re.sub(r"\s+", " ", (label or "").strip())
            if label:
                labels_seen.add(label)
                price_map[label] = float(price_val or 0.0)

        return price_map, labels_seen

    def _extract_price_map_generic_table(soup: BeautifulSoup) -> tuple[dict, set]:
        price_map = {}
        labels_seen = set()

        for tr in soup.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            label = re.sub(r"\s+", " ", (cells[0].get_text(" ", strip=True) or "")).strip()
            if not label:
                continue

            price_val = 0.0
            for c in cells[1:]:
                pv = _parse_money(c.get_text(" ", strip=True))
                if pv > 0:
                    price_val = pv
                    break

            if price_val == 0.0:
                pcell = tr.select_one("td.price, span.price, div.price")
                if pcell:
                    price_val = _parse_money(pcell.get_text(" ", strip=True))

            if price_val != 0.0:
                labels_seen.add(label)
                price_map[label] = float(price_val or 0.0)

        return price_map, labels_seen

    def _extract_prices_from_jsonld(soup: BeautifulSoup) -> dict:
        result = {}
        scripts = soup.find_all("script", type="application/ld+json")
        for sc in scripts:
            try:
                txt = sc.string or ""
                if not txt.strip():
                    continue
                data = json.loads(txt)
                items = data if isinstance(data, list) else [data]
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    offers = it.get("offers")
                    if isinstance(offers, dict):
                        price = offers.get("price")
                        if price is not None:
                            try:
                                result["raw"] = float(price)
                            except Exception:
                                pass
                    elif isinstance(offers, list):
                        for off in offers:
                            if isinstance(off, dict) and off.get("price") is not None:
                                try:
                                    result["raw"] = float(off.get("price"))
                                    break
                                except Exception:
                                    pass
            except Exception:
                continue
        return result

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

        r = _http_get_with_backoff(url, headers=headers, timeout=12)
        if r is None:
            out["_debug"] = "exception"
            return out

        if r.status_code == 429:
            out["_debug"] = "http_error_429"
            return out
        if r.status_code >= 400:
            out["_debug"] = f"http_error_{r.status_code}"
            return out

        if _looks_like_bot_or_block((r.text or "")[:5000]):
            out["_debug"] = "blocked_or_captcha"
            return out

        soup = BeautifulSoup(r.text, "lxml")

        price_map, labels_seen = _extract_price_map_from_price_cells(soup)

        raw_val = _pick_from_map(price_map, ["Ungraded", "Raw"])
        psa9_val = _pick_from_map(price_map, ["PSA 9", "Grade 9"])
        psa10_val = _pick_from_map(price_map, ["PSA 10", "Grade 10"])

        target_labels_present = any(
            lab.lower() in {s.lower() for s in labels_seen}
            for lab in ["Ungraded", "Raw", "PSA 9", "Grade 9", "PSA 10", "Grade 10"]
        )

        if raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0:
            pm2, ls2 = _extract_price_map_generic_table(soup)
            if pm2:
                price_map = {**price_map, **pm2}
                labels_seen = set(list(labels_seen) + list(ls2))

            raw_val = raw_val or _pick_from_map(price_map, ["Ungraded", "Raw"])
            psa9_val = psa9_val or _pick_from_map(price_map, ["PSA 9", "Grade 9"])
            psa10_val = psa10_val or _pick_from_map(price_map, ["PSA 10", "Grade 10"])

            target_labels_present = target_labels_present or any(
                lab.lower() in {s.lower() for s in labels_seen}
                for lab in ["Ungraded", "Raw", "PSA 9", "Grade 9", "PSA 10", "Grade 10"]
            )

        if raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0:
            j = _extract_prices_from_jsonld(soup)
            if "raw" in j and j["raw"] > 0:
                out["_debug"] = "success_jsonld_raw_only"
                out["raw"] = float(j["raw"] or 0.0)
                out["psa9"] = 0.0
                out["psa10"] = 0.0
                return out

        if raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0:
            text = soup.get_text("\n", strip=True)

            def _money_regex(pattern: str) -> float:
                m = re.search(pattern, text, flags=re.IGNORECASE)
                if not m:
                    return 0.0
                try:
                    return float(m.group(1).replace(",", ""))
                except Exception:
                    return 0.0

            raw_val = _money_regex(r"(?:Ungraded|Raw)\b[^$]{0,120}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
            psa10_val = _money_regex(r"(?:PSA\s*10|Grade\s*10)\b[^$]{0,120}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
            psa9_val = _money_regex(r"(?:PSA\s*9|Grade\s*9)\b[^$]{0,120}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")

            if raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0:
                out["_debug"] = "parse_failed_no_prices_found"
            else:
                out["_debug"] = "success_text_fallback"
        else:
            if target_labels_present and (raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0):
                out["_debug"] = "no_sales_data_all_targets_0"
            else:
                out["_debug"] = "success"

        out["raw"] = float(raw_val or 0.0)
        out["psa9"] = float(psa9_val or 0.0)
        out["psa10"] = float(psa10_val or 0.0)
        return out

    except requests.Timeout:
        out["_debug"] = "timeout"
        return out
    except Exception:
        out["_debug"] = "exception"
        return out


def _repull_market_values_to_inventory_sheet():
    """
    Runs on Dashboard Refresh:
    - reads inventory sheet rows
    - computes market_price (raw/ungraded) and market_value (grade-selected) from PriceCharting or SportsCardsPro
    - writes back to inventory in ONE column update per market col (quota friendly)
    - writes a debug status column describing success / why 0 was returned

    ✅ New behavior:
    - If market_price_updated_at is within last 12 hours, we SKIP re-scraping
      UNLESS current market_price is 0.
    - If the last debug was a 429 recently, we SKIP for a cooldown window (prevents hammering).
    - We do NOT clear _fetch_market_prices cache here anymore (clearing forces re-hit and causes 429s).
    """
    ws = _open_ws(st.secrets.get("inventory_worksheet", "inventory"))
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return 0

    header = [h.strip() for h in values[0]]
    rows = values[1:]
    nrows = len(rows)

    def base(h):
        return h.split("__dup")[0] if "__dup" in h else h

    def norm(h: str) -> str:
        return _norm_key(h)

    def col_idx(name: str):
        target = norm(name)
        for i, h in enumerate(header):
            if norm(base(h)) == target:
                return i
        return None

    need = [
        "reference_link",
        "inventory_status",
        "product_type",
        "grading_company",
        "grade",
        "condition",
        "market_price",
        "market_value",
        "market_price_updated_at",
        "market_price_debug",
    ]

    changed = False
    for nm in need:
        if col_idx(nm) is None:
            header.append(nm)
            changed = True
    if changed:
        ws.update("1:1", [header], value_input_option="USER_ENTERED")
        for i in range(len(rows)):
            if len(rows[i]) < len(header):
                rows[i] = rows[i] + [""] * (len(header) - len(rows[i]))

    i_ref = col_idx("reference_link")
    i_status = col_idx("inventory_status")
    i_pt = col_idx("product_type")
    i_comp = col_idx("grading_company")
    i_grade = col_idx("grade")
    i_cond = col_idx("condition")

    i_mp = col_idx("market_price")
    i_mv = col_idx("market_value")
    i_mpu = col_idx("market_price_updated_at")
    i_dbg = col_idx("market_price_debug")

    if any(x is None for x in [i_ref, i_mp, i_mv, i_mpu, i_dbg]):
        raise RuntimeError("Inventory sheet is missing required market columns after header update.")

    market_price_raw = []
    market_value_sel = []
    market_updated_ats = []
    market_debug = []

    now_utc = pd.Timestamp.utcnow()
    now_iso = now_utc.isoformat()

    RECENT_HOURS = 12
    COOLDOWN_429_HOURS = 6
    updated = 0

    for r in rows:
        link = (r[i_ref] if i_ref is not None and i_ref < len(r) else "").strip()
        link_canon = _canonicalize_reference_link(link)
        ll = link_canon.lower()

        cur_mp = _to_num(r[i_mp]) if i_mp is not None and i_mp < len(r) else 0.0
        cur_mv = _to_num(r[i_mv]) if i_mv is not None and i_mv < len(r) else 0.0
        cur_mpu = (r[i_mpu] if i_mpu is not None and i_mpu < len(r) else "").strip()
        cur_dbg = (r[i_dbg] if i_dbg is not None and i_dbg < len(r) else "").strip().lower()

        mpu_dt = _to_dt(cur_mpu) if cur_mpu else pd.NaT
        age_hours = None
        if pd.notna(mpu_dt):
            try:
                age_hours = (now_utc - pd.Timestamp(mpu_dt)).total_seconds() / 3600.0
            except Exception:
                age_hours = None

        if age_hours is not None and 0 <= age_hours < COOLDOWN_429_HOURS and "http_error_429" in cur_dbg:
            market_price_raw.append([float(cur_mp or 0.0)])
            market_value_sel.append([float(cur_mv or 0.0)])
            market_updated_ats.append([cur_mpu])
            market_debug.append(["skipped_recent_429_cooldown"])
            continue

        is_recent = False
        if age_hours is not None:
            is_recent = (age_hours >= 0 and age_hours < RECENT_HOURS)

        if is_recent and float(cur_mp or 0.0) > 0.0:
            market_price_raw.append([float(cur_mp or 0.0)])
            market_value_sel.append([float(cur_mv or 0.0)])
            market_updated_ats.append([cur_mpu])
            market_debug.append(["skipped_recent_under_12h"])
            continue

        if not link_canon:
            market_price_raw.append([0.0])
            market_value_sel.append([0.0])
            market_updated_ats.append([""])
            market_debug.append(["no_link"])
            continue

        if ("pricecharting.com" not in ll) and ("sportscardspro.com" not in ll):
            market_price_raw.append([0.0])
            market_value_sel.append([0.0])
            market_updated_ats.append([""])
            market_debug.append(["unsupported_domain"])
            continue

        status = (r[i_status] if i_status is not None and i_status < len(r) else "").strip().upper()
        pt = (r[i_pt] if i_pt is not None and i_pt < len(r) else "").strip().lower()
        comp = (r[i_comp] if i_comp is not None and i_comp < len(r) else "").strip()
        grade = (r[i_grade] if i_grade is not None and i_grade < len(r) else "").strip().upper()
        cond = (r[i_cond] if i_cond is not None and i_cond < len(r) else "").strip().lower()

        prices = _fetch_market_prices(link_canon)

        is_sealed = "sealed" in pt
        is_grading = (status == "GRADING")
        is_graded = ("graded" in pt) or bool(comp) or bool(grade) or ("graded" in cond)

        raw_val = float(prices.get("raw", 0.0) or 0.0)
        psa9_val = float(prices.get("psa9", 0.0) or 0.0)
        psa10_val = float(prices.get("psa10", 0.0) or 0.0)

        mv = raw_val
        chosen = "raw"
        if (not is_sealed) and (not is_grading) and is_graded:
            if "10" in grade:
                mv = psa10_val
                chosen = "psa10_or_grade10"
                if mv <= 0 and raw_val > 0:
                    mv = raw_val
                    chosen = "psa10_missing_fallback_to_raw"
            elif "9" in grade:
                mv = psa9_val
                chosen = "psa9_or_grade9"
                if mv <= 0 and raw_val > 0:
                    mv = raw_val
                    chosen = "psa9_missing_fallback_to_raw"

        market_price_raw.append([float(mv or 0.0)])
        market_value_sel.append([float(mv or 0.0)])
        market_updated_ats.append([now_iso])

        dbg = prices.get("_debug", "unknown")
        if dbg.startswith("success") or dbg == "success":
            if (raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0):
                market_debug.append(["no_sales_data_or_targets_missing"])
            else:
                market_debug.append([f"success ({chosen})"])
        else:
            market_debug.append([f"{dbg} ({chosen})"])

        updated += 1

    def a1_col_letter(n: int) -> str:
        letters = ""
        while n:
            n, r = divmod(n - 1, 26)
            letters = chr(65 + r) + letters
        return letters

    mp_col_letter = a1_col_letter(i_mp + 1)
    ws.update(f"{mp_col_letter}2:{mp_col_letter}{nrows+1}", market_price_raw, value_input_option="USER_ENTERED")

    mv_col_letter = a1_col_letter(i_mv + 1)
    ws.update(f"{mv_col_letter}2:{mv_col_letter}{nrows+1}", market_value_sel, value_input_option="USER_ENTERED")

    mpu_col_letter = a1_col_letter(i_mpu + 1)
    ws.update(f"{mpu_col_letter}2:{mpu_col_letter}{nrows+1}", market_updated_ats, value_input_option="USER_ENTERED")

    dbg_col_letter = a1_col_letter(i_dbg + 1)
    ws.update(f"{dbg_col_letter}2:{dbg_col_letter}{nrows+1}", market_debug, value_input_option="USER_ENTERED")

    return updated


def _styler_table_header():
    return [
        {"selector": "th", "props": [("background-color", "#0f172a"), ("color", "white"), ("font-weight", "800")]},
        {"selector": "td", "props": [("font-weight", "500")]},
    ]


def _style_group_and_total_rows(df: pd.DataFrame, first_col: str):
    def _row_style(row):
        v = _safe_str(row.get(first_col, ""))
        if v.strip().lower() in {"totals", "total"}:
            return ["background-color: #dbeafe; font-weight: 900;"] * len(row)
        if v.startswith("  "):
            return [""] * len(row)
        return ["background-color: #eef2ff; font-weight: 800;"] * len(row)

    return df.style.apply(_row_style, axis=1)


if "refresh_token" not in st.session_state:
    st.session_state["refresh_token"] = 0


@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_sheet_df(worksheet_name: str, refresh_token: int = 0) -> pd.DataFrame:
    ws = _open_ws(worksheet_name)
    values = ws.get_all_values()

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

    df = pd.DataFrame(norm_rows, columns=fixed)
    return df


# =========================
# Refresh button
# =========================
top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Refresh from Sheets", use_container_width=True):
        try:
            n = _repull_market_values_to_inventory_sheet()
            st.success(f"Market values refreshed for {n} row(s). Reloading…")
        except Exception as e:
            st.warning(f"Market refresh ran into an issue: {e}. Reloading anyway…")

        st.session_state["refresh_token"] += 1
        st.rerun()


# =========================
# Sheet names (defaults)
# =========================
INV_WS = st.secrets.get("inventory_worksheet", "inventory")
TXN_WS = st.secrets.get("transactions_worksheet", "transactions")
GRD_WS = st.secrets.get("grading_worksheet", "grading")
MISC_WS = st.secrets.get("misc_worksheet", "misc")


# =========================
# Load all data
# =========================
inv = load_sheet_df(INV_WS, st.session_state["refresh_token"])
txn_all = load_sheet_df(TXN_WS, st.session_state["refresh_token"])
txn = txn_all.copy()
grd = load_sheet_df(GRD_WS, st.session_state["refresh_token"])
misc = load_sheet_df(MISC_WS, st.session_state["refresh_token"])


# =========================
# Normalize Inventory
# =========================
if inv.empty:
    inv = pd.DataFrame()

if not inv.empty:
    inv = _ensure_unique_columns(inv)

    inv_id_col = _pick_col(inv, "inventory_id", "inventory_id")
    inv_status_col = _pick_col(inv, "inventory_status", "inventory_status")
    inv_total_col = _pick_col(inv, "total_price", "total_price")
    inv_purchase_date_col = _pick_col(inv, "purchase_date", "purchase_date")
    inv_ref_col = _pick_col(inv, "reference_link", "reference_link")

    inv_product_type_col = "product_type" if "product_type" in inv.columns else _pick_col(inv, "product_type", "product_type")
    inv_card_type_col = "card_type" if "card_type" in inv.columns else _pick_col(inv, "card_type", "card_type")
    inv_grade_col = _pick_col(inv, "grade", "grade")
    inv_company_col = _pick_col(inv, "grading_company", "grading_company")
    inv_condition_col = _pick_col(inv, "condition", "condition")

    inv_purchased_from_col = (
        _pick_col(inv, "purchased_from", None)
        or _pick_col(inv, "purchase_from", None)
        or _pick_col(inv, "source", None)
        or "purchased_from"
    )

    inv_market_col = _pick_col(inv, "market_value", None) or _pick_col(inv, "market_price", None)

    for needed in [inv_id_col, inv_status_col, inv_total_col, inv_purchase_date_col, inv_ref_col]:
        if needed not in inv.columns:
            inv[needed] = ""

    for needed in [inv_product_type_col, inv_card_type_col, inv_grade_col, inv_company_col, inv_condition_col, inv_purchased_from_col]:
        if needed not in inv.columns:
            inv[needed] = ""

    inv[inv_status_col] = inv[inv_status_col].replace("", "ACTIVE").fillna("ACTIVE").astype(str)
    inv[inv_total_col] = _to_num(inv[inv_total_col])
    inv["__purchase_dt"] = _to_dt(inv[inv_purchase_date_col])

    inv["__market_price"] = 0.0
    if inv_market_col and inv_market_col in inv.columns:
        inv["__market_price"] = _to_num(inv[inv_market_col])

    # Inventory is now the source of truth for sales. These columns may not
    # exist on older sheets until the migration or updated pages add them.
    inv_sold_date_col = _pick_col(inv, "sold_date", None) or _pick_col(inv, "sale_date", None)
    inv_sold_price_col = _pick_col(inv, "sold_price", None) or _pick_col(inv, "sale_price", None) or _pick_col(inv, "sell_price", None)
    inv_fees_col = _pick_col(inv, "fees", None)
    inv_shipping_charged_col = _pick_col(inv, "shipping_charged", None)
    inv_fees_total_col = _pick_col(inv, "fees_total", None) or _pick_col(inv, "total_fees", None)
    inv_net_col = _pick_col(inv, "net_proceeds", None) or _pick_col(inv, "net", None)
    inv_profit_col = _pick_col(inv, "profit", None) or _pick_col(inv, "profit_loss", None)
    inv_sale_channel_col = _pick_col(inv, "sale_channel", None)
    inv_show_name_col = _pick_col(inv, "show_name", None)

else:
    inv_id_col = "inventory_id"
    inv_status_col = "inventory_status"
    inv_total_col = "total_price"
    inv_purchase_date_col = "purchase_date"
    inv_product_type_col = "product_type"
    inv_card_type_col = "card_type"
    inv_grade_col = "grade"
    inv_company_col = "grading_company"
    inv_condition_col = "condition"
    inv_purchased_from_col = "purchased_from"

    inv_sold_date_col = None
    inv_sold_price_col = None
    inv_fees_col = None
    inv_shipping_charged_col = None
    inv_fees_total_col = None
    inv_net_col = None
    inv_profit_col = None
    inv_sale_channel_col = None
    inv_show_name_col = None

    inv = pd.DataFrame(columns=[
        inv_id_col, inv_status_col, inv_total_col, inv_purchase_date_col,
        inv_product_type_col, inv_card_type_col, inv_grade_col, inv_company_col, inv_condition_col,
        "__purchase_dt", "__market_price"
    ])


# =========================
# Normalize Transactions
# =========================
if txn.empty:
    txn = pd.DataFrame()

if not txn.empty:
    txn = _ensure_unique_columns(txn)

    tx_date_col = _pick_col(txn, "sold_date", None) or _pick_col(txn, "sale_date", None) or _pick_col(txn, "date", None)
    tx_inv_col = _pick_col(txn, "inventory_id", None) or _pick_col(txn, "inv_id", None)
    tx_sold_price_col = _pick_col(txn, "sold_price", None) or _pick_col(txn, "sale_price", None) or _pick_col(txn, "price", None)

    tx_fees_total_col = _pick_col(txn, "fees_total", None)
    tx_fees_col = _pick_col(txn, "fees", None) or _pick_col(txn, "platform_fees", None) or _pick_col(txn, "fee", None)

    tx_card_type_col = "card_type" if "card_type" in txn.columns else _pick_col(txn, "card_type", None)
    tx_product_type_col = "Product Type" if "Product Type" in txn.columns else _pick_col(txn, "product_type", None)

    if tx_date_col is None:
        txn["__sold_dt"] = pd.NaT
    else:
        txn["__sold_dt"] = _to_dt(txn[tx_date_col])

    if tx_inv_col is None:
        txn["__inventory_id"] = ""
    else:
        txn["__inventory_id"] = txn[tx_inv_col].apply(lambda x: _safe_str(x).strip())

    if tx_sold_price_col is None:
        txn["__sold_price"] = 0.0
    else:
        txn["__sold_price"] = _to_num(txn[tx_sold_price_col])

    if tx_fees_total_col and tx_fees_total_col in txn.columns:
        txn["__fees"] = _to_num(txn[tx_fees_total_col])
    elif tx_fees_col and tx_fees_col in txn.columns:
        txn["__fees"] = _to_num(txn[tx_fees_col])
    else:
        txn["__fees"] = 0.0

    tx_status_col = _pick_col(txn, "status", None) or _pick_col(txn, "tx_status", None)
    tx_net_proceeds_col = _pick_col(txn, "net_proceeds", None) or _pick_col(txn, "net", None)
    tx_ship_charged_col = _pick_col(txn, "shipping_charged", None) or _pick_col(txn, "shipping", None)

    if tx_status_col and tx_status_col in txn.columns:
        txn["__status"] = txn[tx_status_col].astype(str).str.upper().str.strip()
        txn = txn[txn["__status"].eq("SOLD")].copy()

    if tx_ship_charged_col and tx_ship_charged_col in txn.columns:
        txn["__ship_charged"] = _to_num(txn[tx_ship_charged_col])
    else:
        txn["__ship_charged"] = 0.0

    tx_total_fees_col = (
        _pick_col(txn, "total_fees", None)
        or _pick_col(txn, "fees_total", None)
        or _pick_col(txn, "total_fee", None)
    )
    tx_profit_col = _pick_col(txn, "profit", None)
    tx_all_in_cost_col = (
        "All In Cost" if "All In Cost" in txn.columns else
        _pick_col(txn, "all_in_cost", None)
        or _pick_col(txn, "all_in", None)
        or _pick_col(txn, "cogs", None)
        or _pick_col(txn, "cost_of_goods", None)
    )

    txn["__dollar_sales"] = _to_num(txn["__sold_price"])

    if tx_total_fees_col and tx_total_fees_col in txn.columns:
        txn["__total_fees"] = _to_num(txn[tx_total_fees_col])
    else:
        txn["__total_fees"] = (txn["__fees"] + txn["__ship_charged"]).fillna(0.0)

    # Proceeds are always gross sales minus total selling fees/shipping costs.
    # Do not trust stored net/profit fields here because older migrations wrote
    # incorrect values for show sales.
    txn["__net"] = (txn["__dollar_sales"] - txn["__total_fees"]).fillna(0.0)

    # Profit is recalculated after COGS is resolved from inventory/all-in cost.
    txn["__profit"] = np.nan

    if tx_all_in_cost_col and tx_all_in_cost_col in txn.columns:
        txn["__all_in_cost"] = _to_num(txn[tx_all_in_cost_col])
    else:
        txn["__all_in_cost"] = np.nan

    txn = txn[~(txn["__sold_dt"].isna() & (txn["__sold_price"] <= 0) & (txn["__net"] <= 0))].copy()

    txn["__sold_month"] = _month_start(txn["__sold_dt"])

    if tx_card_type_col and tx_card_type_col in txn.columns:
        txn["__txn_card_type"] = txn[tx_card_type_col].apply(_normalize_card_type)
    else:
        txn["__txn_card_type"] = ""

    if tx_product_type_col and tx_product_type_col in txn.columns:
        txn["__txn_product_type"] = txn[tx_product_type_col].astype(str).fillna("")
    else:
        txn["__txn_product_type"] = ""

else:
    tx_date_col = None
    tx_inv_col = None
    txn = pd.DataFrame(columns=[
        "__sold_dt",
        "__sold_month",
        "__inventory_id",
        "__sold_price",
        "__fees",
        "__ship_charged",
        "__total_fees",
        "__dollar_sales",
        "__net",
        "__profit",
        "__all_in_cost",
        "__txn_card_type",
        "__txn_product_type",
    ])


# =========================
# Inventory-first sales ledger
# =========================
def _build_inventory_sales_ledger(inv_df: pd.DataFrame) -> pd.DataFrame:
    if inv_df is None or inv_df.empty:
        return pd.DataFrame(columns=[
            "__sold_dt", "__sold_month", "__inventory_id", "__sold_price", "__fees",
            "__ship_charged", "__total_fees", "__dollar_sales", "__net", "__profit",
            "__all_in_cost", "__txn_card_type", "__txn_product_type", "__sale_channel", "__show_name"
        ])

    d = inv_df.copy()
    if inv_id_col not in d.columns:
        return pd.DataFrame()

    d["__inventory_id"] = d[inv_id_col].apply(lambda x: _safe_str(x).strip())
    status = d[inv_status_col].astype(str).str.upper().str.strip() if inv_status_col in d.columns else pd.Series("", index=d.index)

    if inv_sold_price_col and inv_sold_price_col in d.columns:
        d["__sold_price"] = _to_num(d[inv_sold_price_col])
    else:
        d["__sold_price"] = 0.0

    if inv_sold_date_col and inv_sold_date_col in d.columns:
        d["__sold_dt"] = _to_dt(d[inv_sold_date_col])
    else:
        d["__sold_dt"] = pd.NaT

    # Include rows explicitly marked SOLD with either a sold price or a sold date.
    d = d[(status.eq("SOLD")) & ((d["__sold_price"] > 0) | d["__sold_dt"].notna())].copy()
    if d.empty:
        return pd.DataFrame()

    d["__fees"] = _to_num(d[inv_fees_col]) if inv_fees_col and inv_fees_col in d.columns else 0.0
    d["__ship_charged"] = _to_num(d[inv_shipping_charged_col]) if inv_shipping_charged_col and inv_shipping_charged_col in d.columns else 0.0

    if inv_fees_total_col and inv_fees_total_col in d.columns:
        d["__total_fees"] = _to_num(d[inv_fees_total_col])
        d["__total_fees"] = np.where(d["__total_fees"] > 0, d["__total_fees"], d["__fees"] + d["__ship_charged"])
    else:
        d["__total_fees"] = d["__fees"] + d["__ship_charged"]

    d["__dollar_sales"] = d["__sold_price"]

    # Proceeds are always gross sales minus fees. Ignore stored net_proceeds
    # because prior migration versions could write bad values.
    d["__net"] = (d["__dollar_sales"] - d["__total_fees"]).fillna(0.0)

    # COGS: prefer total_cost, otherwise total_price + grading lookup.
    inv_total_cost_col = _pick_col(d, "total_cost", None) or _pick_col(d, "all_in_cost", None)
    if inv_total_cost_col and inv_total_cost_col in d.columns:
        d["__all_in_cost"] = _to_num(d[inv_total_cost_col])
        fallback_cost = _to_num(d[inv_total_col]) if inv_total_col in d.columns else 0.0
        d["__all_in_cost"] = np.where(d["__all_in_cost"] > 0, d["__all_in_cost"], fallback_cost)
    else:
        d["__all_in_cost"] = _to_num(d[inv_total_col]) if inv_total_col in d.columns else 0.0

    # Profit is always proceeds minus cost of goods. Do not trust stored profit
    # fields because show-sale migrations previously wrote sold_price as profit.
    d["__profit"] = (d["__net"] - d["__all_in_cost"]).fillna(0.0)

    d["__sold_month"] = _month_start(d["__sold_dt"])
    d["__txn_card_type"] = d[inv_card_type_col].apply(_normalize_card_type) if inv_card_type_col in d.columns else ""
    d["__txn_product_type"] = d[inv_product_type_col].astype(str).fillna("") if inv_product_type_col in d.columns else ""
    d["__sale_channel"] = d[inv_sale_channel_col].astype(str).fillna("") if inv_sale_channel_col and inv_sale_channel_col in d.columns else ""
    d["__show_name"] = d[inv_show_name_col].astype(str).fillna("") if inv_show_name_col and inv_show_name_col in d.columns else ""

    return d

# Replace transaction-derived sales with inventory-derived sales wherever possible.
# Legacy transaction rows remain as a fallback only for inventory_ids that do not
# yet have sold fields populated in inventory.
_inventory_sales = _build_inventory_sales_ledger(inv)
if not _inventory_sales.empty:
    inv_sold_ids = set(_inventory_sales["__inventory_id"].astype(str).str.strip())
    if not txn.empty and "__inventory_id" in txn.columns:
        txn_legacy_only = txn[~txn["__inventory_id"].astype(str).str.strip().isin(inv_sold_ids)].copy()
        txn = pd.concat([_inventory_sales, txn_legacy_only], ignore_index=True, sort=False)
    else:
        txn = _inventory_sales.copy()



# =========================
# LIST PRICE LOOKUP (from txn_all)
# =========================
list_price_by_inv_id = {}

if "txn_all" in locals() and isinstance(txn_all, pd.DataFrame) and not txn_all.empty:
    txa = _ensure_unique_columns(txn_all.copy())

    txa_status_col = _pick_col(txa, "status", None) or _pick_col(txa, "tx_status", None)
    txa_inv_col = _pick_col(txa, "inventory_id", None) or _pick_col(txa, "inv_id", None)

    txa_list_price_col = (
        _pick_col(txa, "list_price", None)
        or _pick_col(txa, "listed_price", None)
        or _pick_col(txa, "asking_price", None)
        or _pick_col(txa, "price", None)
        or _pick_col(txa, "amount", None)
    )

    txa_dt_col = (
        _pick_col(txa, "listed_date", None)
        or _pick_col(txa, "date", None)
        or _pick_col(txa, "created_at", None)
        or _pick_col(txa, "timestamp", None)
    )

    if txa_status_col and txa_inv_col and txa_list_price_col:
        txa["__status"] = txa[txa_status_col].astype(str).str.upper().str.strip()
        txa["__inventory_id"] = txa[txa_inv_col].apply(lambda x: _safe_str(x).strip())

        txa = txa[txa["__status"].eq("LISTED")].copy()
        txa["__list_price"] = _to_num(txa[txa_list_price_col])

        if txa_dt_col and txa_dt_col in txa.columns:
            txa["__dt"] = _to_dt(txa[txa_dt_col])
        else:
            txa["__dt"] = pd.NaT
            txa["__row"] = np.arange(len(txa))

        sort_cols = ["__inventory_id"]
        if "__row" in txa.columns:
            sort_cols += ["__row"]
        else:
            sort_cols += ["__dt"]
        txa = txa.sort_values(by=sort_cols, na_position="last")
        last_rows = txa.groupby("__inventory_id", as_index=False).tail(1)

        list_price_by_inv_id = last_rows.set_index("__inventory_id")["__list_price"].to_dict()


# =========================
# Normalize Grading
# =========================
if grd.empty:
    grd = pd.DataFrame()

if not grd.empty:
    grd = _ensure_unique_columns(grd)

    g_status_col = _pick_col(grd, "status", "status")
    g_sub_dt_col = _pick_col(grd, "submission_date", None) or _pick_col(grd, "created_at", None)
    g_est_ret_col = _pick_col(grd, "estimated_return_date", "estimated_return_date")

    g_inv_col = _pick_col(grd, "inventory_id", "inventory_id")

    g_psa10_col = _pick_col(grd, "psa10_price", "psa10_price")
    g_psa9_col = _pick_col(grd, "psa9_price", "psa9_price")

    g_fee_init_col = _pick_col(grd, "grading_fee_initial", "grading_fee_initial")
    g_add_col = _pick_col(grd, "additional_costs", "additional_costs")

    g_fee_per_card_col = _pick_col(grd, "grading_fee_per_card", None)
    g_extra_costs_col = _pick_col(grd, "extra_costs", None)

    g_purchase_total_col = _pick_col(grd, "purchase_total", None) or _pick_col(grd, "purchase_price", None) or "purchase_total"

    for c in [g_status_col, g_est_ret_col, g_inv_col]:
        if c not in grd.columns:
            grd[c] = ""

    for c in [g_psa10_col, g_psa9_col, g_fee_init_col, g_add_col, g_purchase_total_col]:
        if c not in grd.columns:
            grd[c] = 0.0

    if g_fee_per_card_col and g_fee_per_card_col in grd.columns:
        base = grd[g_fee_init_col].astype(str)
        fb = grd[g_fee_per_card_col].astype(str)
        grd[g_fee_init_col] = base.where(base.str.strip() != "", fb)

    if g_extra_costs_col and g_extra_costs_col in grd.columns:
        base = grd[g_add_col].astype(str)
        fb = grd[g_extra_costs_col].astype(str)
        grd[g_add_col] = base.where(base.str.strip() != "", fb)

    grd["__status"] = grd[g_status_col].replace("", "SUBMITTED").fillna("SUBMITTED").astype(str).str.upper()
    grd["__est_return_dt"] = _to_dt(grd[g_est_ret_col])
    grd["__est_return_month"] = _month_start(grd["__est_return_dt"])

    grd["__psa10"] = _to_num(grd[g_psa10_col])
    grd["__psa9"] = _to_num(grd[g_psa9_col])

    grd["__grading_cost"] = _to_num(grd[g_fee_init_col]) + _to_num(grd[g_add_col])
    grd["__purchase_total"] = _to_num(grd[g_purchase_total_col])

    if g_sub_dt_col and g_sub_dt_col in grd.columns:
        grd["__grading_dt"] = _to_dt(grd[g_sub_dt_col])
    else:
        grd["__grading_dt"] = pd.NaT
    grd["__grading_month"] = _month_start(grd["__grading_dt"])

    open_grading = grd[grd["__status"].isin(["SUBMITTED", "IN_GRADING", "SENT", "IN_TRANSIT"])].copy()
else:
    grd = pd.DataFrame(columns=["__status", "__est_return_dt", "__est_return_month", "__psa10", "__psa9", "__grading_cost", "__purchase_total", "__grading_dt", "__grading_month"])
    open_grading = grd.copy()


# =========================
# Normalize Misc
# =========================
if misc.empty:
    misc = pd.DataFrame()

if not misc.empty:
    misc = _ensure_unique_columns(misc)

    m_date_col = _pick_col(misc, "date", None) or _pick_col(misc, "expense_date", None) or "date"
    m_amt_col = _pick_col(misc, "amount", None) or _pick_col(misc, "cost", None) or "amount"
    m_cat_col = _pick_col(misc, "category", None) or _pick_col(misc, "type", None) or "category"

    if m_date_col not in misc.columns:
        misc[m_date_col] = ""
    if m_amt_col not in misc.columns:
        misc[m_amt_col] = 0.0
    if m_cat_col not in misc.columns:
        misc[m_cat_col] = ""

    misc["__dt"] = _to_dt(misc[m_date_col])
    misc["__month"] = _month_start(misc["__dt"])
    misc["__amount"] = _to_num(misc[m_amt_col])
    misc["__category"] = misc[m_cat_col].astype(str).replace("", "Misc").fillna("Misc")
else:
    misc = pd.DataFrame(columns=["__dt", "__month", "__amount", "__category"])


# =========================
# Build Year/Month filter options
# =========================
def _build_year_month_options():
    months = []

    if not inv.empty and "__purchase_dt" in inv.columns:
        months.append(_month_start(inv["__purchase_dt"]).dropna())
    if not txn.empty and "__sold_dt" in txn.columns:
        months.append(_month_start(txn["__sold_dt"]).dropna())
    if not grd.empty and "__grading_dt" in grd.columns:
        months.append(_month_start(grd["__grading_dt"]).dropna())
    if not misc.empty and "__dt" in misc.columns:
        months.append(_month_start(misc["__dt"]).dropna())

    if not months:
        return ["All"], ["All"]

    allm = pd.concat(months).dropna().unique()
    allm = pd.to_datetime(sorted(allm))
    years = sorted({int(pd.Timestamp(m).year) for m in allm})

    year_opts = ["All"] + [str(y) for y in years]
    month_opts_all = ["All"] + [pd.Timestamp(m).strftime("%Y-%m") for m in allm]

    return year_opts, month_opts_all


year_opts, month_opts_all = _build_year_month_options()


# =========================
# Tabs
# =========================
tab_bs, tab_forecast = st.tabs(["Balance Sheet", "Monthly Summary"])


# =========================================================
# TAB 1: Balance Sheet
# =========================================================
with tab_bs:
    st.subheader("Balance Sheet (Filtered)")

    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        year_choice = st.selectbox("Year", options=year_opts, index=0)

    with f2:
        if year_choice != "All":
            try:
                y = int(year_choice)
                month_opts = ["All"] + [m for m in month_opts_all[1:] if m.startswith(f"{y}-")]
            except Exception:
                month_opts = month_opts_all
        else:
            month_opts = month_opts_all

        month_choice = st.selectbox("Month", options=month_opts, index=0)

    with f3:
        purchased_from_opts = []
        if not inv.empty and inv_purchased_from_col in inv.columns:
            purchased_from_opts = sorted({
                _safe_str(x).strip()
                for x in inv[inv_purchased_from_col].dropna().tolist()
                if _safe_str(x).strip()
            })

        purchased_from_choice = st.multiselect(
            "Purchased From",
            options=purchased_from_opts,
            default=purchased_from_opts,
        )

    inv_f = _apply_period_filter(inv, "__purchase_dt", year_choice, month_choice) if not inv.empty else inv
    txn_f = _apply_period_filter(txn, "__sold_dt", year_choice, month_choice) if not txn.empty else txn
    grd_f = _apply_period_filter(grd, "__grading_dt", year_choice, month_choice) if not grd.empty else grd
    misc_f = _apply_period_filter(misc, "__dt", year_choice, month_choice) if not misc.empty else misc

    if purchased_from_choice and (not inv.empty) and (inv_purchased_from_col in inv.columns):
        if not inv_f.empty and inv_purchased_from_col in inv_f.columns:
            inv_f = inv_f[inv_f[inv_purchased_from_col].astype(str).str.strip().isin(purchased_from_choice)].copy()

        allowed_ids = set(
            inv.loc[
                inv[inv_purchased_from_col].astype(str).str.strip().isin(purchased_from_choice),
                inv_id_col
            ].astype(str).str.strip().tolist()
        )

        if not txn_f.empty and "__inventory_id" in txn_f.columns:
            txn_f = txn_f[txn_f["__inventory_id"].astype(str).str.strip().isin(allowed_ids)].copy()

    inv_by_id = {}
    if not inv.empty:
        inv_keyed = inv.copy()
        inv_keyed[inv_id_col] = inv_keyed[inv_id_col].apply(lambda x: _safe_str(x).strip())
        inv_by_id = inv_keyed.set_index(inv_id_col, drop=False).to_dict("index")

    grading_cost_by_inv_id = {}
    if not grd.empty:
        g = grd.copy()
        for col in ["inventory_id", "grading_fee_initial", "additional_costs", "status", "synced_to_inventory"]:
            if col not in g.columns:
                g[col] = ""

        g["__inv_id"] = g["inventory_id"].apply(lambda x: _safe_str(x).strip())

        def _num(v):
            try:
                s = _safe_str(v).strip().replace("$", "").replace(",", "")
                if s == "":
                    return 0.0
                return float(pd.to_numeric(s, errors="coerce") or 0.0)
            except Exception:
                return 0.0

        g["__fee"] = g["grading_fee_initial"].apply(_num)
        g["__add"] = g["additional_costs"].apply(_num)

        g["__status"] = g["status"].astype(str).str.upper().str.strip()
        g["__synced"] = g["synced_to_inventory"].astype(str).str.upper().str.strip()

        inflight = g[
            (g["__synced"] != "YES")
            & (g["__status"].isin(["SUBMITTED", "IN_GRADING", "SENT", "IN_TRANSIT", "RETURNED"]))
        ].copy()

        if not inflight.empty:
            inflight["__grading_cost"] = (inflight["__fee"] + inflight["__add"]).fillna(0.0)
            grading_cost_by_inv_id = inflight.groupby("__inv_id")["__grading_cost"].sum().to_dict()

    def _tx_card_type_from_inv(inv_id: str) -> str:
        rec = inv_by_id.get(_safe_str(inv_id).strip())
        if rec is None:
            return ""
        return _normalize_card_type(rec.get(inv_card_type_col, ""))

    def _tx_card_type_rowaware(row) -> str:
        row_ct = _normalize_card_type(row.get("__txn_card_type", ""))
        if row_ct in {"Sports", "Pokemon"}:
            return row_ct

        raw_ct = _normalize_card_type(row.get("card_type", ""))
        if raw_ct in {"Sports", "Pokemon"}:
            return raw_ct

        return _tx_card_type_from_inv(row.get("__inventory_id", ""))

    def _tx_product_bucket(inv_id: str) -> str:
        rec = inv_by_id.get(_safe_str(inv_id).strip())
        if rec is None:
            return "Cards"
        return _bucket_product(
            rec.get(inv_product_type_col, ""),
            rec.get(inv_company_col, ""),
            rec.get(inv_grade_col, ""),
            rec.get(inv_condition_col, ""),
            rec.get(inv_status_col, ""),
        )

    def _tx_product_bucket_rowaware(row) -> str:
        tx_pt = _safe_str(row.get("__txn_product_type", "")).strip().lower()
        if tx_pt:
            if "sealed" in tx_pt:
                return "Sealed"
            if "graded" in tx_pt:
                return "Graded Cards"
            return "Cards"
        return _tx_product_bucket(row.get("__inventory_id", ""))

    def _period_end_dt(year_choice: str, month_choice: str) -> pd.Timestamp:
        today = pd.Timestamp(date.today())
        if month_choice != "All":
            m = pd.to_datetime(month_choice + "-01", errors="coerce")
            if pd.notna(m):
                return (m + pd.offsets.MonthEnd(0)) + pd.Timedelta(hours=23, minutes=59, seconds=59)
        if year_choice != "All":
            try:
                y = int(year_choice)
                return pd.Timestamp(year=y, month=12, day=31, hour=23, minute=59, second=59)
            except Exception:
                pass
        return today + pd.Timedelta(hours=23, minutes=59, seconds=59)

    asof_cutoff = _period_end_dt(year_choice, month_choice)

    inv_holdings = pd.DataFrame()
    if not inv.empty and "__purchase_dt" in inv.columns:
        inv_holdings = inv.copy()
        inv_holdings[inv_id_col] = inv_holdings[inv_id_col].apply(lambda x: _safe_str(x).strip())
        inv_holdings["__status_upper"] = inv_holdings[inv_status_col].astype(str).str.upper().str.strip()

        inv_holdings = inv_holdings[
            inv_holdings["__purchase_dt"].notna()
            & (inv_holdings["__purchase_dt"] <= asof_cutoff)
            & (inv_holdings["__status_upper"].isin(["ACTIVE", "LISTED", "GRADING"]))
        ].copy()

        if purchased_from_choice and inv_purchased_from_col in inv_holdings.columns:
            inv_holdings = inv_holdings[
                inv_holdings[inv_purchased_from_col].astype(str).str.strip().isin(purchased_from_choice)
            ].copy()

    # -------------------------
    # ASSETS
    # -------------------------
    left, right = st.columns([1.15, 1.0])

    # These totals are reused in Business Summary. For this app, the business
    # summary is an operating/cash-investment view, not strict accrual P&L.
    # Therefore unsold inventory cost remains part of total business spend.
    asset_item_total = 0
    asset_cost_total = 0.0
    asset_market_value_total = 0.0

    with left:
        st.markdown("### Assets")

        if inv_holdings.empty:
            st.info("No inventory held as of the selected period end.")
            assets_df = pd.DataFrame(columns=["Inventory", "# of items", "Cost of Goods", "Market Value"])
        else:
            inv_asof = inv_holdings.copy()
            inv_asof["__card_type"] = inv_asof[inv_card_type_col].apply(_normalize_card_type)
            inv_asof = inv_asof[inv_asof["__card_type"].isin(["Sports", "Pokemon"])].copy()

            inv_asof["__bucket"] = inv_asof.apply(
                lambda r: _bucket_product(
                    r.get(inv_product_type_col, ""),
                    r.get(inv_company_col, ""),
                    r.get(inv_grade_col, ""),
                    r.get(inv_condition_col, ""),
                    r.get(inv_status_col, ""),
                ),
                axis=1,
            )
            inv_asof["__cost"] = _to_num(inv_asof[inv_total_col])

            inv_asof["__inv_id_key"] = inv_asof[inv_id_col].apply(lambda x: _safe_str(x).strip())
            inv_asof["__grading_cost_inflight"] = inv_asof["__inv_id_key"].map(grading_cost_by_inv_id).fillna(0.0)

            inv_asof["__status_upper"] = inv_asof[inv_status_col].astype(str).str.upper().str.strip()
            mask_grading = inv_asof["__status_upper"] == "GRADING"
            inv_asof.loc[mask_grading, "__cost"] = inv_asof.loc[mask_grading, "__cost"] + inv_asof.loc[mask_grading, "__grading_cost_inflight"]

            inv_asof["__mv"] = 0.0
            market_value_col_assets = _pick_col(inv_asof, "market_value", None)
            if market_value_col_assets and market_value_col_assets in inv_asof.columns:
                inv_asof["__mv"] = _to_num(inv_asof[market_value_col_assets])

            rows = []
            for ct in ["Sports", "Pokemon"]:
                sub = inv_asof[inv_asof["__card_type"].str.upper() == ct.upper()].copy()
                if sub.empty:
                    continue

                rows.append([ct, int(len(sub)), float(sub["__cost"].sum()), float(sub["__mv"].sum())])

                bucket_order = ["Cards", "Grading In-Process", "Graded Cards", "Sealed"]
                for b in bucket_order:
                    sb = sub[sub["__bucket"] == b]
                    rows.append([f"  {b}", int(len(sb)), float(sb["__cost"].sum()), float(sb["__mv"].sum())])

            assets_df = pd.DataFrame(rows, columns=["Inventory", "# of items", "Cost of Goods", "Market Value"])

            if not assets_df.empty:
                asset_item_total = int(len(inv_asof))
                asset_cost_total = float(inv_asof["__cost"].sum())
                asset_market_value_total = float(inv_asof["__mv"].sum())

                assets_df = pd.concat(
                    [
                        assets_df,
                        pd.DataFrame([{
                            "Inventory": "Totals",
                            "# of items": asset_item_total,
                            "Cost of Goods": asset_cost_total,
                            "Market Value": asset_market_value_total,
                        }])
                    ],
                    ignore_index=True
                )

        sty = (
            _style_group_and_total_rows(assets_df, "Inventory")
            .format({"Cost of Goods": "${:,.2f}", "Market Value": "${:,.2f}"})
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty, use_container_width=True, hide_index=True)

        st.markdown("### Other Expenses")

        misc_total = float(misc_f["__amount"].sum()) if not misc_f.empty else 0.0
        other_df = pd.DataFrame(
            [["Misc", int(len(misc_f)) if not misc_f.empty else 0, misc_total]],
            columns=["Other Expenses", "# of lines", "Dollar Cost"],
        )

        other_df = pd.concat(
            [
                other_df,
                pd.DataFrame([{
                    "Other Expenses": "Totals",
                    "# of lines": int(other_df["# of lines"].sum()),
                    "Dollar Cost": float(other_df["Dollar Cost"].sum()),
                }])
            ],
            ignore_index=True
        )

        sty2 = (
            _style_group_and_total_rows(other_df, "Other Expenses")
            .format({"Dollar Cost": "${:,.2f}"})
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty2, use_container_width=True, hide_index=True)

    # -------------------------
    # SALES (right side)
    # -------------------------
    with right:
        st.markdown("### Listings")

        if inv_holdings.empty:
            listed_df = pd.DataFrame(columns=["Listed Items", "# of items", "Cost of Goods", "List Price Total", "Market Value"])
        else:
            inv_listed = inv_holdings.copy()
            inv_listed["__status_upper"] = inv_listed[inv_status_col].astype(str).str.upper().str.strip()
            inv_listed = inv_listed[inv_listed["__status_upper"].eq("LISTED")].copy()

            if inv_listed.empty:
                listed_df = pd.DataFrame([{
                    "Listed Items": "Totals",
                    "# of items": 0,
                    "Cost of Goods": 0.0,
                    "List Price Total": 0.0,
                    "Market Value": 0.0,
                }])
            else:
                inv_listed["__card_type"] = inv_listed[inv_card_type_col].apply(_normalize_card_type)
                inv_listed = inv_listed[inv_listed["__card_type"].isin(["Sports", "Pokemon"])].copy()

                inv_listed["__mv"] = 0.0
                market_value_col_listed = _pick_col(inv_listed, "market_value", None)
                if market_value_col_listed and market_value_col_listed in inv_listed.columns:
                    inv_listed["__mv"] = _to_num(inv_listed[market_value_col_listed])

                inv_listed["__inv_id_key"] = inv_listed[inv_id_col].apply(lambda x: _safe_str(x).strip())
                inv_listed["__list_price"] = inv_listed["__inv_id_key"].map(list_price_by_inv_id).fillna(0.0)

                inv_listed["__grading_cost_unsynced"] = inv_listed["__inv_id_key"].map(grading_cost_by_inv_id).fillna(0.0)
                inv_listed["__cogs"] = _to_num(inv_listed[inv_total_col]) + _to_num(inv_listed["__grading_cost_unsynced"])

                rows = []
                for ct in ["Sports", "Pokemon"]:
                    sub = inv_listed[inv_listed["__card_type"].str.upper() == ct.upper()].copy()
                    if sub.empty:
                        continue
                    rows.append([
                        ct,
                        int(len(sub)),
                        float(sub["__cogs"].sum()),
                        float(sub["__list_price"].sum()),
                        float(sub["__mv"].sum())
                    ])

                listed_df = pd.DataFrame(rows, columns=["Listed Items", "# of items", "Cost of Goods", "List Price Total", "Market Value"])
                listed_df = pd.concat(
                    [
                        listed_df,
                        pd.DataFrame([{
                            "Listed Items": "Totals",
                            "# of items": int(len(inv_listed)),
                            "Cost of Goods": float(inv_listed["__cogs"].sum()),
                            "List Price Total": float(inv_listed["__list_price"].sum()),
                            "Market Value": float(inv_listed["__mv"].sum()),
                        }])
                    ],
                    ignore_index=True
                )

        sty_listed = (
            _style_group_and_total_rows(listed_df, "Listed Items")
            .format({"Cost of Goods": "${:,.2f}", "List Price Total": "${:,.2f}", "Market Value": "${:,.2f}"})
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty_listed, use_container_width=True, hide_index=True)

        # -------------------------
        # Sales
        # -------------------------
        st.markdown("### Sales")
        if txn_f.empty:
            st.info("No sales in selected period.")
            sales_df = pd.DataFrame(columns=["Sales", "# of Sales", "Cost of Goods", "Dollar Sales", "Total Fees", "Proceeds", "Profit"])
            fees_total = 0.0
            net_total = 0.0
            gross_total = 0.0
            cogs_total = 0.0
            profit_total = 0.0
            sales_count_total = 0
        else:
            tx = txn_f.copy()
            tx["__card_type"] = tx.apply(_tx_card_type_rowaware, axis=1)
            tx = tx[tx["__card_type"].isin(["Sports", "Pokemon"])].copy()
            tx["__bucket"] = tx.apply(_tx_product_bucket_rowaware, axis=1)

            if "__dollar_sales" not in tx.columns:
                tx["__dollar_sales"] = _to_num(tx.get("__sold_price", 0.0))
            if "__total_fees" not in tx.columns:
                tx["__total_fees"] = _to_num(tx.get("__fees", 0.0)) + _to_num(tx.get("__ship_charged", 0.0))
            if "__net" not in tx.columns:
                tx["__net"] = (tx["__dollar_sales"] - tx["__total_fees"]).fillna(0.0)

            def _cogs_for_inv_id(inv_id: str) -> float:
                k = _safe_str(inv_id).strip()
                rec = inv_by_id.get(k)
                base = 0.0
                if rec is not None:
                    base = _to_num(rec.get(inv_total_col, 0.0))
                add = float(grading_cost_by_inv_id.get(k, 0.0) or 0.0)
                return float(base + add)

            if "__all_in_cost" in tx.columns:
                tx["__cogs"] = _to_num(tx["__all_in_cost"])
                tx["__cogs"] = np.where(
                    tx["__cogs"] > 0,
                    tx["__cogs"],
                    tx["__inventory_id"].apply(_cogs_for_inv_id)
                )
            else:
                tx["__cogs"] = tx["__inventory_id"].apply(_cogs_for_inv_id)

            # Recalculate profit from first principles:
            # Profit = Gross Sales - Total Fees - Cost of Goods.
            tx["__net"] = (tx["__dollar_sales"] - tx["__total_fees"]).fillna(0.0)
            tx["__profit"] = (tx["__net"] - tx["__cogs"]).fillna(0.0)

            sales_count_total = int(len(tx))
            gross_total = float(tx["__dollar_sales"].sum())
            fees_total = float(tx["__total_fees"].sum())
            net_total = float(tx["__net"].sum())
            cogs_total = float(tx["__cogs"].sum())
            profit_total = float(tx["__profit"].sum())

            rows = []
            for ct in ["Sports", "Pokemon"]:
                sub = tx[tx["__card_type"].str.upper() == ct.upper()].copy()
                if sub.empty:
                    continue

                rows.append([
                    ct,
                    int(len(sub)),
                    float(sub["__cogs"].sum()),
                    float(sub["__dollar_sales"].sum()),
                    float(sub["__total_fees"].sum()),
                    float(sub["__net"].sum()),
                    float(sub["__profit"].sum()),
                ])

                for b in ["Cards", "Graded Cards", "Sealed"]:
                    sb = sub[sub["__bucket"] == b]
                    rows.append([
                        f"  {b}",
                        int(len(sb)),
                        float(sb["__cogs"].sum()),
                        float(sb["__dollar_sales"].sum()),
                        float(sb["__total_fees"].sum()),
                        float(sb["__net"].sum()),
                        float(sb["__profit"].sum()),
                    ])

            sales_df = pd.DataFrame(rows, columns=["Sales", "# of Sales", "Cost of Goods", "Dollar Sales", "Total Fees", "Proceeds", "Profit"])

            if not sales_df.empty:
                sales_df = pd.concat(
                    [
                        sales_df,
                        pd.DataFrame([{
                            "Sales": "Totals",
                            "# of Sales": sales_count_total,
                            "Cost of Goods": cogs_total,
                            "Dollar Sales": gross_total,
                            "Total Fees": fees_total,
                            "Proceeds": net_total,
                            "Profit": profit_total,
                        }])
                    ],
                    ignore_index=True
                )

        sty3 = (
            _style_group_and_total_rows(sales_df, "Sales")
            .format({
                "Cost of Goods": "${:,.2f}",
                "Dollar Sales": "${:,.2f}",
                "Total Fees": "${:,.2f}",
                "Proceeds": "${:,.2f}",
                "Profit": "${:,.2f}",
            })
            .map(_style_red_green, subset=["Profit"])
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty3, use_container_width=True, hide_index=True)

        st.markdown("### Business Summary")

        # Business summary is an operating/cash-investment view for the selected
        # period/as-of date. It intentionally includes both:
        #   1) inventory still held as an asset, and
        #   2) cost of goods for items sold in the selected sales period.
        # This matches how the business owner wants to see total money tied up
        # or spent by the business: inventory cost + grading cost + fees + misc.
        misc_spend = float(misc_f["__amount"].sum()) if not misc_f.empty else 0.0

        summary_rows = []
        sales_total_summary = 0.0
        cogs_total_summary = 0.0
        fees_total_summary = 0.0

        if asset_cost_total > 0 or asset_item_total > 0:
            summary_rows.append([
                "Inventory Held / Assets",
                asset_cost_total,
                0.0,
                0.0,
                -asset_cost_total,
            ])

        for ct in ["Sports", "Pokemon"]:
            if not txn_f.empty:
                tx_tmp = txn_f.copy()
                tx_tmp["__card_type"] = tx_tmp.apply(_tx_card_type_rowaware, axis=1)
                tx_ct = tx_tmp[tx_tmp["__card_type"].astype(str).str.upper() == ct.upper()].copy()
            else:
                tx_ct = pd.DataFrame()

            if tx_ct.empty:
                continue

            if "__dollar_sales" not in tx_ct.columns:
                tx_ct["__dollar_sales"] = _to_num(tx_ct.get("__sold_price", 0.0))
            if "__total_fees" not in tx_ct.columns:
                tx_ct["__total_fees"] = _to_num(tx_ct.get("__fees", 0.0)) + _to_num(tx_ct.get("__ship_charged", 0.0))

            def _summary_cogs_for_inv_id(inv_id: str) -> float:
                k = _safe_str(inv_id).strip()
                rec = inv_by_id.get(k)
                if rec is None:
                    return 0.0
                cost_col = _pick_col(pd.DataFrame([rec]), "total_cost", None) or _pick_col(pd.DataFrame([rec]), "all_in_cost", None)
                if cost_col and cost_col in rec:
                    val = _to_num(rec.get(cost_col, 0.0))
                    if val > 0:
                        return float(val)
                base = _to_num(rec.get(inv_total_col, 0.0))
                add = float(grading_cost_by_inv_id.get(k, 0.0) or 0.0)
                return float(base + add)

            if "__all_in_cost" in tx_ct.columns:
                tx_ct["__cogs"] = _to_num(tx_ct["__all_in_cost"])
                tx_ct["__cogs"] = np.where(
                    tx_ct["__cogs"] > 0,
                    tx_ct["__cogs"],
                    tx_ct["__inventory_id"].apply(_summary_cogs_for_inv_id),
                )
            else:
                tx_ct["__cogs"] = tx_ct["__inventory_id"].apply(_summary_cogs_for_inv_id)

            sales_ct = float(tx_ct["__dollar_sales"].sum())
            cogs_ct = float(tx_ct["__cogs"].sum())
            fees_ct = float(tx_ct["__total_fees"].sum())
            profit_ct = sales_ct - cogs_ct - fees_ct

            sales_total_summary += sales_ct
            cogs_total_summary += cogs_ct
            fees_total_summary += fees_ct
            summary_rows.append([f"Sold COGS + Fees — {ct}", cogs_ct + fees_ct, sales_ct, fees_ct, profit_ct])

        if misc_spend > 0:
            summary_rows.append(["Misc / Other", misc_spend, 0.0, 0.0, -misc_spend])

        total_expenses = asset_cost_total + cogs_total_summary + fees_total_summary + misc_spend
        totals_pl = sales_total_summary - total_expenses
        summary_rows.append(["Totals", total_expenses, sales_total_summary, fees_total_summary, totals_pl])

        summary_df = pd.DataFrame(summary_rows, columns=["Total", "Total Expenses", "Sales", "Fees/shipping", "Profit/Loss"])

        sty4 = (
            _style_group_and_total_rows(summary_df, "Total")
            .format({
                "Total Expenses": "${:,.2f}",
                "Sales": "${:,.2f}",
                "Fees/shipping": "${:,.2f}",
                "Profit/Loss": "${:,.2f}",
            })
            .map(_style_red_green, subset=["Profit/Loss"])
            
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty4, use_container_width=True, hide_index=True)


# =========================================================
# TAB 2: Monthly Summary
# =========================================================
with tab_forecast:
    st.subheader("Monthly Business Summary")
    st.caption(
        "This is the owner view: realized profit from sold items, other business expenses, "
        "month-end inventory, and conservative business value using 70% of market value as liquidity value."
    )

    LIQUIDITY_RATE = 0.70

    def _to_dt_monthly(x):
        """Parse mixed date formats like 2026-06-06 and 06/09/2026 in the same column."""
        try:
            return pd.to_datetime(x, errors="coerce", format="mixed")
        except TypeError:
            # Older pandas fallback
            return pd.to_datetime(x, errors="coerce", infer_datetime_format=True)

    def _month_start_monthly(dt_series):
        d = _to_dt_monthly(dt_series)
        return d.dt.to_period("M").dt.to_timestamp()

    f1, f2 = st.columns([1, 4])
    with f1:
        year_choice_2 = st.selectbox("Year", options=year_opts, index=0, key="monthly_summary_year")

    # -------------------------
    # Inventory lookup helpers
    # -------------------------
    inv_monthly = _ensure_unique_columns(inv.copy()) if not inv.empty else pd.DataFrame()

    inv_records = {}
    if not inv_monthly.empty and inv_id_col in inv_monthly.columns:
        inv_monthly[inv_id_col] = inv_monthly[inv_id_col].apply(lambda x: _safe_str(x).strip())
        # Duplicates should not break the dashboard. Keep the last populated row for lookup purposes.
        inv_lookup = inv_monthly[inv_monthly[inv_id_col].astype(str).str.strip() != ""].copy()
        inv_lookup = inv_lookup.drop_duplicates(subset=[inv_id_col], keep="last")
        inv_records = inv_lookup.set_index(inv_id_col, drop=False).to_dict("index")

    inv_total_cost_col_m = _pick_col(inv_monthly, "total_cost", None) or _pick_col(inv_monthly, "all_in_cost", None)
    inv_market_col_m = (
        _pick_col(inv_monthly, "market_value", None)
        or _pick_col(inv_monthly, "market_price", None)
        or "__market_price"
    )

    def _inv_cost_monthly(inv_id: str) -> float:
        k = _safe_str(inv_id).strip()
        rec = inv_records.get(k)
        if rec is None:
            return 0.0

        total_cost = 0.0
        if inv_total_cost_col_m and inv_total_cost_col_m in rec:
            total_cost = _to_num(rec.get(inv_total_cost_col_m, 0.0))

        if total_cost <= 0:
            total_cost = _to_num(rec.get(inv_total_col, 0.0))
            status = _safe_str(rec.get(inv_status_col, "")).strip().upper()
            if status == "GRADING":
                total_cost += float(grading_cost_by_inv_id.get(k, 0.0) or 0.0)

        return float(total_cost or 0.0)

    # -------------------------
    # Recalculate sales from first principles
    # -------------------------
    def _monthly_sales_math(tx_df: pd.DataFrame) -> pd.DataFrame:
        if tx_df is None or tx_df.empty:
            return pd.DataFrame(columns=[
                "__sold_dt", "__sold_month", "__inventory_id", "__dollar_sales",
                "__total_fees", "__net", "__cogs", "__profit_calc"
            ])

        out = tx_df.copy()
        if "__inventory_id" not in out.columns:
            out["__inventory_id"] = ""
        out["__inventory_id"] = out["__inventory_id"].apply(lambda x: _safe_str(x).strip())

        if "__sold_dt" not in out.columns:
            out["__sold_dt"] = pd.NaT
        out["__sold_dt"] = _to_dt_monthly(out["__sold_dt"])
        out = out[out["__sold_dt"].notna()].copy()

        if "__dollar_sales" not in out.columns:
            out["__dollar_sales"] = _to_num(out.get("__sold_price", 0.0))
        else:
            out["__dollar_sales"] = _to_num(out["__dollar_sales"])

        # Fees / proceeds: prefer an already-computed net value when available.
        # This keeps the Monthly Summary aligned with the validated sales ledger.
        if "__total_fees" not in out.columns:
            out["__total_fees"] = _to_num(out.get("__fees", 0.0)) + _to_num(out.get("__ship_charged", 0.0))
        else:
            out["__total_fees"] = _to_num(out["__total_fees"])

        existing_net = None
        if "__net" in out.columns:
            existing_net = _to_num(out["__net"])
        elif "net_proceeds" in out.columns:
            existing_net = _to_num(out["net_proceeds"])

        if existing_net is not None:
            # If fees are missing/0 but net proceeds exist, back into fees from gross - net.
            inferred_fees = (out["__dollar_sales"] - existing_net).clip(lower=0.0)
            out["__total_fees"] = np.where(out["__total_fees"] > 0, out["__total_fees"], inferred_fees)
            out["__net"] = existing_net
        else:
            out["__net"] = (out["__dollar_sales"] - out["__total_fees"]).fillna(0.0)

        fallback_cogs = out["__inventory_id"].apply(_inv_cost_monthly)
        if "__all_in_cost" in out.columns:
            all_in = _to_num(out["__all_in_cost"])
            out["__cogs"] = np.where(all_in > 0, all_in, fallback_cogs)
        else:
            out["__cogs"] = fallback_cogs

        out["__profit_calc"] = (out["__net"] - out["__cogs"]).fillna(0.0)
        out["__sold_month"] = _month_start_monthly(out["__sold_dt"])
        return out

    def _monthly_sales_math_from_inventory(inv_df: pd.DataFrame) -> pd.DataFrame:
        """Build the monthly sales ledger directly from the inventory sheet.

        This is intentionally inventory-first so sales written only to inventory
        still count, regardless of sale_channel/platform wording such as
        Card Show, Online, Ebay, or FACEBOOK MARKETPLACE.
        """
        empty_cols = [
            "__sold_dt", "__sold_month", "__inventory_id", "__dollar_sales",
            "__total_fees", "__net", "__cogs", "__profit_calc", "__sale_channel"
        ]
        if inv_df is None or inv_df.empty or inv_id_col not in inv_df.columns:
            return pd.DataFrame(columns=empty_cols)

        d = inv_df.copy()
        d[inv_id_col] = d[inv_id_col].apply(lambda x: _safe_str(x).strip())
        d["__inventory_id"] = d[inv_id_col]

        status = (
            d[inv_status_col].astype(str).str.upper().str.strip()
            if inv_status_col in d.columns
            else pd.Series("", index=d.index)
        )

        if inv_sold_price_col and inv_sold_price_col in d.columns:
            d["__dollar_sales"] = _to_num(d[inv_sold_price_col])
        else:
            d["__dollar_sales"] = 0.0

        if inv_sold_date_col and inv_sold_date_col in d.columns:
            d["__sold_dt"] = _to_dt_monthly(d[inv_sold_date_col])
        else:
            d["__sold_dt"] = pd.NaT

        # Count every inventory row marked SOLD that has a sale date or sold price.
        # Do NOT filter by sale_channel; channel names are inconsistent by design.
        d = d[(status.eq("SOLD")) & ((d["__dollar_sales"] > 0) | d["__sold_dt"].notna())].copy()
        if d.empty:
            return pd.DataFrame(columns=empty_cols)

        d = d[d["__sold_dt"].notna()].copy()
        if d.empty:
            return pd.DataFrame(columns=empty_cols)

        base_fees = _to_num(d[inv_fees_col]) if inv_fees_col and inv_fees_col in d.columns else 0.0
        ship_charged = _to_num(d[inv_shipping_charged_col]) if inv_shipping_charged_col and inv_shipping_charged_col in d.columns else 0.0

        # For sold inventory rows, align to the validated sales fields on the inventory sheet:
        # sold_price = gross sales before fees
        # net_proceeds = cash after selling fees / shipping
        # total_cost = COGS
        # This prevents the monthly summary from drifting away from the Sales table.
        net_col_m = inv_net_col if inv_net_col and inv_net_col in d.columns else None
        if net_col_m:
            net_raw_text = d[net_col_m].astype(str).str.strip()
            has_net = net_raw_text.ne("") & net_raw_text.str.lower().ne("nan")
            stored_net = _to_num(d[net_col_m])
        else:
            has_net = pd.Series(False, index=d.index)
            stored_net = pd.Series(0.0, index=d.index)

        if inv_fees_total_col and inv_fees_total_col in d.columns:
            fees_from_sheet = _to_num(d[inv_fees_total_col])
        else:
            fees_from_sheet = pd.Series(0.0, index=d.index)

        fees_from_net = (d["__dollar_sales"] - stored_net).clip(lower=0.0)
        d["__total_fees"] = np.where(
            fees_from_sheet > 0,
            fees_from_sheet,
            np.where(has_net, fees_from_net, base_fees + ship_charged),
        )

        inv_direct_cost_col = _pick_col(d, "total_cost", None) or _pick_col(d, "all_in_cost", None)
        fallback_cost = _to_num(d[inv_total_col]) if inv_total_col in d.columns else 0.0
        if inv_direct_cost_col and inv_direct_cost_col in d.columns:
            direct_cost = _to_num(d[inv_direct_cost_col])
            d["__cogs"] = np.where(direct_cost > 0, direct_cost, fallback_cost)
        else:
            d["__cogs"] = fallback_cost

        d["__net"] = np.where(has_net, stored_net, (d["__dollar_sales"] - d["__total_fees"]).fillna(0.0))
        d["__profit_calc"] = (d["__net"] - d["__cogs"]).fillna(0.0)
        d["__sold_month"] = _month_start_monthly(d["__sold_dt"])

        if inv_sale_channel_col and inv_sale_channel_col in d.columns:
            d["__sale_channel"] = d[inv_sale_channel_col].astype(str).replace("nan", "").fillna("")
        else:
            d["__sale_channel"] = ""

        # Fill blank channels from platform/transaction_type when available so the audit table is useful.
        for fallback_col in ["platform", "transaction_type"]:
            if fallback_col in d.columns:
                blank = d["__sale_channel"].astype(str).str.strip().eq("")
                d.loc[blank, "__sale_channel"] = d.loc[blank, fallback_col].astype(str).replace("nan", "")

        return d[empty_cols].copy()

    # Use inventory as the source of truth for monthly sales, then add only
    # legacy transaction rows whose inventory_id was not already counted.
    tx_inventory_sales = _monthly_sales_math_from_inventory(inv_monthly)
    tx_legacy_sales = _monthly_sales_math(txn)

    if not tx_inventory_sales.empty and not tx_legacy_sales.empty:
        counted_ids = set(tx_inventory_sales["__inventory_id"].astype(str).str.strip())
        tx_legacy_sales = tx_legacy_sales[~tx_legacy_sales["__inventory_id"].astype(str).str.strip().isin(counted_ids)].copy()

    tx_math_all = pd.concat(
        [x for x in [tx_inventory_sales, tx_legacy_sales] if x is not None and not x.empty],
        ignore_index=True,
        sort=False,
    ) if (not tx_inventory_sales.empty or not tx_legacy_sales.empty) else pd.DataFrame(columns=[
        "__sold_dt", "__sold_month", "__inventory_id", "__dollar_sales",
        "__total_fees", "__net", "__cogs", "__profit_calc", "__sale_channel"
    ])

    # Sold-date lookup lets month-end inventory include items that were held in old months but sold later.
    sold_date_by_inv_id = {}
    if not tx_math_all.empty and "__inventory_id" in tx_math_all.columns:
        sold_date_by_inv_id = (
            tx_math_all.dropna(subset=["__sold_dt"])
                       .groupby("__inventory_id")["__sold_dt"]
                       .min()
                       .to_dict()
        )

    # -------------------------
    # Monthly sales, expenses, purchases, and misc
    # -------------------------
    if not tx_math_all.empty:
        sales_m = (
            tx_math_all.dropna(subset=["__sold_month"])
                       .groupby("__sold_month", as_index=False)
                       .agg(
                           sales=("__dollar_sales", "sum"),
                           cogs_sold=("__cogs", "sum"),
                           selling_fees=("__total_fees", "sum"),
                           proceeds=("__net", "sum"),
                           realized_profit_before_misc=("__profit_calc", "sum"),
                           items_sold=("__inventory_id", "count"),
                       )
                       .rename(columns={"__sold_month": "month"})
        )
    else:
        sales_m = pd.DataFrame(columns=[
            "month", "sales", "cogs_sold", "selling_fees", "proceeds",
            "realized_profit_before_misc", "items_sold"
        ])

    if not misc.empty and "__month" in misc.columns:
        misc_m = (
            misc.dropna(subset=["__month"])
                .groupby("__month", as_index=False)["__amount"]
                .sum()
                .rename(columns={"__month": "month", "__amount": "other_expenses"})
        )
    else:
        misc_m = pd.DataFrame(columns=["month", "other_expenses"])

    if not inv_monthly.empty and "__purchase_dt" in inv_monthly.columns:
        inv_monthly["__purchase_month"] = _month_start(inv_monthly["__purchase_dt"])
        inv_monthly["__inventory_cost"] = inv_monthly[inv_id_col].apply(_inv_cost_monthly)
        purchases_m = (
            inv_monthly.dropna(subset=["__purchase_month"])
                       .groupby("__purchase_month", as_index=False)
                       .agg(
                           items_bought=(inv_id_col, "count"),
                           inventory_spend=("__inventory_cost", "sum"),
                       )
                       .rename(columns={"__purchase_month": "month"})
        )
    else:
        purchases_m = pd.DataFrame(columns=["month", "items_bought", "inventory_spend"])

    if not grd.empty and "__grading_month" in grd.columns and "__grading_cost" in grd.columns:
        grading_m = (
            grd.dropna(subset=["__grading_month"])
               .groupby("__grading_month", as_index=False)["__grading_cost"]
               .sum()
               .rename(columns={"__grading_month": "month", "__grading_cost": "grading_spend"})
        )
    else:
        grading_m = pd.DataFrame(columns=["month", "grading_spend"])

    # -------------------------
    # Month backbone
    # -------------------------
    month_sources = []
    for s in [
        sales_m.get("month"),
        misc_m.get("month"),
        purchases_m.get("month"),
        grading_m.get("month"),
    ]:
        if isinstance(s, pd.Series) and not s.empty:
            month_sources.append(s.dropna())

    if month_sources:
        min_month = min([s.min() for s in month_sources])
        max_month = max([s.max() for s in month_sources])
        months = pd.date_range(min_month, max_month, freq="MS")
    else:
        months = pd.date_range(pd.Timestamp(date.today().replace(day=1)), periods=1, freq="MS")

    monthly = pd.DataFrame({"month": months})
    for df_m in [sales_m, misc_m, purchases_m, grading_m]:
        monthly = monthly.merge(df_m, on="month", how="left")

    for c in [
        "sales", "cogs_sold", "selling_fees", "proceeds", "realized_profit_before_misc",
        "items_sold", "other_expenses", "items_bought", "inventory_spend", "grading_spend"
    ]:
        if c not in monthly.columns:
            monthly[c] = 0.0
        monthly[c] = monthly[c].fillna(0.0)

    # -------------------------
    # Inventory held at each month end
    # -------------------------
    inventory_rows = []
    if not inv_monthly.empty and "__purchase_dt" in inv_monthly.columns:
        inv_asof_base = inv_monthly.copy()
        inv_asof_base["__purchase_dt"] = _to_dt_monthly(inv_asof_base["__purchase_dt"])
        inv_asof_base["__inv_id_key"] = inv_asof_base[inv_id_col].apply(lambda x: _safe_str(x).strip())

        if inv_sold_date_col and inv_sold_date_col in inv_asof_base.columns:
            inv_asof_base["__sold_dt_monthly"] = _to_dt_monthly(inv_asof_base[inv_sold_date_col])
        else:
            inv_asof_base["__sold_dt_monthly"] = pd.NaT

        mapped_sold_dt = inv_asof_base["__inv_id_key"].map(sold_date_by_inv_id)
        inv_asof_base["__sold_dt_monthly"] = inv_asof_base["__sold_dt_monthly"].combine_first(_to_dt_monthly(mapped_sold_dt))
        inv_asof_base["__status_upper"] = inv_asof_base[inv_status_col].astype(str).str.upper().str.strip()
        inv_asof_base["__held_cost"] = inv_asof_base["__inv_id_key"].apply(_inv_cost_monthly)

        if inv_market_col_m in inv_asof_base.columns:
            inv_asof_base["__held_market_value"] = _to_num(inv_asof_base[inv_market_col_m])
        elif "__market_price" in inv_asof_base.columns:
            inv_asof_base["__held_market_value"] = _to_num(inv_asof_base["__market_price"])
        else:
            inv_asof_base["__held_market_value"] = 0.0

        for m in months:
            month_end = (pd.Timestamp(m) + pd.offsets.MonthEnd(0)) + pd.Timedelta(hours=23, minutes=59, seconds=59)
            purchased_by_end = inv_asof_base["__purchase_dt"].notna() & (inv_asof_base["__purchase_dt"] <= month_end)

            # Held as of month end:
            # 1) sold later than this month, OR
            # 2) not sold yet and currently ACTIVE/LISTED/GRADING.
            sold_after_month = inv_asof_base["__sold_dt_monthly"].notna() & (inv_asof_base["__sold_dt_monthly"] > month_end)
            still_active_now = inv_asof_base["__sold_dt_monthly"].isna() & inv_asof_base["__status_upper"].isin(["ACTIVE", "LISTED", "GRADING"])

            held = inv_asof_base[purchased_by_end & (sold_after_month | still_active_now)].copy()

            inv_cost = float(held["__held_cost"].sum()) if not held.empty else 0.0
            market_value = float(held["__held_market_value"].sum()) if not held.empty else 0.0
            liquidity_value = market_value * LIQUIDITY_RATE

            inventory_rows.append({
                "month": pd.Timestamp(m),
                "inventory_items": int(len(held)),
                "inventory_cost": inv_cost,
                "inventory_market_value": market_value,
                "inventory_liquidity_value": liquidity_value,
                "inventory_equity": liquidity_value - inv_cost,
            })

    inventory_m = pd.DataFrame(inventory_rows)
    if inventory_m.empty:
        inventory_m = pd.DataFrame({
            "month": months,
            "inventory_items": 0,
            "inventory_cost": 0.0,
            "inventory_market_value": 0.0,
            "inventory_liquidity_value": 0.0,
            "inventory_equity": 0.0,
        })

    monthly = monthly.merge(inventory_m, on="month", how="left")
    for c in ["inventory_items", "inventory_cost", "inventory_market_value", "inventory_liquidity_value", "inventory_equity"]:
        monthly[c] = monthly[c].fillna(0.0)

    # -------------------------
    # Owner/business-health calculations
    # -------------------------
    monthly = monthly.sort_values("month").copy()
    monthly["realized_expenses"] = monthly["cogs_sold"] + monthly["selling_fees"] + monthly["other_expenses"]
    monthly["realized_profit"] = monthly["sales"] - monthly["realized_expenses"]
    monthly["cumulative_realized_profit"] = monthly["realized_profit"].cumsum()

    # Business value = banked/cumulative profit + liquidation value of inventory on hand.
    monthly["business_value"] = monthly["cumulative_realized_profit"] + monthly["inventory_liquidity_value"]

    # Value created is stricter: cumulative realized profit + conservative unrealized inventory equity.
    monthly["conservative_value_created"] = monthly["cumulative_realized_profit"] + monthly["inventory_equity"]

    monthly["mom_business_value_growth"] = monthly["business_value"].diff().fillna(monthly["business_value"])
    monthly["mom_business_value_growth_pct"] = np.where(
        monthly["business_value"].shift(1).abs() > 0,
        monthly["mom_business_value_growth"] / monthly["business_value"].shift(1).abs(),
        0.0,
    )
    monthly["sales_growth_pct"] = np.where(
        monthly["sales"].shift(1).abs() > 0,
        (monthly["sales"] - monthly["sales"].shift(1)) / monthly["sales"].shift(1).abs(),
        0.0,
    )
    monthly["net_margin"] = np.where(monthly["sales"] > 0, monthly["realized_profit"] / monthly["sales"], 0.0)
    monthly["inventory_roi_liquid"] = np.where(
        monthly["inventory_cost"] > 0,
        monthly["inventory_equity"] / monthly["inventory_cost"],
        0.0,
    )

    monthly_view = monthly.copy()
    if year_choice_2 != "All":
        try:
            y = int(year_choice_2)
            monthly_view = monthly_view[monthly_view["month"].dt.year == y].copy()
        except Exception:
            pass

    if monthly_view.empty:
        st.info("No monthly data found for the selected year.")
        st.stop()

    latest = monthly_view.iloc[-1]
    prev = monthly_view.iloc[-2] if len(monthly_view) >= 2 else None

    business_delta = float(latest["business_value"] - prev["business_value"]) if prev is not None else float(latest["business_value"])
    business_delta_pct = (business_delta / abs(float(prev["business_value"]))) if prev is not None and float(prev["business_value"]) != 0 else 0.0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Business Value", _fmt_money(latest["business_value"]), delta=f"{business_delta:+,.0f} / {business_delta_pct*100:+.1f}%")
    k2.metric("Cumulative Realized Profit", _fmt_money(latest["cumulative_realized_profit"]))
    k3.metric("Inventory Liquidity Value", _fmt_money(latest["inventory_liquidity_value"]), help="70% of current market value")
    k4.metric("Conservative Value Created", _fmt_money(latest["conservative_value_created"]))
    k5.metric("Latest Month Profit", _fmt_money(latest["realized_profit"]), delta=f"Margin {latest['net_margin']*100:,.1f}%")

    st.markdown("---")

    # -------------------------
    # Charts
    # -------------------------
    c1, c2 = st.columns([1.25, 1.0])

    with c1:
        st.markdown("### Business Value Trend")
        trend = monthly_view[[
            "month", "business_value", "cumulative_realized_profit", "inventory_liquidity_value", "conservative_value_created"
        ]].copy()
        trend_long = trend.melt(
            id_vars=["month"],
            value_vars=["business_value", "cumulative_realized_profit", "inventory_liquidity_value", "conservative_value_created"],
            var_name="metric",
            value_name="value",
        )
        trend_long["metric"] = trend_long["metric"].map({
            "business_value": "Business Value",
            "cumulative_realized_profit": "Cum. Realized Profit",
            "inventory_liquidity_value": "Inventory Liquidity Value",
            "conservative_value_created": "Conservative Value Created",
        })

        line = alt.Chart(trend_long).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("month:T", title="Month", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
            y=alt.Y("value:Q", title="$"),
            color=alt.Color("metric:N", legend=alt.Legend(title="")),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%Y-%m"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        ).properties(height=340).interactive()
        st.altair_chart(line, use_container_width=True)

    with c2:
        st.markdown("### Monthly Sales, Expenses, and Profit")
        bars = monthly_view[["month", "sales", "realized_expenses", "realized_profit"]].copy()
        bars_long = bars.melt(
            id_vars=["month"],
            value_vars=["sales", "realized_expenses", "realized_profit"],
            var_name="metric",
            value_name="value",
        )
        bars_long["metric"] = bars_long["metric"].map({
            "sales": "Sales",
            "realized_expenses": "Expenses",
            "realized_profit": "Profit",
        })

        bar_chart = alt.Chart(bars_long).mark_bar().encode(
            x=alt.X("month:T", title="Month", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
            y=alt.Y("value:Q", title="$"),
            color=alt.Color("metric:N", legend=alt.Legend(title="")),
            xOffset="metric:N",
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%Y-%m"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("value:Q", title="Value", format=",.2f"),
            ],
        ).properties(height=340).interactive()
        st.altair_chart(bar_chart, use_container_width=True)

    with st.expander("Audit: sales included by month/channel"):
        if tx_math_all.empty:
            st.info("No sold rows were included in the monthly sales ledger.")
        else:
            audit = tx_math_all.copy()
            audit["Month"] = audit["__sold_month"].dt.strftime("%Y-%m")
            audit["Channel"] = audit.get("__sale_channel", "").astype(str).replace("", "Unknown")
            audit_df = (
                audit.groupby(["Month", "Channel"], as_index=False)
                     .agg(
                         Items=("__inventory_id", "count"),
                         Sales=("__dollar_sales", "sum"),
                         COGS=("__cogs", "sum"),
                         Fees=("__total_fees", "sum"),
                         Profit=("__profit_calc", "sum"),
                     )
                     .sort_values(["Month", "Channel"])
            )
            st.dataframe(
                audit_df.style.format({
                    "Sales": "${:,.2f}",
                    "COGS": "${:,.2f}",
                    "Fees": "${:,.2f}",
                    "Profit": "${:,.2f}",
                }),
                use_container_width=True,
                hide_index=True,
            )

    # -------------------------
    # Monthly summary table
    # -------------------------
    st.markdown("### Monthly Summary Table")
    st.caption(
        "Business Value = cumulative realized profit + 70% inventory market value. "
        "Conservative Value Created = cumulative realized profit + (70% inventory market value - inventory cost)."
    )

    table = monthly_view.copy()
    table["Month"] = table["month"].dt.strftime("%Y-%m")
    table = table[[
        "Month",
        "sales",
        "cogs_sold",
        "selling_fees",
        "other_expenses",
        "realized_profit",
        "cumulative_realized_profit",
        "items_bought",
        "inventory_spend",
        "grading_spend",
        "items_sold",
        "inventory_items",
        "inventory_cost",
        "inventory_market_value",
        "inventory_liquidity_value",
        "inventory_equity",
        "business_value",
        "conservative_value_created",
        "mom_business_value_growth",
        "mom_business_value_growth_pct",
        "sales_growth_pct",
        "net_margin",
        "inventory_roi_liquid",
    ]].rename(columns={
        "sales": "Sales",
        "cogs_sold": "COGS Sold",
        "selling_fees": "Selling Fees / Shipping",
        "other_expenses": "Other Expenses",
        "realized_profit": "Realized Profit",
        "cumulative_realized_profit": "Cum. Realized Profit",
        "items_bought": "Items Bought",
        "inventory_spend": "Inventory Spend",
        "grading_spend": "Grading Spend",
        "items_sold": "Items Sold",
        "inventory_items": "Inventory Items Held",
        "inventory_cost": "Inventory Cost Held",
        "inventory_market_value": "Inventory Market Value",
        "inventory_liquidity_value": "Liquidity Value @ 70%",
        "inventory_equity": "Inventory Equity @ 70%",
        "business_value": "Business Value",
        "conservative_value_created": "Conservative Value Created",
        "mom_business_value_growth": "MoM Business Value Growth",
        "mom_business_value_growth_pct": "MoM Growth %",
        "sales_growth_pct": "Sales Growth %",
        "net_margin": "Net Margin",
        "inventory_roi_liquid": "Inventory ROI @ 70%",
    })

    money_cols = [
        "Sales", "COGS Sold", "Selling Fees / Shipping", "Other Expenses", "Realized Profit",
        "Cum. Realized Profit", "Inventory Spend", "Grading Spend", "Inventory Cost Held",
        "Inventory Market Value", "Liquidity Value @ 70%", "Inventory Equity @ 70%",
        "Business Value", "Conservative Value Created", "MoM Business Value Growth",
    ]
    pct_cols = ["MoM Growth %", "Sales Growth %", "Net Margin", "Inventory ROI @ 70%"]

    fmt = {c: "${:,.2f}" for c in money_cols}
    fmt.update({c: "{:.1%}" for c in pct_cols})
    fmt.update({
        "Items Bought": "{:,.0f}",
        "Items Sold": "{:,.0f}",
        "Inventory Items Held": "{:,.0f}",
    })

    sty_monthly = (
        table.style
             .format(fmt)
             .map(_style_red_green, subset=[
                 "Realized Profit", "Cum. Realized Profit", "Inventory Equity @ 70%",
                 "Business Value", "Conservative Value Created", "MoM Business Value Growth"
             ])
             .set_table_styles(_styler_table_header())
    )
    st.dataframe(sty_monthly, use_container_width=True, hide_index=True)

    with st.expander("How to read this"):
        st.markdown(
            f"""
            - **Realized Profit** = Sales - COGS Sold - Selling Fees / Shipping - Other Expenses.
            - **Liquidity Value @ 70%** = Inventory Market Value × {LIQUIDITY_RATE:.0%}. This is the conservative value you could likely turn into cash faster.
            - **Business Value** = Cumulative Realized Profit + Liquidity Value @ 70%.
            - **Conservative Value Created** = Cumulative Realized Profit + Inventory Equity @ 70%. This is stricter because it subtracts the cost still tied up in inventory.
            - **MoM Growth %** tracks whether business value is compounding month over month.
            """
        )
