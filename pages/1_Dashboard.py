# pages/1_Dashboard.py
import json
import re
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt

import requests
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
        # if already numeric, just coerce
        if pd.api.types.is_numeric_dtype(x):
            return pd.to_numeric(x, errors="coerce").fillna(0.0)

        x = x.astype(str).str.strip()

        # convert (123.45) => -123.45
        x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)

        # remove $ and commas and spaces
        x = x.str.replace(r"[\$,]", "", regex=True)

        # handle blanks / "nan"
        x = x.replace({"": "0", "nan": "0", "None": "0"})

        return pd.to_numeric(x, errors="coerce").fillna(0.0)

    # scalar
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
    # normalize any renamed dup columns like "inventory_id__dup1"
    s = _safe_str(c)
    if "__dup" in s:
        s = s.split("__dup")[0]
    return s

# ✅ FIX: robust normalization so "List Price" == "list_price", "Inventory ID" == "inventory_id", etc.
def _norm_key(s: str) -> str:
    s = _safe_str(s).strip().lower()
    # convert common separators to underscore
    s = re.sub(r"[\s\-\/]+", "_", s)
    # drop any remaining non-word chars (keep underscore)
    s = re.sub(r"[^\w]+", "", s)
    # collapse multiple underscores
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
        # month_choice expected like "2026-01"
        try:
            m = pd.to_datetime(month_choice + "-01", errors="coerce")
            if pd.notna(m):
                out = out[out["__dt_filter"].dt.to_period("M") == m.to_period("M")]
        except Exception:
            pass

    out = out.drop(columns=["__dt_filter"], errors="ignore")
    return out

def _bucket_product(product_type, grading_company, grade, condition, inv_status) -> str:
    # Avoid AttributeError from ints
    pt = _safe_str(product_type).strip().lower()
    comp = _safe_str(grading_company).strip()
    grd = _safe_str(grade).strip()
    cond = _safe_str(condition).strip().lower()
    status = _safe_str(inv_status).strip().upper()

    if status == "GRADING":
        return "Grading In-Process"

    if "sealed" in pt:
        return "Sealed"

    # "Graded Card" product_type or any populated grade/company indicates graded
    if "graded" in pt or comp or grd or ("graded" in cond):
        return "Graded Cards"

    return "Cards"

def _normalize_card_type(val: str) -> str:
    """
    User requirement: ONLY Pokemon or Sports. Never show 'Other'.
    Default any unknown/blank to Pokemon.
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
    return "Pokemon"


@st.cache_data(ttl=60 * 60 * 12, show_spinner=False)
def _fetch_market_prices(link: str) -> dict:
    """
    Supports BOTH:
      - pricecharting.com
      - sportscardspro.com

    Returns dict with:
      raw  = ungraded
      psa9 = PSA 9 (or Grade 9 on SCP)
      psa10 = PSA 10
    """
    out = {"raw": 0.0, "psa9": 0.0, "psa10": 0.0}
    if not link:
        return out

    url = str(link).strip()
    u = url.lower()

    if ("pricecharting.com" not in u) and ("sportscardspro.com" not in u):
        return out

    try:
        headers = {"User-Agent": "Mozilla/5.0 (CardTracker; +Streamlit)"}
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # -------------------------
        # PriceCharting parse
        # -------------------------
        if "pricecharting.com" in u:
            nodes = soup.select(".price.js-price")
            prices = []
            for n in nodes:
                t = n.get_text(" ", strip=True)
                m = re.search(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})", t)
                prices.append(float(m.group(1).replace(",", "")) if m else 0.0)

            # slots: 1=raw, 4=psa9, 6=psa10
            if len(prices) >= 1:
                out["raw"] = float(prices[0] or 0.0)
            if len(prices) >= 4:
                out["psa9"] = float(prices[3] or 0.0)
            if len(prices) >= 6:
                out["psa10"] = float(prices[5] or 0.0)
            return out

        # -------------------------
        # SportsCardsPro parse (TABLE-FIRST, then fallback)
        # -------------------------
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

        def _scp_full_prices_table(soup: BeautifulSoup) -> dict:
            """
            Pull values from the 'Full Price Guide' table:
              <tr><td>Ungraded</td><td class="price js-price">$1.99</td></tr>
              <tr><td>Grade 9</td><td class="price js-price">$12.34</td></tr>
              <tr><td>PSA 10</td><td class="price js-price">$44.25</td></tr>
            Returns dict label->value for anything we find.
            """
            tbl_map = {}

            # Prefer the full prices section if it exists
            tables = []
            full_prices = soup.select_one("#full-prices")
            if full_prices:
                tables = full_prices.find_all("table")

            # Fallback: scan all tables (site markup can vary)
            if not tables:
                tables = soup.find_all("table")

            for tbl in tables:
                for tr in tbl.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue

                    label = tds[0].get_text(" ", strip=True)
                    price_text = tds[1].get_text(" ", strip=True)
                    if not label:
                        continue

                    # Normalize label spacing
                    label = re.sub(r"\s+", " ", label.strip())
                    tbl_map[label] = _parse_money(price_text)

            return tbl_map

        # 1) Table-first (fixes cases where sales text is missing but table shows values)
        tbl = _scp_full_prices_table(soup)

        def _pick_tbl(labels):
            # exact match first
            for lab in labels:
                if lab in tbl:
                    return float(tbl.get(lab, 0.0) or 0.0)
            # case-insensitive match
            lower_map = {k.lower(): k for k in tbl.keys()}
            for lab in labels:
                k = lower_map.get(lab.lower())
                if k:
                    return float(tbl.get(k, 0.0) or 0.0)
            return 0.0

        raw_val = _pick_tbl(["Ungraded", "Raw"])
        psa9_val = _pick_tbl(["PSA 9", "Grade 9"])
        psa10_val = _pick_tbl(["PSA 10", "Grade 10"])

        # 2) Fallback to your previous robust text parsing if table didn't yield anything
        if raw_val == 0.0 and psa9_val == 0.0 and psa10_val == 0.0:
            text = soup.get_text("\n", strip=True)

            def _money_from_text(label_patterns):
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                dollar_pat = re.compile(r"\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
                label_res = [re.compile(p, flags=re.IGNORECASE) for p in label_patterns]

                for i, ln in enumerate(lines):
                    if any(rx.search(ln) for rx in label_res):
                        m = dollar_pat.search(ln)
                        if m:
                            return float(m.group(1).replace(",", ""))
                        for j in range(1, 4):
                            if i + j < len(lines):
                                m2 = dollar_pat.search(lines[i + j])
                                if m2:
                                    return float(m2.group(1).replace(",", ""))
                return 0.0

            def _money_regex(pattern: str) -> float:
                m = re.search(pattern, text, flags=re.IGNORECASE)
                if not m:
                    return 0.0
                return float(m.group(1).replace(",", ""))

            raw_val = _money_regex(r"(?:Ungraded|Raw)\b[^$]{0,50}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
            psa10_val = _money_regex(r"PSA\s*10\b[^$]{0,50}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")
            psa9_val = _money_regex(r"(?:PSA\s*9|Grade\s*9)\b[^$]{0,50}\$\s*([0-9][0-9,]*\.?[0-9]{0,2})")

            if raw_val <= 0:
                raw_val = _money_from_text([r"\bUngraded\b", r"\bRaw\b"])
            if psa10_val <= 0:
                psa10_val = _money_from_text([r"\bPSA\s*10\b"])
            if psa9_val <= 0:
                psa9_val = _money_from_text([r"\bPSA\s*9\b", r"\bGrade\s*9\b"])

        out["raw"] = float(raw_val or 0.0)
        out["psa9"] = float(psa9_val or 0.0)
        out["psa10"] = float(psa10_val or 0.0)
        return out

    except Exception:
        return out


def _repull_market_values_to_inventory_sheet():
    """
    Runs on Dashboard Refresh:
    - reads inventory sheet rows
    - computes market_price/market_value from PriceCharting or SportsCardsPro
    - writes back to inventory in ONE column update per market col (quota friendly)
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

    try:
        _fetch_market_prices.clear()
    except Exception:
        pass

    market_prices = []
    market_updated_ats = []
    now_iso = pd.Timestamp.utcnow().isoformat()
    updated = 0

    for r in rows:
        link = (r[i_ref] if i_ref is not None and i_ref < len(r) else "").strip()
        ll = link.lower()
        if ("pricecharting.com" not in ll) and ("sportscardspro.com" not in ll):
            market_prices.append([0.0])
            market_updated_ats.append([""])
            continue

        status = (r[i_status] if i_status is not None and i_status < len(r) else "").strip().upper()
        pt = (r[i_pt] if i_pt is not None and i_pt < len(r) else "").strip().lower()
        comp = (r[i_comp] if i_comp is not None and i_comp < len(r) else "").strip()
        grade = (r[i_grade] if i_grade is not None and i_grade < len(r) else "").strip().upper()
        cond = (r[i_cond] if i_cond is not None and i_cond < len(r) else "").strip().lower()

        prices = _fetch_market_prices(link)

        is_sealed = "sealed" in pt
        is_grading = (status == "GRADING")
        is_graded = ("graded" in pt) or bool(comp) or bool(grade) or ("graded" in cond)

        mv = float(prices.get("raw", 0.0) or 0.0)
        if (not is_sealed) and (not is_grading) and is_graded:
            if "10" in grade:
                mv = float(prices.get("psa10", 0.0) or 0.0)
            elif "9" in grade:
                mv = float(prices.get("psa9", 0.0) or 0.0)

        market_prices.append([mv])
        market_updated_ats.append([now_iso])
        updated += 1

    def a1_col_letter(n: int) -> str:
        letters = ""
        while n:
            n, r = divmod(n - 1, 26)
            letters = chr(65 + r) + letters
        return letters

    mp_col_letter = a1_col_letter(i_mp + 1)
    ws.update(f"{mp_col_letter}2:{mp_col_letter}{nrows+1}", market_prices, value_input_option="USER_ENTERED")

    mv_col_letter = a1_col_letter(i_mv + 1)
    ws.update(f"{mv_col_letter}2:{mv_col_letter}{nrows+1}", market_prices, value_input_option="USER_ENTERED")

    mpu_col_letter = a1_col_letter(i_mpu + 1)
    ws.update(f"{mpu_col_letter}2:{mpu_col_letter}{nrows+1}", market_updated_ats, value_input_option="USER_ENTERED")

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



@st.cache_data(show_spinner=False, ttl=60 * 10)
def load_sheet_df(worksheet_name: str) -> pd.DataFrame:
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

        try:
            st.cache_data.clear()
        except Exception:
            pass
        try:
            st.cache_resource.clear()
        except Exception:
            pass
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
inv = load_sheet_df(INV_WS)

# Keep an unfiltered copy for LISTED lookups (do NOT filter to SOLD)
txn_all = load_sheet_df(TXN_WS)

# Existing logic below will normalize + filter txn to SOLD for sales reporting
txn = txn_all.copy()

grd = load_sheet_df(GRD_WS)
misc = load_sheet_df(MISC_WS)


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

    inv_product_type_col = _pick_col(inv, "product_type", "product_type")
    inv_card_type_col = _pick_col(inv, "card_type", "card_type")
    inv_grade_col = _pick_col(inv, "grade", "grade")
    inv_company_col = _pick_col(inv, "grading_company", "grading_company")
    inv_condition_col = _pick_col(inv, "condition", "condition")

    inv_market_col = _pick_col(inv, "market_price", None) or _pick_col(inv, "market_value", None)

    for needed in [inv_id_col, inv_status_col, inv_total_col, inv_purchase_date_col, inv_ref_col]:
        if needed not in inv.columns:
            inv[needed] = ""

    for needed in [inv_product_type_col, inv_card_type_col, inv_grade_col, inv_company_col, inv_condition_col]:
        if needed not in inv.columns:
            inv[needed] = ""

    inv[inv_status_col] = inv[inv_status_col].replace("", "ACTIVE").fillna("ACTIVE").astype(str)
    inv[inv_total_col] = _to_num(inv[inv_total_col])
    inv["__purchase_dt"] = _to_dt(inv[inv_purchase_date_col])

    inv["__market_price"] = 0.0
    if inv_market_col and inv_market_col in inv.columns:
        inv["__market_price"] = _to_num(inv[inv_market_col])

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

    tx_card_type_col = _pick_col(txn, "card_type", None)

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

    # SOLD-only alignment
    tx_status_col = _pick_col(txn, "status", None) or _pick_col(txn, "tx_status", None)
    tx_net_proceeds_col = _pick_col(txn, "net_proceeds", None) or _pick_col(txn, "net", None)
    tx_ship_charged_col = _pick_col(txn, "shipping_charged", None) or _pick_col(txn, "shipping", None)

    if tx_status_col and tx_status_col in txn.columns:
        txn["__status"] = txn[tx_status_col].astype(str).str.upper().str.strip()
        txn = txn[txn["__status"].eq("SOLD")].copy()

    if tx_net_proceeds_col and tx_net_proceeds_col in txn.columns:
        txn["__net"] = _to_num(txn[tx_net_proceeds_col])
    else:
        if tx_ship_charged_col and tx_ship_charged_col in txn.columns:
            txn["__ship_charged"] = _to_num(txn[tx_ship_charged_col])
        else:
            txn["__ship_charged"] = 0.0
        txn["__net"] = (txn["__sold_price"] - txn["__fees"] + txn["__ship_charged"]).fillna(0.0)

    # ✅ FIX: fallback “sold rows only” guard even if status col was missing upstream
    # Drop rows that look like LISTED (no sold date, no sold price, no net).
    txn = txn[~(txn["__sold_dt"].isna() & (txn["__sold_price"] <= 0) & (txn["__net"] <= 0))].copy()

    txn["__sold_month"] = _month_start(txn["__sold_dt"])

    if tx_card_type_col and tx_card_type_col in txn.columns:
        txn["__txn_card_type"] = txn[tx_card_type_col].apply(_normalize_card_type)
    else:
        txn["__txn_card_type"] = ""

else:
    tx_date_col = None
    tx_inv_col = None
    txn = pd.DataFrame(columns=["__sold_dt", "__sold_month", "__inventory_id", "__sold_price", "__fees", "__net", "__txn_card_type"])


# =========================
# LIST PRICE LOOKUP (from txn_all)
# - Pull most recent LISTED transaction per inventory_id
# =========================
list_price_by_inv_id = {}

if "txn_all" in locals() and isinstance(txn_all, pd.DataFrame) and not txn_all.empty:
    txa = _ensure_unique_columns(txn_all.copy())

    txa_status_col = _pick_col(txa, "status", None) or _pick_col(txa, "tx_status", None)
    txa_inv_col = _pick_col(txa, "inventory_id", None) or _pick_col(txa, "inv_id", None)

    # ✅ FIX: include "amount" as fallback; normalization now handles "List Price" headers too.
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
            # if no dt column, preserve sheet order via index
            txa["__dt"] = pd.NaT
            txa["__row"] = np.arange(len(txa))

        # keep the most recent row per inventory_id
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
# Build Year/Month filter options (based on any activity)
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
tab_bs, tab_forecast, tab_bench = st.tabs(["Balance Sheet", "Expenses + Forecast", "Benchmarks"])


# =========================================================
# TAB 1: Balance Sheet
# =========================================================
with tab_bs:
    st.subheader("Balance Sheet (Filtered)")

    f1, f2 = st.columns([1, 1])
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

    inv_f = _apply_period_filter(inv, "__purchase_dt", year_choice, month_choice) if not inv.empty else inv
    txn_f = _apply_period_filter(txn, "__sold_dt", year_choice, month_choice) if not txn.empty else txn
    grd_f = _apply_period_filter(grd, "__grading_dt", year_choice, month_choice) if not grd.empty else grd
    misc_f = _apply_period_filter(misc, "__dt", year_choice, month_choice) if not misc.empty else misc

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
            return "Pokemon"
        return _normalize_card_type(rec.get(inv_card_type_col, ""))

    def _tx_card_type_rowaware(row) -> str:
        try:
            if "__txn_card_type" in row and _safe_str(row["__txn_card_type"]).strip():
                return _normalize_card_type(row["__txn_card_type"])
        except Exception:
            pass
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

    sold_dt_by_id = {}
    if not txn.empty and "__sold_dt" in txn.columns and "__inventory_id" in txn.columns:
        tmp = txn[["__inventory_id", "__sold_dt"]].copy().dropna(subset=["__sold_dt"])
        if not tmp.empty:
            sold_dt_by_id = tmp.groupby("__inventory_id")["__sold_dt"].min().to_dict()

    asof_cutoff = _period_end_dt(year_choice, month_choice)

    inv_holdings = pd.DataFrame()
    if not inv.empty and "__purchase_dt" in inv.columns:
        inv_holdings = inv.copy()
        inv_holdings[inv_id_col] = inv_holdings[inv_id_col].apply(lambda x: _safe_str(x).strip())
        inv_holdings["__sold_dt"] = inv_holdings[inv_id_col].map(sold_dt_by_id)
        inv_holdings["__sold_dt"] = _to_dt(inv_holdings["__sold_dt"])

        inv_holdings = inv_holdings[
            inv_holdings["__purchase_dt"].notna()
            & (inv_holdings["__purchase_dt"] <= asof_cutoff)
            & (inv_holdings["__sold_dt"].isna() | (inv_holdings["__sold_dt"] > asof_cutoff))
        ].copy()

    # -------------------------
    # ASSETS
    # -------------------------
    left, right = st.columns([1.15, 1.0])

    with left:
        st.markdown("### Assets")

        if inv_holdings.empty:
            st.info("No inventory held as of the selected period end.")
            assets_df = pd.DataFrame(columns=["Inventory", "# of items", "Cost of Goods", "Market Value"])
        else:
            inv_asof = inv_holdings.copy()
            inv_asof["__card_type"] = inv_asof[inv_card_type_col].apply(_normalize_card_type)
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
            if "__market_price" in inv_asof.columns:
                inv_asof["__mv"] = _to_num(inv_asof["__market_price"])

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
                total_items = int(len(inv_asof))
                total_cost = float(inv_asof["__cost"].sum())
                total_mv = float(inv_asof["__mv"].sum())

                assets_df = pd.concat(
                    [
                        assets_df,
                        pd.DataFrame([{
                            "Inventory": "Totals",
                            "# of items": total_items,
                            "Cost of Goods": total_cost,
                            "Market Value": total_mv,
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
        st.markdown("### Sales")

        # TOP TABLE: Listed Items overview
        if inv_holdings.empty:
            listed_df = pd.DataFrame(columns=["Listed Items", "# of items", "List Price Total", "Market Value"])
        else:
            inv_listed = inv_holdings.copy()
            inv_listed["__status_upper"] = inv_listed[inv_status_col].astype(str).str.upper().str.strip()
            inv_listed = inv_listed[inv_listed["__status_upper"].eq("LISTED")].copy()

            if inv_listed.empty:
                listed_df = pd.DataFrame([{
                    "Listed Items": "Totals",
                    "# of items": 0,
                    "List Price Total": 0.0,
                    "Market Value": 0.0,
                }])
            else:
                inv_listed["__card_type"] = inv_listed[inv_card_type_col].apply(_normalize_card_type)
                inv_listed["__mv"] = _to_num(inv_listed.get("__market_price", 0.0))

                inv_listed["__inv_id_key"] = inv_listed[inv_id_col].apply(lambda x: _safe_str(x).strip())
                inv_listed["__list_price"] = inv_listed["__inv_id_key"].map(list_price_by_inv_id).fillna(0.0)

                rows = []
                for ct in ["Sports", "Pokemon"]:
                    sub = inv_listed[inv_listed["__card_type"].str.upper() == ct.upper()].copy()
                    if sub.empty:
                        continue
                    rows.append([ct, int(len(sub)), float(sub["__list_price"].sum()), float(sub["__mv"].sum())])

                listed_df = pd.DataFrame(rows, columns=["Listed Items", "# of items", "List Price Total", "Market Value"])
                listed_df = pd.concat(
                    [
                        listed_df,
                        pd.DataFrame([{
                            "Listed Items": "Totals",
                            "# of items": int(len(inv_listed)),
                            "List Price Total": float(inv_listed["__list_price"].sum()),
                            "Market Value": float(inv_listed["__mv"].sum()),
                        }])
                    ],
                    ignore_index=True
                )

        sty_listed = (
            _style_group_and_total_rows(listed_df, "Listed Items")
            .format({"List Price Total": "${:,.2f}", "Market Value": "${:,.2f}"})
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty_listed, use_container_width=True, hide_index=True)

        # BOTTOM TABLE: Sales (Net proceeds in period)
        if txn_f.empty:
            st.info("No sales in selected period.")
            sales_df = pd.DataFrame(columns=["Sales", "# of Sales", "Dollar Sales"])
            fees_total = 0.0
            net_total = 0.0
            sales_count_total = 0
        else:
            tx = txn_f.copy()
            tx["__card_type"] = tx.apply(_tx_card_type_rowaware, axis=1)
            tx["__bucket"] = tx["__inventory_id"].map(_tx_product_bucket).fillna("Cards")

            # ✅ FIX: count should only represent sold rows (tx is already sold-filtered; this is now safe)
            sales_count_total = int(len(tx))
            fees_total = float(tx["__fees"].sum())
            net_total = float(tx["__net"].sum())

            rows = []
            for ct in ["Sports", "Pokemon"]:
                sub = tx[tx["__card_type"].str.upper() == ct.upper()].copy()
                if sub.empty:
                    continue

                rows.append([ct, int(len(sub)), float(sub["__net"].sum())])

                for b in ["Cards", "Graded Cards", "Sealed"]:
                    sb = sub[sub["__bucket"] == b]
                    rows.append([f"  {b}", int(len(sb)), float(sb["__net"].sum())])

            sales_df = pd.DataFrame(rows, columns=["Sales", "# of Sales", "Dollar Sales"])

            if not sales_df.empty:
                sales_df = pd.concat(
                    [
                        sales_df,
                        pd.DataFrame([{
                            "Sales": "Totals",
                            "# of Sales": sales_count_total,
                            "Dollar Sales": net_total,
                        }])
                    ],
                    ignore_index=True
                )

        sty3 = (
            _style_group_and_total_rows(sales_df, "Sales")
            .format({"Dollar Sales": "${:,.2f}"})
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty3, use_container_width=True, hide_index=True)

        st.markdown("### Summary")

        if not inv_f.empty:
            tmp_inv = inv_f.copy()
            tmp_inv["__inv_id_key"] = tmp_inv[inv_id_col].apply(lambda x: _safe_str(x).strip())
            tmp_inv["__grading_cost_unsynced"] = tmp_inv["__inv_id_key"].map(grading_cost_by_inv_id).fillna(0.0)
            tmp_inv["__eff_cost"] = _to_num(tmp_inv[inv_total_col]) + _to_num(tmp_inv["__grading_cost_unsynced"])
            inv_spend = float(tmp_inv["__eff_cost"].sum())
        else:
            inv_spend = 0.0

        misc_spend = float(misc_f["__amount"].sum()) if not misc_f.empty else 0.0
        total_expenses = inv_spend + misc_spend

        summary_rows = []
        for ct in ["Sports", "Pokemon"]:
            inv_ct = inv_f[inv_f[inv_card_type_col].apply(_normalize_card_type).astype(str).str.upper() == ct.upper()] if not inv_f.empty else pd.DataFrame()
            if not txn_f.empty:
                tx_tmp = txn_f.copy()
                tx_tmp["__card_type"] = tx_tmp.apply(_tx_card_type_rowaware, axis=1)
                tx_ct = tx_tmp[tx_tmp["__card_type"].astype(str).str.upper() == ct.upper()]
            else:
                tx_ct = pd.DataFrame()

            if not inv_ct.empty:
                inv_ct2 = inv_ct.copy()
                inv_ct2["__inv_id_key"] = inv_ct2[inv_id_col].apply(lambda x: _safe_str(x).strip())
                inv_ct2["__grading_cost_unsynced"] = inv_ct2["__inv_id_key"].map(grading_cost_by_inv_id).fillna(0.0)
                inv_ct2["__eff_cost"] = _to_num(inv_ct2[inv_total_col]) + _to_num(inv_ct2["__grading_cost_unsynced"])
                exp_ct = float(inv_ct2["__eff_cost"].sum())
            else:
                exp_ct = 0.0

            sales_ct = float(tx_ct["__net"].sum()) if not tx_ct.empty else 0.0
            fees_ct = float(tx_ct["__fees"].sum()) if not tx_ct.empty else 0.0
            net_ct = float(tx_ct["__net"].sum()) if not tx_ct.empty else 0.0

            if exp_ct == 0.0 and sales_ct == 0.0:
                continue

            pl_ct = net_ct - exp_ct
            summary_rows.append([ct, exp_ct, sales_ct, fees_ct, pl_ct])

        totals_pl = (net_total - total_expenses)
        summary_rows.append(["Totals", total_expenses, net_total, fees_total, totals_pl])

        summary_df = pd.DataFrame(summary_rows, columns=["Total", "Total Expenses", "Sales", "Fees/shipping", "Profit/Loss"])

        sty4 = (
            _style_group_and_total_rows(summary_df, "Total")
            .format({
                "Total Expenses": "${:,.2f}",
                "Sales": "${:,.2f}",
                "Fees/shipping": "${:,.2f}",
                "Profit/Loss": "${:,.2f}",
            })
            .applymap(_style_red_green, subset=["Profit/Loss"])
            .set_table_styles(_styler_table_header())
        )
        st.dataframe(sty4, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("## Detail Tables (where totals come from)")

    d1, d2 = st.columns(2)
    with d1:
        st.markdown("### Inventory Lines (purchases in period)")
        if inv_f.empty:
            st.info("No inventory lines.")
        else:
            show_cols = []
            for want in ["inventory_id", "card_type", "product_type", "inventory_status", "purchase_date", "total_price", "purchased_from", "set_name", "year", "card_name", "card_number", "variant"]:
                c = _pick_col(inv_f, want, None)
                if c and c in inv_f.columns:
                    show_cols.append(c)

            out = inv_f[show_cols].copy() if show_cols else inv_f.copy()
            out = _ensure_unique_columns(out)
            st.dataframe(out, use_container_width=True, hide_index=True)

    with d2:
        st.markdown("### Sales Lines (sold in period)")
        if txn_f.empty:
            st.info("No sales lines.")
        else:
            out = txn_f.copy()
            out["card_type"] = out.apply(_tx_card_type_rowaware, axis=1)
            out["bucket"] = out["__inventory_id"].map(_tx_product_bucket).fillna("Cards")

            cols = []
            if tx_date_col and tx_date_col in out.columns:
                cols.append(tx_date_col)
            if tx_inv_col and tx_inv_col in out.columns:
                cols.append(tx_inv_col)
            cols += ["card_type", "bucket", "__sold_price", "__fees", "__net"]
            cols = [c for c in cols if c in out.columns]

            view = out[cols].copy()
            view = view.rename(columns={
                "__sold_price": "sold_price",
                "__fees": "fees",
                "__net": "net",
            })
            view = _ensure_unique_columns(view)
            st.dataframe(view, use_container_width=True, hide_index=True)

    d3, d4 = st.columns(2)
    with d3:
        st.markdown("### Grading Cost Lines (submitted/created in period)")
        if grd_f.empty:
            st.info("No grading lines.")
        else:
            out = grd_f.copy()
            cols = []
            for want in ["submission_date", "created_at", "inventory_id", "grading_company", "grading_fee_initial", "additional_costs", "__grading_cost", "status", "estimated_return_date", "received_grade", "returned_grade"]:
                c = _pick_col(out, want, None)
                if c and c in out.columns:
                    cols.append(c)
            if "__grading_cost" in out.columns and "__grading_cost" not in cols:
                cols.append("__grading_cost")

            view = out[cols].copy() if cols else out.copy()
            view = view.rename(columns={"__grading_cost": "total_grading_cost"})
            view = _ensure_unique_columns(view)
            st.dataframe(view, use_container_width=True, hide_index=True)

    with d4:
        st.markdown("### Misc Expense Lines (in period)")
        if misc_f.empty:
            st.info("No misc lines.")
        else:
            out = misc_f.copy()
            view = out.copy()
            view = view.rename(columns={"__category": "category", "__amount": "amount"})
            keep = [c for c in ["category", "amount"] if c in view.columns]
            m_date = _pick_col(misc_f, "date", None) or _pick_col(misc_f, "expense_date", None)
            if m_date and m_date in misc_f.columns:
                view["date"] = misc_f[m_date]
                keep = ["date"] + keep
            view = view[keep].copy() if keep else view.copy()
            view = _ensure_unique_columns(view)
            st.dataframe(view, use_container_width=True, hide_index=True)


# =========================================================
# TAB 2: Expenses + Forecast (unchanged)
# =========================================================
with tab_forecast:
    st.subheader("Cumulative View (Monthly)")

    inv_monthly = pd.DataFrame(columns=["month", "inventory_expense"])
    if not inv.empty and "__purchase_dt" in inv.columns:
        inv_monthly = (
            inv.dropna(subset=["__purchase_dt"])
              .assign(month=_month_start(inv["__purchase_dt"]))
              .groupby("month", as_index=False)[inv_total_col]
              .sum()
              .rename(columns={inv_total_col: "inventory_expense"})
        )

    misc_monthly = pd.DataFrame(columns=["month", "misc_expense"])
    if not misc.empty and "__month" in misc.columns:
        misc_monthly = misc.groupby("__month", as_index=False)["__amount"].sum().rename(columns={"__month": "month", "__amount": "misc_expense"})

    grading_monthly = pd.DataFrame(columns=["month", "grading_expense"])
    if not grd.empty and "__grading_month" in grd.columns:
        grading_monthly = grd.groupby("__grading_month", as_index=False)["__grading_cost"].sum().rename(columns={"__grading_month": "month", "__grading_cost": "grading_expense"})

    sales_monthly = pd.DataFrame(columns=["month", "sales_net"])
    if not txn.empty and "__sold_month" in txn.columns:
        sales_monthly = txn.groupby("__sold_month", as_index=False)["__net"].sum().rename(columns={"__sold_month": "month", "__net": "sales_net"})
        sales_monthly = sales_monthly.dropna(subset=["month"])

    inv_market_by_month = pd.DataFrame(columns=["month", "inventory_market_value"])
    if not inv.empty and "__purchase_dt" in inv.columns and inv["__purchase_dt"].notna().any():
        sold_dt_by_id = {}
        if not txn.empty and "__sold_dt" in txn.columns and "__inventory_id" in txn.columns:
            tmp = txn[["__inventory_id", "__sold_dt"]].copy()
            tmp = tmp.dropna(subset=["__sold_dt"])
            sold_dt_by_id = tmp.groupby("__inventory_id")["__sold_dt"].min().to_dict()

        inv_tmp = inv.copy()
        inv_tmp[inv_id_col] = inv_tmp[inv_id_col].apply(lambda x: _safe_str(x).strip())
        inv_tmp["__sold_dt"] = inv_tmp[inv_id_col].astype(str).map(sold_dt_by_id)
        inv_tmp["__sold_dt"] = _to_dt(inv_tmp["__sold_dt"])

        min_month = _month_start(inv_tmp["__purchase_dt"]).min()
        today_month = pd.Timestamp(date.today().replace(day=1))
        max_month = today_month

        if not open_grading.empty and "__est_return_month" in open_grading.columns and open_grading["__est_return_month"].notna().any():
            max_month = max(max_month, open_grading["__est_return_month"].max())

        months = pd.date_range(min_month, max_month, freq="MS")
        rows = []
        for m in months:
            end_m = (m + pd.offsets.MonthEnd(1)).to_pydatetime()
            held = (inv_tmp["__purchase_dt"] <= end_m) & ((inv_tmp["__sold_dt"].isna()) | (inv_tmp["__sold_dt"] > end_m))
            mv = float(inv_tmp.loc[held, "__market_price"].sum()) if "__market_price" in inv_tmp.columns else 0.0
            rows.append({"month": m, "inventory_market_value": mv})
        inv_market_by_month = pd.DataFrame(rows)

    all_months = []
    for d in [inv_monthly.get("month"), misc_monthly.get("month"), grading_monthly.get("month"), sales_monthly.get("month"), inv_market_by_month.get("month")]:
        if isinstance(d, pd.Series) and not d.empty:
            all_months.append(d.dropna())

    if all_months:
        min_m = min([s.min() for s in all_months])
        max_m = max([s.max() for s in all_months])
    else:
        min_m = pd.Timestamp(date.today().replace(day=1))
        max_m = min_m

    if not open_grading.empty and "__est_return_month" in open_grading.columns and open_grading["__est_return_month"].notna().any():
        max_m = max(max_m, open_grading["__est_return_month"].max())

    months = pd.date_range(min_m, max_m, freq="MS")
    base = pd.DataFrame({"month": months})

    base = base.merge(inv_monthly, on="month", how="left")
    base = base.merge(misc_monthly, on="month", how="left")
    base = base.merge(grading_monthly, on="month", how="left")
    base = base.merge(sales_monthly, on="month", how="left")
    base = base.merge(inv_market_by_month, on="month", how="left")

    for c in ["inventory_expense", "misc_expense", "grading_expense", "sales_net", "inventory_market_value"]:
        if c not in base.columns:
            base[c] = 0.0
        base[c] = base[c].fillna(0.0)

    base["total_expense"] = base["inventory_expense"] + base["misc_expense"] + base["grading_expense"]
    base["cum_expense"] = base["total_expense"].cumsum()
    base["cum_sales_net"] = base["sales_net"].cumsum()
    base["assets_plus_sales"] = base["inventory_market_value"] + base["cum_sales_net"]

    current_month = pd.Timestamp(date.today().replace(day=1))
    forecast = base[["month", "assets_plus_sales"]].copy()
    forecast["upside"] = np.nan
    forecast["downside"] = np.nan

    if not open_grading.empty and "__psa10" in open_grading.columns and "__psa9" in open_grading.columns:
        exp10 = (
            open_grading.dropna(subset=["__est_return_month"])
                       .groupby("__est_return_month", as_index=False)["__psa10"].sum()
                       .rename(columns={"__est_return_month": "month", "__psa10": "add_10"})
        )
        exp9 = (
            open_grading.dropna(subset=["__est_return_month"])
                       .groupby("__est_return_month", as_index=False)["__psa9"].sum()
                       .rename(columns={"__est_return_month": "month", "__psa9": "add_9"})
        )

        f = base[["month", "assets_plus_sales"]].copy()
        f = f.merge(exp10, on="month", how="left").merge(exp9, on="month", how="left")
        f["add_10"] = f["add_10"].fillna(0.0)
        f["add_9"] = f["add_9"].fillna(0.0)

        next_month = current_month + pd.offsets.MonthBegin(1)
        mask_future = f["month"] >= next_month

        f.loc[mask_future, "cum_add_10_future"] = f.loc[mask_future, "add_10"].cumsum()
        f.loc[mask_future, "cum_add_9_future"] = f.loc[mask_future, "add_9"].cumsum()

        base_anchor = f.loc[f["month"] <= current_month, "assets_plus_sales"]
        anchor_val = float(base_anchor.iloc[-1]) if len(base_anchor) else float(f["assets_plus_sales"].iloc[0])

        f.loc[mask_future, "upside"] = anchor_val + f.loc[mask_future, "cum_add_10_future"]
        f.loc[mask_future, "downside"] = anchor_val + f.loc[mask_future, "cum_add_9_future"]

        forecast = f[["month", "assets_plus_sales", "upside", "downside"]].copy()

    chart_df = base[["month", "cum_expense", "assets_plus_sales"]].copy().rename(columns={
        "cum_expense": "Total Expenses (Cumulative)",
        "assets_plus_sales": "Inventory Market Value + Sales (Actual)",
    })
    chart_long = chart_df.melt("month", var_name="series", value_name="value")

    forecast_long = forecast[["month", "upside", "downside"]].copy()
    forecast_long = forecast_long.melt("month", var_name="series", value_name="value").dropna(subset=["value"])
    forecast_long["series"] = forecast_long["series"].map({
        "upside": "Forecast Upside (All PSA 10s)",
        "downside": "Forecast Downside (All PSA 9s)"
    })

    base_area = alt.Chart(chart_long).mark_area(opacity=0.35).encode(
        x=alt.X("month:T", title="Month", axis=alt.Axis(format="%Y-%m", labelAngle=-45)),
        y=alt.Y("value:Q", title="$"),
        color=alt.Color("series:N", legend=alt.Legend(title="")),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%Y-%m"),
            alt.Tooltip("series:N", title="Series"),
            alt.Tooltip("value:Q", title="Value", format=",.2f"),
        ],
    )

    actual_line = alt.Chart(chart_df).mark_line(size=2).encode(
        x="month:T",
        y=alt.Y("Inventory Market Value + Sales (Actual):Q"),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%Y-%m"),
            alt.Tooltip("Inventory Market Value + Sales (Actual):Q", title="Actual", format=",.2f"),
        ],
    )

    forecast_line = alt.Chart(forecast_long).mark_line(size=2, strokeDash=[6, 6]).encode(
        x="month:T",
        y="value:Q",
        color=alt.Color("series:N", legend=alt.Legend(title="")),
        tooltip=[
            alt.Tooltip("month:T", title="Month", format="%Y-%m"),
            alt.Tooltip("series:N", title="Forecast"),
            alt.Tooltip("value:Q", title="Value", format=",.2f"),
        ],
    )

    chart = (base_area + actual_line + forecast_line).properties(height=420).interactive()
    st.altair_chart(chart, use_container_width=True)

    st.markdown("### Monthly rollup")
    roll = base[["month", "total_expense", "sales_net", "cum_expense", "cum_sales_net", "inventory_market_value", "assets_plus_sales"]].copy()
    roll["month"] = roll["month"].dt.strftime("%Y-%m")
    roll = roll.rename(columns={
        "total_expense": "expenses",
        "sales_net": "sales_net",
        "cum_expense": "expenses_cum",
        "cum_sales_net": "sales_cum",
        "inventory_market_value": "inventory_market_value",
        "assets_plus_sales": "inventory_market_value_plus_sales",
    })

    st.dataframe(
        roll.style.format({
            "expenses": "${:,.2f}",
            "sales_net": "${:,.2f}",
            "expenses_cum": "${:,.2f}",
            "sales_cum": "${:,.2f}",
            "inventory_market_value": "${:,.2f}",
            "inventory_market_value_plus_sales": "${:,.2f}",
        }).set_table_styles(_styler_table_header()),
        use_container_width=True,
        hide_index=True,
    )


# =========================================================
# TAB 3: Benchmarks
# =========================================================
with tab_bench:
    st.subheader("Benchmarks vs Targets")

    target_gem_rate = 0.75
    gem_rate = None

    if not grd.empty:
        got = grd.copy()
        grade_col = None
        for cand in ["received_grade", "returned_grade", "grade"]:
            c = _pick_col(got, cand, None)
            if c and c in got.columns:
                grade_col = c
                break
        status_col = _pick_col(got, "status", "status")

        if grade_col and grade_col in got.columns:
            got["__grade"] = got[grade_col].astype(str).str.strip().str.upper()
            got["__is_returned"] = got[status_col].astype(str).str.upper().eq("RETURNED")
            returned = got[got["__is_returned"]].copy()
            if len(returned):
                returned["__gem"] = returned["__grade"].isin(["10", "PRISTINE 10", "BLACK LABEL 10"])
                gem_rate = returned["__gem"].mean()

    inv_count = len(inv) if not inv.empty else 0
    graded_count = len(grd) if not grd.empty else 0
    target_grade_rate = 0.50
    grade_rate = (graded_count / inv_count) if inv_count else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Inventory Items", f"{inv_count:,}")
    c2.metric("Cards Submitted for Grading", f"{graded_count:,}")
    c3.metric("Grade Rate (Submitted / Inventory)", f"{grade_rate*100:,.1f}%", delta=f"Target {target_grade_rate*100:.0f}%")

    st.markdown("---")
    if gem_rate is None:
        st.info("Gem rate will populate once you have RETURNED submissions with a received grade.")
    else:
        st.metric("Gem Rate (10 / Returned)", f"{gem_rate*100:,.1f}%", delta=f"Target {target_gem_rate*100:.0f}%")
