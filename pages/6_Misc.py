import json
import re
import time
import uuid
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Misc", layout="wide")
st.title("Misc / Tax Tracking")

# ----------------------------
# Config
# ----------------------------
SPREADSHEET_ID = st.secrets["spreadsheet_id"]
MISC_WS_NAME = st.secrets.get("misc_worksheet", "misc")
MILEAGE_WS_NAME = st.secrets.get("mileage_worksheet", "mileage")

MISC_COLUMNS = [
    "misc_id",
    "expense_date",
    "category",
    "description",
    "amount",
    "notes",
    "created_at",
]

MILEAGE_COLUMNS = [
    "mileage_id",
    "trip_date",
    "show_name",
    "business_purpose",
    "start_location",
    "end_location",
    "round_trip",
    "miles",
    "parking_tolls",
    "notes",
    "created_at",
]

CATEGORY_OPTIONS = [
    "Packaging materials",
    "Card show fees",
    "Supplies",
    "Shipping supplies",
    "Subscriptions",
    "Mileage/Travel",
    "Other",
]

# Do not hard-code a tax deduction rate into the books. The yearly IRS standard
# mileage rate can change, so the app records miles and lets you apply the rate
# when you prepare taxes.


# ----------------------------
# Sheets Auth / Helpers
# ----------------------------
def _is_retryable_gspread_error(e: Exception) -> bool:
    try:
        if not isinstance(e, gspread.exceptions.APIError):
            return False
        response = getattr(e, "response", None)
        status_code = getattr(response, "status_code", None)
        return status_code in {429, 500, 502, 503, 504}
    except Exception:
        return False


def _with_backoff(fn, tries: int = 6, base_sleep: float = 0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if _is_retryable_gspread_error(e):
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
    raise last


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
        sa_info = json.loads(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: JSON file
    if "service_account_json_path" in st.secrets:
        p = Path(st.secrets["service_account_json_path"])
        if not p.is_absolute():
            p = Path.cwd() / p
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets: add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')


@st.cache_resource
def get_sheet():
    client = get_gspread_client()
    return _with_backoff(lambda: client.open_by_key(SPREADSHEET_ID))


def get_or_create_ws(ws_name: str, headers: list[str]):
    sh = get_sheet()
    try:
        ws = _with_backoff(lambda: sh.worksheet(ws_name))
    except gspread.exceptions.WorksheetNotFound:
        ws = _with_backoff(lambda: sh.add_worksheet(title=ws_name, rows=1000, cols=max(len(headers) + 5, 20)))
        _with_backoff(lambda: ws.update("1:1", [headers], value_input_option="USER_ENTERED"))
        return ws

    ensure_headers(ws, headers)
    return ws


def ensure_headers(ws, headers: list[str]) -> list[str]:
    existing = _with_backoff(lambda: ws.row_values(1))
    if not existing:
        _with_backoff(lambda: ws.update("1:1", [headers], value_input_option="USER_ENTERED"))
        return headers

    missing = [c for c in headers if c not in existing]
    if missing:
        new_headers = existing + missing
        _with_backoff(lambda: ws.update("1:1", [new_headers], value_input_option="USER_ENTERED"))
        return new_headers
    return existing


def _safe_money(x) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip()
        if not s:
            return 0.0
        neg = s.startswith("(") and s.endswith(")")
        s = re.sub(r"[^0-9.\-]", "", s.replace(",", ""))
        if s in {"", ".", "-", "-."}:
            return 0.0
        val = float(s)
        return -val if neg and val > 0 else val
    except Exception:
        return 0.0


def _read_ws_df(ws_name: str, headers: list[str]) -> pd.DataFrame:
    ws = get_or_create_ws(ws_name, headers)
    values = _with_backoff(lambda: ws.get_all_values())
    if not values or not values[0]:
        return pd.DataFrame(columns=headers)

    sheet_headers = values[0]
    rows = values[1:] if len(values) > 1 else []
    normalized_rows = []
    for r in rows:
        if len(r) < len(sheet_headers):
            r = r + [""] * (len(sheet_headers) - len(r))
        elif len(r) > len(sheet_headers):
            r = r[:len(sheet_headers)]
        normalized_rows.append(r)

    df = pd.DataFrame(normalized_rows, columns=sheet_headers)
    for c in headers:
        if c not in df.columns:
            df[c] = ""
    return df[headers].copy()


@st.cache_data(ttl=30, show_spinner=False)
def load_misc_df() -> pd.DataFrame:
    df = _read_ws_df(MISC_WS_NAME, MISC_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=MISC_COLUMNS)

    df["amount"] = df["amount"].apply(_safe_money)
    df["expense_date"] = pd.to_datetime(df["expense_date"], errors="coerce").dt.date
    df = df.sort_values(by=["expense_date", "created_at"], ascending=[False, False], na_position="last")
    return df[MISC_COLUMNS].copy()


@st.cache_data(ttl=30, show_spinner=False)
def load_mileage_df() -> pd.DataFrame:
    df = _read_ws_df(MILEAGE_WS_NAME, MILEAGE_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=MILEAGE_COLUMNS)

    df["trip_date"] = pd.to_datetime(df["trip_date"], errors="coerce").dt.date
    df["miles"] = df["miles"].apply(_safe_money)
    df["parking_tolls"] = df["parking_tolls"].apply(_safe_money)
    df = df.sort_values(by=["trip_date", "created_at"], ascending=[False, False], na_position="last")
    return df[MILEAGE_COLUMNS].copy()


def append_row(ws_name: str, headers: list[str], row: dict):
    ws = get_or_create_ws(ws_name, headers)
    sheet_headers = ensure_headers(ws, headers)
    _with_backoff(lambda: ws.append_row([row.get(h, "") for h in sheet_headers], value_input_option="USER_ENTERED"))


def refresh():
    load_misc_df.clear()
    load_mileage_df.clear()
    st.rerun()


def _month_options(date_series: pd.Series) -> list[str]:
    parsed = pd.to_datetime(date_series, errors="coerce").dropna()
    if parsed.empty:
        return ["All"]
    return ["All"] + sorted(parsed.dt.to_period("M").astype(str).unique().tolist(), reverse=True)


# ----------------------------
# UI
# ----------------------------
top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Refresh", use_container_width=True):
        refresh()

tab_expense, tab_mileage, tab_history, tab_mileage_history, tab_summary = st.tabs([
    "New Expense",
    "Mileage Log",
    "Expense History",
    "Mileage History",
    "Summary",
])

with tab_expense:
    st.subheader("Add a Misc Expense")

    with st.form("misc_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 1, 2])
        with c1:
            expense_date = st.date_input("Expense date*", value=date.today())
        with c2:
            category = st.selectbox("Category*", CATEGORY_OPTIONS, index=0)
        with c3:
            description = st.text_input("Description*", placeholder="e.g., table fee, top loaders, bubble mailers...")

        c4, c5 = st.columns([1, 3])
        with c4:
            amount = st.number_input("Amount*", min_value=0.0, step=1.0, format="%.2f")
        with c5:
            notes = st.text_area("Notes (optional)", height=80)

        submit = st.form_submit_button("Add Expense", type="primary", use_container_width=True)
        if submit:
            if not description.strip():
                st.error("Description is required.")
            else:
                row = {
                    "misc_id": str(uuid.uuid4())[:10],
                    "expense_date": str(expense_date),
                    "category": category,
                    "description": description.strip(),
                    "amount": float(amount),
                    "notes": notes.strip() if notes else "",
                    "created_at": datetime.utcnow().isoformat(),
                }
                append_row(MISC_WS_NAME, MISC_COLUMNS, row)
                st.success("Added misc expense.")
                refresh()

with tab_mileage:
    st.subheader("Add a Mileage / Travel Record")
    st.caption("Track business miles and any parking/tolls for shows, sourcing trips, post office runs, bank runs, etc. Keep the miles here; apply the correct tax rate when filing.")

    with st.form("mileage_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 1.4, 1.6])
        with c1:
            trip_date = st.date_input("Trip date*", value=date.today())
        with c2:
            show_name = st.text_input("Show / trip name", placeholder="e.g., Knoxville Card Show")
        with c3:
            business_purpose = st.text_input("Business purpose*", placeholder="e.g., vending, sourcing inventory, post office drop-off")

        c4, c5 = st.columns(2)
        with c4:
            start_location = st.text_input("Start location", placeholder="e.g., Home / Huntsville")
        with c5:
            end_location = st.text_input("End location", placeholder="e.g., Knoxville, TN")

        c6, c7, c8 = st.columns([1, 1, 1])
        with c6:
            round_trip = st.checkbox("Round trip", value=True)
        with c7:
            miles = st.number_input("Business miles*", min_value=0.0, step=1.0, format="%.1f")
        with c8:
            parking_tolls = st.number_input("Parking / tolls", min_value=0.0, step=1.0, format="%.2f")

        notes = st.text_area("Notes (optional)", height=80, placeholder="Anything useful for tax backup: route, reason, who/what show, etc.")

        submit_miles = st.form_submit_button("Add Mileage Record", type="primary", use_container_width=True)
        if submit_miles:
            if not business_purpose.strip():
                st.error("Business purpose is required.")
            elif miles <= 0:
                st.error("Miles must be greater than 0.")
            else:
                row = {
                    "mileage_id": str(uuid.uuid4())[:10],
                    "trip_date": str(trip_date),
                    "show_name": show_name.strip(),
                    "business_purpose": business_purpose.strip(),
                    "start_location": start_location.strip(),
                    "end_location": end_location.strip(),
                    "round_trip": "Yes" if round_trip else "No",
                    "miles": float(miles),
                    "parking_tolls": float(parking_tolls),
                    "notes": notes.strip() if notes else "",
                    "created_at": datetime.utcnow().isoformat(),
                }
                append_row(MILEAGE_WS_NAME, MILEAGE_COLUMNS, row)
                st.success("Added mileage record.")
                refresh()

with tab_history:
    st.subheader("Misc Expense History")
    df = load_misc_df()
    if df.empty:
        st.info("No misc expenses yet.")
    else:
        f1, f2, f3 = st.columns([1.2, 1.2, 2.6])
        with f1:
            cat_filter = st.multiselect("Category", sorted(df["category"].dropna().unique().tolist()), default=[])
        with f2:
            month_filter = st.selectbox("Month", options=_month_options(df["expense_date"]), index=0, key="expense_month")
        with f3:
            search = st.text_input("Search", placeholder="Search description/notes...", key="expense_search")

        view = df.copy()
        if cat_filter:
            view = view[view["category"].isin(cat_filter)]
        if month_filter != "All":
            view = view[pd.to_datetime(view["expense_date"], errors="coerce").dt.strftime("%Y-%m") == month_filter]
        if search.strip():
            q = search.strip().lower()
            view = view[view.apply(lambda r: q in str(r.get("description", "")).lower() or q in str(r.get("notes", "")).lower(), axis=1)]

        st.caption(f"{len(view):,} expense(s) shown")
        show = view.copy()
        show["amount"] = show["amount"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(show, use_container_width=True, hide_index=True)

with tab_mileage_history:
    st.subheader("Mileage History")
    mdf = load_mileage_df()
    if mdf.empty:
        st.info("No mileage records yet.")
    else:
        f1, f2 = st.columns([1.2, 3])
        with f1:
            month_filter = st.selectbox("Month", options=_month_options(mdf["trip_date"]), index=0, key="mileage_month")
        with f2:
            search = st.text_input("Search", placeholder="Search show, purpose, location, notes...", key="mileage_search")

        view = mdf.copy()
        if month_filter != "All":
            view = view[pd.to_datetime(view["trip_date"], errors="coerce").dt.strftime("%Y-%m") == month_filter]
        if search.strip():
            q = search.strip().lower()
            view = view[view.apply(
                lambda r: any(q in str(r.get(c, "")).lower() for c in ["show_name", "business_purpose", "start_location", "end_location", "notes"]),
                axis=1,
            )]

        st.caption(f"{len(view):,} mileage record(s) shown")
        show = view.copy()
        show["miles"] = show["miles"].apply(lambda x: f"{float(x):,.1f}")
        show["parking_tolls"] = show["parking_tolls"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(show, use_container_width=True, hide_index=True)

with tab_summary:
    st.subheader("Tax Tracking Summary")
    df = load_misc_df()
    mdf = load_mileage_df()

    total_misc = float(df["amount"].sum()) if not df.empty else 0.0
    total_miles = float(mdf["miles"].sum()) if not mdf.empty else 0.0
    total_parking_tolls = float(mdf["parking_tolls"].sum()) if not mdf.empty else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Misc Spend", f"${total_misc:,.2f}")
    k2.metric("Business Miles", f"{total_miles:,.1f}")
    k3.metric("Parking / Tolls", f"${total_parking_tolls:,.2f}")

    st.markdown("---")

    left, right = st.columns(2)
    with left:
        st.markdown("### Expense Summary by Month")
        if df.empty:
            st.info("No expenses yet.")
        else:
            df2 = df.copy()
            df2["month"] = pd.to_datetime(df2["expense_date"], errors="coerce").dt.to_period("M").astype(str)
            monthly = df2.groupby("month", dropna=False).agg(total=("amount", "sum"), count=("misc_id", "count")).reset_index()
            monthly = monthly.sort_values("month")
            st.dataframe(monthly, use_container_width=True, hide_index=True)
            st.bar_chart(monthly.set_index("month")[["total"]])

    with right:
        st.markdown("### Mileage Summary by Month")
        if mdf.empty:
            st.info("No mileage records yet.")
        else:
            m2 = mdf.copy()
            m2["month"] = pd.to_datetime(m2["trip_date"], errors="coerce").dt.to_period("M").astype(str)
            monthly_miles = m2.groupby("month", dropna=False).agg(
                miles=("miles", "sum"),
                parking_tolls=("parking_tolls", "sum"),
                trips=("mileage_id", "count"),
            ).reset_index().sort_values("month")
            st.dataframe(monthly_miles, use_container_width=True, hide_index=True)
            st.bar_chart(monthly_miles.set_index("month")[["miles"]])
