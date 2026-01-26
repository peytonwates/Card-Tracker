import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Misc", layout="wide")
st.title("Misc Expenses")

# ----------------------------
# Config
# ----------------------------
SPREADSHEET_ID = st.secrets["spreadsheet_id"]
MISC_WS_NAME = st.secrets.get("misc_worksheet", "misc")

MISC_COLUMNS = [
    "misc_id",
    "expense_date",
    "category",
    "description",
    "amount",
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


# ----------------------------
# Sheets Auth / Helpers
# ----------------------------
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


def get_ws():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(MISC_WS_NAME)


def ensure_headers(ws):
    existing = ws.row_values(1)
    if not existing:
        ws.append_row(MISC_COLUMNS)
        return MISC_COLUMNS
    missing = [c for c in MISC_COLUMNS if c not in existing]
    if missing:
        ws.update("1:1", [existing + missing])
        return existing + missing
    return existing


@st.cache_data(ttl=30)
def load_misc_df():
    ws = get_ws()
    ensure_headers(ws)
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        return pd.DataFrame(columns=MISC_COLUMNS)

    for c in MISC_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # Date parse
    df["expense_date"] = pd.to_datetime(df["expense_date"], errors="coerce").dt.date

    # Sort newest first
    df = df.sort_values(by=["expense_date", "created_at"], ascending=[False, False], na_position="last")

    return df[MISC_COLUMNS].copy()


def append_misc_row(row: dict):
    ws = get_ws()
    headers = ensure_headers(ws)
    ws.append_row([row.get(h, "") for h in headers], value_input_option="USER_ENTERED")


def refresh():
    load_misc_df.clear()
    st.rerun()


# ----------------------------
# UI
# ----------------------------
top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Refresh", use_container_width=True):
        refresh()

tab_new, tab_history, tab_summary = st.tabs(["New Expense", "History", "Summary"])

with tab_new:
    st.subheader("Add a Misc Expense")

    with st.form("misc_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1, 1, 2])

        with c1:
            expense_date = st.date_input("Expense date*", value=date.today())
        with c2:
            category = st.selectbox("Category*", CATEGORY_OPTIONS, index=0)
        with c3:
            description = st.text_input("Description*", placeholder="e.g., bubble mailers, table fee, top loaders...")

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
                    "misc_id": str(uuid.uuid4())[:10] if "uuid" in globals() else datetime.utcnow().strftime("%y%m%d%H%M%S"),
                    "expense_date": str(expense_date),
                    "category": category,
                    "description": description.strip(),
                    "amount": float(amount),
                    "notes": notes.strip() if notes else "",
                    "created_at": datetime.utcnow().isoformat(),
                }
                append_misc_row(row)
                st.success("Added misc expense.")
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
            month_filter = st.selectbox(
                "Month",
                options=["All"] + sorted(
                    [d.strftime("%Y-%m") for d in pd.to_datetime(df["expense_date"], errors="coerce").dropna().dt.to_period("M").dt.to_timestamp().dt.date.unique()],
                    reverse=True,
                ),
                index=0,
            )
        with f3:
            search = st.text_input("Search", placeholder="Search description/notes...")

        view = df.copy()
        if cat_filter:
            view = view[view["category"].isin(cat_filter)]
        if month_filter != "All":
            view = view[pd.to_datetime(view["expense_date"]).dt.strftime("%Y-%m") == month_filter]
        if search.strip():
            s = search.strip().lower()
            view = view[view.apply(lambda r: s in str(r.get("description", "")).lower() or s in str(r.get("notes", "")).lower(), axis=1)]

        st.caption(f"{len(view):,} expense(s) shown")

        show = view.copy()
        show["amount"] = show["amount"].apply(lambda x: f"${float(x):,.2f}")
        st.dataframe(show, use_container_width=True, hide_index=True)

with tab_summary:
    st.subheader("Summary")

    df = load_misc_df()
    if df.empty:
        st.info("No misc expenses yet.")
    else:
        df2 = df.copy()
        df2["month"] = pd.to_datetime(df2["expense_date"], errors="coerce").dt.to_period("M").astype(str)

        monthly = df2.groupby("month", dropna=False).agg(total=("amount", "sum"), count=("misc_id", "count")).reset_index()
        monthly = monthly.sort_values("month")

        total_all = df2["amount"].sum()
        st.metric("Total Misc Spend (all time)", f"${total_all:,.2f}")

        st.dataframe(monthly, use_container_width=True, hide_index=True)
        st.bar_chart(monthly.set_index("month")[["total"]])
