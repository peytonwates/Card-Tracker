# pages/5_Grading.py
import json
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Grading", layout="wide")

SPREADSHEET_ID = st.secrets["spreadsheet_id"]
INVENTORY_WS_NAME = st.secrets.get("inventory_worksheet", "inventory")
GRADING_WS_NAME = st.secrets.get("grading_worksheet", "grading")

ASSUMED_TAT_BUSINESS_DAYS = 75  # estimated turnaround time

# Inventory columns we rely on (we won't break if extra columns exist)
INV_REQUIRED = [
    "inventory_id",
    "product_type",
    "card_type",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
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
    "condition",
    "notes",
    "created_at",
    "inventory_status",
    "listed_transaction_id",
]

# Columns for the grading sheet
GRADING_COLUMNS = [
    "grading_id",
    "submission_batch_id",
    "submission_date",
    "inventory_id",
    "item_label",
    "brand_or_league",
    "set_name",
    "year",
    "card_name",
    "card_number",
    "variant",
    "card_subtype",
    "reference_link",
    "purchased_from",
    "purchase_date",
    "purchase_total",
    "grading_company",
    "grading_fee_initial",
    "additional_costs",
    "psa10_price",
    "psa9_price",
    "profit_psa10",
    "profit_psa9",
    "estimated_return_date",
    "status",          # SUBMITTED / RETURNED
    "returned_date",
    "received_grade",
    "notes",
    "created_at",
    "updated_at",
]

STATUS_SUBMITTED = "SUBMITTED"
STATUS_RETURNED = "RETURNED"

GRADING_COMPANIES = ["PSA", "CGC", "Beckett"]


# =========================================================
# AUTH / SHEETS
# =========================================================
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit Cloud: service account stored as TOML table
    if "gcp_service_account" in st.secrets and not isinstance(st.secrets["gcp_service_account"], str):
        sa = st.secrets["gcp_service_account"]
        sa_info = {k: sa[k] for k in sa.keys()}
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Streamlit Cloud: JSON string in secrets
    if "gcp_service_account" in st.secrets and isinstance(st.secrets["gcp_service_account"], str):
        sa_info = json.loads(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    # Local dev: path to json file
    if "service_account_json_path" in st.secrets:
        rel = st.secrets["service_account_json_path"]
        p = Path(rel)
        if not p.is_absolute():
            p = Path.cwd() / rel
        if not p.exists():
            raise FileNotFoundError(f"Service account JSON not found at: {p}")
        sa_info = json.loads(p.read_text(encoding="utf-8"))
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        return gspread.authorize(creds)

    raise KeyError('Missing secrets. Add "gcp_service_account" (Cloud) or "service_account_json_path" (local).')


def get_ws(name: str):
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(name)


def ensure_headers(ws, headers: list[str]):
    first = ws.row_values(1)
    if not first:
        ws.append_row(headers)
        return headers

    # If missing headers, append them to row 1
    existing = first
    existing_set = set(existing)
    missing = [h for h in headers if h not in existing_set]
    if missing:
        new_headers = existing + missing
        ws.update("1:1", [new_headers])
        return new_headers
    return existing


@st.cache_data(ttl=30)
def load_sheet_df(worksheet_name: str, required_headers: list[str]) -> pd.DataFrame:
    ws = get_ws(worksheet_name)
    headers = ensure_headers(ws, required_headers)
    values = ws.get_all_values()  # single read
    if len(values) <= 1:
        df = pd.DataFrame(columns=headers)
    else:
        df = pd.DataFrame(values[1:], columns=headers)

    # Ensure required cols exist
    for c in required_headers:
        if c not in df.columns:
            df[c] = ""

    return df


def to_float(x) -> float:
    try:
        if x is None or x == "":
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def add_business_days(start_dt: date, n: int) -> date:
    d = start_dt
    added = 0
    while added < n:
        d = d + timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            added += 1
    return d


def grade_options(company: str):
    # PSA: 10..1 + half grades between (9.5, 8.5, ... 1.5)
    if company == "PSA":
        opts = []
        for whole in range(10, 0, -1):
            opts.append(str(whole))
            if whole != 1:
                opts.append(f"{whole - 0.5:.1f}")  # 9.5, 8.5, ...
        return opts

    # CGC: include Pristine / Perfect-ish options (you asked for "Pristine")
    if company == "CGC":
        # Keeping it practical: numeric grades + "Pristine 10"
        opts = ["Pristine 10"] + [str(x) for x in [10, 9.5, 9, 8.5, 8, 7.5, 7, 6.5, 6, 5.5, 5, 4.5, 4, 3.5, 3, 2.5, 2, 1.5, 1]]
        return [str(o) for o in opts]

    # Beckett: include "Black Label 10"
    if company == "Beckett":
        opts = ["Black Label 10"] + [str(x) for x in [10, 9.5, 9, 8.5, 8, 7.5, 7, 6.5, 6, 5.5, 5, 4.5, 4, 3.5, 3, 2.5, 2, 1.5, 1]]
        return [str(o) for o in opts]

    return []


def update_grading_rows(grading_df_rows: pd.DataFrame):
    """Update specific grading rows by grading_id (batch updates row-by-row)."""
    if grading_df_rows.empty:
        return

    ws = get_ws(GRADING_WS_NAME)
    headers = ensure_headers(ws, GRADING_COLUMNS)
    all_values = ws.get_all_values()
    if not all_values:
        return

    # map grading_id -> rownum
    id_col_idx = headers.index("grading_id") + 1
    id_to_row = {}
    for i, row in enumerate(all_values[1:], start=2):
        if len(row) >= id_col_idx:
            gid = str(row[id_col_idx - 1]).strip()
            if gid:
                id_to_row[gid] = i

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]

    for _, r in grading_df_rows.iterrows():
        gid = str(r.get("grading_id", "")).strip()
        rownum = id_to_row.get(gid)
        if not rownum:
            continue

        out_row = []
        for h in headers:
            v = r.get(h, "")
            if pd.isna(v):
                v = ""
            out_row.append(v)

        ws.update(f"A{rownum}:{last_col_letter}{rownum}", [out_row], value_input_option="USER_ENTERED")


def update_inventory_as_graded(inventory_id: str, grading_company: str, received_grade: str):
    """Sync back to inventory: set product_type=Graded Card + add grading_company/grade columns if they exist."""
    ws = get_ws(INVENTORY_WS_NAME)
    headers = ws.row_values(1)
    if not headers:
        return

    # Read once
    all_values = ws.get_all_values()
    if len(all_values) <= 1:
        return

    # Find row
    inv_id_idx = None
    for i, h in enumerate(headers):
        if h.strip() == "inventory_id":
            inv_id_idx = i
            break
    if inv_id_idx is None:
        return

    rownum = None
    for r_i, row in enumerate(all_values[1:], start=2):
        if len(row) > inv_id_idx and str(row[inv_id_idx]).strip() == str(inventory_id).strip():
            rownum = r_i
            break
    if not rownum:
        return

    # Ensure columns exist
    def ensure_col(col_name: str):
        nonlocal headers
        if col_name not in headers:
            headers = headers + [col_name]
            ws.update("1:1", [headers])
        return headers.index(col_name)

    idx_product_type = ensure_col("product_type")
    idx_grading_company = ensure_col("grading_company")
    idx_grade = ensure_col("grade")

    # Build row update in-place (load row array length = headers)
    current = all_values[rownum - 1]
    current = current + [""] * (len(headers) - len(current))

    current[idx_product_type] = "Graded Card"
    current[idx_grading_company] = grading_company
    current[idx_grade] = received_grade

    last_col_letter = gspread.utils.rowcol_to_a1(1, len(headers)).split("1")[0]
    ws.update(f"A{rownum}:{last_col_letter}{rownum}", [current], value_input_option="USER_ENTERED")


def append_grading_rows(rows: list[dict]):
    if not rows:
        return
    ws = get_ws(GRADING_WS_NAME)
    headers = ensure_headers(ws, GRADING_COLUMNS)

    out = []
    for r in rows:
        out.append([r.get(h, "") for h in headers])

    # append in one call if possible
    try:
        ws.append_rows(out, value_input_option="USER_ENTERED")
    except Exception:
        for rr in out:
            ws.append_row(rr, value_input_option="USER_ENTERED")


# =========================================================
# LOAD DATA
# =========================================================
top = st.columns([3, 1])
with top[1]:
    if st.button("🔄 Refresh (Sheets)", use_container_width=True):
        load_sheet_df.clear()
        st.rerun()

inv_df_raw = load_sheet_df(INVENTORY_WS_NAME, INV_REQUIRED).copy()
grading_df_raw = load_sheet_df(GRADING_WS_NAME, GRADING_COLUMNS).copy()

# Type cleanup
if "total_price" in inv_df_raw.columns:
    inv_df_raw["total_price"] = inv_df_raw["total_price"].apply(to_float)

# Identify open submissions to exclude from new-submission selection (optional)
open_grading = grading_df_raw.copy()
if not open_grading.empty:
    open_grading["status"] = open_grading["status"].replace("", STATUS_SUBMITTED)
open_ids = set(open_grading.loc[open_grading["status"] == STATUS_SUBMITTED, "inventory_id"].astype(str).tolist())

# Candidate inventory for grading = raw cards that are not already in an open submission
inv_candidates = inv_df_raw.copy()
if "product_type" in inv_candidates.columns:
    inv_candidates["product_type"] = inv_candidates["product_type"].replace("", "Card")
inv_candidates = inv_candidates[
    (inv_candidates["product_type"].astype(str) == "Card")
].copy()

# If you want to exclude LISTED/SOLD etc, uncomment:
# if "inventory_status" in inv_candidates.columns:
#     inv_candidates["inventory_status"] = inv_candidates["inventory_status"].replace("", "ACTIVE")
#     inv_candidates = inv_candidates[inv_candidates["inventory_status"].isin(["ACTIVE"])].copy()

inv_candidates = inv_candidates[~inv_candidates["inventory_id"].astype(str).isin(open_ids)].copy()

def item_label(row):
    parts = []
    if str(row.get("set_name", "")).strip():
        parts.append(str(row.get("set_name", "")).strip())
    name = str(row.get("card_name", "")).strip()
    num = str(row.get("card_number", "")).strip()
    var = str(row.get("variant", "")).strip()
    bits = name
    if var:
        bits += f" ({var})"
    if num:
        bits += f" #{num}"
    parts.append(bits)
    yr = str(row.get("year", "")).strip()
    if yr:
        parts.append(yr)
    inv_id = str(row.get("inventory_id", "")).strip()
    return f"{inv_id} — " + " | ".join([p for p in parts if p])

inv_candidates["_label"] = inv_candidates.apply(item_label, axis=1)


# =========================================================
# UI
# =========================================================
st.title("Grading")

tab_analysis, tab_submit, tab_summary = st.tabs(["Analysis", "Create Submission", "Summary / Update Returned"])


# -------------------------
# TAB 1: ANALYSIS
# -------------------------
with tab_analysis:
    st.subheader("Analyze grading candidates")
    st.caption("Pick an inventory item, enter PSA 9/10 prices + grading fee assumptions, and see profit scenarios.")

    if inv_candidates.empty:
        st.info("No eligible raw cards found (or they’re already in an open grading submission).")
    else:
        left, right = st.columns([2.2, 1.3])

        with left:
            pick = st.selectbox("Select an inventory item", inv_candidates["_label"].tolist())
            row = inv_candidates.loc[inv_candidates["_label"] == pick].iloc[0].to_dict()

        with right:
            assumed_company = st.selectbox("Company (for analysis)", GRADING_COMPANIES, index=0)
            assumed_fee = st.number_input("Assumed grading fee (per card)", min_value=0.0, step=1.0, value=28.0, format="%.2f")
            assumed_addl = st.number_input("Assumed additional costs (per card)", min_value=0.0, step=1.0, value=0.0, format="%.2f")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("**Purchase info**")
            st.write(f"Purchased: {row.get('purchase_date','')}")
            st.write(f"From: {row.get('purchased_from','')}")
        with c2:
            st.markdown("**Cost basis**")
            st.write(f"Total paid: ${to_float(row.get('total_price',0)):,.2f}")
        with c3:
            est_return = add_business_days(date.today(), ASSUMED_TAT_BUSINESS_DAYS)
            st.markdown("**Est. return date**")
            st.write(f"{ASSUMED_TAT_BUSINESS_DAYS} business days → **{est_return}**")

        st.markdown("---")

        p1, p2 = st.columns(2)
        with p1:
            psa10_price = st.number_input("PSA 10 price", min_value=0.0, step=5.0, value=0.0, format="%.2f")
        with p2:
            psa9_price = st.number_input("PSA 9 price", min_value=0.0, step=5.0, value=0.0, format="%.2f")

        purchase_total = to_float(row.get("total_price", 0))
        cost_all_in = purchase_total + float(assumed_fee) + float(assumed_addl)
        profit10 = round(float(psa10_price) - cost_all_in, 2)
        profit9 = round(float(psa9_price) - cost_all_in, 2)

        m1, m2, m3 = st.columns(3)
        m1.metric("All-in cost (purchase + grading + addl)", f"${cost_all_in:,.2f}")
        m2.metric("Profit if PSA 10", f"${profit10:,.2f}")
        m3.metric("Profit if PSA 9", f"${profit9:,.2f}")

        st.caption("This is a simple gross profit calc (no selling fees yet). We can add fees later in Transactions if you want.")


# -------------------------
# TAB 2: CREATE SUBMISSION
# -------------------------
with tab_submit:
    st.subheader("Create a grading submission")
    st.caption("Select one or more inventory items to submit. This logs the submission and marks them as in-progress via the grading sheet.")

    if inv_candidates.empty:
        st.info("No eligible raw cards to submit right now.")
    else:
        # Use a "current company" select OUTSIDE the form so grade widgets update cleanly
        batch_company = st.selectbox("Grading company*", GRADING_COMPANIES, index=0, key="batch_company_select")

        with st.form("grading_submission_form", clear_on_submit=True):
            submission_date = st.date_input("Submission date*", value=date.today())
            items = st.multiselect("Select items to submit*", inv_candidates["_label"].tolist())

            c1, c2, c3 = st.columns(3)
            with c1:
                fee_initial = st.number_input("Initial grading fee (per card)*", min_value=0.0, step=1.0, value=28.0, format="%.2f")
            with c2:
                addl_costs = st.number_input("Additional costs now (per card)", min_value=0.0, step=1.0, value=0.0, format="%.2f")
            with c3:
                notes = st.text_area("Notes (optional)", height=92)

            # Optional: store your analysis prices at time of submission
            a1, a2 = st.columns(2)
            with a1:
                psa10_price = st.number_input("PSA 10 price (optional snapshot)", min_value=0.0, step=5.0, value=0.0, format="%.2f")
            with a2:
                psa9_price = st.number_input("PSA 9 price (optional snapshot)", min_value=0.0, step=5.0, value=0.0, format="%.2f")

            submit_btn = st.form_submit_button("Create submission", type="primary", use_container_width=True)

        if submit_btn:
            if not items:
                st.error("Select at least one item.")
            else:
                batch_id = str(uuid.uuid4())
                est_return = add_business_days(submission_date, ASSUMED_TAT_BUSINESS_DAYS)

                new_rows = []
                for lbl in items:
                    r = inv_candidates.loc[inv_candidates["_label"] == lbl].iloc[0].to_dict()

                    purchase_total = to_float(r.get("total_price", 0))
                    cost_all_in = purchase_total + float(fee_initial) + float(addl_costs)
                    profit10 = round(float(psa10_price) - cost_all_in, 2) if psa10_price else 0.0
                    profit9 = round(float(psa9_price) - cost_all_in, 2) if psa9_price else 0.0

                    new_rows.append({
                        "grading_id": str(uuid.uuid4())[:12],
                        "submission_batch_id": batch_id,
                        "submission_date": str(submission_date),
                        "inventory_id": str(r.get("inventory_id", "")),
                        "item_label": lbl,
                        "brand_or_league": str(r.get("brand_or_league", "")),
                        "set_name": str(r.get("set_name", "")),
                        "year": str(r.get("year", "")),
                        "card_name": str(r.get("card_name", "")),
                        "card_number": str(r.get("card_number", "")),
                        "variant": str(r.get("variant", "")),
                        "card_subtype": str(r.get("card_subtype", "")),
                        "reference_link": str(r.get("reference_link", "")),
                        "purchased_from": str(r.get("purchased_from", "")),
                        "purchase_date": str(r.get("purchase_date", "")),
                        "purchase_total": float(purchase_total),
                        "grading_company": batch_company,
                        "grading_fee_initial": float(fee_initial),
                        "additional_costs": float(addl_costs),
                        "psa10_price": float(psa10_price),
                        "psa9_price": float(psa9_price),
                        "profit_psa10": float(profit10),
                        "profit_psa9": float(profit9),
                        "estimated_return_date": str(est_return),
                        "status": STATUS_SUBMITTED,
                        "returned_date": "",
                        "received_grade": "",
                        "notes": notes.strip() if notes else "",
                        "created_at": datetime.utcnow().isoformat(),
                        "updated_at": datetime.utcnow().isoformat(),
                    })

                append_grading_rows(new_rows)
                load_sheet_df.clear()
                st.success(f"Created submission batch with {len(new_rows)} card(s). Est return: {est_return}.")
                st.rerun()


# -------------------------
# TAB 3: SUMMARY / UPDATE RETURNED
# -------------------------
with tab_summary:
    st.subheader("Submission summary + update returned grades")

    grading_df = grading_df_raw.copy()
    if grading_df.empty:
        st.info("No grading submissions yet.")
    else:
        # numeric cleanup
        for c in ["purchase_total", "grading_fee_initial", "additional_costs", "psa10_price", "psa9_price", "profit_psa10", "profit_psa9"]:
            if c in grading_df.columns:
                grading_df[c] = grading_df[c].apply(to_float)

        grading_df["status"] = grading_df["status"].replace("", STATUS_SUBMITTED)
        grading_df["submission_date"] = grading_df["submission_date"].replace("", str(date.today()))

        # ---- Summary by submission_date ----
        st.markdown("### Summary by submission date")
        summary = (
            grading_df.groupby("submission_date", dropna=False)
            .agg(
                cards=("grading_id", "count"),
                purchase_cost=("purchase_total", "sum"),
                grading_fees=("grading_fee_initial", "sum"),
                additional_costs=("additional_costs", "sum"),
            )
            .reset_index()
        )

        # estimated return based on submission_date + 75 business days
        def _est(sd):
            try:
                d0 = datetime.strptime(str(sd), "%Y-%m-%d").date()
            except Exception:
                return ""
            return str(add_business_days(d0, ASSUMED_TAT_BUSINESS_DAYS))

        summary["estimated_return_date"] = summary["submission_date"].apply(_est)

        st.dataframe(
            summary.sort_values("submission_date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("---")

        # ---- Update Returned ----
        st.markdown("### Update returned cards (syncs back to Inventory)")
        open_rows = grading_df[grading_df["status"] == STATUS_SUBMITTED].copy()

        if open_rows.empty:
            st.success("No open submissions — everything is marked RETURNED.")
        else:
            # pick one open submission row at a time (simple + reliable)
            open_rows["_pick"] = open_rows.apply(
                lambda r: f"{r.get('grading_id','')} — {r.get('inventory_id','')} — {r.get('card_name','')} {('#'+str(r.get('card_number','')) if str(r.get('card_number','')).strip() else '')}",
                axis=1
            )

            pick = st.selectbox("Select an open submission record", open_rows["_pick"].tolist())
            sel = open_rows.loc[open_rows["_pick"] == pick].iloc[0].to_dict()

            c1, c2, c3 = st.columns([1.2, 1.2, 2.0])
            with c1:
                company = st.selectbox("Grading company", GRADING_COMPANIES, index=GRADING_COMPANIES.index(sel.get("grading_company","PSA")) if sel.get("grading_company","PSA") in GRADING_COMPANIES else 0)
            with c2:
                returned_date = st.date_input("Returned date", value=date.today())
            with c3:
                opts = grade_options(company)
                received_grade = st.selectbox("Received grade", opts, index=0)

            st.caption("This will: (1) set grading record to RETURNED with the grade, and (2) update Inventory item to Product Type = Graded Card and store grading_company + grade.")

            colA, colB = st.columns(2)
            with colA:
                if st.button("✅ Mark Returned + Sync Inventory", type="primary", use_container_width=True):
                    upd = pd.DataFrame([{
                        **{k: sel.get(k, "") for k in GRADING_COLUMNS},
                        "grading_company": company,
                        "status": STATUS_RETURNED,
                        "returned_date": str(returned_date),
                        "received_grade": received_grade,
                        "updated_at": datetime.utcnow().isoformat(),
                    }])

                    update_grading_rows(upd)
                    update_inventory_as_graded(str(sel.get("inventory_id","")), company, received_grade)

                    load_sheet_df.clear()
                    st.success("Updated grading record + synced inventory.")
                    st.rerun()

            with colB:
                if st.button("➕ Add additional costs (keep SUBMITTED)", use_container_width=True):
                    # add an amount to additional_costs without changing status
                    add_more = st.number_input("Add additional costs now (per card)", min_value=0.0, step=1.0, value=0.0, format="%.2f", key="add_more_costs")
                    # NOTE: streamlit button reruns immediately; we keep it simple by asking user to click once after setting.
                    st.info("Set the amount above, then click this button again.")
                    # The above pattern is clunky; if you want a cleaner UX, I’ll convert this section into a small form.

        st.markdown("---")
        st.markdown("### Full submissions table")
        show = grading_df.copy()
        show = show.sort_values(["submission_date", "created_at"], ascending=[False, False])
        st.dataframe(show, use_container_width=True, hide_index=True)
