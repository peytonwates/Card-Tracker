import streamlit as st

st.set_page_config(page_title="Card Tracker", layout="wide")

st.title("Card Tracker")
st.caption("Prototype branch — Inventory intake is under Pages → Inventory")

st.markdown(
    """
**Next steps**
- Inventory → New Inventory: add cards
- Inventory → Inventory List: view/edit/export/delete
- Inventory → Inventory Summary: totals and breakdowns
"""
)
