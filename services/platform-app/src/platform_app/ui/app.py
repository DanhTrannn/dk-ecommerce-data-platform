from __future__ import annotations

import streamlit as st

from platform_app.ui.views import access, scenarios

SECTIONS = ("Mô phỏng API", "Quản trị quyền")


def main() -> None:
    st.set_page_config(page_title="Platform Console", page_icon="🧪", layout="wide")
    if st.session_state.get("section") not in SECTIONS:
        st.session_state["section"] = SECTIONS[0]

    st.sidebar.title("Platform Console")
    section = st.sidebar.radio("Màn hình", SECTIONS, key="section")
    st.title("Platform Console")

    if section == "Mô phỏng API":
        scenarios.render()
    else:
        access.render()


if __name__ == "__main__":
    main()
