from __future__ import annotations

import streamlit as st
from generator import __version__ as GENERATOR_VERSION

from platform_app.ui.job_store import JobStore
from platform_app.ui.paths import jobs_root
from platform_app.ui.views import config, jobs, profile, scenarios

SECTIONS = ("Cấu hình", "Theo dõi job", "Kết quả & lịch sử", "Mô phỏng API")


def main() -> None:
    st.set_page_config(page_title="Generator Console", page_icon="🧪", layout="wide")
    store = JobStore(jobs_root())
    store.reconcile()
    _handle_navigation()
    if st.session_state.get("section") not in SECTIONS:
        st.session_state["section"] = SECTIONS[0]

    st.sidebar.title("Generator Console")
    st.sidebar.caption(f"Generator {GENERATOR_VERSION}")
    st.sidebar.caption(f"Data: {jobs_root()}")
    active = store.active_job()
    if active is not None:
        st.sidebar.info(f"Đang chạy: {active.job_id}")
    else:
        st.sidebar.caption("Không có job đang chạy")
    section = st.sidebar.radio("Màn hình", SECTIONS, key="section")

    if flash := st.session_state.pop("flash", None):
        st.success(str(flash))
    st.title("Generator Console")
    st.caption(f"Điều khiển generator {GENERATOR_VERSION} · job nền · SQL và access log")

    if section == "Cấu hình":
        config.render(store)
        profile.render()
    elif section == "Theo dõi job":
        jobs.render_monitor(store)
    elif section == "Kết quả & lịch sử":
        jobs.render_history(store)
    else:
        scenarios.render()


def _handle_navigation() -> None:
    target = st.session_state.pop("navigate_to", None)
    if target in SECTIONS:
        st.session_state["section"] = target


if __name__ == "__main__":
    main()
