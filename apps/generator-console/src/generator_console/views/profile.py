from __future__ import annotations

import json

import streamlit as st

from generator_console.reference_profile import load_profile


def render() -> None:
    with st.expander("Profile tham chiếu UCI", expanded=False):
        try:
            profile = load_profile()
        except (OSError, ValueError, json.JSONDecodeError) as error:
            st.warning(f"Không đọc được profile tham chiếu: {error}")
            return
        st.caption(
            "Profile đo từ UCI Online Retail, chỉ dùng để tham chiếu. "
            "Sampler 2c chưa được áp dụng trong phiên bản console này."
        )
        source = profile.get("source", {})
        audit = profile.get("audit", {})
        columns = st.columns(3)
        columns[0].metric("Raw rows", f"{int(audit.get('raw_rows', 0)):,}")
        columns[1].metric("Kept rows", f"{int(audit.get('kept_rows', 0)):,}")
        columns[2].metric("Invoices", f"{int(audit.get('raw_distinct_invoice_ids', 0)):,}")
        st.markdown(
            f"**Nguồn:** {source.get('name', '—')} · "
            f"**License:** {source.get('license', '—')} · "
            f"**Profile ID:** `{profile.get('profile_id', '—')}`"
        )
        st.json(
            {
                "status": profile.get("status"),
                "profile_version": profile.get("profile_version"),
                "source": source,
                "audit": audit,
                "periods": profile.get("periods"),
            }
        )
        st.download_button(
            "Tải profile JSON",
            data=json.dumps(profile, ensure_ascii=False, indent=2),
            file_name="uci-online-retail-uk-v1.json",
            mime="application/json",
            key="download-reference-profile",
        )
