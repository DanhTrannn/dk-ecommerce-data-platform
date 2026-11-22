import json

import streamlit as st

from platform_app.ui.access_client import SESSION, AccessUIError, clear_session


SECRET_STATE = "access_created_credentials"


def submit(payload):
    try:
        result = st.session_state[SESSION].request("POST", "polaris/resources", payload)
        st.session_state["access_flash"] = f"Đã tạo {payload['name']} · {result['operation_id']}"
        if result.get("credentials"):
            st.session_state[SECRET_STATE] = {"name": payload["name"], **result["credentials"]}
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            clear_session()
    finally:
        st.session_state.pop("access_polaris", None)


def _create_polaris():
    payload = {"kind": st.session_state["create_polaris_kind"], "name": st.session_state["create_polaris_name"].strip()}
    if payload["kind"] == "catalog":
        payload["bucket"] = st.session_state["create_polaris_bucket"].strip()
    submit(payload)


def polaris(state, *, section="all"):
    if section in {"all", "create"}:
        with st.expander("Tạo catalog, principal hoặc role Polaris"):
            if not state.get("catalog_exists"):
                st.info("Chưa có catalog. Bucket và account storage của Polaris đã được minio-init tạo; tạo catalog trước.")
            kind = st.selectbox(
                "Đối tượng Polaris mới",
                ("catalog", "principal", "principal_role", "catalog_role"),
                key="create_polaris_kind",
                format_func={
                    "catalog": "Catalog",
                    "principal": "Principal (client account)",
                    "principal_role": "Principal role",
                    "catalog_role": "Catalog role",
                }.get,
            )
            with st.form("create-polaris"):
                st.text_input("Tên đối tượng Polaris", key="create_polaris_name")
                if kind == "catalog":
                    st.caption(f"Catalog của nền tảng: {state['catalog']}; storage: s3://<bucket>/warehouse.")
                    st.text_input("Bucket MinIO", key="create_polaris_bucket")
                st.form_submit_button("Tạo trên Polaris", on_click=_create_polaris)
    if section == "create":
        return
    if state["principals"]:
        with st.expander("Lấy credential principal đã tạo bằng Console"):
            name = st.selectbox("Principal", [row["name"] for row in state["principals"]], key="recover_principal")
            if st.button("Lấy credential đã lưu"):
                try:
                    result = st.session_state[SESSION].request("POST", "polaris/credentials/" + name)
                    st.session_state[SECRET_STATE] = {"name": name, **result["credentials"]}
                except AccessUIError as error:
                    st.error(str(error))
    if credential := st.session_state.get(SECRET_STATE):
        st.caption(f"Credential riêng tư: {credential['name']}. Chỉ lưu trong session này và kho riêng của backend.")
        st.text_input("Client ID", value=credential["clientId"], disabled=True)
        st.text_input("Client secret", value=credential["clientSecret"], type="password", disabled=True)
        st.download_button(
            "Tải credential JSON",
            json.dumps(credential, indent=2),
            file_name=credential["name"] + "-credentials.json",
            mime="application/json",
        )
        if st.button("Ẩn và xóa credential khỏi session"):
            st.session_state.pop(SECRET_STATE, None)
            st.rerun()
