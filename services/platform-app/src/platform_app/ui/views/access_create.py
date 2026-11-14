import json

import streamlit as st

from platform_app.access_admin.schemas import ResourceCreate
from platform_app.access_admin.policy import policy_document
from platform_app.ui.access_client import AccessUIError


SECRET_STATE = "access_created_credentials"


def submit(provider, payload, state):
    from platform_app.ui.views.access import SESSION, _clear

    try:
        result = st.session_state[SESSION].request(
            "POST", provider + "/resources", {**payload, "revision": state["revision"]}
        )
        st.session_state["access_flash"] = f"Đã tạo và xác minh {payload['name']} · {result['operation_id']}"
        if result.get("credentials"):
            st.session_state[SECRET_STATE] = {"name": payload["name"], **result["credentials"]}
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            _clear()
    finally:
        st.session_state.pop("access_" + provider, None)
        st.session_state["create_minio_secret"] = ""


def _create_minio(state):
    kind = st.session_state["create_minio_kind"]
    payload = {"kind": kind, "name": st.session_state["create_minio_name"].strip()}
    if kind == "user":
        payload["secret"] = st.session_state["create_minio_secret"]
    if kind == "policy":
        payload.update(
            bucket=st.session_state.get("create_minio_bucket"),
            prefixes=[line.strip() for line in st.session_state["create_minio_prefixes"].splitlines() if line.strip()],
            actions=st.session_state["create_minio_actions"],
        )
    try:
        change = ResourceCreate(**payload, revision=state["revision"])
        if kind == "policy":
            st.session_state["access_policy_preview"] = policy_document(change)
        submit("minio", payload, state)
    except ValueError:
        st.session_state["access_error"] = (
            "Thông tin chưa hợp lệ: kiểm tra tên, secret, bucket, prefix và quyền. Prefix phải kết thúc bằng /."
        )
        st.session_state["create_minio_secret"] = ""


def _create_polaris(state):
    payload = {"kind": st.session_state["create_polaris_kind"], "name": st.session_state["create_polaris_name"].strip()}
    if payload["kind"] == "catalog":
        payload["bucket"] = st.session_state["create_polaris_bucket"].strip()
    submit("polaris", payload, state)


def minio(state):
    with st.expander("Tạo bucket, account hoặc policy MinIO"):
        kind = st.selectbox(
            "Đối tượng mới",
            ("bucket", "user", "policy"),
            key="create_minio_kind",
            format_func={"bucket": "Bucket", "user": "Account", "policy": "Policy dữ liệu"}.get,
        )
        with st.form("create-minio", clear_on_submit=False):
            st.text_input("Tên đối tượng / access key", key="create_minio_name")
            if kind == "user":
                st.text_input(
                    "Secret key (8–128 ký tự ASCII, không khoảng trắng)", type="password", key="create_minio_secret"
                )
                st.caption("Lưu secret trước khi tạo; secret không được lưu trong nhật ký hoặc hiển thị lại.")
            if kind == "policy":
                st.selectbox("Bucket", state.get("buckets", []), key="create_minio_bucket")
                st.text_area(
                    "Prefix được truy cập (mỗi dòng một prefix, ví dụ warehouse/)", key="create_minio_prefixes"
                )
                st.multiselect(
                    "Quyền object",
                    ("read", "write", "delete"),
                    key="create_minio_actions",
                    format_func={"read": "Đọc", "write": "Ghi (kèm multipart upload)", "delete": "Xóa"}.get,
                )
                st.caption("Policy tự bao gồm quyền xem vị trí bucket và liệt kê các prefix đã chọn.")
            st.form_submit_button(
                "Tạo trên MinIO",
                on_click=_create_minio,
                args=(state,),
                disabled=kind == "policy" and not state.get("buckets"),
            )
        if preview := st.session_state.get("access_policy_preview"):
            st.caption("Policy JSON đã gửi gần nhất")
            st.json(preview)


def polaris(state, *, section="all"):
    from platform_app.ui.views.access import SESSION

    if section in {"all", "create"}:
        with st.expander("Tạo catalog, principal hoặc role Polaris"):
            if not state.get("catalog_exists"):
                st.info("Chưa có catalog. Tạo bucket và account storage Polaris trên MinIO trước, sau đó tạo catalog.")
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
                st.form_submit_button("Tạo trên Polaris", on_click=_create_polaris, args=(state,))
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
