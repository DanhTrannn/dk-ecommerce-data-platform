import streamlit as st

from platform_app.access_admin.schemas import WorkloadAssignment
from platform_app.ui.access_client import AccessUIError


def submit(payload, state):
    from platform_app.ui.views.access import SESSION, _clear

    try:
        change = WorkloadAssignment(**payload, revision=state["revision"])
        result = st.session_state[SESSION].request(
            "POST", "polaris/workload-credentials", change.model_dump(mode="json")
        )
        st.session_state["access_flash"] = (
            f"Đã {'gỡ' if change.action == 'remove' else 'cấp'} file credential cho {change.workload} · {result['operation_id']}. "
            "File đã được xác minh; workload cần đọc lại credential để áp dụng."
        )
    except ValueError:
        st.error("Chọn principal hợp lệ hoặc nhập chính xác workload để xác nhận gỡ credential.")
        return
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            _clear()
    st.session_state.pop("access_workload_state", None)
    st.rerun()


def render(polaris_state):
    from platform_app.ui.views.access import SESSION

    st.write("**Cấp credential Polaris cho workload**")
    st.caption("Chọn principal đã tạo, cấp file vào volume consumer. Không cần sao chép secret hoặc sửa file bằng tay.")
    try:
        if st.button("Tải lại trạng thái workload"):
            st.session_state.pop("access_workload_state", None)
        if "access_workload_state" not in st.session_state:
            st.session_state["access_workload_state"] = st.session_state[SESSION].request(
                "GET", "polaris/workload-credentials"
            )
        state = st.session_state["access_workload_state"]
    except AccessUIError as error:
        st.error(str(error))
        return
    workloads = state["workloads"]
    st.dataframe(
        [
            {
                "Workload": row["label"],
                "Volume": row["volume"],
                "Credential": "Đã có" if row["configured"] else "Chưa có hoặc chưa hợp lệ",
                "Principal": row.get("principal")
                or ("Ngoài Console / chưa xác định" if row.get("file_present") else "—"),
                "Quyền file": "Đúng" if row["permission_ok"] else "Chưa được xác minh",
            }
            for row in workloads
        ],
        hide_index=True,
        width="stretch",
    )
    selected = st.selectbox(
        "Workload nhận credential",
        [row["id"] for row in workloads],
        format_func=lambda value: next(row["label"] for row in workloads if row["id"] == value),
        key="export_workload",
    )
    row = next(row for row in workloads if row["id"] == selected)
    st.info(row["reload_hint"] + ". Thao tác này cập nhật file; không tự restart dịch vụ hoặc submit lại job.")
    st.caption(
        "MinIO access key/secret vẫn dùng cấu hình riêng hiện có; cấp principal Polaris không thay đổi quyền MinIO."
    )
    st.caption(
        "Gán role và quyền ở tab Gán / Gỡ quyền trước khi chạy workload. Credential hợp lệ chưa chứng minh principal có đủ quyền đọc/ghi dữ liệu."
    )
    principals = [principal["name"] for principal in polaris_state["principals"]]
    if not principals:
        st.warning("Tạo principal trong tab Tạo mới trước khi cấp credential.")
    with st.form("assign-workload-credential"):
        principal = st.selectbox("Principal cấp cho workload", principals, key="export_principal")
        replace = st.checkbox(
            "Cho phép thay thế nếu workload đang dùng credential khác",
            key="export_replace_" + selected + "_" + state["revision"][:12],
        )
        st.caption("Backend xác minh credential còn hợp lệ với Polaris và đặt đúng ownership/quyền file cho consumer.")
        if st.form_submit_button("Cấp credential cho workload", type="primary", disabled=not principals):
            submit({"workload": selected, "principal": principal, "confirm_replace": replace}, state)
    with st.expander("Gỡ credential khỏi workload"):
        st.warning(
            "Gỡ file không thu hồi principal hoặc token đang chạy. Consumer cần nạp lại cấu hình; dữ liệu Lakehouse không bị xóa."
        )
        with st.form("remove-workload-credential"):
            confirmation = st.text_input(
                "Nhập chính xác " + selected + " để gỡ", key="remove_export_" + selected + "_" + state["revision"][:12]
            )
            if st.form_submit_button("Gỡ credential khỏi workload"):
                submit({"action": "remove", "workload": selected, "confirm_remove": confirmation}, state)
