import streamlit as st

from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.schemas import ResourceMutation
from platform_app.ui.access_client import SESSION, AccessUIError, clear_session

LABELS = {
    "principal": "Principal",
    "principal_role": "Principal role",
    "catalog_role": "Catalog role",
    "catalog": "Catalog",
}


def resources(state):
    return {
        "principal": state["principals"],
        "principal_role": state["principal_roles"],
        "catalog_role": state["catalog_roles"],
        "catalog": [{"name": state["catalog"], **(state.get("catalog_details") or {})}]
        if state["catalog_exists"]
        else [],
    }


def overview(state):
    groups = resources(state)
    columns = st.columns(len(groups))
    for column, (kind, rows) in zip(columns, groups.items()):
        column.metric(LABELS[kind], len(rows))
    for kind, rows in groups.items():
        st.write(f"**{LABELS[kind]}**")
        if rows:
            st.dataframe(rows, hide_index=True, width="stretch")
        else:
            st.info("Chưa có đối tượng. Mở tab Tạo mới để bắt đầu.")
    st.caption("Danh sách thể hiện quyền gán trực tiếp; quyền kế thừa từ role vẫn có thể còn hiệu lực.")


def _send(payload):
    try:
        result = st.session_state[SESSION].request("PATCH", "polaris/resources", payload)
        st.session_state["access_flash"] = (
            f"Đã {'xóa' if payload['operation'] == 'delete' else 'cập nhật'} {payload['name']} · {result['operation_id']}"
        )
        if payload["operation"] == "delete":
            st.session_state.pop("access_created_credentials", None)
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            clear_session()
    finally:
        st.session_state.pop("access_polaris", None)
    st.rerun()


def editor(state, operation):
    st.write("**Sửa đối tượng**" if operation == "update" else "**Xóa đối tượng**")
    groups = resources(state)
    prefix = f"crud_polaris_{operation}"
    kind = st.selectbox("Loại đối tượng", list(groups), format_func=LABELS.get, key=prefix + "_kind")
    rows = groups[kind]
    if not rows:
        st.info("Chưa có đối tượng có thể thao tác. Danh tính quản trị hệ thống được bảo vệ.")
        return
    name = st.selectbox("Chọn đối tượng", [r["name"] for r in rows], key=prefix + "_name")
    row = next(r for r in rows if r["name"] == name)
    with st.expander("Chi tiết hiện tại", expanded=operation == "delete"):
        st.json(row)
    if kind == "principal_role":
        st.write("Principal được gán role:", [r["name"] for r in state["principals"] if name in r["roles"]])
    if kind == "catalog_role":
        st.write(
            "Principal role đang dùng:", [r["name"] for r in state["principal_roles"] if name in r["catalog_roles"]]
        )
    payload = {"operation": operation, "kind": kind, "name": name}
    key = prefix + ":" + kind + ":" + name
    with st.form(key + "_form"):
        if operation == "delete":
            st.warning(
                "Xóa đối tượng không thể hoàn tác. Backend sẽ từ chối nếu còn liên kết hoặc dữ liệu; không xóa dây chuyền."
            )
            payload["confirm_name"] = st.text_input(f"Nhập chính xác {name} để xác nhận", key=key + "_confirm")
        else:
            st.caption("Sửa properties mô tả; tên định danh không đổi. Không nhập secret tại đây.")
            pairs = st.data_editor(
                [
                    {"Khóa": k, "Giá trị": v}
                    for k, v in row.get("properties", {}).items()
                    if not PolarisAccess._reserved_property(k)
                ]
                or [{"Khóa": "description", "Giá trị": ""}],
                num_rows="dynamic",
                hide_index=True,
                key=key + "_props",
            )
            payload["properties"] = {entry["Khóa"]: entry["Giá trị"] for entry in pairs if entry.get("Khóa")}
        if st.form_submit_button("Xác nhận xóa" if operation == "delete" else "Lưu thay đổi", type="primary"):
            try:
                ResourceMutation(**payload)
                _send(payload)
            except ValueError:
                st.error("Thông tin chưa hợp lệ: kiểm tra tên xác nhận và properties.")
