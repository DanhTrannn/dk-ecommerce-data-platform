"""Manage Polaris resources, bindings and workload credential assignments."""

import streamlit as st

from platform_app.ui.access_client import (
    SESSION,
    AccessSession,
    AccessUIError,
    AccessUISettings,
    clear_session,
)
from platform_app.ui.views import access_create, access_manage, access_workloads


def _login():
    session = None
    try:
        session = AccessSession(AccessUISettings().access_admin_url)
        identity = session.login(st.session_state.get("access_email", ""), st.session_state.get("access_password", ""))
        clear_session()
        st.session_state[SESSION] = session
        st.session_state["access_identity"] = identity
    except AccessUIError as error:
        if session:
            session.close()
        st.session_state["access_error"] = str(error)
    except Exception:
        if session:
            session.close()
        st.session_state["access_error"] = "Cấu hình API quản trị không hợp lệ."
    finally:
        st.session_state["access_password"] = ""


def _apply(kind, subject, target, granted, *, scope="catalog", namespace=None, table=None):
    try:
        result = st.session_state[SESSION].request(
            "POST",
            "polaris/bindings",
            {
                "kind": kind,
                "subject": subject,
                "target": target,
                "granted": granted,
                "scope": scope,
                "namespace": namespace or [],
                "table": table,
            },
        )
        st.session_state["access_flash"] = f"Đã áp dụng: {result['status']} · {result['operation_id']}"
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            clear_session()
    st.session_state.pop("access_polaris", None)
    st.rerun()


def _polaris(state):
    st.caption(
        "Liên kết trực tiếp: principal → principal role → catalog role → quyền. Không tự tính toàn bộ quyền kế thừa."
    )
    st.dataframe(
        [{"Principal": row["name"], "Principal roles": ", ".join(row["roles"])} for row in state["principals"]],
        hide_index=True,
    )
    st.dataframe(
        [
            {"Principal role": row["name"], "Catalog roles": ", ".join(row["catalog_roles"])}
            for row in state["principal_roles"]
        ],
        hide_index=True,
    )
    grants = [{"Catalog role": row["name"], **grant} for row in state["catalog_roles"] for grant in row["grants"]]
    st.dataframe(grants, hide_index=True)
    if not state["principals"] or not state["principal_roles"] or not state["catalog_roles"]:
        st.info("Tạo principal, principal role và catalog role ở phần Tạo mới trước khi gán quyền.")
        return
    selected = st.selectbox(
        "Loại liên kết",
        ("principal_role", "catalog_role", "catalog_privilege"),
        key="access_binding_kind",
        format_func={
            "principal_role": "Principal → Principal role",
            "catalog_role": "Principal role → Catalog role",
            "catalog_privilege": "Catalog role → Quyền catalog",
        }.get,
    )
    source, target = {
        "principal_role": ("principals", "principal_roles"),
        "catalog_role": ("principal_roles", "catalog_roles"),
        "catalog_privilege": ("catalog_roles", "privileges"),
    }[selected]
    scope = (
        st.selectbox("Phạm vi quyền", ("catalog", "namespace", "table"))
        if selected == "catalog_privilege"
        else "catalog"
    )
    with st.form("polaris-apply"):
        subject = st.selectbox("Đối tượng", [row["name"] for row in state[source]])
        value = st.selectbox(
            "Role / quyền",
            state[{"catalog": "privileges", "namespace": "namespace_privileges", "table": "table_privileges"}[scope]]
            if target == "privileges"
            else [row["name"] for row in state[target]],
        )
        namespace = st.text_input("Namespace (các cấp phân cách bằng dấu chấm)") if scope != "catalog" else ""
        table = st.text_input("Tên bảng") if scope == "table" else None
        action = st.radio("Thao tác Polaris", ("Gán", "Gỡ"), horizontal=True)
        if st.form_submit_button("Áp dụng Polaris"):
            _apply(
                selected,
                subject,
                value,
                action == "Gán",
                scope=scope,
                namespace=namespace.split(".") if namespace else [],
                table=table,
            )


def render():
    st.subheader("Quản trị quyền Lakehouse")
    if message := st.session_state.pop("access_error", None):
        st.error(message)
    if message := st.session_state.pop("access_flash", None):
        st.success(message)
    session = st.session_state.get(SESSION)
    if session is None:
        st.caption("Đăng nhập bằng tài khoản admin bootstrap của nền tảng.")
        with st.form("access-login"):
            st.text_input("Email quản trị", key="access_email")
            st.text_input("Mật khẩu", type="password", key="access_password")
            st.form_submit_button("Đăng nhập quản trị", on_click=_login)
        return
    try:
        identity = session.request("GET", "me")
    except AccessUIError as error:
        st.error(str(error))
        if error.status in {401, 403}:
            st.session_state["access_error"] = str(error)
            clear_session()
            st.rerun()
        return
    st.caption(f"Role: {identity['role']}")
    if st.button("Đăng xuất quản trị"):
        try:
            session.request("POST", "logout")
        except AccessUIError:
            pass
        clear_session()
        st.rerun()
    if st.button("Tải lại quyền"):
        st.session_state.pop("access_polaris", None)
        st.session_state.pop("access_workload_state", None)
    try:
        if "access_polaris" not in st.session_state:
            st.session_state["access_polaris"] = session.request("GET", "polaris")
        state = st.session_state["access_polaris"]
        tabs = st.tabs(["Danh sách", "Tạo mới", "Sửa", "Xóa", "Gán / Gỡ quyền", "Credential", "Cấp cho workload"])
        with tabs[0]:
            access_manage.overview(state)
        with tabs[1]:
            access_create.polaris(state, section="create")
        with tabs[2]:
            access_manage.editor(state, "update")
        with tabs[3]:
            access_manage.editor(state, "delete")
        with tabs[4]:
            _polaris(state)
        with tabs[5]:
            access_create.polaris(state, section="credentials")
        with tabs[6]:
            access_workloads.render(state)
        with st.expander("Audit thao tác gần đây"):
            st.dataframe(session.request("GET", "audit"), hide_index=True)
    except AccessUIError as error:
        st.error(str(error))
        st.session_state.pop("access_polaris", None)
        if error.status in {401, 403}:
            st.session_state["access_error"] = str(error)
            clear_session()
            st.rerun()
