"""Read assignments and explicitly grant/revoke supported existing bindings."""

import streamlit as st

from platform_app.ui.views import access_create, access_manage, access_workloads
from platform_app.ui.access_client import AccessSession, AccessUIError, AccessUISettings

SESSION = "access_admin_session"


def _clear():
    session = st.session_state.pop(SESSION, None)
    if session:
        session.close()
    for key in (
        "access_polaris",
        "access_minio",
        "access_identity",
        "access_created_credentials",
        "access_policy_preview",
        "access_workload_state",
    ):
        st.session_state.pop(key, None)


def _login():
    session = None
    try:
        session = AccessSession(AccessUISettings().access_admin_url)
        identity = session.login(st.session_state.get("access_email", ""), st.session_state.get("access_password", ""))
        _clear()
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


def _apply(provider, kind, subject, target, granted, state, *, scope="catalog", namespace=None, table=None):
    try:
        result = st.session_state[SESSION].request(
            "POST",
            provider + "/bindings",
            {
                "kind": kind,
                "subject": subject,
                "target": target,
                "granted": granted,
                "revision": state["revision"],
                "scope": scope,
                "namespace": namespace or [],
                "table": table,
            },
        )
        st.session_state["access_flash"] = f"Đã xác minh thao tác: {result['status']} · {result['operation_id']}"
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            _clear()
    st.session_state.pop("access_" + provider, None)
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
                "polaris",
                selected,
                subject,
                value,
                action == "Gán",
                state,
                scope=scope,
                namespace=namespace.split(".") if namespace else [],
                table=table,
            )


def _minio(state):
    st.caption("Buckets: " + ", ".join(state.get("buckets", [])))
    st.caption(
        "Policy trực tiếp và group membership được hiển thị riêng. Gỡ policy trực tiếp không loại bỏ quyền kế thừa từ group hoặc policy khác."
    )
    st.dataframe(
        [
            {
                "Account": row["name"],
                "Trạng thái": row["status"],
                "Policy trực tiếp": ", ".join(row["policies"]),
                "Groups": ", ".join(row["groups"]),
            }
            for row in state["users"]
        ],
        hide_index=True,
    )
    with st.expander("Xem policy hiện có"):
        for policy in state["policies"]:
            st.write(policy["name"])
            st.json(policy["document"])
    if not state["users"] or not state["policies"]:
        st.info("Tạo account và policy ở phần Tạo mới trước khi gán quyền.")
        return
    with st.form("minio-binding"):
        user = st.selectbox("Account MinIO", [row["name"] for row in state["users"]])
        policy = st.selectbox("Policy", [row["name"] for row in state["policies"]])
        action = st.radio("Thao tác MinIO", ("Gán", "Gỡ"), horizontal=True)
        if st.form_submit_button("Áp dụng MinIO"):
            _apply("minio", "policy", user, policy, action == "Gán", state)


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
            _clear()
            st.rerun()
        return
    st.caption(f"Role: {identity['role']}")
    if st.button("Đăng xuất quản trị"):
        try:
            session.request("POST", "logout")
        except AccessUIError:
            pass
        _clear()
        st.rerun()
    provider = st.radio(
        "Hệ thống", ("polaris", "minio"), format_func={"polaris": "Polaris", "minio": "MinIO"}.get, horizontal=True
    )
    if st.button("Tải lại quyền"):
        st.session_state.pop("access_" + provider, None)
        st.session_state.pop("access_workload_state", None)
    try:
        key = "access_" + provider
        if key not in st.session_state:
            st.session_state[key] = session.request("GET", provider)
        state = st.session_state[key]
        labels = ["Danh sách", "Tạo mới", "Sửa", "Xóa", "Gán / Gỡ quyền"]
        if provider == "polaris":
            labels.extend(["Credential", "Cấp cho workload"])
        tabs = st.tabs(labels)
        with tabs[0]:
            access_manage.overview(provider, state)
        with tabs[1]:
            if provider == "polaris":
                access_create.polaris(state, section="create")
            else:
                access_create.minio(state)
        with tabs[2]:
            access_manage.editor(provider, state, "update")
        with tabs[3]:
            access_manage.editor(provider, state, "delete")
        with tabs[4]:
            _polaris(state) if provider == "polaris" else _minio(state)
        if provider == "polaris":
            with tabs[5]:
                access_create.polaris(state, section="credentials")
            with tabs[6]:
                access_workloads.render(state)
        with st.expander("Audit thao tác gần đây"):
            st.dataframe(session.request("GET", "audit"), hide_index=True)
    except AccessUIError as error:
        st.error(str(error))
        st.session_state.pop("access_" + provider, None)
        if error.status in {401, 403}:
            st.session_state["access_error"] = str(error)
            _clear()
            st.rerun()
