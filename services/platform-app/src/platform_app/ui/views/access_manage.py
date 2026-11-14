import streamlit as st

from platform_app.access_admin.schemas import ResourceMutation
from platform_app.access_admin.polaris import PolarisAccess
from platform_app.access_admin.policy import policy_document
from platform_app.ui.access_client import AccessUIError

LABELS = {
    "user": "Account",
    "bucket": "Bucket",
    "policy": "Policy",
    "principal": "Principal",
    "principal_role": "Principal role",
    "catalog_role": "Catalog role",
    "catalog": "Catalog",
}
BUILTINS = {"readonly", "readwrite", "writeonly"}


def resources(provider, state):
    if provider == "minio":
        return {
            "user": state["users"],
            "policy": state["policies"],
            "bucket": state.get("bucket_details", [{"name": n, "properties": {}} for n in state["buckets"]]),
        }
    return {
        "principal": state["principals"],
        "principal_role": state["principal_roles"],
        "catalog_role": state["catalog_roles"],
        "catalog": [{"name": state["catalog"], **(state.get("catalog_details") or {})}]
        if state["catalog_exists"]
        else [],
    }


def policy_defaults(document):
    prefixes, actions, bucket = set(), set(), ""
    for statement in document.get("Statement", []):
        if statement.get("Effect") != "Allow":
            continue
        allowed = statement.get("Action", [])
        if isinstance(allowed, str):
            allowed = [allowed]
        for action, label in (("s3:GetObject", "read"), ("s3:PutObject", "write"), ("s3:DeleteObject", "delete")):
            if action in allowed:
                actions.add(label)
        targets = statement.get("Resource", [])
        if isinstance(targets, str):
            targets = [targets]
        for target in targets:
            if target.startswith("arn:aws:s3:::"):
                parts = target[len("arn:aws:s3:::") :].split("/", 1)
                bucket = parts[0]
                if len(parts) == 2 and parts[1].endswith("/*"):
                    prefixes.add(parts[1][:-1])
    return {
        "bucket": bucket,
        "prefixes": sorted(prefixes),
        "actions": [a for a in ("read", "write", "delete") if a in actions],
    }


def overview(provider, state):
    groups = resources(provider, state)
    columns = st.columns(len(groups))
    for column, (kind, rows) in zip(columns, groups.items()):
        column.metric(LABELS[kind], len(rows))
    for kind, rows in groups.items():
        st.write(f"**{LABELS[kind]}**")
        if rows:
            st.dataframe(rows, hide_index=True, width="stretch")
        else:
            st.info("Chưa có đối tượng. Mở tab Tạo mới để bắt đầu.")
    st.caption("Danh sách thể hiện quyền gán trực tiếp; quyền kế thừa từ role/group vẫn có thể còn hiệu lực.")


def _send(provider, payload, state):
    from platform_app.ui.views.access import SESSION, _clear

    try:
        result = st.session_state[SESSION].request(
            "PATCH", provider + "/resources", {**payload, "revision": state["revision"]}
        )
        st.session_state["access_flash"] = (
            f"Đã {'xóa' if payload['operation'] == 'delete' else 'cập nhật'} và xác minh {payload['name']} · {result['operation_id']}"
        )
        if payload["operation"] == "delete":
            st.session_state.pop("access_created_credentials", None)
    except AccessUIError as error:
        st.session_state["access_error"] = str(error)
        if error.status in {401, 403}:
            _clear()
    finally:
        st.session_state.pop("access_" + provider, None)
    st.rerun()


def editor(provider, state, operation):
    st.write("**Sửa đối tượng**" if operation == "update" else "**Xóa đối tượng**")
    groups = resources(provider, state)
    prefix = f"crud_{provider}_{operation}"
    kind = st.selectbox("Loại đối tượng", list(groups), format_func=LABELS.get, key=prefix + "_kind")
    rows = [r for r in groups[kind] if not (provider == "minio" and kind == "policy" and r["name"] in BUILTINS)]
    if not rows:
        st.info("Chưa có đối tượng có thể thao tác. Policy mặc định và danh tính quản trị hệ thống được bảo vệ.")
        return
    name = st.selectbox("Chọn đối tượng", [r["name"] for r in rows], key=prefix + "_name")
    row = next(r for r in rows if r["name"] == name)
    with st.expander("Chi tiết hiện tại", expanded=operation == "delete"):
        st.json(row)
    if provider == "minio" and kind == "policy":
        bindings = [binding for binding in state.get("policy_bindings", []) if binding.get("policy") == name]
        st.warning(
            "Sửa policy tác động đến mọi account/group được gán policy; gỡ policy khỏi một account không làm mất quyền từ group."
        )
        st.write("Account dùng trực tiếp:", [r["name"] for r in state["users"] if name in r["policies"]])
        st.write("Liên kết policy:", bindings)
    if provider == "polaris" and kind == "principal_role":
        st.write("Principal được gán role:", [r["name"] for r in state["principals"] if name in r["roles"]])
    if provider == "polaris" and kind == "catalog_role":
        st.write(
            "Principal role đang dùng:", [r["name"] for r in state["principal_roles"] if name in r["catalog_roles"]]
        )
    if operation == "delete":
        st.caption("Backend còn kiểm tra liên kết ngoài màn hình, credential con và dữ liệu trước khi xóa.")
    payload = {"operation": operation, "kind": kind, "name": name}
    key = prefix + ":" + kind + ":" + name
    with st.form(key + "_form"):
        if operation == "delete":
            st.warning(
                "Xóa đối tượng không thể hoàn tác. Backend sẽ từ chối nếu còn liên kết hoặc dữ liệu; không xóa dây chuyền."
            )
            payload["confirm_name"] = st.text_input(f"Nhập chính xác {name} để xác nhận", key=key + "_confirm")
        elif provider == "minio" and kind == "user":
            payload["enabled"] = st.selectbox(
                "Trạng thái",
                [True, False],
                index=0 if row.get("status") in {"enabled", "on"} else 1,
                format_func=lambda value: "Hoạt động" if value else "Vô hiệu hóa",
                key=key + "_enabled",
            )
            st.caption("Giữ nguyên access key và secret hiện có.")
        elif provider == "minio" and kind == "policy":
            defaults = policy_defaults(row["document"])
            buckets = state["buckets"]
            payload["bucket"] = st.selectbox(
                "Bucket của policy",
                buckets,
                index=buckets.index(defaults["bucket"]) if defaults["bucket"] in buckets else 0,
                key=key + "_bucket",
            )
            payload["prefixes"] = [
                line.strip()
                for line in st.text_area(
                    "Prefix (mỗi dòng một prefix, kết thúc bằng /)",
                    value="\n".join(defaults["prefixes"]),
                    key=key + "_prefixes",
                ).splitlines()
                if line.strip()
            ]
            payload["actions"] = st.multiselect(
                "Quyền dữ liệu",
                ["read", "write", "delete"],
                default=defaults["actions"],
                format_func={"read": "Đọc", "write": "Ghi", "delete": "Xóa"}.get,
                key=key + "_actions",
            )
            st.caption("Lưu sẽ thay thế policy bằng phạm vi được chọn; đối chiếu JSON cũ trước khi xác nhận.")
            payload["acknowledge_impact"] = st.checkbox(
                "Tôi đã xem phạm vi và các account/group bị ảnh hưởng", key=key + "_ack"
            )
        else:
            props = row.get("properties", {})
            st.caption(
                "Sửa metadata mô tả; tên định danh không đổi. Bucket dùng tags, Polaris dùng properties. Không nhập secret tại đây."
            )
            pairs = st.data_editor(
                [
                    {"Khóa": k, "Giá trị": v}
                    for k, v in props.items()
                    if provider != "polaris" or not PolarisAccess._reserved_property(k)
                ]
                or [{"Khóa": "description", "Giá trị": ""}],
                num_rows="dynamic",
                hide_index=True,
                key=key + "_props",
            )
            payload["properties"] = {entry["Khóa"]: entry["Giá trị"] for entry in pairs if entry.get("Khóa")}
        preview = st.form_submit_button("Xem trước thay đổi") if operation == "update" else False
        save = st.form_submit_button("Xác nhận xóa" if operation == "delete" else "Lưu thay đổi", type="primary")
        if preview or save:
            try:
                candidate = {**payload, **({"acknowledge_impact": True} if preview and kind == "policy" else {})}
                change = ResourceMutation(**candidate, revision=state["revision"])
                if preview:
                    before_column, after_column = st.columns(2)
                    before_column.write("**Trước thay đổi**")
                    before_column.json(row)
                    after_column.write("**Sau thay đổi (chưa lưu)**")
                    after_column.json(policy_document(change) if kind == "policy" else candidate)
                if save:
                    _send(provider, payload, state)
            except ValueError:
                st.error("Thông tin chưa hợp lệ: kiểm tra tên xác nhận, phạm vi policy, xác nhận tác động và metadata.")
