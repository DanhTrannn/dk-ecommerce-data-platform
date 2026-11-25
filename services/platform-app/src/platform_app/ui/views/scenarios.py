"""Explicit controls and persisted outcomes for API purchase scenarios."""

import streamlit as st
from pydantic import ValidationError

from platform_app.ui.paths import scenarios_root
from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
from platform_app.ui.scenarios.flows import SCENARIOS
from platform_app.ui.scenarios.store import ACTIVE, ScenarioStore, ScenarioStoreError


@st.fragment(run_every=2)
def render():
    store = ScenarioStore(scenarios_root())
    st.subheader("Mô phỏng mua hàng qua API")
    st.caption("Mỗi lượt chạy kịch bản đã chọn qua API thật và xác minh kết quả từng bước.")
    try:
        settings = ScenarioSettings()
        store.reconcile()
        history = store.history()
    except OSError:
        st.error(
            "Không ghi được thư mục /data/scenarios; kiểm tra quyền thư mục dữ liệu (platform-admin-init tạo thư mục này)."
        )
        return
    except (ValidationError, ScenarioStoreError, ValueError, KeyError):
        st.error("Không đọc được cấu hình hoặc lịch sử kịch bản; kiểm tra SCENARIO_API_BASE_URL và thư mục dữ liệu.")
        return
    st.text(f"API đích: {settings.api_base_url}")
    active = next((row for row in history if row["status"] in ACTIVE), None)
    # Outside the form so that the fields below follow the chosen scenario.
    scenario = st.selectbox("Kịch bản", list(SCENARIOS), format_func=lambda code: SCENARIOS[code][0])
    st.caption("Mart Gold thay đổi sau lần chạy pipeline kế tiếp: " + SCENARIOS[scenario][2] + ".")
    method, name, phone, address = "cod", "Scenario customer", "0000000000", "Scenario, Quận 1, TP. Hồ Chí Minh"
    with st.form("purchase-scenario"):
        columns = st.columns(3)
        count = columns[0].number_input("Số lượt mua", min_value=1, max_value=1000, value=5)
        quantity = columns[1].number_input("Số lượng mỗi đơn (1 SKU)", min_value=1, max_value=20, value=1)
        interval = columns[2].number_input("Nghỉ giữa hai lượt (giây)", min_value=0.0, max_value=3600.0, value=2.0)
        if scenario == "purchase":
            method = st.selectbox("Thanh toán", ("cod", "vietqr"))
        if scenario != "pos":
            name = st.text_input("Tên người nhận mô phỏng", value=name, max_chars=160)
            phone = st.text_input("Điện thoại mô phỏng", value=phone, max_chars=32)
            address = st.text_input("Địa chỉ mô phỏng", value=address, max_chars=1000)
        st.caption(
            "Ghi dữ liệu nghiệp vụ thật vào API đích. Không xóa dữ liệu khi dừng; mỗi lần bắt đầu là một run mới."
        )
        submit = st.form_submit_button("Bắt đầu mô phỏng", disabled=active is not None)
    if submit:
        try:
            store.start(
                PurchaseConfig(
                    scenario=scenario,
                    purchases=count,
                    quantity=quantity,
                    interval_seconds=interval,
                    payment_method=method,
                    receiver_name=name,
                    receiver_phone=phone,
                    shipping_address_text=address,
                ),
                settings,
            )
        except ScenarioStoreError as error:
            st.error(str(error))
        except (ValidationError, OSError):
            st.error("Không tạo được run; kiểm tra cấu hình và tác vụ đang chạy.")
        else:
            st.rerun()
    if active:
        st.info("Đang chạy nền. Yêu cầu dừng sẽ hoàn tất lượt hiện tại, rồi dừng trước lượt kế tiếp.")
        if st.button("Yêu cầu dừng", disabled=active["stop_requested"]):
            store.stop(active["run_id"])
            st.rerun()
    if not history:
        st.info("Chưa có kịch bản. Khởi tạo schema và nạp catalog/tồn kho trước khi chạy.")
        return
    # Runs saved before scenarios existed were purchases.
    runs = [row for row in history if row["config"].get("scenario", "purchase") == scenario]
    if not runs:
        st.info(f"Chưa có run cho kịch bản {SCENARIOS[scenario][0]}.")
        return
    selected = st.selectbox("Lịch sử mô phỏng", [row["run_id"] for row in runs])
    record = next(row for row in runs if row["run_id"] == selected)
    st.text(f"Kịch bản: {SCENARIOS[scenario][0]} · Trạng thái: {record['status']} · API: {record['target']}")
    if record.get("message"):
        st.warning(record["message"])
    if record["status"] == "unknown":
        st.warning(
            "Có kết quả chưa xác định. Tra cứu customer email và mã đơn (idempotency key có trong journal); không tự gửi lại."
        )
    attempts = record["attempts"]
    columns = st.columns(3)
    columns[0].metric("Đã xác minh", sum(row["status"] == "succeeded" for row in attempts))
    columns[1].metric("Thất bại", sum(row["status"] == "failed" for row in attempts))
    columns[2].metric("Chưa xác định", sum(row["status"] == "unknown" for row in attempts))
    # The journal keeps these for reconciliation; they are not results to report.
    hidden = {"index", "idempotency_key"}
    st.dataframe([{k: v for k, v in row.items() if k not in hidden} for row in attempts], hide_index=True)
    with st.expander("Cấu hình run"):
        st.json(record["config"])
