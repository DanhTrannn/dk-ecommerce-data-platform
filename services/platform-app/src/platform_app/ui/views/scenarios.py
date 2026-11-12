"""Explicit controls and persisted outcomes for API purchase scenarios."""

import streamlit as st
from pydantic import ValidationError

from platform_app.ui.paths import jobs_root
from platform_app.ui.scenarios.config import PurchaseConfig, ScenarioSettings
from platform_app.ui.scenarios.store import ACTIVE, ScenarioStore, ScenarioStoreError


@st.fragment(run_every=2)
def render():
    store = ScenarioStore(jobs_root().parent / "scenarios")
    st.subheader("Mô phỏng mua hàng qua API")
    st.caption("Mỗi lượt tạo khách hàng mới → đăng nhập → chọn hàng còn tồn → giỏ hàng → checkout → xác minh đơn.")
    try:
        settings = ScenarioSettings()
        store.reconcile()
        history = store.history()
    except (ValidationError, ScenarioStoreError, OSError, ValueError, KeyError):
        st.error("Không đọc được cấu hình hoặc lịch sử kịch bản; kiểm tra SCENARIO_API_BASE_URL và thư mục dữ liệu.")
        return
    st.text(f"API đích: {settings.api_base_url}")
    active = next((row for row in history if row["status"] in ACTIVE), None)
    with st.form("purchase-scenario"):
        columns = st.columns(3)
        count = columns[0].number_input("Số lượt mua", min_value=1, max_value=1000, value=5)
        quantity = columns[1].number_input("Số lượng mỗi đơn (1 SKU)", min_value=1, max_value=20, value=1)
        interval = columns[2].number_input("Nghỉ giữa hai lượt (giây)", min_value=0.0, max_value=3600.0, value=2.0)
        method = st.selectbox("Thanh toán", ("cod", "vietqr"))
        name = st.text_input("Tên người nhận mô phỏng", value="Scenario customer", max_chars=160)
        phone = st.text_input("Điện thoại mô phỏng", value="0000000000", max_chars=32)
        address = st.text_input("Địa chỉ mô phỏng", value="Scenario, Quận 1, TP. Hồ Chí Minh", max_chars=1000)
        st.caption(
            "Ghi dữ liệu nghiệp vụ thật vào API đích. Không xóa dữ liệu khi dừng; mỗi lần bắt đầu là một run mới."
        )
        submit = st.form_submit_button("Bắt đầu mô phỏng", disabled=active is not None)
    if submit:
        try:
            store.start(
                PurchaseConfig(
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
        except (ScenarioStoreError, ValidationError, OSError):
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
    selected = st.selectbox("Lịch sử mô phỏng", [row["run_id"] for row in history])
    record = next(row for row in history if row["run_id"] == selected)
    st.text(f"Trạng thái: {record['status']} · API: {record['target']}")
    if record.get("message"):
        st.warning(record["message"])
    if record["status"] == "unknown":
        st.warning(
            "Có kết quả chưa xác định. Tra cứu customer email, mã đơn và idempotency key; không tự gửi lại checkout."
        )
    attempts = record["attempts"]
    columns = st.columns(3)
    columns[0].metric("Đã xác minh", sum(row["status"] == "succeeded" for row in attempts))
    columns[1].metric("Thất bại", sum(row["status"] == "failed" for row in attempts))
    columns[2].metric("Chưa xác định", sum(row["status"] == "unknown" for row in attempts))
    st.dataframe(attempts, hide_index=True)
    with st.expander("Cấu hình run"):
        st.json(record["config"])
