"""Explicit PostgreSQL loading from completed generator job results."""

import streamlit as st

from platform_app.ui.import_store import ACTIVE, BLOCKING, ImportStore, configured_target
from platform_app.ui.job_store import JobStore, JobStoreError

LABELS = {
    "queued": "Đang chờ",
    "running": "Đang nạp",
    "succeeded": "Đã nạp",
    "failed": "Thất bại",
    "unknown": "Chưa xác định",
    "already_present": "Dataset đã có",
}


@st.fragment(run_every=2)
def render(store: JobStore, job_id: str) -> None:
    imports = ImportStore(store)
    st.subheader("Nạp vào PostgreSQL")
    try:
        imports.reconcile()
        history = imports.history(job_id)
        job = imports.job(job_id)
    except (JobStoreError, ValueError, OSError, KeyError):
        st.error("Không đọc được lịch sử nạp. Kiểm tra các bản ghi của job trước khi tiếp tục.")
        return
    try:
        target = configured_target()
    except Exception:
        target = None
        st.info("Cấu hình API_DATABASE_URL PostgreSQL và API_SECRET_KEY để nạp dữ liệu.")
    ready = True
    try:
        _, artifact = imports.sql_artifact(job_id)
        st.caption(f"SQL SHA-256: {artifact['sha256']}")
    except (JobStoreError, OSError, KeyError) as error:
        ready = False
        st.info(str(error) if isinstance(error, JobStoreError) else "SQL artifact không khả dụng.")
    if target:
        st.text(f"Đích: {target['label']}")
    blocked = (
        any(
            row["target"]["id"] == target["id"]
            and row["dataset_identity"] == job.state["dataset_identity"]
            and row["status"] in ACTIVE | BLOCKING
            for row in history
        )
        if target
        else True
    )
    if st.button(
        "Nạp vào PostgreSQL",
        key=f"import-{job_id}",
        disabled=not ready or blocked or target is None,
        help="Nạp SQL đã kiểm tra checksum; không tự tạo schema hoặc xóa dữ liệu.",
    ):
        try:
            imports.start(job_id, target)
        except (JobStoreError, OSError, ValueError, KeyError) as error:
            st.error(str(error) if isinstance(error, JobStoreError) else "Không khởi động được lần nạp.")
        else:
            st.rerun()
    if not history:
        st.caption("Chưa có lần nạp. Kết quả sinh file và trạng thái nạp được lưu riêng.")
        return
    latest = history[0]
    if latest["status"] in ACTIVE:
        st.info("Tác vụ nạp đang chạy nền; giao diện cập nhật mỗi 2 giây. Đóng tab không dừng tác vụ.")
    elif latest["status"] == "succeeded":
        st.success("Đã nạp và commit dữ liệu vào PostgreSQL.")
    elif latest["status"] == "unknown":
        st.warning(latest["message"])
    elif latest["status"] == "failed":
        st.error(latest["message"])
    elif latest.get("message"):
        st.info(latest["message"])
    if receipt := latest.get("receipt"):
        columns = st.columns(3)
        columns[0].metric("Khách hàng đã nạp", f"{receipt['customers']:,}")
        columns[1].metric("Đơn hàng đã nạp", f"{receipt['orders']:,}")
        columns[2].metric("Giá trị đơn đã đặt (VND)", f"{receipt['placed_order_value_vnd']:,}")
        with st.expander("Kết quả nạp và checksum"):
            st.json(receipt)
    with st.expander("Lịch sử nạp", expanded=latest["status"] in {"failed", "unknown"}):
        st.dataframe(
            [
                {
                    "Bắt đầu": row["started_at"] or row["created_at"],
                    "Kết thúc": row["finished_at"],
                    "Đích": row["target"]["label"],
                    "Trạng thái": LABELS.get(row["status"], row["status"]),
                    "Thông báo": row.get("message") or "",
                }
                for row in history
            ],
            hide_index=True,
        )
