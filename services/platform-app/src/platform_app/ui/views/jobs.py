from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import streamlit as st

from platform_app.ui.job_store import STAGE_ORDER, JobNotFound, JobRecord, JobStore

STATUS_LABELS = {
    "queued": "Đang chờ",
    "running": "Đang chạy",
    "succeeded": "Thành công",
    "partial": "Thành công một phần",
    "failed": "Thất bại",
    "interrupted": "Bị gián đoạn",
}
STAGE_LABELS = {
    "validate": "Kiểm tra cấu hình",
    "sql": "Xuất SQL",
    "logs": "Xuất access log",
}
STAGE_STATUS_LABELS = {
    "pending": "Chờ chạy",
    "running": "Đang chạy",
    "succeeded": "Thành công",
    "failed": "Thất bại",
    "skipped": "Bỏ qua",
    "interrupted": "Bị gián đoạn",
}
KIND_LABELS = {
    "config": "Cấu hình",
    "request": "Yêu cầu",
    "manifest": "Manifest",
    "sql": "SQL",
    "logs_directory": "Thư mục access log",
    "logs_archive": "Zip access log",
    "checksums": "Checksum",
    "log": "Log",
    "artifact": "Tệp",
}
MAX_DOWNLOAD_BYTES = 200 * 1024 * 1024


def render_monitor(store: JobStore) -> None:
    st.header("Theo dõi job")
    jobs = store.list_jobs()
    if not jobs:
        st.info("Chưa có job. Tạo job ở màn hình Cấu hình.")
        return
    ids = [job.job_id for job in jobs]
    active = next((job for job in jobs if job.is_active), None)
    selected = st.session_state.get("selected_job_id")
    if selected not in ids:
        selected = active.job_id if active else ids[0]
    if st.session_state.get("monitor_job_id") not in ids:
        st.session_state["monitor_job_id"] = selected
    selected = st.selectbox(
        "Job",
        ids,
        key="monitor_job_id",
        format_func=lambda job_id: _job_option(store, job_id),
    )
    st.session_state["selected_job_id"] = selected
    job = store.get_job(selected)
    if job.is_active:
        st.caption("Tự động cập nhật mỗi 2 giây. Đóng tab không dừng worker.")
        st.fragment(run_every=2)(_job_panel)(store, selected)
    else:
        _job_panel(store, selected)


def render_history(store: JobStore) -> None:
    st.header("Kết quả & lịch sử")
    jobs = store.list_jobs()
    if not jobs:
        st.info("Chưa có job nào trong lịch sử.")
        return
    ids = [job.job_id for job in jobs]
    selected = st.session_state.get("history_job_id")
    if selected not in ids:
        selected = ids[0]
    if st.session_state.get("history_job_id") not in ids:
        st.session_state["history_job_id"] = selected
    selected = st.selectbox(
        "Job",
        ids,
        key="history_job_id",
        format_func=lambda job_id: _job_option(store, job_id),
    )
    job = store.get_job(selected)
    _result_panel(store, job)


def _job_panel(store: JobStore, job_id: str) -> None:
    try:
        job = store.get_job(job_id)
    except JobNotFound:
        st.error(f"Không tìm thấy job {job_id}.")
        return
    state = job.state
    _status_callout(job)
    st.markdown(f"**Job ID:** `{job.job_id}`")
    metrics = st.columns(4)
    metrics[0].metric("Trạng thái", STATUS_LABELS.get(job.status, job.status))
    metrics[1].metric("Scenario", str(state.get("scenario_id", "—")))
    metrics[2].metric("Bắt đầu", _time_label(state.get("started_at")))
    metrics[3].metric("Kết thúc", _time_label(state.get("finished_at")))
    details = st.columns(4)
    details[0].metric("Preset", str(state.get("preset", "—")))
    details[1].metric("Đầu ra", str(state.get("output", "—")))
    details[2].metric("Generator", str(state.get("generator_version", "—")))
    details[3].metric("Expected requests", f"{int(state.get('expected_requests', 0)):,}")
    st.caption(
        f"Dataset identity: `{state.get('dataset_identity', '—')}` · "
        f"Log identity: `{state.get('log_identity') or '—'}`"
    )
    if error := state.get("error"):
        st.error(str(error))
    st.subheader("Công đoạn")
    st.dataframe(_stage_rows(job), hide_index=True)
    _render_logs(job)
    if job.is_active:
        st.caption(f"Thư mục job: `{job.directory}`")


def _result_panel(store: JobStore, job: JobRecord) -> None:
    state = job.state
    _status_callout(job)
    st.markdown(f"**Job ID:** `{job.job_id}`")
    metrics = st.columns(4)
    metrics[0].metric("Trạng thái", STATUS_LABELS.get(job.status, job.status))
    metrics[1].metric("Scenario", str(state.get("scenario_id", "—")))
    metrics[2].metric("Tạo lúc", _time_label(state.get("created_at")))
    metrics[3].metric("Kết thúc", _time_label(state.get("finished_at")))
    st.caption(
        f"Dataset identity: `{state.get('dataset_identity', '—')}` · "
        f"Log identity: `{state.get('log_identity') or '—'}` · "
        f"Generator: {state.get('generator_version', '—')}"
    )
    if error := state.get("error"):
        st.error(str(error))
    st.subheader("Kết quả từng công đoạn")
    st.dataframe(_stage_rows(job), hide_index=True)
    with st.expander("Chi tiết từng công đoạn"):
        for stage in STAGE_ORDER:
            stage_state = state.get("stages", {}).get(stage, {})
            st.markdown(
                f"**{STAGE_LABELS[stage]}** — "
                f"{STAGE_STATUS_LABELS.get(stage_state.get('status', ''), stage_state.get('status', '—'))}"
            )
            if stage_state.get("message"):
                st.caption(str(stage_state["message"]))
            if stage_state.get("summary"):
                st.json(stage_state["summary"])
    _render_frozen_config(job)
    _render_manifest(job)
    _render_artifacts(job)
    _render_request(job)
    st.caption(f"Thư mục job: `{job.directory}`")


def _status_callout(job: JobRecord) -> None:
    status = job.status
    label = STATUS_LABELS.get(status, status)
    if status == "succeeded":
        st.success(f"Trạng thái: {label}")
    elif status == "partial":
        st.warning(f"Trạng thái: {label}. Một số công đoạn đã thất bại; xem chi tiết bên dưới.")
    elif status == "interrupted":
        st.warning(f"Trạng thái: {label}. Worker không còn xác minh được; artifact đã có vẫn được giữ.")
    elif status == "failed":
        st.error(f"Trạng thái: {label}")
    else:
        st.info(f"Trạng thái: {label}")


def _stage_rows(job: JobRecord) -> list[dict[str, Any]]:
    rows = []
    for stage in STAGE_ORDER:
        stage_state = job.state.get("stages", {}).get(stage, {})
        rows.append(
            {
                "Công đoạn": STAGE_LABELS[stage],
                "Trạng thái": STAGE_STATUS_LABELS.get(
                    stage_state.get("status", ""),
                    stage_state.get("status", "—"),
                ),
                "Bắt đầu": _time_label(stage_state.get("started_at")),
                "Kết thúc": _time_label(stage_state.get("finished_at")),
                "Thông báo": stage_state.get("message") or "",
            }
        )
    return rows


def _render_logs(job: JobRecord) -> None:
    candidates = [
        (STAGE_LABELS[stage], job.directory / "logs" / f"{stage}.log")
        for stage in STAGE_ORDER
    ]
    candidates.append(("Worker", job.directory / "logs" / "worker.log"))
    existing = [(label, path) for label, path in candidates if path.is_file()]
    if not existing:
        return
    with st.expander("Log thực thi"):
        tabs = st.tabs([label for label, _ in existing])
        for tab, (label, path) in zip(tabs, existing):
            with tab:
                st.code(_tail(path), language="text")
                _download_path(
                    path,
                    "Tải log",
                    "text/plain",
                    key=f"log-{job.job_id}-{path.name}",
                )


def _render_frozen_config(job: JobRecord) -> None:
    config_path = job.directory / "config.yml"
    if not config_path.is_file():
        return
    with st.expander("Cấu hình đã đóng băng"):
        st.code(config_path.read_text(encoding="utf-8"), language="yaml")
        _download_path(
            config_path,
            "Tải config.yml",
            "text/yaml",
            key=f"config-{job.job_id}",
        )


def _render_manifest(job: JobRecord) -> None:
    artifacts = job.state.get("artifacts", [])
    manifest = next((item for item in artifacts if item.get("kind") == "manifest"), None)
    if manifest is None:
        st.info("Manifest chưa có (job chưa qua bước kiểm tra cấu hình).")
        return
    path = job.directory / manifest["path"]
    if not path.is_file():
        return
    with st.expander("Manifest"):
        try:
            st.json(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            st.code(path.read_text(encoding="utf-8", errors="replace"), language="json")
        _download_path(path, "Tải manifest", "application/json", key=f"manifest-{job.job_id}")


def _render_artifacts(job: JobRecord) -> None:
    artifacts = job.state.get("artifacts", [])
    if not artifacts:
        st.info("Chưa có artifact. Job có thể chưa chạy xong.")
        return
    st.subheader("Tệp kết quả")
    rows = []
    for artifact in artifacts:
        rows.append(
            {
                "Tệp": artifact.get("path", ""),
                "Loại": KIND_LABELS.get(artifact.get("kind", ""), artifact.get("kind", "")),
                "Dung lượng": _format_size(int(artifact.get("size", 0))),
                "SHA-256": str(artifact.get("sha256", ""))[:16],
                "Số file": (
                    str(artifact["file_count"])
                    if artifact.get("file_count") is not None
                    else ""
                ),
            }
        )
    st.dataframe(rows, hide_index=True)

    downloadable = [
        artifact
        for artifact in artifacts
        if artifact.get("kind") != "logs_directory"
        and (job.directory / artifact["path"]).is_file()
    ]
    if downloadable:
        options = [artifact["path"] for artifact in downloadable]
        key = f"artifact-{job.job_id}"
        if st.session_state.get(key) not in options:
            st.session_state[key] = options[0]
        selected = st.selectbox("Chọn tệp để tải", options, key=key)
        artifact = next(item for item in downloadable if item["path"] == selected)
        path = job.directory / artifact["path"]
        if path.stat().st_size > MAX_DOWNLOAD_BYTES:
            st.warning(
                f"Tệp lớn hơn {_format_size(MAX_DOWNLOAD_BYTES)}; tải trực tiếp từ `{path}`."
            )
        else:
            _download_path(
                path,
                "Tải tệp đã chọn",
                _mime_for(path),
                key=f"artifact-download-{job.job_id}-{artifact['path']}",
            )

    logs = next((item for item in artifacts if item.get("kind") == "logs_directory"), None)
    if logs is not None:
        st.caption(
            f"Access log: {logs.get('file_count', 0)} file, "
            f"{_format_size(int(logs.get('size', 0)))}. "
            "Checksum cây log nằm trong cột SHA-256."
        )
        if not logs.get("archive"):
            st.info(
                "Không tạo zip access log (chưa có file hoặc vượt ngưỡng "
                "GENERATOR_CONSOLE_MAX_ARCHIVE_BYTES). Tải từ thư mục job."
            )


def _render_request(job: JobRecord) -> None:
    request_path = job.directory / "request.json"
    if not request_path.is_file():
        return
    with st.expander("Yêu cầu job"):
        try:
            st.json(json.loads(request_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            st.code(request_path.read_text(encoding="utf-8", errors="replace"), language="json")


def _download_path(path: Path, label: str, mime: str, *, key: str) -> None:
    if path.stat().st_size > MAX_DOWNLOAD_BYTES:
        st.warning(f"Tệp lớn hơn {_format_size(MAX_DOWNLOAD_BYTES)}; tải trực tiếp từ `{path}`.")
        return
    with path.open("rb") as stream:
        st.download_button(label, data=stream, file_name=path.name, mime=mime, key=key)


def _job_option(store: JobStore, job_id: str) -> str:
    try:
        job = store.get_job(job_id)
    except JobNotFound:
        return job_id
    return (
        f"{str(job.state.get('created_at', ''))[:19]} · "
        f"{job.state.get('scenario_id', '—')} · "
        f"{STATUS_LABELS.get(job.status, job.status)}"
    )


def _time_label(value: Any) -> str:
    if not value:
        return "—"
    return str(value)[:19].replace("T", " ")


def _tail(path: Path, lines: int = 200) -> str:
    try:
        content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as error:
        return f"Không đọc được log: {error}"
    return "\n".join(content[-lines:])


def _format_size(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _mime_for(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".yml", ".yaml"}:
        return "text/yaml"
    if suffix == ".json":
        return "application/json"
    if suffix == ".sql":
        return "application/sql"
    if suffix == ".zip":
        return "application/zip"
    if suffix == ".gz":
        return "application/gzip"
    return "text/plain"
