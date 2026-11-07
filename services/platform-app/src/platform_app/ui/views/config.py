from __future__ import annotations

from pathlib import Path
from typing import Any

import streamlit as st
import yaml
from generator import __version__ as GENERATOR_VERSION

from platform_app.ui.config_builder import (
    FormValues,
    build_config_document,
    freeze_config,
    list_presets,
    load_preset,
    validate_config_document,
)
from platform_app.ui.job_store import JobAlreadyRunning, JobStore, JobStoreError

DEFAULT_PRESET = "small.yml"


def render(store: JobStore) -> None:
    st.header("Cấu hình")
    st.caption(
        "Chọn preset, chỉnh tham số và bấm Tạo job. Cấu hình chỉ được đọc và đóng băng khi submit."
    )
    presets = list_presets()
    if not presets:
        st.error("Không tìm thấy preset nào trong generator/configs.")
        return
    preset_paths = {path.name: path for path in presets}
    names = list(preset_paths)
    if st.session_state.get("preset_name") not in names:
        st.session_state["preset_name"] = DEFAULT_PRESET if DEFAULT_PRESET in names else names[0]
        st.session_state["loaded_preset"] = None
    if st.session_state.get("loaded_preset") != st.session_state["preset_name"]:
        try:
            _apply_preset(st.session_state["preset_name"], preset_paths)
        except (OSError, ValueError, yaml.YAMLError) as error:
            st.error(f"Không đọc được preset: {error}")
            return

    def on_preset_change() -> None:
        try:
            _apply_preset(st.session_state["preset_name"], preset_paths)
        except (OSError, ValueError, yaml.YAMLError) as error:
            st.session_state["preset_error"] = str(error)

    st.selectbox(
        "Preset",
        names,
        format_func=lambda name: Path(name).stem,
        key="preset_name",
        on_change=on_preset_change,
    )
    if preset_error := st.session_state.pop("preset_error", None):
        st.error(f"Không đọc được preset: {preset_error}")
    preset = load_preset(preset_paths[st.session_state["preset_name"]])
    modes = ", ".join(str(mode) for mode in preset.get("modes", []))
    st.caption(
        f"Preset: {preset_paths[st.session_state['preset_name']].stem} · "
        f"dataset_size={preset.get('dataset_size', '—')} · modes={modes or '—'}"
    )

    with st.form("generator-config-form", clear_on_submit=False):
        left, right = st.columns(2)
        with left:
            st.text_input(
                "Scenario ID",
                key="scenario_id",
                max_chars=64,
                help="Chỉ dùng chữ, số, dấu chấm, gạch dưới hoặc gạch ngang.",
            )
            st.number_input(
                "Seed",
                key="seed",
                min_value=0,
                max_value=2_147_483_647,
                step=1,
            )
        with right:
            st.text_input(
                "Anchor time (ISO 8601, có timezone)",
                key="anchor_time",
                help="Ví dụ: 2026-07-01T00:00:00+00:00",
            )
            st.number_input(
                "Số tháng lịch sử",
                key="history_months",
                min_value=1,
                max_value=120,
                step=1,
            )
        st.markdown("**Quy mô**")
        scale_columns = st.columns(4)
        with scale_columns[0]:
            st.number_input("Customers", key="customers", min_value=1, step=1)
        with scale_columns[1]:
            st.number_input("Products", key="products", min_value=1, step=1)
        with scale_columns[2]:
            st.number_input("Variants", key="variants", min_value=1, step=1)
        with scale_columns[3]:
            st.number_input("Orders", key="orders", min_value=0, step=1)

        with st.expander("Phân phối nâng cao (YAML)"):
            st.caption(
                "Chỉ sửa khối distributions. Các trường còn lại lấy từ preset và form; "
                "cấu hình cuối cùng được lưu nguyên văn vào job."
            )
            st.text_area("Distributions YAML", key="distributions_yaml", height=320)

        submitted = st.form_submit_button("Tạo job", type="primary")

    if submitted:
        _submit(store, preset_paths[st.session_state["preset_name"]])

    if config_yaml := st.session_state.get("last_config_yaml"):
        with st.expander("YAML của lần tạo job gần nhất"):
            st.code(config_yaml, language="yaml")


def _submit(store: JobStore, preset_path: Path) -> None:
    values = _values_from_state()
    try:
        preset = load_preset(preset_path)
        document = build_config_document(preset, values)
        config = validate_config_document(document)
        config_yaml = freeze_config(document)
        request = {"preset": preset_path.stem}
        job = store.create_job(
            document,
            request,
            preset_name=preset_path.stem,
            config_yaml=config_yaml,
            generator_version=GENERATOR_VERSION,
            dataset_identity=config.logical_identity,
        )
    except (
        JobAlreadyRunning,
        JobStoreError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        yaml.YAMLError,
    ) as error:
        st.error(str(error))
        return

    st.session_state["selected_job_id"] = job.job_id
    st.session_state["monitor_job_id"] = job.job_id
    st.session_state["history_job_id"] = job.job_id
    st.session_state["last_config_yaml"] = config_yaml
    st.session_state["flash"] = f"Đã tạo job {job.job_id}."
    st.session_state["navigate_to"] = "Theo dõi job"
    st.rerun()


def _values_from_state() -> FormValues:
    return FormValues(
        scenario_id=str(st.session_state["scenario_id"]),
        seed=int(st.session_state["seed"]),
        anchor_time=str(st.session_state["anchor_time"]),
        history_months=int(st.session_state["history_months"]),
        customers=int(st.session_state["customers"]),
        products=int(st.session_state["products"]),
        variants=int(st.session_state["variants"]),
        orders=int(st.session_state["orders"]),
        distributions_yaml=str(st.session_state["distributions_yaml"]),
    )


def _apply_preset(name: str, preset_paths: dict[str, Path]) -> None:
    preset = load_preset(preset_paths[name])
    defaults = _defaults(preset)
    for key, value in defaults.items():
        st.session_state[key] = value
    st.session_state["loaded_preset"] = name


def _defaults(preset: dict[str, Any]) -> dict[str, Any]:
    scale = preset.get("scale", {})
    orders = int(scale.get("orders", 0))
    return {
        "scenario_id": str(preset.get("scenario_id", "")),
        "seed": int(preset.get("seed", 1)),
        "anchor_time": str(preset.get("anchor_time", "")),
        "history_months": int(preset.get("history_months", 12)),
        "customers": int(scale.get("customers", 1)),
        "products": int(scale.get("products", 1)),
        "variants": int(scale.get("variants", 1)),
        "orders": orders,
        "distributions_yaml": yaml.safe_dump(
            preset.get("distributions", {}),
            sort_keys=False,
            allow_unicode=True,
        ),
    }
