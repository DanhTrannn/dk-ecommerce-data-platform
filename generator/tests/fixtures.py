"""Shared test helpers: a tiny preset derived from the real 1m preset."""

import copy

import yaml

from datagen.presets import OltpPreset, preset_path


def preset_data():
    return yaml.safe_load(preset_path("1m").read_text(encoding="utf-8"))


def tiny_preset_data(**scale_overrides):
    data = preset_data()
    data["scale"].update(buying_customers=300, products=40, stores=2)
    data["scale"].update(scale_overrides)
    return data


def set_path(data, dotted, value):
    data = copy.deepcopy(data)
    *parents, leaf = dotted.split(".")
    node = data
    for key in parents:
        node = node[key]
    node[leaf] = value
    return data


def tiny_preset(**scale_overrides):
    return OltpPreset.model_validate(tiny_preset_data(**scale_overrides))


def tiny_preset_with(dotted, value, **scale_overrides):
    return OltpPreset.model_validate(set_path(tiny_preset_data(**scale_overrides), dotted, value))
