"""Explicit serving targets and isolated namespace prefixes for native jobs."""

import re


def namespace(prefix, layer):
    if layer not in {"landing", "bronze", "silver", "quarantine", "gold", "system"}:
        raise ValueError("Unknown data layer")
    if prefix == "serving":
        return layer
    if not re.fullmatch(r"polars_batch_[a-z0-9_]{1,40}", prefix):
        raise ValueError("Use serving or a dedicated polars_batch_* prefix")
    return prefix + "_" + layer
