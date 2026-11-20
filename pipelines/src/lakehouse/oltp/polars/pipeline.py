"""Shared guards for batch writers: target namespace prefix, run ID, bucket and page size."""

import re


def validate_run(prefix, run_id, bucket, page_size):
    if prefix != "serving" and not re.fullmatch(r"polars_batch_[a-z0-9_]{1,40}", prefix):
        raise ValueError("Use serving or a dedicated polars_batch_* namespace prefix")
    if run_id in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.~:+@=-]{1,250}", run_id):
        raise ValueError("Run ID must be a path-safe identifier")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid batch bucket")
    if type(page_size) is not int or not 1 <= page_size <= 10000:
        raise ValueError("page_size must be between 1 and 10000")
