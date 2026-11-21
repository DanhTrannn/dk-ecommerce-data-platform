"""Shared guards for batch writers: run ID and bucket."""

import re


def validate_run(run_id, bucket):
    if run_id in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.~:+@=-]{1,250}", run_id):
        raise ValueError("Run ID must be a path-safe identifier")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid batch bucket")
