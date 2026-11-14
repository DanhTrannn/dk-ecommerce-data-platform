def policy_document(change):
    bucket = f"arn:aws:s3:::{change.bucket}"
    prefixes = sorted(set(change.prefixes))
    actions = []
    if "read" in change.actions:
        actions.append("s3:GetObject")
    if "write" in change.actions:
        actions.extend(["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"])
    if "delete" in change.actions:
        actions.append("s3:DeleteObject")
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetBucketLocation"], "Resource": [bucket]},
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": [bucket],
                "Condition": {"StringLike": {"s3:prefix": [p + suffix for p in prefixes for suffix in ("", "*")]}},
            },
            {"Effect": "Allow", "Action": actions, "Resource": [bucket + "/" + p + "*" for p in prefixes]},
        ],
    }


def data_policy(document):
    statements = document.get("Statement", [])
    if not isinstance(statements, list) or not statements:
        return False
    for statement in statements:
        actions = statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        if "NotAction" in statement or not actions or any(not a.startswith("s3:") for a in actions):
            return False
    return True
