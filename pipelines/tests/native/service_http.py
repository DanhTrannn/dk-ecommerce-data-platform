"""HTTP calls from inside the isolated application's Docker network."""

import json
import sys
import httpx

origin, method, path = sys.argv[1:4]
base = {"flink": "http://flink-jobmanager:8081", "connect": "http://debezium-connect:8083"}[origin]
body = json.loads(sys.argv[4]) if len(sys.argv) > 4 else None
with httpx.Client(base_url=base, timeout=30, trust_env=False) as client:
    response = client.request(method, path, json=body)
    if response.status_code not in (200, 201, 202, 204):
        raise RuntimeError(f"{origin} {method} {path}: HTTP {response.status_code}: {response.text[:1000]}")
    print("STACK_RESULT " + json.dumps(response.json() if response.content else {}), flush=True)
