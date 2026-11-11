"""Private test handoff of actual API DLQ envelopes; never log this result."""

import json
import os
from pathlib import Path

path = Path(os.environ["KAFKA_DLQ_FILE_PATH"])
payloads = [json.dumps(json.loads(line), ensure_ascii=False) for line in path.read_text().splitlines()]
print("STACK_RESULT " + json.dumps({"payloads": payloads}))
