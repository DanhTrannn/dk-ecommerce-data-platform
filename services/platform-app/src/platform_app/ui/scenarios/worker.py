"""Detached sequential purchase runner with cooperative stopping, no write retries."""

import sys
import time
from pathlib import Path

import httpx

from platform_app.ui.scenarios.config import PurchaseConfig
from platform_app.ui.scenarios.purchase import ScenarioFailure, purchase
from platform_app.ui.scenarios.store import ScenarioStore, stamp


def run(root, run_id, client_factory=None, csrf_cookie=None):
    store = ScenarioStore(root)
    claimed = False
    try:
        with store.lock(execution=True):
            record = store.claim(run_id)
            claimed = True
            config = PurchaseConfig(**record["config"])
            if csrf_cookie is None:
                from platform_app.api.core.config import get_settings

                csrf_cookie = get_settings().csrf_cookie_name
            factory = client_factory or (
                lambda: httpx.Client(
                    base_url=record["target"], timeout=record["timeout"], follow_redirects=False, trust_env=False
                )
            )
            for index in range(config.purchases):
                if store.read(run_id)["stop_requested"]:
                    break
                attempt = dict(
                    index=index + 1,
                    customer_email=f"scenario-{run_id}-{index + 1}@example.com",
                    idempotency_key=f"sim-{run_id}-{index + 1}",
                    status="running",
                    stage="starting",
                    started_at=stamp(),
                    finished_at=None,
                )
                store.update(run_id, lambda row: row["attempts"].append(attempt))

                def checkpoint(**values):
                    store.update(run_id, lambda row: row["attempts"][-1].update(values))

                try:
                    with factory() as client:
                        purchase(client, config, attempt, checkpoint, csrf_cookie=csrf_cookie)
                except ScenarioFailure as error:
                    checkpoint(status="unknown" if error.unknown else "failed", message=str(error))
                except Exception:
                    # Unexpected parsing/persistence errors may follow a committed write.
                    checkpoint(
                        status="unknown", message="Không xác định kết quả; kiểm tra lượt này trước khi chạy lại."
                    )
                checkpoint(finished_at=stamp())
                latest = store.read(run_id)
                if latest["attempts"][-1]["status"] == "unknown":
                    break
                if index + 1 < config.purchases:
                    deadline = time.monotonic() + config.interval_seconds
                    while time.monotonic() < deadline and not store.read(run_id)["stop_requested"]:
                        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
            record = store.read(run_id)
            statuses = [item["status"] for item in record["attempts"]]
            if "unknown" in statuses:
                status = "unknown"
            elif record["stop_requested"]:
                status = "stopped"
            elif len(statuses) == config.purchases and all(value == "succeeded" for value in statuses):
                status = "succeeded"
            elif "succeeded" in statuses:
                status = "partial"
            else:
                status = "failed"
            store.update(run_id, lambda row: row.update(status=status, finished_at=stamp()))
            return 0 if status == "succeeded" else 1
    except Exception:
        if claimed:
            try:

                def mark_unknown(row):
                    row.update(
                        status="unknown",
                        finished_at=stamp(),
                        message="Worker lỗi; cần kiểm tra kết quả trước khi chạy lại.",
                    )
                    if row["attempts"] and row["attempts"][-1]["status"] == "running":
                        row["attempts"][-1].update(status="unknown", finished_at=stamp())

                store.update(run_id, mark_unknown)
            except Exception:
                pass  # Reconciliation handles a journal that could not be saved.
        return 1


def main():
    if len(sys.argv) != 3:
        raise SystemExit(2)
    raise SystemExit(run(Path(sys.argv[1]), sys.argv[2]))


if __name__ == "__main__":
    main()
