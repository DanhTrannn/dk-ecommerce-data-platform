"""Optional real Chromium check; dependencies are installed separately, never by the test."""

import json
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

config = json.loads(Path(sys.argv[1]).read_text())
results = []
errors = []
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(headless=True, executable_path=config.get("chromium"))
    page = browser.new_page(viewport={"width": 1440, "height": 1000})

    def record(response):
        if "/api/v1/chart/data" not in response.url:
            return
        try:
            payload = response.json()
            if response.status != 200 or payload.get("errors"):
                errors.append(
                    {
                        "status": response.status,
                        "payload": payload,
                        "request": response.request.post_data_json,
                        "url": response.url,
                    }
                )
            else:
                results.append(payload)
        except Exception as error:
            errors.append({"status": response.status, "error": str(error)})

    page.on("response", record)
    page.on("pageerror", lambda error: errors.append({"page_error": str(error)}))
    page.goto(config["url"], wait_until="domcontentloaded", timeout=60000)
    try:
        page.locator('input#username, input[data-test="username-input"]').fill(config["username"])
    except Exception:
        page.screenshot(path=config["screenshot"], full_page=True)
        Path(config["screenshot"]).with_suffix(".txt").write_text(page.url + "\n" + page.locator("body").inner_text())
        raise
    page.locator('input#password, input[data-test="password-input"]').fill(config["password"])
    page.locator('input[type="submit"], button[type="submit"]').first.click()
    page.wait_for_url("**/superset/dashboard/**", timeout=60000)
    screenshots = []
    response_count = 0
    for index, url in enumerate([config["url"]] + ([config["next_url"]] if config.get("next_url") else [])):
        if index:
            results.clear()
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        deadline = time.monotonic() + 120
        while len(results) < 9 and not errors and time.monotonic() < deadline:
            page.wait_for_timeout(500)
        screenshot = Path(config["screenshot"])
        if index:
            screenshot = screenshot.with_name("sales-dashboard-latest.png")
        page.wait_for_timeout(1500)
        page.screenshot(path=str(screenshot), full_page=True)
        assert not errors, errors
        assert len(results) >= 9, f"Only {len(results)} chart/filter queries returned data"
        assert page.get_by_text("Ngày đặt hàng", exact=False).count() > 0
        assert page.get_by_text("Publication:", exact=False).count() > 0
        response_count += len(results)
        screenshots.append(str(screenshot))
    print(
        "STACK_RESULT "
        + json.dumps(
            {
                "dashboards": len(screenshots),
                "chart_responses": response_count,
                "errors": errors,
                "screenshots": screenshots,
            }
        ),
        flush=True,
    )
    browser.close()
