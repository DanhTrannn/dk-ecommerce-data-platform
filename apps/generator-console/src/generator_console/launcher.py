from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    app_path = Path(__file__).with_name("app.py")
    arguments = [
        "streamlit",
        "run",
        str(app_path),
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]
    arguments.extend(sys.argv[1:])
    sys.argv = arguments
    from streamlit.web import cli as streamlit_cli

    raise SystemExit(streamlit_cli.main())
