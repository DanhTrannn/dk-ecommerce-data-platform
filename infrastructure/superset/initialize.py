"""Explicit metadata initialization; reruns preserve existing admin credentials."""

import os
import subprocess


def main():
    password = os.environ.get("SUPERSET_ADMIN_PASSWORD")
    if not password:
        raise ValueError("Set SUPERSET_ADMIN_PASSWORD for explicit initialization")
    subprocess.run(["superset", "db", "upgrade"], check=True)
    subprocess.run(["superset", "init"], check=True)
    from superset.app import create_app
    from superset import security_manager

    app = create_app()
    with app.app_context():
        username = os.environ.get("SUPERSET_ADMIN_USER", "admin")
        if security_manager.find_user(username=username) is None:
            user = security_manager.add_user(
                username=username,
                first_name="BI",
                last_name="Admin",
                email=os.environ.get("SUPERSET_ADMIN_EMAIL", "bi-admin@example.com"),
                role=security_manager.find_role("Admin"),
                password=password,
            )
            if user is None:
                raise RuntimeError("Could not create Superset admin")
        print("Superset metadata ready; existing admin credentials retained", flush=True)


if __name__ == "__main__":
    main()
