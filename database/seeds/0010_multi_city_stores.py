"""Seed multi-city and store data.

Idempotent per city/store code; existing operational records are preserved.
"""

from platform_app.api.models.multicity import City, Store
from sqlalchemy import func, select

CITIES = [
    {"code": "HCM", "name": "Hồ Chí Minh"},
    {"code": "HN", "name": "Hà Nội"},
    {"code": "DN", "name": "Đà Nẵng"},
    {"code": "CT", "name": "Cần Thơ"},
    {"code": "HP", "name": "Hải Phòng"},
]

STORES = [
    {
        "city_code": "HCM",
        "code": "HCM-CENTRE",
        "name": "Trung tâm Sài Gòn",
        "address": "123 Nguyễn Huệ, Quận 1",
        "phone": "028-1234-5678",
    },
    {
        "city_code": "HCM",
        "code": "HCM-NORTHEAST",
        "name": "Cửa hàng Thủ Đức",
        "address": "456 Lê Văn Việt, Thủ Đức",
        "phone": "028-8765-4321",
    },
    {
        "city_code": "HN",
        "code": "HN-CENTRE",
        "name": "Cửa hàng Hoàn Kiếm",
        "address": "789 Hàng Bài, Hoàn Kiếm",
        "phone": "024-1111-2222",
    },
    {
        "city_code": "DN",
        "code": "DN-CENTRE",
        "name": "Cửa hàng Sơn Trà",
        "address": "321 Bạch Đằng, Sơn Trà",
        "phone": "023-3333-4444",
    },
    {
        "city_code": "CT",
        "code": "CT-CENTRE",
        "name": "Cửa hàng Ninh Kiều",
        "address": "654 Nguyễn Trãi, Ninh Kiều",
        "phone": "029-5555-6666",
    },
    {
        "city_code": "HP",
        "code": "HP-CENTRE",
        "name": "Cửa hàng Hồng Bàng",
        "address": "987 Tô Hiệu, Hồng Bàng",
        "phone": "022-7777-8888",
    },
]


def _seed_cities(session) -> dict[str, int]:
    city_map = {}
    for entry in CITIES:
        city = session.scalar(select(City).where(func.lower(City.code) == entry["code"].lower()))
        if city is None:
            city = City(**entry, is_active=True)
            session.add(city)
            session.flush()
        city_map[entry["code"]] = city.city_id
    return city_map


def _seed_stores(session, city_map: dict[str, int]) -> None:
    for entry in STORES:
        existing = session.scalar(select(Store).where(func.lower(Store.code) == entry["code"].lower()))
        if existing is None:
            session.add(
                Store(
                    city_id=city_map[entry["city_code"]],
                    **{key: value for key, value in entry.items() if key != "city_code"},
                    is_active=True,
                )
            )


def seed(session_factory=None) -> None:
    if session_factory is None:
        from platform_app.api.db.session import SessionLocal

        session_factory = SessionLocal
    with session_factory.begin() as session:
        _seed_stores(session, _seed_cities(session))
    print("[seed] city/store reference records ensured; existing records preserved.")


if __name__ == "__main__":
    seed()
