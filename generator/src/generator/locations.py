"""Shared retail locations and deterministic PostgreSQL reference-data export."""

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

CITIES = (
    {"code": "HCM", "name": "Hồ Chí Minh"},
    {"code": "HN", "name": "Hà Nội"},
    {"code": "DN", "name": "Đà Nẵng"},
    {"code": "CT", "name": "Cần Thơ"},
    {"code": "HP", "name": "Hải Phòng"},
)

STORES = (
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
)

DEFAULT_STORE_CODES = tuple(store["code"] for store in STORES)


@dataclass(frozen=True)
class LocationConfig:
    store_codes: tuple[str, ...] = DEFAULT_STORE_CODES

    def __post_init__(self):
        if not isinstance(self.store_codes, tuple) or any(not isinstance(code, str) for code in self.store_codes):
            raise ValueError("locations.store_codes must contain strings")
        normalized = tuple(sorted(code.strip().upper() for code in self.store_codes))
        if len(set(normalized)) != len(normalized):
            raise ValueError("locations.store_codes must be unique")
        if not set(normalized) <= set(DEFAULT_STORE_CODES):
            raise ValueError("Unknown store code in locations.store_codes")
        object.__setattr__(self, "store_codes", normalized)

    @property
    def stores(self):
        return tuple(store for store in STORES if store["code"] in self.store_codes)

    @property
    def cities(self):
        codes = {store["city_code"] for store in self.stores}
        return tuple(city for city in CITIES if city["code"] in codes)

    @property
    def counts(self) -> dict[str, int]:
        return {"cities": len(self.cities), "stores": len(self.stores)}


def parse_locations(raw: dict) -> LocationConfig:
    if not isinstance(raw, dict) or set(raw) - {"store_codes"}:
        raise ValueError("locations must be a mapping containing only store_codes")
    if "store_codes" not in raw:
        return LocationConfig()
    if not isinstance(raw["store_codes"], list):
        raise ValueError("locations.store_codes must be a list")
    return LocationConfig(tuple(raw["store_codes"]))


def locations_sql(config: LocationConfig, at: datetime, literal: Callable[[str | datetime], str]) -> str:
    if not config.stores:
        return ""
    timestamp = literal(at)
    city_values = ",\n".join(
        f"({index}, {literal(city['code'])}, {literal(city['name'])})" for index, city in enumerate(config.cities, 1)
    )
    store_values = ",\n".join(
        "("
        + ", ".join([str(index)] + [literal(store[key]) for key in ("city_code", "code", "name", "address", "phone")])
        + ")"
        for index, store in enumerate(config.stores, 1)
    )
    # Explicit IDs avoid nextval gaps on reused codes or rollback. The exporter locks
    # both tables before this SQL; location IDs are resolved by code in the target DB.
    return f"""INSERT INTO public.cities(city_id,code,name,is_active,created_at,updated_at)
SELECT (SELECT COALESCE(MAX(city_id),0) FROM public.cities)+v.ordinal,
       v.code,v.name,TRUE,{timestamp},{timestamp}
FROM (VALUES
{city_values}
) AS v(ordinal,code,name)
WHERE NOT EXISTS (SELECT 1 FROM public.cities c WHERE lower(c.code)=lower(v.code))
ORDER BY v.ordinal;

INSERT INTO public.stores(store_id,city_id,code,name,address,phone,is_active,created_at,updated_at)
SELECT (SELECT COALESCE(MAX(store_id),0) FROM public.stores)+v.ordinal,
       c.city_id,v.code,v.name,v.address,v.phone,TRUE,{timestamp},{timestamp}
FROM (VALUES
{store_values}
) AS v(ordinal,city_code,code,name,address,phone)
JOIN public.cities c ON lower(c.code)=lower(v.city_code)
WHERE NOT EXISTS (SELECT 1 FROM public.stores s WHERE lower(s.code)=lower(v.code))
ORDER BY v.ordinal;

DO $tlcn_locations$
BEGIN
    IF (SELECT COUNT(*) FROM (VALUES
{store_values}
    ) AS v(ordinal,city_code,code,name,address,phone)
    JOIN public.stores s ON lower(s.code)=lower(v.code)
    JOIN public.cities c ON c.city_id=s.city_id AND lower(c.code)=lower(v.city_code)) <> {len(config.stores)} THEN
        RAISE EXCEPTION 'Generated store/city references do not match configuration';
    END IF;
END;
$tlcn_locations$;

"""
