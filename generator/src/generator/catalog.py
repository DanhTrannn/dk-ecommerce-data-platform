"""Build master data and map measured product demand onto the synthetic catalog."""

import hashlib
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import product

from argon2.low_level import Type, hash_secret

from generator.config import CATEGORY_NAMES, GeneratorConfig
from generator.validation import COLORS, SIZES


# Synthetic vocabulary retained from the previous scenario; these are assumptions.
STREET_NAMES = (
    "Nguyễn Huệ",
    "Lê Lợi",
    "Trần Hưng Đạo",
    "Cách Mạng Tháng Tám",
    "Hai Bà Trưng",
    "Nguyễn Trãi",
    "Nguyễn Thị Minh Khai",
    "Phạm Ngọc Thạch",
    "Võ Thị Sáu",
    "Xô Viết Nghệ Tĩnh",
    "Cao Thắng",
    "Nguyễn Công Trứ",
    "Lê Thánh Tôn",
    "Phan Đăng Lưu",
    "Ngô Văn Năm",
    "Trần Phú",
    "Lê Văn Sỹ",
    "Phạm Văn Đồng",
    "Tô Hiệu",
    "Võ Văn Tần",
    "Trương Định",
    "Ngô Gia Tự",
    "Bạch Đằng",
    "Điện Biên Phủ",
    "Nguyễn Thái Học",
    "Quang Trung",
    "Nguyễn Văn Cừ",
    "An Dương Vương",
    "Hồng Bàng",
    "Phan Đăng Phúc",
    "Tôn Đức Thắng",
    "Trần Quốc Toản",
    "Nguyễn Tri Phương",
    "Lý Thường Kiệt",
    "Trần Quang Khải",
    "Phan Bội Châu",
    "Lý Tự Trọng",
    "Châu Văn Liêm",
    "Lê Lai",
    "Ngô Quyền",
    "Đề Thám",
    "Nguyễn Thượng Hiền",
    "Yết Kiêu",
    "Trần Khánh Dư",
    "Nguyễn Oanh",
    "Huỳnh Tấn Phát",
    "Lê Đức Thọ",
    "Hoàng Văn Hoan",
    "Nguyễn Khánh Biểu",
)

WARD_NAMES = (
    "Thảo Điền",
    "Bình Khánh",
    "An Phú",
    "Cát Lái",
    "Thủ Thiêm",
    "Bến Nghé",
    "Bến Thành",
    "Cầu Ông Lãnh",
    "Nguyễn Cư Trinh",
    "Cầu Giấy",
    "Đa Cao",
    "Tân Định",
    "Võ Thị Sáu",
    "Tân Sơn Nhất",
    "Phú Thọ",
    "Linh Tây",
    "An Hòa",
    "Linh Xuân",
    "Bình Trưng Tây",
    "Thạnh Mỹ Lợi",
    "Vĩnh Lộc B",
    "An Lạc",
)

DISTRICT_NAMES = (
    "Quận 1",
    "Quận 3",
    "Quận 5",
    "Quận 10",
    "Bình Thạnh",
    "Gò Vấp",
    "Tân Bình",
    "Tân Phú",
    "Phú Nhuận",
    "Thủ Đức",
    "Hóc Môn",
    "Bình Chánh",
)

FAMILY_NAMES = (
    "Nguyễn",
    "Trần",
    "Lê",
    "Phạm",
    "Hoàng",
    "Huỳnh",
    "Phan",
    "Vũ",
    "Võ",
    "Đặng",
    "Bùi",
    "Đỗ",
    "Hồ",
    "Ngô",
    "Dương",
    "Lý",
    "Đinh",
    "Mai",
    "Tô",
    "Đoàn",
    "Hà",
    "Trịnh",
    "Lâm",
    "Tăng",
    "Châu",
    "Lương",
    "Phùng",
    "Quách",
    "Hứa",
    "Triệu",
    "Thạch",
    "Giang",
    "Khương",
    "Bạch",
    "Tạ",
    "Đồng",
    "Cao",
    "Đàm",
    "Kiều",
)

FEMALE_MIDDLE_NAMES = (
    "Thị",
    "Thu",
    "Thuỳ",
    "Hồng",
    "Ngọc",
    "Kim",
    "Thanh",
    "Minh",
    "Anh",
    "Diệu",
    "Khánh",
    "Lan",
    "Huệ",
    "Cẩm",
    "Mỹ",
    "Trúc",
    "Ánh",
)

FEMALE_GIVEN_NAMES = (
    "Lan",
    "Hoa",
    "Hồng",
    "Mai",
    "Ngọc",
    "Thảo",
    "Trang",
    "Linh",
    "Hương",
    "Ngân",
    "Anh",
    "Hà",
    "Vy",
    "Uyên",
    "Phương",
    "Quỳnh",
    "Thu",
    "Hiền",
    "Yến",
    "Như",
    "Xuân",
    "Hạnh",
    "Dung",
    "Diễm",
    "Châu",
    "Thanh",
    "Khánh",
    "Tuyết",
    "Hiếu",
    "Huyền",
    "Vân",
    "Thư",
    "Nhi",
    "Bích",
    "Cúc",
    "Đào",
    "Trâm",
    "Quyên",
    "Hằng",
    "Sen",
    "Trúc",
    "Oanh",
)

MALE_MIDDLE_NAMES = (
    "Văn",
    "Quốc",
    "Minh",
    "Đức",
    "Công",
    "Hữu",
    "Anh",
    "Tuấn",
    "Huy",
    "Quang",
    "Mạnh",
    "Nhật",
    "Hồng",
    "Kim",
    "Trung",
    "Duy",
    "Chí",
    "Gia",
)

MALE_GIVEN_NAMES = (
    "Minh",
    "Hùng",
    "Đức",
    "Nam",
    "Huy",
    "Tuấn",
    "Long",
    "Sơn",
    "Dũng",
    "An",
    "Bình",
    "Cường",
    "Dương",
    "Hải",
    "Khánh",
    "Kiên",
    "Lâm",
    "Mạnh",
    "Nghĩa",
    "Phúc",
    "Quang",
    "Thắng",
    "Thọ",
    "Tiến",
    "Trung",
    "Việt",
    "Bảo",
    "Chí",
    "Giang",
    "Hiếu",
    "Hoàng",
    "Khoa",
    "Luân",
)

DELIVERY_STAFF_SEEDS = (
    ("DK-SHIP-01", "Trần Văn An", "0981001001", "29A-123.45"),
    ("DK-SHIP-02", "Lê Văn Bình", "0981001002", "29B-234.56"),
    ("DK-SHIP-03", "Phạm Văn Cường", "0981001003", "59C-345.67"),
    ("DK-SHIP-04", "Nguyễn Văn Dũng", "0981001004", "59D-456.78"),
    ("DK-SHIP-05", "Hoàng Văn Em", "0981001005", "43E-567.89"),
    ("DK-SHIP-06", "Vũ Văn Giang", "0981001006", "43F-678.90"),
    ("DK-SHIP-07", "Đỗ Văn Hải", "0981001007", "65G-789.01"),
    ("DK-SHIP-08", "Ngô Văn Hùng", "0981001008", "65H-890.12"),
    ("DK-SHIP-09", "Bùi Văn Kiên", "0981001009", "15K-901.23"),
    ("DK-SHIP-10", "Dương Văn Long", "0981001010", "15L-012.34"),
)

PRODUCT_NAME_TEMPLATES = {
    "ao": (
        "Áo thun nữ cổ tròn form rộng",
        "Áo sơ mi nữ tay dài form suông",
        "Áo kiểu nữ cổ vuông tay phồng",
        "Áo croptop nữ dáng ôm",
        "Áo polo nữ dệt kim",
        "Áo blouse nữ tay bồng",
        "Áo len nữ cổ lọ dệt gân",
        "Áo cardigan nữ dáng ngắn",
        "Áo hai dây nữ chất satin",
        "Áo peplum nữ chiết eo",
        "Áo hoodie nữ form rộng",
        "Áo thun nữ cổ tim dáng ôm",
    ),
    "quan": (
        "Quần jeans nữ ống rộng lưng cao",
        "Quần tây nữ ống suông công sở",
        "Quần culottes nữ cạp cao",
        "Quần short nữ lưng cao",
        "Quần jogger nữ form thoải mái",
        "Quần legging nữ co giãn",
        "Quần kaki nữ ống đứng",
        "Quần jeans nữ ống loe",
        "Quần linen nữ ống rộng",
        "Quần baggy nữ lưng cao",
    ),
    "vay": (
        "Chân váy chữ A lưng cao",
        "Chân váy xếp ly dáng midi",
        "Chân váy satin dáng dài",
        "Chân váy jeans chữ A",
        "Chân váy tennis xếp ly",
        "Chân váy bút chì công sở",
        "Chân váy maxi hoa nhí",
        "Chân váy đuôi cá nữ",
        "Chân váy cargo túi hộp",
        "Chân váy midi xẻ tà",
    ),
    "dam": (
        "Đầm midi hoa nhí cổ vuông",
        "Đầm suông nữ cổ tròn",
        "Đầm body nữ tay dài",
        "Đầm dự tiệc nữ chiết eo",
        "Đầm sơ mi nữ dáng midi",
        "Đầm babydoll nữ tay phồng",
        "Đầm hai dây nữ chất satin",
        "Đầm wrap nữ cổ chữ V",
        "Đầm maxi nữ dáng xòe",
        "Đầm công sở nữ cổ vest",
    ),
    "khoac": (
        "Áo blazer nữ dáng suông",
        "Áo khoác denim nữ form rộng",
        "Áo bomber nữ dáng ngắn",
        "Áo trench coat nữ thắt eo",
        "Áo khoác dạ nữ dáng dài",
        "Áo khoác chống nắng nữ",
        "Áo phao nữ dáng ngắn",
        "Áo khoác tweed nữ cổ tròn",
        "Áo gile nữ phong cách công sở",
        "Áo khoác kaki nữ túi hộp",
    ),
    "phu-kien": (
        "Thắt lưng nữ bản nhỏ",
        "Mũ bucket nữ vành mềm",
        "Khăn lụa nữ họa tiết thanh lịch",
        "Kính mát nữ gọng vuông",
        "Bông tai nữ dáng tròn",
        "Vòng cổ nữ dây mảnh",
        "Kẹp tóc nữ bản lớn",
        "Tất nữ cổ cao phong cách Hàn Quốc",
        "Mũ lưỡi trai nữ tối giản",
        "Khăn choàng nữ chất mềm",
    ),
    "giay": (
        "Giày sneaker nữ đế nhẹ",
        "Sandal nữ quai mảnh",
        "Giày cao gót nữ mũi nhọn",
        "Giày loafer nữ đế thấp",
        "Giày búp bê nữ đính nơ",
        "Giày mule nữ mũi vuông",
        "Boot nữ cổ thấp",
        "Dép nữ quai ngang đế mềm",
        "Giày thể thao nữ đế tăng chiều cao",
        "Sandal nữ đế xuồng",
    ),
    "tui-xach": (
        "Túi đeo vai nữ dáng hộp",
        "Túi tote nữ cỡ lớn",
        "Túi baguette nữ dáng dài",
        "Túi đeo chéo nữ mini",
        "Túi xách nữ khóa kim loại",
        "Túi bucket nữ dây rút",
        "Ví cầm tay nữ dáng mỏng",
        "Túi laptop nữ chống sốc",
        "Túi hobo nữ dáng mềm",
        "Túi hộp nữ quai xách",
    ),
}

PRODUCT_DESCRIPTION_BY_CATEGORY = {
    "ao": "Thiết kế dễ phối cùng quần hoặc chân váy cho nhiều hoàn cảnh.",
    "quan": "Phom quần cân đối, phù hợp phong cách đi làm và dạo phố.",
    "vay": "Dáng váy tôn tỷ lệ cơ thể và dễ kết hợp với nhiều kiểu áo.",
    "dam": "Thiết kế nữ tính, phù hợp đi làm, đi chơi hoặc dự tiệc tùy kiểu dáng.",
    "khoac": "Lớp khoác hoàn thiện trang phục và phù hợp nhiều điều kiện thời tiết.",
    "phu-kien": "Phụ kiện tạo điểm nhấn và hoàn thiện phong cách hằng ngày.",
    "giay": "Thiết kế cân bằng giữa thẩm mỹ và sự thoải mái khi di chuyển.",
    "tui-xach": "Kiểu túi tiện dụng, phù hợp mang theo các vật dụng cá nhân thiết yếu.",
}


@dataclass
class Catalog:
    tables: dict[str, list[dict]]
    variants: list[dict]
    variants_by_product: dict[int, list[dict]]
    product_weights: dict[int, float]
    customer_addresses: dict[int, str]
    id_base: int
    namespace: uuid.UUID
    history_start: datetime
    history_end: datetime

    def public_id(self, kind: str, index: int) -> uuid.UUID:
        return uuid.uuid5(self.namespace, f"{kind}:{index}")


def popularity_weights(product_count: int, decile_counts: tuple[float, ...]) -> list[float]:
    """Integrate the empirical rank bins over each target product's rank interval."""
    total = sum(decile_counts)
    return [
        sum(
            count
            / total
            * max(0.0, min((rank + 1) / product_count, (bin_ + 1) / 10) - max(rank / product_count, bin_ / 10))
            * 10
            for bin_, count in enumerate(decile_counts)
        )
        for rank in range(product_count)
    ]


def build_catalog(config: GeneratorConfig, randomizer: random.Random) -> Catalog:
    distribution = config.distributions
    policy = config.policies
    customer_policy, catalog_policy = policy["customers"], policy["catalog"]
    customer_count, product_count, variant_count = (config.scale[key] for key in ("customers", "products", "variants"))
    namespace = uuid.UUID(config.logical_identity)
    id_base = 10_000_000 + (int(namespace.hex[:8], 16) % 10_000) * 10_000_000
    end = config.anchor_time.astimezone(UTC)
    start = end - timedelta(days=config.history_months * policy["history"]["days_per_month"])
    created = start - timedelta(days=policy["history"]["master_data_lead_days"])
    catalog = Catalog({}, [], {}, {}, {}, id_base, namespace, start, end)
    tables = catalog.tables
    names = random.Random(f"{config.seed}-names")
    addresses = random.Random(f"{config.seed}-addresses")
    archive = random.Random(f"{config.seed}-catalog-archive")
    tables["customers"] = []
    for index in range(customer_count):
        female = names.random() < customer_policy["female_name_rate"]
        middle = FEMALE_MIDDLE_NAMES if female else MALE_MIDDLE_NAMES
        given = FEMALE_GIVEN_NAMES if female else MALE_GIVEN_NAMES
        name = f"{names.choice(FAMILY_NAMES)} {names.choice(middle)} {names.choice(given)}"
        customer_id = id_base + index + 1
        created_at = start - timedelta(days=randomizer.randint(*customer_policy["created_before_history_days"]))
        active = index == 0 or randomizer.random() >= customer_policy["inactive_rate"]
        inactive_at = (
            None
            if active
            else end - timedelta(days=randomizer.randint(*customer_policy["inactive_before_anchor_days"]))
        )
        if inactive_at is not None:
            created_at = min(created_at, inactive_at - timedelta(seconds=1))
        tables["customers"].append(
            {
                "customer_id": customer_id,
                "public_id": catalog.public_id("customer", index),
                "display_name": name,
                "status": "active" if active else "inactive",
                "role": "customer",
                "boom_count": 0,
                "is_cod_blocked": False,
                "data_origin": "synthetic",
                "generation_run_id": config.generation_run_id,
                "created_at": created_at,
                "updated_at": created_at if active else inactive_at,
            }
        )
        catalog.customer_addresses[customer_id] = (
            f"{addresses.randint(*customer_policy['house_number_range'])} {addresses.choice(STREET_NAMES)}, "
            f"{addresses.choice(WARD_NAMES)}, {addresses.choice(DISTRICT_NAMES)}, TP. Hồ Chí Minh"
        )
    tables["customers"].append(
        {
            "customer_id": id_base + customer_count + 1,
            "public_id": catalog.public_id("moderator", 0),
            "display_name": "Kiểm duyệt viên dữ liệu tổng hợp",
            "status": "active",
            "role": "admin",
            "boom_count": 0,
            "is_cod_blocked": False,
            "data_origin": "synthetic",
            "generation_run_id": None,
            "created_at": created,
            "updated_at": created,
        }
    )
    salt = hashlib.sha256(config.logical_identity.encode()).digest()[:16]
    password_hash = hash_secret(
        b"Synthetic@12345",
        salt,
        time_cost=3,
        memory_cost=65536,
        parallelism=4,
        hash_len=32,
        type=Type.ID,
    ).decode()
    tables["customer_credentials"] = [
        {
            "customer_id": row["customer_id"],
            "email_normalized": f"customer.{index + 1:04d}.{namespace.hex[:8]}@web.local",
            "password_hash": password_hash,
            "is_enabled": True,
            "password_changed_at": start,
            "created_at": start,
            "updated_at": start,
        }
        for index, row in enumerate(tables["customers"][:-1])
        if row["status"] == "active"
    ]
    root_id = id_base + 1
    tables["categories"] = [
        {
            "category_id": root_id,
            "public_id": catalog.public_id("category", 0),
            "parent_category_id": None,
            "code": f"syn-{namespace.hex[:8]}",
            "name": "Thời trang tổng hợp",
            "is_active": True,
            "created_at": created,
            "updated_at": created,
        }
    ]
    for index, (code, name) in enumerate(CATEGORY_NAMES.items(), 1):
        tables["categories"].append(
            {
                "category_id": root_id + index,
                "public_id": catalog.public_id("category", index),
                "parent_category_id": root_id,
                "code": f"syn-{namespace.hex[:8]}-{code}",
                "name": name,
                "is_active": True,
                "created_at": created,
                "updated_at": created,
            }
        )
    category_ids = {code: root_id + index for index, code in enumerate(CATEGORY_NAMES, 1)}
    archived = set(archive.sample(range(product_count), int(product_count * catalog_policy["archive_rate"])))
    combinations = list(product(SIZES, COLORS))
    bands = distribution.price_bands
    max_price = max(band.max_vnd for band in bands)
    tables["products"] = []
    tables["product_variants"] = catalog.variants
    per_product, extra = divmod(variant_count, product_count)
    for index in range(product_count):
        code = randomizer.choices(list(distribution.categories), weights=list(distribution.categories.values()))[0]
        band = randomizer.choices(bands, weights=[band.weight for band in bands])[0]
        price = randomizer.randrange(band.min_vnd, band.max_vnd + 1, catalog_policy["price_step_vnd"])
        product_id = id_base + index + 1
        tables["products"].append(
            {
                "product_id": product_id,
                "public_id": catalog.public_id("product", index),
                "category_id": category_ids[code],
                "slug": f"syn-{namespace.hex[:8]}-{index + 1:05d}",
                "name": f"{PRODUCT_NAME_TEMPLATES[code][index % len(PRODUCT_NAME_TEMPLATES[code])]} {index + 1:05d}",
                "description": PRODUCT_DESCRIPTION_BY_CATEGORY[code],
                "is_active": index not in archived,
                "created_at": created,
                "updated_at": end if index in archived else created,
                "archived_at": end if index in archived else None,
                "archived_by_customer_id": id_base + customer_count + 1 if index in archived else None,
                "archive_reason": "Ngừng kinh doanh theo kịch bản synthetic" if index in archived else None,
            }
        )
        catalog.variants_by_product[product_id] = []
        for variant_index in range(per_product + (index < extra)):
            size, color = combinations[variant_index]
            variant_id = id_base + len(catalog.variants) + 1
            unit_price = min(max_price, price + variant_index * catalog_policy["variant_price_step_vnd"])
            variant = {
                "variant_id": variant_id,
                "public_id": catalog.public_id("variant", len(catalog.variants)),
                "product_id": product_id,
                "sku": f"SYN-{namespace.hex[:8]}-{variant_id - id_base:06d}",
                "size_code": size,
                "color_code": color,
                "price_vnd": unit_price,
                "cost_price_vnd": int(
                    unit_price
                    * (
                        catalog_policy["cost_ratio_base"]
                        + variant_index % catalog_policy["cost_ratio_levels"] * catalog_policy["cost_ratio_step"]
                    )
                ),
                "is_active": True,
                "created_at": created,
                "updated_at": created,
            }
            catalog.variants.append(variant)
            catalog.variants_by_product[product_id].append(variant)
    ranked_ids = list(catalog.variants_by_product)
    randomizer.shuffle(ranked_ids)
    catalog.product_weights = dict(zip(ranked_ids, popularity_weights(product_count, distribution.product_popularity)))
    tables["delivery_staff"] = [
        {
            "staff_id": id_base + index + 1,
            "public_id": catalog.public_id("delivery-staff", index),
            "full_name": row[1],
            "phone": f"09{int(namespace.hex[:6], 16) % 1_000_000:06d}{index + 1:02d}",
            "vehicle_plate": row[3],
            "is_active": True,
            "created_at": created,
            "updated_at": created,
        }
        for index, row in enumerate(DELIVERY_STAFF_SEEDS)
    ]
    return catalog
