"""Reference data every batch looks up: cities, stores, staff, catalog and coupons."""

from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import polars as pl

from datagen.distributions import split_by_shares
from datagen.oltp.plan import IdAllocator
from datagen.oltp.tables import conform
from datagen.presets import OltpPreset
from datagen.randomness import uuid4_strings

CATEGORY_NAMES = {
    "ao": "Áo",
    "quan": "Quần",
    "dam": "Đầm",
    "khoac": "Áo khoác",
    "vay": "Váy",
    "giay": "Giày",
    "phu-kien": "Phụ kiện",
    "tui-xach": "Túi xách",
}
SIZES = ("S", "M", "L", "XL")
COLORS = ("black", "white", "navy", "beige", "red", "green", "pink", "grey")
ARCHIVE_REASON = "Ngừng kinh doanh"


@dataclass(frozen=True)
class Reference:
    cities: pl.DataFrame  # city_id, code, name, share
    stores: pl.DataFrame  # store_id, city_id, address
    store_staff: pl.DataFrame  # customer_id, store_id
    delivery_staff_ids: np.ndarray
    back_office_customer_id: int
    variants: pl.DataFrame  # sellable variants with their order-item snapshot columns
    products: pl.DataFrame  # product_id, weight (sellable products, weights sum to 1)
    coupons: pl.DataFrame  # coupon_id, code, percentage, minimum_subtotal_vnd, per_customer_usage_limit, sale_event


def build_reference(
    preset: OltpPreset,
    rng: np.random.Generator,
    ids: IdAllocator,
    existing_category_ids: dict[str, int],
) -> tuple[dict[str, pl.DataFrame], Reference]:
    created = preset.history_start - timedelta(days=preset.policies.history.master_data_lead_days)
    stamps = {"created_at": created, "updated_at": created}

    cities = _cities(preset, ids)
    stores = _stores(preset, ids, cities)
    customers, store_staff, back_office_id = _staff_customers(preset, rng, ids, stores)
    delivery_staff = _delivery_staff(preset, rng, ids)
    categories, category_ids = _categories(preset, rng, ids, existing_category_ids)
    products, variants = _catalog(preset, rng, ids, category_ids, back_office_id, created)
    coupons = _coupons(preset, rng, ids)

    tables = {
        "cities": conform("cities", cities.drop("share").with_columns(is_active=pl.lit(True), **stamps)),
        "stores": conform("stores", stores.with_columns(is_active=pl.lit(True), **stamps)),
        "customers": conform(
            "customers",
            customers.with_columns(
                status=pl.lit("active"),
                is_cod_blocked=pl.lit(False),
                boom_count=pl.lit(0),
                data_origin=pl.lit("synthetic"),
                generation_run_id=pl.lit(preset.run_id),
                **stamps,
            ),
            null_columns=("cod_reinstated_at", "cod_reinstated_by", "cod_reinstatement_reason", "anonymized_at"),
        ),
        "delivery_staff": conform("delivery_staff", delivery_staff.with_columns(is_active=pl.lit(True), **stamps)),
        "categories": conform(
            "categories",
            categories.with_columns(is_active=pl.lit(True), **stamps),
            null_columns=("parent_category_id",),
        ),
        "products": conform(
            "products",
            products.drop("weight", "category_code").with_columns(**stamps),
            null_columns=("description", "image_url"),
        ),
        "product_variants": conform(
            "product_variants",
            variants.select(
                "variant_id",
                "public_id",
                "product_id",
                "sku",
                "size_code",
                "color_code",
                "price_vnd",
                "cost_price_vnd",
                "is_active",
            ).with_columns(**stamps),
        ),
        "coupons": conform(
            "coupons",
            coupons.select(
                "coupon_id",
                "public_id",
                pl.col("code").alias("code_normalized"),
                pl.col("percentage").alias("discount_value"),
                "minimum_subtotal_vnd",
                "starts_at",
                "ends_at",
                "total_usage_limit",
                "per_customer_usage_limit",
            ).with_columns(discount_type=pl.lit("percentage"), is_active=pl.lit(True), used_count=pl.lit(0), **stamps),
            null_columns=("archived_at", "archived_by_customer_id", "archive_reason"),
        ),
    }

    sellable = products.filter(pl.col("is_active"))
    reference = Reference(
        cities=cities,
        stores=stores.select("store_id", "city_id", "address"),
        store_staff=store_staff,
        delivery_staff_ids=delivery_staff["staff_id"].to_numpy(),
        back_office_customer_id=back_office_id,
        variants=variants.filter(pl.col("is_active"))
        .join(
            sellable.select(
                "product_id",
                pl.col("public_id").alias("product_public_id"),
                "category_code",
                pl.col("name").alias("product_name"),
            ),
            on="product_id",
            maintain_order="left",
        )
        .join(
            pl.DataFrame({"category_code": list(CATEGORY_NAMES), "category_name": list(CATEGORY_NAMES.values())}),
            on="category_code",
            maintain_order="left",
        )
        .select(
            "variant_id",
            "product_id",
            "product_public_id",
            "category_code",
            "category_name",
            "product_name",
            "sku",
            "size_code",
            "color_code",
            "price_vnd",
            "cost_price_vnd",
        ),
        products=sellable.select("product_id", pl.col("weight") / pl.col("weight").sum()),
        coupons=coupons.select(
            "coupon_id", "code", "percentage", "minimum_subtotal_vnd", "per_customer_usage_limit", "sale_event"
        ),
    )
    return tables, reference


def _cities(preset, ids):
    rules = preset.locations.cities
    return pl.DataFrame(
        {
            "city_id": ids.take("cities", len(rules)),
            "code": [c.code for c in rules],
            "name": [c.name for c in rules],
            "share": [c.share for c in rules],
        }
    )


def _stores(preset, ids, cities):
    per_city = split_by_shares(preset.scale.stores, cities["share"].to_list())
    city_rows = cities.with_columns(count=pl.Series(per_city)).filter(pl.col("count") > 0)
    stores = city_rows.select("city_id", "code", "name", pl.int_ranges(1, pl.col("count") + 1).alias("number")).explode(
        "number"
    )
    store_ids = ids.take("stores", stores.height)
    return stores.select(
        pl.Series("store_id", store_ids),
        "city_id",
        pl.format("{}-{}", "code", pl.col("number").cast(pl.String).str.zfill(2)).alias("code"),
        pl.format("D&K {} {}", "name", "number").alias("name"),
        pl.format("{} Đường Trung Tâm, {}", pl.col("number") * 10, "name").alias("address"),
        pl.format("0280000{}", pl.Series(store_ids).cast(pl.String).str.zfill(4)).alias("phone"),
    )


def _staff_customers(preset, rng, ids, stores):
    per_store = preset.pos.staff_per_store
    staff = (
        stores.select("store_id", "city_id", "code")
        .with_columns(number=pl.int_ranges(1, per_store + 1))
        .explode("number")
    )
    customer_ids = ids.take("customers", staff.height + 1)
    back_office_id = int(customer_ids[0])
    staff = staff.with_columns(customer_id=pl.Series(customer_ids[1:]))
    rows = pl.concat(
        [
            pl.DataFrame(
                {
                    "customer_id": [back_office_id],
                    "role": ["operations_manager"],
                    "display_name": ["Vận hành hệ thống"],
                    "city_id": [None],
                    "store_id": [None],
                },
                schema={
                    "customer_id": pl.Int64,
                    "role": pl.String,
                    "display_name": pl.String,
                    "city_id": pl.Int64,
                    "store_id": pl.Int64,
                },
            ),
            staff.select(
                "customer_id",
                pl.lit("store_manager").alias("role"),
                pl.format("Nhân viên {} số {}", "code", "number").alias("display_name"),
                "city_id",
                "store_id",
            ),
        ]
    )
    rows = rows.with_columns(public_id=uuid4_strings(rng, rows.height))
    return rows, staff.select("customer_id", "store_id"), back_office_id


def _delivery_staff(preset, rng, ids):
    staff_ids = ids.take("delivery_staff", preset.policies.shipping.delivery_staff)
    return pl.DataFrame({"staff_id": staff_ids}).with_columns(
        public_id=uuid4_strings(rng, len(staff_ids)),
        full_name=pl.format("Tài xế số {}", "staff_id"),
        phone=pl.format("09{}", pl.col("staff_id").cast(pl.String).str.zfill(8)),
        vehicle_plate=pl.format("59X-{}", pl.col("staff_id").cast(pl.String).str.zfill(5)),
    )


def _categories(preset, rng, ids, existing_category_ids):
    codes = list(preset.products.categories)
    new_codes = [code for code in codes if code not in existing_category_ids]
    new_ids = ids.take("categories", len(new_codes))
    category_ids = dict(existing_category_ids) | dict(zip(new_codes, new_ids.tolist()))
    categories = pl.DataFrame(
        {
            "category_id": new_ids,
            "public_id": uuid4_strings(rng, len(new_codes)),
            "code": new_codes,
            "name": [CATEGORY_NAMES[code] for code in new_codes],
        },
        schema={"category_id": pl.Int64, "public_id": pl.String, "code": pl.String, "name": pl.String},
    )
    return categories, {code: category_ids[code] for code in codes}


def _catalog(preset, rng, ids, category_ids, back_office_id, created):
    rules = preset.products.categories
    codes = list(rules)
    per_category = split_by_shares(preset.scale.products, [rules[c].product_share for c in codes])
    category_code = np.repeat(codes, per_category)
    count = len(category_code)
    product_ids = ids.take("products", count)
    number = np.concatenate([np.arange(1, n + 1) for n in per_category])
    weight = rng.lognormal(0.0, preset.products.popularity.sigma, count) * np.array(
        [rules[c].demand_multiplier for c in category_code]
    )
    bands = preset.products.price_bands.bands
    band = rng.choice(len(bands), size=count, p=np.array([b.weight for b in bands]) / sum(b.weight for b in bands))
    low = np.array([bands[b].min_vnd for b in band])
    high = np.array([bands[b].max_vnd for b in band])
    step = preset.policies.catalog.price_step_vnd
    price = (rng.uniform(low, high) // step * step).astype(np.int64)
    archived = np.zeros(count, dtype=bool)
    archived[rng.choice(count, size=round(count * preset.policies.catalog.archive_rate), replace=False)] = True

    products = pl.DataFrame(
        {
            "product_id": product_ids,
            "public_id": uuid4_strings(rng, count),
            "category_code": category_code,
            "category_id": [category_ids[c] for c in category_code],
            "slug": [f"syn-{c}-{n:05d}" for c, n in zip(category_code, number)],
            "name": [f"{CATEGORY_NAMES[c]} mẫu {n}" for c, n in zip(category_code, number)],
            "is_active": ~archived,
            "weight": weight,
            "price_vnd": price,
            "color_code": rng.choice(COLORS, size=count),
        }
    ).with_columns(
        archived_at=pl.when(~pl.col("is_active")).then(pl.lit(created + timedelta(days=1))),
        archived_by_customer_id=pl.when(~pl.col("is_active")).then(pl.lit(back_office_id)),
        archive_reason=pl.when(~pl.col("is_active")).then(pl.lit(ARCHIVE_REASON)),
    )

    per_product = preset.products.variants_per_product
    variants = (
        products.select("product_id", "price_vnd", "color_code", "is_active")
        .with_columns(size_code=pl.lit(list(SIZES[:per_product])))
        .explode("size_code")
    )
    low_ratio, high_ratio = preset.policies.catalog.cost_ratio_range
    variants = variants.with_columns(
        variant_id=ids.take("product_variants", variants.height),
        public_id=uuid4_strings(rng, variants.height),
        cost_price_vnd=(pl.col("price_vnd") * pl.Series(rng.uniform(low_ratio, high_ratio, variants.height)))
        .round()
        .cast(pl.Int64),
    ).with_columns(sku=pl.format("SYN-{}-{}-{}", "product_id", "size_code", "color_code").str.to_uppercase())
    return products.drop("price_vnd", "color_code"), variants


def _coupons(preset, rng, ids):
    events = list(dict.fromkeys(event.name for event in preset.calendar.sale_events.dates))
    rules, policy = preset.coupons, preset.policies.coupons
    count = 1 + len(events)
    return pl.DataFrame(
        {
            "coupon_id": ids.take("coupons", count),
            "public_id": uuid4_strings(rng, count),
            "code": [f"SYN{rules.percentage}"] + [f"SALE-{name}".upper() for name in events],
            "percentage": [rules.percentage] + [rules.sale_day_percentage] * len(events),
            "minimum_subtotal_vnd": [rules.minimum_subtotal_vnd] * count,
            "per_customer_usage_limit": [policy.ordinary_customer_usage_limit]
            + [policy.sale_day_customer_usage_limit] * len(events),
            "total_usage_limit": [policy.minimum_total_usage_limit] * count,
            "sale_event": [None] + events,
            "starts_at": [preset.history_start - timedelta(days=policy.start_before_history_days)] * count,
            "ends_at": [preset.anchor_time + timedelta(days=policy.valid_after_anchor_days)] * count,
        },
        schema_overrides={"sale_event": pl.String},
    )
