"""Generate related transaction rows; parameter provenance is in docs/data.md."""

import random
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from generator.catalog import Catalog
from generator.config import GeneratorConfig


REVIEW_CONTENT_BY_RATING = {
    1: ("Sản phẩm khác mô tả, chất liệu không như mong đợi.", "Form không phù hợp và màu thực tế lệch khá nhiều."),
    2: (
        "Đóng gói ổn nhưng form hơi khó mặc, cần cải thiện chất liệu.",
        "Giao đúng mẫu nhưng đường may chưa tốt như kỳ vọng.",
    ),
    3: (
        "Sản phẩm dùng ổn trong tầm giá, form hơi rộng một chút.",
        "Màu đẹp, chất liệu ở mức khá và giao hàng đúng hẹn.",
    ),
    4: (
        "Form đẹp, màu giống ảnh và chất vải khá thoải mái.",
        "Đóng gói cẩn thận, mặc vừa và sẽ cân nhắc mua thêm màu khác.",
    ),
    5: (
        "Rất ưng form và chất liệu, màu lên đẹp như hình.",
        "Sản phẩm đẹp, giao nhanh, đóng gói kỹ và đúng size đã chọn.",
    ),
}

REVIEW_REJECTION_REASONS = (
    "Nội dung không liên quan đến sản phẩm",
    "Nội dung lặp hoặc không cung cấp thông tin hữu ích",
    "Ngôn từ không phù hợp quy định cộng đồng",
)


def campaign_dates(config: GeneratorConfig, catalog: Catalog) -> list[tuple[dict, date]]:
    zone = ZoneInfo(config.distributions.business_timezone)
    start, end = (value.astimezone(zone).date() for value in (catalog.history_start, catalog.history_end))
    result = []
    for year in range(start.year, end.year + 1):
        for event in config.distributions.sales:
            if event.get("day") is not None:
                day = date(year, event["month"], event["day"])
            else:
                first = date(year, event["month"], 1)
                day = first + timedelta(days=(event["weekday"] - first.weekday()) % 7 + 7 * (event["week_index"] - 1))
            if start <= day <= end:
                result.append((event, day))
    return sorted(result, key=lambda item: (item[1], item[0]["name"]))


def build_coupons(config: GeneratorConfig, catalog: Catalog) -> dict[int, str]:
    behavior = config.distributions.coupons
    start, end, base = catalog.history_start, catalog.history_end, catalog.id_base
    zone = ZoneInfo(config.distributions.business_timezone)
    specs = [
        (
            "everyday",
            "percentage",
            behavior["everyday_percentage"],
            behavior["everyday_minimum_subtotal_vnd"],
            start - timedelta(days=1),
            end + timedelta(days=365),
        ),
        (
            "welcome",
            "fixed_amount",
            behavior["welcome_fixed_vnd"],
            behavior["campaign_minimum_subtotal_vnd"],
            start - timedelta(days=1),
            end + timedelta(days=365),
        ),
    ]
    for campaign_index, (event, day) in enumerate(campaign_dates(config, catalog)):
        moment = datetime.combine(day, datetime.min.time(), zone).astimezone(UTC)
        specs.append(
            (
                "campaign",
                "percentage",
                behavior["campaign_percentage_values"][campaign_index % len(behavior["campaign_percentage_values"])],
                behavior["campaign_minimum_subtotal_vnd"],
                moment,
                moment + timedelta(days=event["after_days"] + 1),
            )
        )
        specs.append(
            (
                "midnight",
                "fixed_amount",
                behavior["midnight_fixed_values_vnd"][campaign_index % len(behavior["midnight_fixed_values_vnd"])],
                behavior["campaign_minimum_subtotal_vnd"],
                moment,
                moment + timedelta(hours=2),
            )
        )
    rows, kinds = [], {}
    for index, (kind, discount_type, value, minimum, begins, ends) in enumerate(specs, 1):
        archived = kind in {"campaign", "midnight"} and ends <= end
        code = f"SYN-{catalog.namespace.hex[:8]}-{index:04d}"
        rows.append(
            {
                "coupon_id": base + index,
                "public_id": catalog.public_id("coupon", index),
                "code_normalized": code.lower(),
                "discount_type": discount_type,
                "discount_value": value,
                "minimum_subtotal_vnd": minimum,
                "starts_at": begins,
                "ends_at": ends,
                "is_active": not archived,
                "total_usage_limit": max(100, config.scale["orders"] + 100),
                "per_customer_usage_limit": 10 if kind == "everyday" else 1,
                "used_count": 0,
                "archived_at": ends if archived else None,
                "archived_by_customer_id": base + config.scale["customers"] + 1 if archived else None,
                "archive_reason": "Kết thúc chiến dịch synthetic" if archived else None,
                "created_at": start - timedelta(days=30),
                "updated_at": ends if archived else start - timedelta(days=30),
            }
        )
        kinds[base + index] = kind
    catalog.tables["coupons"] = rows
    return kinds


def _allocate(weights: list[float], total: int) -> list[int]:
    total_weight = sum(weights)
    scaled = [weight / total_weight * total for weight in weights]
    counts = [int(value) for value in scaled]
    order = sorted(range(len(weights)), key=lambda i: scaled[i] - counts[i], reverse=True)
    for index in order[: total - sum(counts)]:
        counts[index] += 1
    return counts


def order_schedule(config: GeneratorConfig, catalog: Catalog, randomizer: random.Random):
    distribution = config.distributions
    classes = list(distribution.customers)
    assignments = [
        name
        for name, count in zip(
            classes,
            _allocate(
                [distribution.customers[name]["share"] for name in classes],
                config.scale["customers"],
            ),
        )
        for _ in range(count)
    ]
    randomizer.shuffle(assignments)
    limits = [
        catalog.history_end if row["status"] == "active" else row["updated_at"] - timedelta(seconds=1)
        for row in catalog.tables["customers"][:-1]
    ]
    raw_counts = [
        randomizer.randint(*distribution.customers[name]["orders_per_year"]) if limit >= catalog.history_start else 0
        for name, limit in zip(assignments, limits)
    ]
    targets = _allocate(raw_counts, config.scale["orders"])
    zone = ZoneInfo(distribution.business_timezone)
    events = campaign_dates(config, catalog)
    days, weights, boosts = [], [], {}
    first = catalog.history_start.astimezone(zone).date()
    last = catalog.history_end.astimezone(zone).date()
    for index in range((last - first).days + 1):
        day = first + timedelta(days=index)
        boost = 1.0
        for event, event_day in events:
            if event_day <= day <= event_day + timedelta(days=event["after_days"]):
                boost *= event["boost"]
        tet = distribution.tet
        tet_start = date(day.year, tet["month_start"], tet["day_start"])
        tet_end = date(day.year + (tet["month_end"] < tet["month_start"]), tet["month_end"], tet["day_end"])
        tet_factor = 1.0
        if tet_start <= day <= tet_end:
            center = tet_start + (tet_end - tet_start) / 2
            tet_factor += (tet["peak"] - 1) * max(
                0, 1 - abs((day - center).days) / max(1, (tet_end - tet_start).days // 2)
            )
        days.append(day)
        weights.append(distribution.day_of_week[day.weekday()] * tet_factor * boost)
        boosts[day] = boost
    campaign_days = [day for day in days if boosts[day] > 1]
    for customer_index, (name, count) in enumerate(zip(assignments, targets)):
        if count == 0:
            continue
        group = distribution.customers[name]
        limit = limits[customer_index]
        valid_day_indices = [index for index, day in enumerate(days) if day <= limit.astimezone(zone).date()]
        previous = None
        for position in range(count):
            if previous is None:
                day_index = randomizer.choices(
                    valid_day_indices, weights=[weights[index] for index in valid_day_indices]
                )[0]
                day = days[day_index]
            else:
                remaining = count - position
                cap = max(0, (limit - previous).days) // remaining
                interval = randomizer.randint(*group.get("interval_days", [1, 1]))
                day = (previous + timedelta(days=min(interval, cap))).astimezone(zone).date()
                if randomizer.random() < group["campaign_affinity"]:
                    candidates = [
                        item
                        for item in campaign_days
                        if previous.astimezone(zone).date() <= item
                        and item <= limit.astimezone(zone).date()
                        and abs((item - day).days) <= 7
                    ]
                    if candidates:
                        day = randomizer.choice(candidates)
            hour_weights = distribution.campaign_hour_of_day if boosts.get(day, 1) > 1 else distribution.hour_of_day
            hour = randomizer.choices(range(24), weights=hour_weights)[0]
            moment = datetime(day.year, day.month, day.day, hour, randomizer.randrange(60), tzinfo=zone).astimezone(UTC)
            moment = min(limit, max(catalog.history_start, previous or catalog.history_start, moment))
            yield customer_index, name, position, moment, boosts.get(day, 1) > 1
            previous = moment


def select_order_variants(catalog: Catalog, randomizer: random.Random, count: int) -> list[dict]:
    available = {
        key: list(values) for key, values in catalog.variants_by_product.items() if catalog.product_weights[key] > 0
    }
    selected = []
    for _ in range(min(count, sum(len(values) for values in available.values()))):
        products = list(available)
        product_id = randomizer.choices(products, weights=[catalog.product_weights[key] for key in products])[0]
        variant = randomizer.choice(available[product_id])
        selected.append(variant)
        available[product_id].remove(variant)
        if not available[product_id]:
            del available[product_id]
    return sorted(selected, key=lambda row: row["variant_id"])


def transaction_batches(
    config: GeneratorConfig, catalog: Catalog, randomizer: random.Random, coupon_kinds: dict[int, str]
):
    distribution, base, end = config.distributions, catalog.id_base, catalog.history_end
    reviews = random.Random(f"{config.seed}-reviews")
    counts = defaultdict(int)
    net, outbound, versions = defaultdict(int), defaultdict(int), defaultdict(int)
    bought = defaultdict(dict)
    coupon_usage = defaultdict(int)
    failure_times = defaultdict(list)
    variants_by_id = {row["variant_id"]: row for row in catalog.variants}
    products = {row["product_id"]: row for row in catalog.tables["products"]}
    categories = {row["category_id"]: row for row in catalog.tables["categories"]}
    batch = defaultdict(list)

    def add(table, pk, row, public=True):
        counts[table] += 1
        row[pk] = base + counts[table]
        if public:
            row["public_id"] = catalog.public_id(table, counts[table])
        batch[table].append(row)
        return row

    def movement(variant_id, quantity, kind, reference, moment):
        net[variant_id] += quantity
        outbound[variant_id] += max(0, -quantity)
        versions[variant_id] += 1
        add(
            "inventory_transactions",
            "transaction_id",
            {
                "variant_id": variant_id,
                "location_type": "central_warehouse",
                "store_id": None,
                "movement_type": kind,
                "quantity_delta": quantity,
                "reference_code": reference,
                "notes": "Dữ liệu tổng hợp",
                "created_at": moment,
            },
        )

    for index, (customer_index, group, position, moment, campaign) in enumerate(
        order_schedule(config, catalog, randomizer)
    ):
        customer = catalog.tables["customers"][customer_index]
        customer_id, order_id = customer["customer_id"], base + index + 1
        age = end - moment
        cart_time = moment - timedelta(minutes=randomizer.randint(5, 10080))
        cart = add(
            "carts",
            "cart_id",
            {
                "customer_id": customer_id,
                "status": "checked_out",
                "created_at": cart_time,
                "updated_at": moment,
                "checked_out_at": moment,
            },
        )
        size = randomizer.choices(range(1, len(distribution.order_size) + 1), weights=distribution.order_size)[0]
        selected = select_order_variants(catalog, randomizer, size)
        items = [
            (
                variant,
                randomizer.choices(
                    range(1, len(distribution.quantity_per_item) + 1), weights=distribution.quantity_per_item
                )[0],
            )
            for variant in selected
        ]
        subtotal = sum(variant["price_vnd"] * quantity for variant, quantity in items)
        behavior = distribution.coupons
        eligible = [
            row
            for row in catalog.tables["coupons"]
            if row["starts_at"] <= moment < row["ends_at"]
            and subtotal >= row["minimum_subtotal_vnd"]
            and coupon_usage[customer_id, row["coupon_id"]] < row["per_customer_usage_limit"]
        ]
        local_hour = moment.astimezone(ZoneInfo(distribution.business_timezone)).hour
        coupon = None
        for kind, rate in [
            ("midnight", behavior["midnight_usage_rate"]),
            ("campaign", behavior["campaign_usage_rate"]),
            ("welcome", behavior["first_order_usage_rate"]),
            ("everyday", behavior["base_usage_rate"]),
        ]:
            candidates = [row for row in eligible if coupon_kinds[row["coupon_id"]] == kind]
            if (
                not candidates
                or (kind == "midnight" and local_hour not in (0, 1))
                or (kind == "welcome" and position != 0)
            ):
                continue
            if randomizer.random() < min(1, rate * behavior["customer_multipliers"][group]):
                coupon = randomizer.choice(candidates)
            break
        discount = (
            0
            if coupon is None
            else (
                subtotal * coupon["discount_value"] // 100
                if coupon["discount_type"] == "percentage"
                else min(subtotal, coupon["discount_value"])
            )
        )
        if discount == 0:
            coupon = None
        total = subtotal - discount + (0 if subtotal >= 500000 else 30000)
        cancellation = distribution.cancellations
        rate = cancellation["campaign_rate"] if campaign else cancellation["base_rate"]
        rate += cancellation["customer_addons"][group] + (cancellation["coupon_addon"] if coupon else 0)
        cancelled = age >= timedelta(hours=2) and randomizer.random() < max(0, min(1, rate))
        completed = not cancelled and age >= timedelta(days=5) and randomizer.random() < 0.97
        confirmed = not cancelled and not completed and age >= timedelta(hours=8)
        cod_blocked = sum(time <= moment for time in failure_times[customer_id]) >= 3
        cod = (customer_index + position) % 10 < 6 and not cod_blocked
        risky = customer_index % 8 == 3
        boom = cod and not cancelled and age >= timedelta(days=3) and position % (3 if risky else 31) == 0
        completed = completed and not boom
        confirmed = confirmed or boom
        paid_at = None if cod else moment
        confirmed_at = moment if cod else None
        if not cod and (completed or confirmed):
            confirmed_at = min(end - timedelta(minutes=2), moment + timedelta(hours=randomizer.randint(2, 18)))
        completed_at = (
            min(end - timedelta(minutes=1), confirmed_at + timedelta(hours=randomizer.randint(24, 96)))
            if completed
            else None
        )
        if cod and completed:
            paid_at = completed_at
        cancelled_at = (
            min(end - timedelta(minutes=1), moment + timedelta(minutes=randomizer.randint(15, 480)))
            if cancelled
            else None
        )
        failed_at = min(end, (confirmed_at or moment) + timedelta(days=2)) if boom else None
        status = (
            "failed_delivery"
            if boom
            else "cancelled"
            if cancelled
            else "completed"
            if completed
            else "confirmed"
            if cod or confirmed
            else "paid"
        )
        updated = cancelled_at or failed_at or completed_at or confirmed_at or moment
        reason = (
            randomizer.choices(list(cancellation["reasons"]), weights=list(cancellation["reasons"].values()))[0]
            if cancelled
            else None
        )
        order = add(
            "orders",
            "order_id",
            {
                "order_number": f"SYN-{catalog.namespace.hex[:8]}-{index + 1:08d}",
                "cart_id": cart["cart_id"],
                "customer_id": customer_id,
                "checkout_idempotency_key": str(catalog.public_id("checkout", index)),
                "status": status,
                "currency_code": "VND",
                "subtotal_vnd": subtotal,
                "discount_amount_vnd": discount,
                "shipping_fee_vnd": total - subtotal + discount,
                "total_vnd": total,
                "receiver_name": customer["display_name"],
                "receiver_phone": f"09{customer_index + 1:08d}",
                "shipping_address_text": catalog.customer_addresses[customer_id],
                "data_origin": "synthetic",
                "generation_run_id": config.generation_run_id,
                "created_at": moment,
                "updated_at": updated,
                "paid_at": paid_at,
                "confirmed_at": confirmed_at,
                "completed_at": completed_at,
                "cancelled_at": cancelled_at,
                "channel": "online",
                "payment_method": "cod" if cod else "vietqr",
                "coupon_id": coupon["coupon_id"] if coupon else None,
                "coupon_code_snapshot": coupon["code_normalized"] if coupon else None,
                "coupon_type_snapshot": coupon["discount_type"] if coupon else None,
                "coupon_value_snapshot": coupon["discount_value"] if coupon else None,
            },
            public=False,
        )
        order_items = []
        for variant, quantity in items:
            product_row = products[variant["product_id"]]
            category = categories[product_row["category_id"]]
            add(
                "cart_items",
                "cart_item_id",
                {
                    "cart_id": cart["cart_id"],
                    "variant_id": variant["variant_id"],
                    "quantity": quantity,
                    "is_present": True,
                    "first_added_at": cart_time,
                    "removed_at": None,
                    "updated_at": moment,
                },
                public=False,
            )
            item = add(
                "order_items",
                "order_item_id",
                {
                    "order_id": order_id,
                    "variant_id": variant["variant_id"],
                    "product_public_id_snapshot": product_row["public_id"],
                    "category_code_snapshot": category["code"],
                    "category_name_snapshot": category["name"],
                    "product_name_snapshot": product_row["name"],
                    "sku_snapshot": variant["sku"],
                    "size_code_snapshot": variant["size_code"],
                    "color_code_snapshot": variant["color_code"],
                    "unit_price_vnd": variant["price_vnd"],
                    "quantity": quantity,
                    "line_total_vnd": variant["price_vnd"] * quantity,
                    "cost_price_vnd": variant["cost_price_vnd"],
                    "created_at": moment,
                },
            )
            order_items.append(item)
            movement(variant["variant_id"], -quantity, "outbound_order", f"ORD-{order_id}", moment)
            if cancelled or boom:
                movement(
                    variant["variant_id"],
                    quantity,
                    "order_cancellation" if cancelled else "return_boom",
                    f"ORD-{order_id}",
                    updated,
                )
            if not cancelled:
                bought[customer_id].setdefault(variant["product_id"], moment)
        payment = add(
            "payments",
            "payment_id",
            {
                "payment_reference": f"PAY-{catalog.namespace.hex[:8]}-{index + 1:08d}",
                "order_id": order_id,
                "payment_idempotency_key": str(catalog.public_id("payment", index)),
                "status": "succeeded" if paid_at else "pending",
                "currency_code": "VND",
                "amount_vnd": total,
                "failure_code": None,
                "attempted_at": paid_at or moment,
                "created_at": moment,
                "updated_at": paid_at or moment,
            },
            public=False,
        )
        if coupon:
            released = cancelled or boom
            add(
                "coupon_redemptions",
                "coupon_redemption_id",
                {
                    "coupon_id": coupon["coupon_id"],
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "status": "released" if released else "redeemed",
                    "redeemed_at": moment,
                    "released_at": updated if released else None,
                    "created_at": moment,
                    "updated_at": updated,
                },
                public=False,
            )
            if not released:
                coupon_usage[customer_id, coupon["coupon_id"]] += 1
                coupon["used_count"] += 1
        refund_amount, refund_time, refund_reason = (
            (total, cancelled_at, reason) if cancelled and not cod else (0, None, None)
        )
        if completed and completed_at + timedelta(days=3) <= end and index % 25 == 7:
            first = order_items[0]
            replacements = [
                row
                for row in catalog.variants_by_product[variants_by_id[first["variant_id"]]["product_id"]]
                if row["variant_id"] != first["variant_id"]
            ]
            exchange = index % 10 < 7 and bool(replacements)
            replacement = replacements[0] if exchange else None
            returned_at, resolved_at = completed_at + timedelta(days=2), completed_at + timedelta(days=3)
            return_code = f"RET-{catalog.namespace.hex[:8]}-{index + 1:08d}"
            request = add(
                "return_requests",
                "return_id",
                {
                    "return_code": return_code,
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "action_type": "exchange" if exchange else "refund",
                    "status": "completed",
                    "customer_reason": "Đổi size do mặc chật/rộng" if exchange else "Không ưng form dáng / chất liệu",
                    "reviewed_at": returned_at,
                    "resolved_at": resolved_at,
                    "created_at": returned_at,
                    "updated_at": resolved_at,
                },
            )
            amount = 0 if exchange else round(first["unit_price_vnd"] * (1 - discount / subtotal)) if subtotal else 0
            add(
                "return_items",
                "return_item_id",
                {
                    "return_id": request["return_id"],
                    "order_item_id": first["order_item_id"],
                    "variant_id": first["variant_id"],
                    "quantity": 1,
                    "exchange_variant_id": replacement["variant_id"] if replacement else None,
                    "refund_amount_vnd": amount,
                    "inspection_status": "passed",
                    "created_at": returned_at,
                    "updated_at": resolved_at,
                },
            )
            movement(first["variant_id"], 1, "return_customer", return_code, resolved_at)
            if exchange:
                movement(replacement["variant_id"], -1, "exchange_out", return_code, resolved_at)
            else:
                refund_amount, refund_time, refund_reason = amount, resolved_at, request["customer_reason"]
                if len(order_items) == 1 and first["quantity"] == 1:
                    order["status"], order["updated_at"] = "returned", resolved_at
        if refund_time is not None and refund_amount > 0:
            add(
                "refunds",
                "refund_id",
                {
                    "payment_id": payment["payment_id"],
                    "refund_idempotency_key": str(catalog.public_id("refund", index)),
                    "status": "succeeded",
                    "currency_code": "VND",
                    "amount_vnd": refund_amount,
                    "reason": refund_reason,
                    "requested_by_customer_id": customer_id,
                    "created_at": refund_time,
                    "completed_at": refund_time,
                    "updated_at": refund_time,
                },
            )
        transitions = [("confirmed" if cod else "paid", moment, None)]
        if not cod and confirmed_at:
            transitions.append(("confirmed", confirmed_at, None))
        if cancelled:
            transitions.append(("cancelled", cancelled_at, reason))
        elif boom:
            transitions.append(("failed_delivery", failed_at, "Khách không nhận hàng"))
        elif completed:
            transitions.append(("completed", completed_at, None))
            if order["status"] == "returned":
                transitions.append(("returned", order["updated_at"], "Hoàn trả toàn bộ hàng"))
        previous = None
        for state, state_time, state_reason in transitions:
            add(
                "order_status_history",
                "order_status_history_id",
                {
                    "order_id": order_id,
                    "from_status": previous,
                    "to_status": state,
                    "transition_source": "generator",
                    "reason": state_reason,
                    "transition_idempotency_key": str(
                        catalog.public_id(f"transition-{index}", len(batch["order_status_history"]))
                    ),
                    "transitioned_at": state_time,
                    "created_at": state_time,
                },
                public=False,
            )
            previous = state
        if completed or boom:
            staff_id = base + (index % len(catalog.tables["delivery_staff"])) + 1
            shipment = add(
                "shipments",
                "shipment_id",
                {
                    "shipment_code": f"SHP-{catalog.namespace.hex[:8]}-{index + 1:08d}",
                    "order_id": order_id,
                    "delivery_staff_id": staff_id,
                    "status": "failed" if boom else "delivered",
                    "attempt_count": 3 if boom else 1,
                    "cod_amount_vnd": total if cod else 0,
                    "cod_collected_vnd": total if cod and completed else 0,
                    "dispatched_at": confirmed_at or moment,
                    "delivered_at": completed_at,
                    "failed_at": failed_at,
                    "failure_reason": "Khách không nhận hàng" if boom else None,
                    "created_at": confirmed_at or moment,
                    "updated_at": updated,
                },
            )
            for attempt in range(1, 4 if boom else 2):
                attempt_at = (
                    min(failed_at, (confirmed_at or moment) + timedelta(hours=attempt * 6)) if boom else completed_at
                )
                add(
                    "shipment_attempts",
                    "attempt_id",
                    {
                        "shipment_id": shipment["shipment_id"],
                        "attempt_number": attempt,
                        "delivery_staff_id": staff_id,
                        "status": "attempted_failed" if boom else "attempted_success",
                        "is_buyer_fault": boom,
                        "failure_reason": "Khách không nhận hàng" if boom else None,
                        "idempotency_key": str(catalog.public_id(f"attempt-{index}", attempt)),
                        "attempted_at": attempt_at,
                        "created_at": attempt_at,
                        "updated_at": attempt_at,
                    },
                )
        if boom:
            failure_times[customer_id].append(failed_at)
            customer["boom_count"] = customer.get("boom_count", 0) + 1
            customer["is_cod_blocked"] = customer["boom_count"] >= 3
        for item in order_items if completed else []:
            if reviews.random() >= distribution.reviews["completed_order_rates"][group]:
                continue
            review_time = completed_at + timedelta(
                days=reviews.randint(*distribution.reviews["delay_days"]), hours=reviews.randint(0, 23)
            )
            if review_time > end or (order["status"] == "returned" and review_time >= order["updated_at"]):
                continue
            rating = reviews.choices(range(1, 6), weights=distribution.reviews["rating_weights"])[0]
            review_status = reviews.choices(
                list(distribution.reviews["status_weights"]),
                weights=list(distribution.reviews["status_weights"].values()),
            )[0]
            moderated_at = (
                min(end, review_time + timedelta(hours=reviews.randint(1, 48))) if review_status == "rejected" else None
            )
            add(
                "product_reviews",
                "review_id",
                {
                    "order_item_id": item["order_item_id"],
                    "customer_id": customer_id,
                    "product_id": variants_by_id[item["variant_id"]]["product_id"],
                    "rating": rating,
                    "content": reviews.choice(REVIEW_CONTENT_BY_RATING[rating]),
                    "status": review_status,
                    "moderation_reason": reviews.choice(REVIEW_REJECTION_REASONS) if moderated_at else None,
                    "moderated_by_customer_id": base + config.scale["customers"] + 1 if moderated_at else None,
                    "moderated_at": moderated_at,
                    "created_at": review_time,
                    "updated_at": moderated_at or review_time,
                },
            )
        yield dict(batch)
        batch.clear()

    for customer in catalog.tables["customers"][:-1]:
        customer_id = customer["customer_id"]
        if randomizer.random() < 0.55:
            count = randomizer.randint(1, min(5, len(products)))
            purchased = bought[customer_id]
            converted = min(len(purchased), max(1, round(count * 0.4)) if purchased else 0)
            selected_ids = randomizer.sample(sorted(purchased), converted)
            selected_ids += randomizer.sample([key for key in products if key not in selected_ids], count - converted)
            for product_id in selected_ids:
                purchase_time = purchased.get(product_id)
                if purchase_time:
                    first = max(
                        catalog.history_start,
                        purchase_time - timedelta(days=randomizer.randint(1, 30), hours=randomizer.randint(0, 23)),
                    )
                    last = min(purchase_time, first + timedelta(days=randomizer.randint(0, 7)))
                    removed = purchase_time
                else:
                    first = catalog.history_start + timedelta(
                        seconds=randomizer.randrange(max(1, int((end - catalog.history_start).total_seconds())))
                    )
                    last = min(end, first + timedelta(days=randomizer.randrange(30)))
                    removed = (
                        min(end, last + timedelta(days=randomizer.randrange(15)))
                        if randomizer.random() < 0.18
                        else None
                    )
                add(
                    "wishlist_items",
                    "wishlist_item_id",
                    {
                        "customer_id": customer_id,
                        "product_id": product_id,
                        "is_present": removed is None,
                        "first_added_at": first,
                        "last_added_at": last,
                        "removed_at": removed,
                        "updated_at": removed or last,
                    },
                    public=False,
                )
        if customer["status"] == "active":
            updated = end - (
                timedelta(days=randomizer.randint(2, 20))
                if randomizer.random() < 0.65
                else timedelta(hours=randomizer.randint(1, 12))
            )
            created = updated - timedelta(hours=randomizer.randint(1, 72))
            cart = add(
                "carts",
                "cart_id",
                {
                    "customer_id": customer_id,
                    "status": "active",
                    "created_at": created,
                    "updated_at": updated,
                    "checked_out_at": None,
                },
            )
            for variant in randomizer.sample(catalog.variants, randomizer.randint(1, min(3, len(catalog.variants)))):
                add(
                    "cart_items",
                    "cart_item_id",
                    {
                        "cart_id": cart["cart_id"],
                        "variant_id": variant["variant_id"],
                        "quantity": randomizer.randint(1, 3),
                        "is_present": True,
                        "first_added_at": created,
                        "removed_at": None,
                        "updated_at": updated,
                    },
                    public=False,
                )
        yield dict(batch)
        batch.clear()
    for variant_id in variants_by_id:
        opening = outbound[variant_id] + randomizer.randint(80, 200)
        batch["inventory"].append(
            {
                "variant_id": variant_id,
                "opening_on_hand": opening,
                "on_hand": opening + net[variant_id],
                "version": versions[variant_id],
                "updated_at": end,
            }
        )
    yield dict(batch)
