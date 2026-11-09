"""Generate related transaction rows; parameter provenance is in GENERATOR_METRICS.md."""

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
    policy = config.policies["coupons"]
    master_created = catalog.history_start - timedelta(days=config.policies["history"]["master_data_lead_days"])
    start, end, base = catalog.history_start, catalog.history_end, catalog.id_base
    zone = ZoneInfo(config.distributions.business_timezone)
    specs = [
        (
            "everyday",
            behavior["percentage"],
            start - timedelta(days=policy["start_before_history_days"]),
            end + timedelta(days=policy["valid_after_anchor_days"]),
        )
    ]
    for event, day in campaign_dates(config, catalog):
        moment = datetime.combine(day, datetime.min.time(), zone).astimezone(UTC)
        specs.append(
            (
                "campaign",
                behavior["campaign_percentage"],
                max(master_created, moment),
                moment + timedelta(days=event["after_days"] + 1),
            )
        )
    rows, kinds = [], {}
    for index, (kind, percentage, begins, ends) in enumerate(specs, 1):
        archived = kind == "campaign" and ends <= end
        code = f"SYN-{catalog.namespace.hex[:8]}-{index:04d}"
        rows.append(
            {
                "coupon_id": base + index,
                "public_id": catalog.public_id("coupon", index),
                "code_normalized": code.lower(),
                "discount_type": "percentage",
                "discount_value": percentage,
                "minimum_subtotal_vnd": behavior["minimum_subtotal_vnd"],
                "starts_at": begins,
                "ends_at": ends,
                "is_active": not archived,
                "total_usage_limit": max(
                    policy["minimum_total_usage_limit"], config.scale["orders"] + policy["extra_total_usage_limit"]
                ),
                "per_customer_usage_limit": policy["ordinary_customer_usage_limit"]
                if kind == "everyday"
                else policy["campaign_customer_usage_limit"],
                "used_count": 0,
                "archived_at": ends if archived else None,
                "archived_by_customer_id": base + config.scale["customers"] + 1 if archived else None,
                "archive_reason": "Kết thúc chiến dịch synthetic" if archived else None,
                "created_at": master_created,
                "updated_at": ends if archived else master_created,
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
                interval = randomizer.randint(*group["interval_days"])
                day = (previous + timedelta(days=min(interval, cap))).astimezone(zone).date()
                if randomizer.random() < group["campaign_affinity"]:
                    candidates = [
                        item
                        for item in campaign_days
                        if previous.astimezone(zone).date() <= item
                        and item <= limit.astimezone(zone).date()
                        and abs((item - day).days) <= config.policies["timing"]["campaign_pull_days"]
                    ]
                    if candidates:
                        day = randomizer.choice(candidates)
            hour_weights = distribution.campaign_hour_of_day if boosts.get(day, 1) > 1 else distribution.hour_of_day
            hour = randomizer.choices(range(24), weights=hour_weights)[0]
            moment = datetime(
                day.year,
                day.month,
                day.day,
                hour,
                randomizer.randint(*config.policies["timing"]["minute_of_hour"]),
                tzinfo=zone,
            ).astimezone(UTC)
            moment = min(limit, max(catalog.history_start, previous or catalog.history_start, moment))
            yield customer_index, moment
            previous = moment


def select_order_variants(catalog: Catalog, randomizer: random.Random, count: int) -> list[dict]:
    available = {key: weight for key, weight in catalog.product_weights.items() if weight > 0}
    selected = []
    for _ in range(min(count, len(available))):
        product_id = randomizer.choices(list(available), weights=list(available.values()))[0]
        selected.append(randomizer.choice(catalog.variants_by_product[product_id]))
        del available[product_id]
    return sorted(selected, key=lambda row: row["variant_id"])


def transaction_batches(
    config: GeneratorConfig, catalog: Catalog, randomizer: random.Random, coupon_kinds: dict[int, str]
):
    distribution, base, end = config.distributions, catalog.id_base, catalog.history_end
    policy = config.policies
    shipping, timing, returns = policy["shipping"], policy["timing"], policy["returns"]
    wishlist, carts, review_policy = policy["wishlist"], policy["carts"], policy["reviews"]
    return_delay = timedelta(days=returns["request_delay_days"] + returns["processing_delay_days"])
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

    for index, (customer_index, moment) in enumerate(order_schedule(config, catalog, randomizer)):
        customer = catalog.tables["customers"][customer_index]
        customer_id, order_id = customer["customer_id"], base + index + 1
        age = end - moment
        cart_time = moment - timedelta(minutes=randomizer.randint(*carts["checkout_lead_minutes"]))
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
        campaign_coupons = [row for row in eligible if coupon_kinds[row["coupon_id"]] == "campaign"]
        candidates = campaign_coupons or eligible
        usage_rate = behavior["campaign_usage_rate"] if campaign_coupons else behavior["usage_rate"]
        coupon = randomizer.choice(candidates) if candidates and randomizer.random() < usage_rate else None
        discount = subtotal * coupon["discount_value"] // 100 if coupon else 0
        if discount == 0:
            coupon = None
        total = subtotal - discount + (0 if subtotal >= shipping["free_from_subtotal_vnd"] else shipping["fee_vnd"])
        outcomes = distribution.outcomes
        cancelled = (
            age >= timedelta(hours=timing["cancellation_min_age_hours"])
            and randomizer.random() < outcomes["cancellation_rate"]
        )
        completed = not cancelled and age >= timedelta(days=timing["completion_min_age_days"])
        confirmed = not cancelled and not completed and age >= timedelta(hours=timing["confirmation_min_age_hours"])
        cod_blocked = sum(time <= moment for time in failure_times[customer_id]) >= shipping["cod_block_after_failures"]
        cod = not cod_blocked and randomizer.random() < outcomes["cod_rate"]
        boom = (
            cod
            and not cancelled
            and age >= timedelta(days=timing["failure_min_age_days"])
            and randomizer.random() < outcomes["cod_failure_rate"]
        )
        completed = completed and not boom
        confirmed = confirmed or boom
        paid_at = None if cod else moment
        confirmed_at = moment if cod else None
        if not cod and (completed or confirmed):
            confirmed_at = min(
                end - timedelta(minutes=2),
                moment + timedelta(hours=randomizer.randint(*timing["confirmation_delay_hours"])),
            )
        completed_at = (
            min(
                end - timedelta(minutes=1),
                confirmed_at + timedelta(hours=randomizer.randint(*timing["completion_delay_hours"])),
            )
            if completed
            else None
        )
        if cod and completed:
            paid_at = completed_at
        cancelled_at = (
            min(
                end - timedelta(minutes=1),
                moment + timedelta(minutes=randomizer.randint(*timing["cancellation_delay_minutes"])),
            )
            if cancelled
            else None
        )
        failed_at = (
            min(end, (confirmed_at or moment) + timedelta(days=shipping["failure_after_days"])) if boom else None
        )
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
            randomizer.choices(
                list(outcomes["cancellation_reasons"]), weights=list(outcomes["cancellation_reasons"].values())
            )[0]
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
        if completed and completed_at + return_delay <= end and randomizer.random() < outcomes["return_rate"]:
            first = order_items[0] if returns["item_selection"] == "first" else randomizer.choice(order_items)
            return_quantity = min(returns["quantity"], first["quantity"])
            replacements = [
                row
                for row in catalog.variants_by_product[variants_by_id[first["variant_id"]]["product_id"]]
                if row["variant_id"] != first["variant_id"]
            ]
            exchange = bool(replacements) and randomizer.random() < outcomes["exchange_rate"]
            replacement = (
                (replacements[0] if returns["replacement_selection"] == "first" else randomizer.choice(replacements))
                if exchange
                else None
            )
            returned_at = completed_at + timedelta(days=returns["request_delay_days"])
            resolved_at = completed_at + return_delay
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
            amount = (
                0
                if exchange or not subtotal
                else min(
                    payment["amount_vnd"], round(first["unit_price_vnd"] * (1 - discount / subtotal)) * return_quantity
                )
            )
            add(
                "return_items",
                "return_item_id",
                {
                    "return_id": request["return_id"],
                    "order_item_id": first["order_item_id"],
                    "variant_id": first["variant_id"],
                    "quantity": return_quantity,
                    "exchange_variant_id": replacement["variant_id"] if replacement else None,
                    "refund_amount_vnd": amount,
                    "inspection_status": "passed",
                    "created_at": returned_at,
                    "updated_at": resolved_at,
                },
            )
            movement(first["variant_id"], return_quantity, "return_customer", return_code, resolved_at)
            if exchange:
                movement(replacement["variant_id"], -return_quantity, "exchange_out", return_code, resolved_at)
            else:
                refund_amount, refund_time, refund_reason = amount, resolved_at, request["customer_reason"]
                if len(order_items) == 1 and first["quantity"] == return_quantity:
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
                    "attempt_count": shipping["failed_attempts"] if boom else 1,
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
            for attempt in range(1, (shipping["failed_attempts"] if boom else 1) + 1):
                attempt_at = (
                    min(
                        failed_at,
                        (confirmed_at or moment) + timedelta(hours=attempt * shipping["attempt_interval_hours"]),
                    )
                    if boom
                    else completed_at
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
            customer["is_cod_blocked"] = customer["boom_count"] >= shipping["cod_block_after_failures"]
        for item in order_items if completed else []:
            if reviews.random() >= distribution.reviews["write_rate"]:
                continue
            review_time = completed_at + timedelta(
                days=reviews.randint(*distribution.reviews["delay_days"]),
                hours=reviews.randint(*review_policy["extra_delay_hours"]),
            )
            if review_time > end or (order["status"] == "returned" and review_time >= order["updated_at"]):
                continue
            rating = reviews.choices(range(1, 6), weights=distribution.reviews["rating_weights"])[0]
            review_status = reviews.choices(
                list(distribution.reviews["status_weights"]),
                weights=list(distribution.reviews["status_weights"].values()),
            )[0]
            moderated_at = (
                min(end, review_time + timedelta(hours=reviews.randint(*review_policy["moderation_delay_hours"])))
                if review_status == "rejected"
                else None
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
        if randomizer.random() < wishlist["creation_rate"]:
            count = min(randomizer.randint(*wishlist["item_count"]), len(products))
            purchased = bought[customer_id]
            converted = min(
                count,
                len(purchased),
                max(wishlist["minimum_purchased_items"], round(count * wishlist["purchased_share"]))
                if purchased
                else 0,
            )
            selected_ids = randomizer.sample(sorted(purchased), converted)
            selected_ids += randomizer.sample([key for key in products if key not in selected_ids], count - converted)
            for product_id in selected_ids:
                purchase_time = purchased.get(product_id)
                if purchase_time:
                    first = max(
                        catalog.history_start,
                        purchase_time
                        - timedelta(
                            days=randomizer.randint(*wishlist["added_before_purchase_days"]),
                            hours=randomizer.randint(*wishlist["added_before_purchase_hours"]),
                        ),
                    )
                    last = min(
                        purchase_time, first + timedelta(days=randomizer.randint(*wishlist["readded_after_days"]))
                    )
                    removed = purchase_time
                else:
                    first = catalog.history_start + timedelta(
                        seconds=randomizer.randrange(max(1, int((end - catalog.history_start).total_seconds())))
                    )
                    last = min(
                        end, first + timedelta(days=randomizer.randint(*wishlist["unpurchased_readded_after_days"]))
                    )
                    removed = (
                        min(end, last + timedelta(days=randomizer.randint(*wishlist["removed_after_days"])))
                        if randomizer.random() < wishlist["removal_rate"]
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
                timedelta(days=randomizer.randint(*carts["stale_before_anchor_days"]))
                if randomizer.random() < carts["stale_rate"]
                else timedelta(hours=randomizer.randint(*carts["recent_before_anchor_hours"]))
            )
            created = updated - timedelta(hours=randomizer.randint(*carts["created_before_update_hours"]))
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
            for variant in randomizer.sample(
                catalog.variants, min(randomizer.randint(*carts["active_item_count"]), len(catalog.variants))
            ):
                add(
                    "cart_items",
                    "cart_item_id",
                    {
                        "cart_id": cart["cart_id"],
                        "variant_id": variant["variant_id"],
                        "quantity": randomizer.randint(*carts["active_quantity"]),
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
        opening = outbound[variant_id] + randomizer.randint(*policy["inventory"]["buffer_units"])
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
