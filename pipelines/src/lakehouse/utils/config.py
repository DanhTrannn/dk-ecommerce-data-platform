from dataclasses import dataclass

MUTABLE_CURSOR = "updated_at"
APPEND_ONLY_CURSOR = "created_at"
SILVER_PREFIX = "silver_"


MUTABLE_TABLES = frozenset(
    {
        "customers",
        "categories",
        "products",
        "product_variants",
        "carts",
        "cart_items",
        "wishlist_items",
        "orders",
        "inventory",
        "coupons",
        "coupon_redemptions",
        "product_reviews",
        "cities",
        "stores",
        "store_inventory",
        "delivery_staff",
        "shipments",
        "return_requests",
        "return_items",
        "refunds",
        "payments",
        "inbound_receipts",
    }
)

APPEND_ONLY_TABLES = frozenset(
    {
        "order_items",
        "order_status_history",
        "inventory_transactions",
        "inbound_receipt_items",
        "shipment_attempts",
    }
)

KNOWN_TABLES = MUTABLE_TABLES | APPEND_ONLY_TABLES
EXPECTED_CURSOR = {name: (MUTABLE_CURSOR if name in MUTABLE_TABLES else APPEND_ONLY_CURSOR) for name in KNOWN_TABLES}


@dataclass(frozen=True)
class TableSpec:
    name: str
    cursor_field: str
    pk: str
    mutability: str
    silver_table: str
    pseudonymize: tuple[str, ...] = ()


SOURCE_TABLES = [
    TableSpec(
        "customers",
        "updated_at",
        "customer_id",
        "mutable",
        "silver_customers",
        ("display_name", "email_normalized", "phone", "full_name"),
    ),
    TableSpec("categories", "updated_at", "category_id", "mutable", "silver_categories"),
    TableSpec("products", "updated_at", "product_id", "mutable", "silver_products"),
    TableSpec("product_variants", "updated_at", "variant_id", "mutable", "silver_product_variants"),
    TableSpec("carts", "updated_at", "cart_id", "mutable", "silver_carts"),
    TableSpec("cart_items", "updated_at", "cart_item_id", "mutable", "silver_cart_items"),
    TableSpec("wishlist_items", "updated_at", "wishlist_item_id", "mutable", "silver_wishlist_items"),
    TableSpec(
        "orders",
        "updated_at",
        "order_id",
        "mutable",
        "silver_orders",
        ("receiver_name", "receiver_phone", "shipping_address_text"),
    ),
    TableSpec("order_items", "created_at", "order_item_id", "append_only", "silver_order_items"),
    TableSpec("payments", "updated_at", "payment_id", "mutable", "silver_payments"),
    TableSpec(
        "order_status_history", "created_at", "order_status_history_id", "append_only", "silver_order_status_history"
    ),
    TableSpec("inventory", "updated_at", "variant_id", "mutable", "silver_inventory"),
    TableSpec("coupons", "updated_at", "coupon_id", "mutable", "silver_coupons"),
    TableSpec("coupon_redemptions", "updated_at", "coupon_redemption_id", "mutable", "silver_coupon_redemptions"),
    TableSpec("refunds", "updated_at", "refund_id", "mutable", "silver_refunds"),
    TableSpec("product_reviews", "updated_at", "review_id", "mutable", "silver_product_reviews"),
    TableSpec("cities", "updated_at", "city_id", "mutable", "silver_cities"),
    TableSpec("stores", "updated_at", "store_id", "mutable", "silver_stores"),
    TableSpec("store_inventory", "updated_at", "store_inventory_id", "mutable", "silver_store_inventory"),
    TableSpec("delivery_staff", "updated_at", "staff_id", "mutable", "silver_delivery_staff", ("phone", "full_name")),
    TableSpec("shipments", "updated_at", "shipment_id", "mutable", "silver_shipments"),
    TableSpec("return_requests", "updated_at", "return_id", "mutable", "silver_return_requests"),
    TableSpec("return_items", "updated_at", "return_item_id", "mutable", "silver_return_items"),
    TableSpec("inventory_transactions", "created_at", "transaction_id", "append_only", "silver_inventory_transactions"),
    TableSpec("inbound_receipts", "updated_at", "receipt_id", "mutable", "silver_inbound_receipts"),
    TableSpec("inbound_receipt_items", "created_at", "item_id", "append_only", "silver_inbound_receipt_items"),
    TableSpec("shipment_attempts", "created_at", "attempt_id", "append_only", "silver_shipment_attempts"),
]
