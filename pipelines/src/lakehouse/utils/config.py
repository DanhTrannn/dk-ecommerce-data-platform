from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    name: str
    cursor_field: str
    pk: str
    silver_table: str
    pseudonymize: tuple[str, ...] = ()


SOURCE_TABLES = [
    TableSpec(
        "customers",
        "updated_at",
        "customer_id",
        "silver_customers",
        ("display_name", "email_normalized", "phone", "full_name"),
    ),
    TableSpec("categories", "updated_at", "category_id", "silver_categories"),
    TableSpec("products", "updated_at", "product_id", "silver_products"),
    TableSpec("product_variants", "updated_at", "variant_id", "silver_product_variants"),
    TableSpec("carts", "updated_at", "cart_id", "silver_carts"),
    TableSpec("cart_items", "updated_at", "cart_item_id", "silver_cart_items"),
    TableSpec("wishlist_items", "updated_at", "wishlist_item_id", "silver_wishlist_items"),
    TableSpec(
        "orders",
        "updated_at",
        "order_id",
        "silver_orders",
        ("receiver_name", "receiver_phone", "shipping_address_text"),
    ),
    TableSpec("order_items", "created_at", "order_item_id", "silver_order_items"),
    TableSpec("payments", "updated_at", "payment_id", "silver_payments"),
    TableSpec("order_status_history", "created_at", "order_status_history_id", "silver_order_status_history"),
    TableSpec("inventory", "updated_at", "variant_id", "silver_inventory"),
    TableSpec("coupons", "updated_at", "coupon_id", "silver_coupons"),
    TableSpec("coupon_redemptions", "updated_at", "coupon_redemption_id", "silver_coupon_redemptions"),
    TableSpec("refunds", "updated_at", "refund_id", "silver_refunds"),
    TableSpec("product_reviews", "updated_at", "review_id", "silver_product_reviews"),
    TableSpec("cities", "updated_at", "city_id", "silver_cities"),
    TableSpec("stores", "updated_at", "store_id", "silver_stores"),
    TableSpec("store_inventory", "updated_at", "store_inventory_id", "silver_store_inventory"),
    TableSpec("delivery_staff", "updated_at", "staff_id", "silver_delivery_staff", ("phone", "full_name")),
    TableSpec("shipments", "updated_at", "shipment_id", "silver_shipments"),
    TableSpec("return_requests", "updated_at", "return_id", "silver_return_requests"),
    TableSpec("return_items", "updated_at", "return_item_id", "silver_return_items"),
    TableSpec("inventory_transactions", "created_at", "transaction_id", "silver_inventory_transactions"),
    TableSpec("inbound_receipts", "updated_at", "receipt_id", "silver_inbound_receipts"),
    TableSpec("inbound_receipt_items", "created_at", "item_id", "silver_inbound_receipt_items"),
    TableSpec("shipment_attempts", "created_at", "attempt_id", "silver_shipment_attempts"),
]
