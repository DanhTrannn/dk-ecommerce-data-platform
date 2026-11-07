"""PostgreSQL artifact contract, verification and transactional identity restart."""

SQL_FORMAT = "postgresql-v1"
GENERATED_TABLES = (
    "cart_items",
    "carts",
    "categories",
    "cities",
    "coupon_redemptions",
    "coupons",
    "customer_credentials",
    "customers",
    "delivery_staff",
    "inventory",
    "inventory_transactions",
    "order_items",
    "order_status_history",
    "orders",
    "payments",
    "product_reviews",
    "product_variants",
    "products",
    "refunds",
    "return_items",
    "return_requests",
    "shipment_attempts",
    "shipments",
    "stores",
    "wishlist_items",
)


def lock_sql() -> str:
    return (
        "LOCK TABLE " + ", ".join(f'public."{name}"' for name in GENERATED_TABLES) + " IN SHARE ROW EXCLUSIVE MODE;\n"
    )


def verification_sql(run_id: str, block: int, counts: dict[str, int]) -> str:
    conditions = [
        f"(SELECT COUNT(*) FROM customers WHERE generation_run_id='{run_id}') <> {counts['customers']}",
        f"(SELECT COUNT(*) FROM orders WHERE generation_run_id='{run_id}') <> {counts['orders']}",
        f"(SELECT COUNT(*) FROM products WHERE product_id BETWEEN {block + 1} AND {block + counts['products']}) <> {counts['products']}",
        f"(SELECT COUNT(*) FROM product_variants WHERE variant_id BETWEEN {block + 1} AND {block + counts['variants']}) <> {counts['variants']}",
    ]
    return f"""DO $tlcn_verify$
BEGIN
    IF {" OR ".join(conditions)} THEN
        RAISE EXCEPTION 'Generated dataset row counts do not match metadata';
    END IF;
    IF EXISTS (
        SELECT 1 FROM orders o JOIN payments p USING(order_id)
        WHERE o.generation_run_id='{run_id}' AND
        ((o.paid_at IS NOT NULL) <> (p.status='succeeded'))
    ) THEN
        RAISE EXCEPTION 'Generated payment/paid_at mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM return_items r JOIN return_requests q USING(return_id)
        JOIN order_items i USING(order_item_id) JOIN orders o ON o.order_id=q.order_id
        WHERE o.generation_run_id='{run_id}'
          AND (i.order_id<>q.order_id OR i.variant_id<>r.variant_id OR r.quantity>i.quantity)
    ) THEN
        RAISE EXCEPTION 'Generated return lineage mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM refunds r JOIN payments p USING(payment_id) JOIN orders o USING(order_id)
        WHERE o.generation_run_id='{run_id}' AND (p.status<>'succeeded' OR r.amount_vnd>p.amount_vnd)
    ) THEN
        RAISE EXCEPTION 'Generated refund/payment mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM inventory i LEFT JOIN (
            SELECT variant_id,SUM(quantity_delta) delta FROM inventory_transactions GROUP BY variant_id
        ) tx USING(variant_id)
        WHERE i.variant_id BETWEEN {block + 1} AND {block + counts["variants"]}
          AND i.opening_on_hand+COALESCE(tx.delta,0)<>i.on_hand
    ) THEN
        RAISE EXCEPTION 'Generated inventory ledger mismatch';
    END IF;
END;
$tlcn_verify$;
"""


def restart_identities_sql() -> str:
    tables = ",".join("'" + name + "'" for name in GENERATED_TABLES)
    return f"""DO $tlcn_sequences$
DECLARE
    col record;
    sequence_name text;
    largest_id bigint;
    previous_value bigint;
    was_called boolean;
    restart_at bigint;
BEGIN
    FOR col IN SELECT table_name,column_name FROM information_schema.columns
               WHERE table_schema='public' AND is_identity='YES'
                 AND table_name IN ({tables}) ORDER BY table_name,column_name LOOP
        sequence_name := pg_get_serial_sequence(format('public.%I',col.table_name),col.column_name);
        EXECUTE format('SELECT MAX(%I) FROM public.%I',col.column_name,col.table_name) INTO largest_id;
        EXECUTE format('SELECT last_value,is_called FROM %s',sequence_name) INTO previous_value,was_called;
        restart_at := GREATEST(COALESCE(largest_id,0)+1,previous_value+CASE WHEN was_called THEN 1 ELSE 0 END);
        EXECUTE format('ALTER SEQUENCE %s RESTART WITH %s',sequence_name,restart_at);
    END LOOP;
END;
$tlcn_sequences$;
"""
