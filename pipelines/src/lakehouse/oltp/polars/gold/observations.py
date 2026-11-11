"""Independent singleton/window observations at a fixed business timestamp."""

from .core import query

SQL = {
    "mart_operations_summary": """WITH o AS (SELECT count(order_id) order_rows,
        sum(CAST(status IN ('paid','confirmed') AS BIGINT)) pending_orders,
        sum(CAST(status IN ('paid','confirmed') AND created_at<=c.observed_at-INTERVAL '2 hours' AS BIGINT)) over_two_hours_orders,
        avg(CASE WHEN status IN ('paid','confirmed') AND created_at<=c.observed_at
        THEN (FLOOR(EXTRACT(EPOCH FROM c.observed_at))-FLOOR(EXTRACT(EPOCH FROM created_at)))/3600.0 END) average_waiting_hours
        FROM fact_order CROSS JOIN clock c),s AS (SELECT count(shipment_id) shipment_rows,
        sum(CAST(attempt_count>1 OR status='failed' AS BIGINT)) problematic_shipments,
        sum(CAST(status='failed' AS BIGINT)) failed_shipments FROM fact_shipment),r AS (
        SELECT count(return_id) return_rows,sum(CAST(status<>'cancelled' AS BIGINT)) return_requests_count FROM return_requests),
        f AS (SELECT count(refund_id) refund_rows,sum(CAST(status='succeeded' AS BIGINT)) successful_refunds_count,
        sum(CASE WHEN status='succeeded' THEN amount_vnd ELSE 0 END) successful_refund_amount_vnd FROM refunds)
        SELECT 'current' summary_key,o.*,s.*,r.*,f.*,c.observed_at operations_observed_at FROM o CROSS JOIN s CROSS JOIN r CROSS JOIN f CROSS JOIN clock c""",
    "mart_cod_region_daily": """SELECT s.shipment_date,coalesce(o.shipping_region,'Khu vực khác') region,
        count(s.shipment_id) total_cod_shipments,sum(CAST(s.status='failed' AS BIGINT)) failed_cod_shipments
        FROM fact_shipment s JOIN fact_order o ON s.order_id=o.order_id WHERE o.payment_method='cod'
        GROUP BY s.shipment_date,region""",
    "mart_marketing_summary": """SELECT 'current' summary_key,q.coupon_rows,r.review_rows,c.observed_at marketing_observed_at
        FROM (SELECT count(coupon_id) coupon_rows FROM coupons) q CROSS JOIN
        (SELECT count(review_id) review_rows FROM product_reviews) r CROSS JOIN clock c""",
    "mart_coupon_usage": """WITH recent AS (SELECT coupon_id,count(coupon_redemption_id) recent_redemptions_count
        FROM coupon_redemptions CROSS JOIN clock WHERE status='redeemed'
        AND redeemed_at>=observed_at-INTERVAL '15 minutes' AND redeemed_at<=observed_at GROUP BY coupon_id)
        SELECT q.coupon_id,q.code_normalized coupon_code,q.used_count,q.total_usage_limit usage_limit,
        (q.is_active AND q.archived_at IS NULL AND q.starts_at<=c.observed_at AND q.ends_at>=c.observed_at) is_available,
        coalesce(r.recent_redemptions_count,0) recent_redemptions_count,c.observed_at marketing_observed_at
        FROM coupons q LEFT JOIN recent r ON q.coupon_id=r.coupon_id CROSS JOIN clock c""",
    "mart_product_review_summary": """SELECT r.product_id,coalesce(p.product_name,'Unknown') product_name,
        count(r.review_id) review_count,sum(CAST(r.status='approved' AND r.rating<=2
        AND r.created_at>=c.observed_at-INTERVAL '45 minutes' AND r.created_at<=c.observed_at AS BIGINT)) recent_negative_count,
        c.observed_at marketing_observed_at FROM product_reviews r LEFT JOIN dim_product p ON r.product_id=p.product_id
        CROSS JOIN clock c GROUP BY r.product_id,product_name,c.observed_at""",
    "mart_admin_overview": """WITH p AS (SELECT count(*) product_rows,sum(CAST(is_active AS BIGINT)) active_products FROM products),
        v AS (SELECT count(*) variant_rows FROM product_variants),active AS (SELECT v.variant_id FROM product_variants v
        JOIN products p ON v.product_id=p.product_id WHERE v.is_active AND p.is_active),
        ac AS (SELECT count(*) active_variants FROM active),i AS (SELECT count(*) inventory_rows FROM fact_inventory_current WHERE location_type='warehouse'),
        low AS (SELECT count(*) low_stock_variants FROM fact_inventory_current i JOIN active a ON i.variant_key=a.variant_id
        WHERE i.location_type='warehouse' AND i.is_low_stock),c AS (SELECT count(*) customer_rows,
        sum(CAST(role='customer' AS BIGINT)) customers FROM customers),r AS (SELECT count(*) review_rows FROM product_reviews),
        q AS (SELECT count(*) coupon_rows,sum(CAST(is_active AND archived_at IS NULL AND starts_at<=observed_at
        AND ends_at>observed_at AS BIGINT)) active_coupons FROM coupons CROSS JOIN clock),o AS (
        SELECT count(*) order_rows,
        sum(CAST(status='pending_payment' AS BIGINT)) pending_payment_orders,sum(CAST(status='paid' AS BIGINT)) paid_orders,
        sum(CAST(status='confirmed' AS BIGINT)) confirmed_orders,sum(CAST(status='shipping' AS BIGINT)) shipping_orders,
        sum(CAST(status='delivered' AS BIGINT)) delivered_orders,sum(CAST(status='completed' AS BIGINT)) completed_orders,
        sum(CAST(status='cancelled' AS BIGINT)) cancelled_orders,sum(CAST(status='failed_delivery' AS BIGINT)) failed_delivery_orders,
        sum(CAST(status='returned' AS BIGINT)) returned_orders FROM fact_order)
        SELECT 'current' summary_key,p.*,v.variant_rows,i.inventory_rows,c.*,r.review_rows,q.*,o.*,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 THEN ac.active_variants END active_variants,
        CASE WHEN p.product_rows>0 AND v.variant_rows>0 AND i.inventory_rows>0 THEN low.low_stock_variants END low_stock_variants,
        CASE WHEN r.review_rows>0 THEN r.review_rows END total_reviews,clock.observed_at admin_observed_at
        FROM p CROSS JOIN v CROSS JOIN i CROSS JOIN c CROSS JOIN r CROSS JOIN q CROSS JOIN o CROSS JOIN ac CROSS JOIN low CROSS JOIN clock""",
}


def build_observation(name, frames):
    return query(frames, SQL[name])
