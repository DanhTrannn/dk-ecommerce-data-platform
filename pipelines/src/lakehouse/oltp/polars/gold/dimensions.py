"""Conformed dimensions using the current Silver contracts."""

from datetime import date

import polars as pl

from .core import query, round_money_rate

SQL = {
    "dim_product": """SELECT p.product_id product_key,p.product_id,p.name product_name,p.slug,p.category_id,
        c.code category_code,coalesce(c.name,'Unknown') category_name,CAST(0 AS BIGINT) base_price_vnd,
        p.is_active,p.created_at FROM products p LEFT JOIN categories c ON p.category_id=c.category_id""",
    "dim_variant": """SELECT v.variant_id variant_key,v.variant_id,v.product_id,coalesce(p.name,'Unknown') product_name,
        v.sku,v.color_code color,v.size_code size,v.price_vnd,coalesce(v.cost_price_vnd,0) cost_price_vnd,
        CASE WHEN v.price_vnd>0 THEN ((v.price_vnd-coalesce(v.cost_price_vnd,0))/v.price_vnd)*100.0 ELSE 0.0 END margin_pct,
        v.is_active FROM product_variants v LEFT JOIN products p ON v.product_id=p.product_id""",
    "dim_store": """SELECT s.store_id store_key,s.store_id,s.code store_code,s.name store_name,s.city_id,
        coalesce(c.name,'Unknown') city_name,s.address,s.phone,s.is_active FROM stores s LEFT JOIN cities c ON s.city_id=c.city_id""",
    "dim_delivery_staff": """SELECT staff_id staff_key,staff_id,concat('DK-SHIP-',CAST(staff_id AS VARCHAR)) staff_code,
        full_name_pseudonymized full_name,phone_pseudonymized phone,CAST(NULL AS BIGINT) assigned_city_id,
        'Toàn quốc' assigned_city_name,vehicle_plate vehicle_type,is_active FROM delivery_staff""",
    "dim_customer": """SELECT c.customer_id customer_key,c.customer_id,c.email_pseudonymized email,c.phone_pseudonymized phone,
        c.display_name_pseudonymized full_name,c.role,c.status,c.city_id,coalesce(i.name,'Unknown') city_name,c.store_id,
        coalesce(c.is_cod_blocked,false) is_cod_blocked,coalesce(c.boom_count,0) boom_count,c.created_at
        FROM customers c LEFT JOIN cities i ON c.city_id=i.city_id""",
}


def build_dimension(name, frames):
    if name == "dim_date":
        day = pl.col("full_date")
        return pl.DataFrame(
            {"full_date": pl.date_range(date(2025, 1, 1), date(2027, 12, 31), eager=True)}
        ).with_columns(
            day.dt.strftime("%Y%m%d").cast(pl.Int32).alias("date_key"),
            ((day.dt.weekday() % 7) + 1).alias("day_of_week"),
            day.dt.strftime("%A").alias("day_name"),
            day.dt.day().alias("day_of_month"),
            day.dt.ordinal_day().alias("day_of_year"),
            day.dt.week().alias("week_of_year"),
            day.dt.month().alias("month"),
            day.dt.strftime("%B").alias("month_name"),
            day.dt.quarter().alias("quarter"),
            day.dt.year().alias("year"),
            day.dt.weekday().is_in([6, 7]).alias("is_weekend"),
            pl.lit(False).alias("is_holiday"),
        )
    frame = query(frames, SQL[name])
    return round_money_rate(frame, "margin_pct") if name == "dim_variant" else frame
