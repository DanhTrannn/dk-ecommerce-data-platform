"""Shared scale rules for every generator export path."""

from collections.abc import Mapping

SIZES = ("XS", "S", "M", "L", "XL")
COLORS = (
    "BLACK",
    "WHITE",
    "RED",
    "GREEN",
    "BLUE",
    "YELLOW",
    "PINK",
    "PURPLE",
    "ORANGE",
    "BROWN",
    "GRAY",
    "BEIGE",
)

REQUIRED_SCALE_KEYS = ("customers", "products", "variants", "orders")


def validate_scale(scale: Mapping[str, int]) -> tuple[int, int, int, int]:
    """Validate and return the four required scale counts."""

    missing = [key for key in REQUIRED_SCALE_KEYS if key not in scale]
    if missing:
        raise ValueError(f"missing scale keys: {missing}")

    customer_count = scale["customers"]
    product_count = scale["products"]
    variant_count = scale["variants"]
    order_count = scale["orders"]
    if customer_count < 1 or product_count < 1 or order_count < 0:
        raise ValueError(
            "customers/products must be positive and orders must be non-negative"
        )
    if variant_count < product_count:
        raise ValueError("variants must be greater than or equal to products")
    if variant_count > product_count * len(SIZES) * len(COLORS):
        raise ValueError("variant scale exceeds unique size/color combinations")
    return customer_count, product_count, variant_count, order_count
