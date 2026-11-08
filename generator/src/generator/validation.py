"""Scale constraints shared by the CLI and configuration form."""

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


def validate_scale(scale: Mapping[str, int]) -> tuple[int, int, int, int]:
    keys = ("customers", "products", "variants", "orders")
    missing = [key for key in keys if key not in scale]
    if missing:
        raise ValueError(f"missing scale keys: {missing}")
    customers, products, variants, orders = (scale[key] for key in keys)
    if customers < 1 or products < 1:
        raise ValueError("customers/products must be positive")
    if orders < 0:
        raise ValueError("orders must be non-negative")
    if variants < products:
        raise ValueError("variants must be greater than or equal to products")
    if variants > products * len(SIZES) * len(COLORS):
        raise ValueError("variant scale exceeds unique size/color combinations")
    return customers, products, variants, orders
