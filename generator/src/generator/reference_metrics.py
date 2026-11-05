"""Aggregate four reference features without publishing customer or SKU IDs."""

import math
from collections import Counter, defaultdict


def describe(values, *, histogram=True):
    values = sorted(values)
    if not values:
        return {"count": 0, "mean": None, "quantiles": None, **({"histogram": {}} if histogram else {})}
    result = {
        "count": len(values),
        "mean": sum(values) / len(values),
        "quantiles": {str(q): values[max(0, math.ceil(q * len(values)) - 1)] for q in (0.5, 0.9, 0.95, 0.99, 1.0)},
    }
    if histogram:
        result["histogram"] = {str(key): count for key, count in sorted(Counter(values).items())}
    return result


def summarize_invoices(invoices, *, start, end):
    baskets, quantities = [], []
    sku_units, customers, relation = Counter(), defaultdict(list), defaultdict(lambda: [0, 0, 0])
    missing_customer = 0
    for invoice in invoices:
        size = len(invoice["items"])
        units = sum(invoice["items"].values())
        baskets.append(size)
        quantities.extend(invoice["items"].values())
        sku_units.update(invoice["items"])
        if invoice["customer"]:
            customers[invoice["customer"]].append(invoice["date"])
        else:
            missing_customer += 1
        group = "1" if size == 1 else "2–4" if size <= 4 else "5–10" if size <= 10 else "11–20" if size <= 20 else "21+"
        bucket = relation[group]
        bucket[0] += 1
        bucket[1] += size
        bucket[2] += units
    gaps, customer_orders, exposure_days = [], [], []
    for dates in customers.values():
        dates.sort()
        customer_orders.append(len(dates))
        exposure_days.append((end - dates[0]).total_seconds() / 86400)
        gaps.extend((b - a).total_seconds() / 86400 for a, b in zip(dates, dates[1:]))
    ranked_units = sorted(sku_units.values(), reverse=True)
    total_units = sum(ranked_units)
    customer_count = len(customers)
    return {
        "invoices": len(baskets),
        "window_days": (end - start).total_seconds() / 86400,
        "eligible_rows": sum(item["rows"] for item in invoices),
        "invoices_without_customer": missing_customer,
        "basket_distinct_skus": describe(baskets),
        "quantity_per_invoice_sku": describe(quantities),
        "repeat_purchase": {
            "identified_customers": customer_count,
            "orders_per_identified_customer": describe(customer_orders),
            "customers_with_two_or_more_orders": sum(n >= 2 for n in customer_orders),
            "repeat_customer_rate": sum(n >= 2 for n in customer_orders) / customer_count if customer_count else None,
            "observed_interpurchase_gap_days": describe(gaps, histogram=False),
            "days_from_first_observed_order_to_window_end": describe(exposure_days, histogram=False),
        },
        "product_concentration": {
            "distinct_skus": len(ranked_units),
            "total_units": total_units,
            "sku_units_ranked": ranked_units,
            "top_sku_fractions": {
                str(fraction): {
                    "sku_count": math.ceil(len(ranked_units) * fraction),
                    "unit_share": sum(ranked_units[: math.ceil(len(ranked_units) * fraction)]) / total_units
                    if total_units
                    else None,
                }
                for fraction in (0.01, 0.05, 0.1, 0.2)
            },
        },
        "quantity_conditioned_on_basket": {
            group: {"invoices": count, "invoice_sku_pairs": skus, "units_per_invoice_sku": units / skus}
            for group, (count, skus, units) in sorted(relation.items())
        },
    }
