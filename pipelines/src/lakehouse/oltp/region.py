"""Address grouping shared by OLTP transformations."""

from pyspark.sql import functions as F

REGIONS = ("Bình Tân", "Quận 1", "Quận 7", "Thủ Đức", "Gò Vấp", "Tân Bình", "Cầu Giấy", "Đống Đa", "Hoàn Kiếm")


def region_column(address):
    result = F.lit("Khu vực khác")
    for region in reversed(REGIONS):
        result = F.when(F.lower(address).contains(region.lower()), F.lit(region)).otherwise(result)
    return result
