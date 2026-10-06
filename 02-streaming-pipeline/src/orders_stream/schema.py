"""Kontrak event pesanan dan nilai-nilai referensinya."""

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Bobot kira-kira mengikuti ukuran pasar, supaya data terlihat realistis di dashboard.
CITIES = {
    "Jakarta": 30,
    "Surabaya": 14,
    "Bandung": 12,
    "Medan": 9,
    "Semarang": 8,
    "Makassar": 7,
    "Yogyakarta": 6,
    "Denpasar": 5,
}
PAYMENT_METHODS = {"qris": 35, "ewallet": 25, "virtual_account": 20, "credit_card": 10, "cod": 10}
CATEGORIES = ["elektronik", "fashion", "makanan", "kecantikan", "rumah_tangga"]

ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_time", TimestampType()),
        StructField("city", StringType()),
        StructField("payment_method", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("amount_idr", LongType()),
    ]
)
