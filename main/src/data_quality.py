import argparse
import os
import sys
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import load_config
from utils.spark_session import get_spark_session


def standardize_string(col_name: str, uppercase: bool = True):
    expr = F.trim(F.col(col_name))
    expr = F.when((expr == "") | (expr == "null") | (expr == "NULL"), F.lit(None)).otherwise(expr)
    return F.upper(expr) if uppercase else expr


def parse_timestamp_safe(col_name: str):
    """Parses timestamp string safely, returning null if malformed."""
    return F.to_timestamp(F.trim(F.col(col_name)), "yyyy-MM-dd HH:mm:ss")


def validate_orders(df: DataFrame, rules: dict) -> Tuple[DataFrame, DataFrame]:
    """Cleans, casts, validates rules, quarantines invalid rows, and deduplicates valid order rows."""
    valid_order_statuses = rules.get("valid_order_statuses", [
        "PLACED", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"
    ])
    valid_payment_statuses = rules.get("valid_payment_statuses", [
        "PENDING", "PAID", "FAILED", "REFUNDED"
    ])

    staged = (
        df.withColumn("order_id_clean", standardize_string("order_id", uppercase=True))
        .withColumn("customer_id_clean", standardize_string("customer_id", uppercase=True))
        .withColumn("product_id_clean", standardize_string("product_id", uppercase=True))
        .withColumn("quantity_clean", F.col("quantity").cast("int"))
        .withColumn("unit_price_clean", F.col("unit_price").cast("double"))
        .withColumn("order_status_clean", standardize_string("order_status", uppercase=True))
        .withColumn("payment_status_clean", standardize_string("payment_status", uppercase=True))
        .withColumn("order_date_clean", parse_timestamp_safe("order_date"))
        .withColumn("updated_at_clean", parse_timestamp_safe("updated_at"))
        .withColumn("event_sequence_clean", F.col("event_sequence").cast("int"))
    )

    dq_checks = [
        F.when(F.col("order_id_clean").isNull(), F.lit("order_id IS NULL")),
        F.when(F.col("product_id_clean").isNull(), F.lit("product_id IS NULL")),
        F.when(
            F.col("quantity_clean").isNull() | (F.col("quantity_clean") <= 0),
            F.lit("quantity <= 0 or non-numeric"),
        ),
        F.when(
            F.col("unit_price_clean").isNull() | (F.col("unit_price_clean") < 0),
            F.lit("unit_price < 0 or non-numeric"),
        ),
        F.when(
            F.col("order_status_clean").isNull()
            | (~F.col("order_status_clean").isin(valid_order_statuses)),
            F.lit("Invalid order_status"),
        ),
        F.when(
            F.col("payment_status_clean").isNull()
            | (~F.col("payment_status_clean").isin(valid_payment_statuses)),
            F.lit("Invalid payment_status"),
        ),
        F.when(F.col("order_date_clean").isNull(), F.lit("Invalid order_date timestamp")),
        F.when(F.col("updated_at_clean").isNull(), F.lit("Invalid updated_at timestamp")),
        F.when(
            F.col("event_sequence_clean").isNull() | (F.col("event_sequence_clean") < 0),
            F.lit("Invalid event_sequence"),
        ),
    ]

    evaluated = staged.withColumn("dq_failure_reason", F.concat_ws(" | ", *dq_checks))

    quarantine_df = (
        evaluated.filter(F.col("dq_failure_reason") != "")
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_status",
            "payment_status",
            "order_date",
            "updated_at",
            "event_sequence",
            "_ingestion_timestamp",
            "_batch_id",
            "dq_failure_reason",
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    valid_df = (
        evaluated.filter(F.col("dq_failure_reason") == "")
        .select(
            F.col("order_id_clean").alias("order_id"),
            F.col("customer_id_clean").alias("customer_id"),
            F.col("product_id_clean").alias("product_id"),
            F.col("quantity_clean").alias("quantity"),
            F.col("unit_price_clean").alias("unit_price"),
            F.col("order_status_clean").alias("order_status"),
            F.col("payment_status_clean").alias("payment_status"),
            F.col("order_date_clean").alias("order_date"),
            F.col("updated_at_clean").alias("updated_at"),
            F.col("event_sequence_clean").alias("event_sequence"),
        )
        # Event sequence is primary business ordering field; timestamp is secondary tie-breaker.
        .withColumn(
            "ordering_key",
            F.col("event_sequence").cast("long") * F.lit(10000000000)
            + F.col("updated_at").