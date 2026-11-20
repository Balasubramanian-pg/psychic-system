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
            + F.col("updated_at").cast("long"),
        )
    )

    # Intra-batch deduplication: keep highest ordering_key per order_id
    w = Window.partitionBy("order_id").orderBy(F.col("ordering_key").desc())
    valid_dedup_df = (
        valid_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return valid_dedup_df, quarantine_df


def validate_inventory(df: DataFrame, rules: dict) -> Tuple[DataFrame, DataFrame]:
    """Cleans, casts, validates rules, quarantines invalid rows, and deduplicates valid inventory rows."""
    valid_categories = rules.get("valid_categories", [
        "APPAREL", "ELECTRONICS", "SPORTS", "HOME", "BOOKS"
    ])

    staged = (
        df.withColumn("product_id_clean", standardize_string("product_id", uppercase=True))
        .withColumn("product_name_clean", standardize_string("product_name", uppercase=False))
        .withColumn("category_clean", standardize_string("category", uppercase=True))
        .withColumn("available_quantity_clean", F.col("available_quantity").cast("int"))
        .withColumn("warehouse_id_clean", standardize_string("warehouse_id", uppercase=True))
        .withColumn("updated_at_clean", parse_timestamp_safe("updated_at"))
        .withColumn("event_sequence_clean", F.col("event_sequence").cast("int"))
    )

    dq_checks = [
        F.when(F.col("product_id_clean").isNull(), F.lit("product_id IS NULL")),
        F.when(
            F.col("available_quantity_clean").isNull()
            | (F.col("available_quantity_clean") < 0),
            F.lit("available_quantity < 0 or non-numeric"),
        ),
        F.when(
            F.col("category_clean").isNull()
            | (~F.col("category_clean").isin(valid_categories)),
            F.lit("Invalid category"),
        ),
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
            "product_id",
            "product_name",
            "category",
            "available_quantity",
            "warehouse_id",
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
            F.col("product_id_clean").alias("product_id"),
            F.col("product_name_clean").alias("product_name"),
            F.col("category_clean").alias("category"),
            F.col("available_quantity_clean").alias("available_quantity"),
            F.col("warehouse_id_clean").alias("warehouse_id"),
            F.col("updated_at_clean").alias("updated_at"),
            F.col("event_sequence_clean").alias("event_sequence"),
        )
        .withColumn(
            "ordering_key",
            F.col("event_sequence").cast("long") * F.lit(10000000000)
            + F.col("updated_at").cast("long"),
        )
    )

    w = Window.partitionBy("product_id").orderBy(F.col("ordering_key").desc())
    valid_dedup_df = (
        valid_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return valid_dedup_df, quarantine_df


def validate_movements(df: DataFrame, rules: dict) -> Tuple[DataFrame, DataFrame]:
    """Cleans, casts, validates rules, quarantines invalid rows, and deduplicates valid inventory movements."""
    valid_movement_types = rules.get("valid_movement_types", [
        "PURCHASE", "RETURN", "RESTOCK", "DAMAGE"
    ])

    staged = (
        df.withColumn("movement_id_clean", standardize_string("movement_id", uppercase=True))
        .withColumn("product_id_clean", standardize_string("product_id", uppercase=True))
        .withColumn("movement_type_clean", standardize_string("movement_type", uppercase=True))
        .withColumn("quantity_clean", F.col("quantity").cast("int"))
        .withColumn("movement_date_clean", parse_timestamp_safe("movement_date"))
        .withColumn("updated_at_clean", parse_timestamp_safe("updated_at"))
        .withColumn("event_sequence_clean", F.col("event_sequence").cast("int"))
    )

    dq_checks = [
        F.when(F.col("movement_id_clean").isNull(), F.lit("movement_id IS NULL")),
        F.when(F.col("product_id_clean").isNull(), F.lit("product_id IS NULL")),
        F.when(
            F.col("quantity_clean").isNull() | (F.col("quantity_clean") <= 0),
            F.lit("quantity <= 0 or non-numeric"),
        ),
        F.when(
            F.col("movement_type_clean").isNull()
            | (~F.col("movement_type_clean").isin(valid_movement_types)),
            F.lit("Invalid movement_type"),
        ),
        F.when(F.col("movement_date_clean").isNull(), F.lit("Invalid movement_date timestamp")),
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
            "movement_id",
            "product_id",
            "movement_type",
            "quantity",
            "movement_date",
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
            F.col("movement_id_clean").alias("movement_id"),
            F.col("product_id_clean").alias("product_id"),
            F.col("movement_type_clean").alias("movement_type"),
            F.col("quantity_clean").alias("quantity"),
            F.col("movement_date_clean").alias("movement_date"),
            F.col("updated_at_clean").alias("updated_at"),
            F.col("event_sequence_clean").alias("event_sequence"),
        )
        .withColumn(
            "ordering_key",
            F.col("event_sequence").cast("long") * F.lit(10000000000)
            + F.col("updated_at").cast("long"),
        )
    )

    w = Window.partitionBy("movement_id").orderBy(F.col("ordering_key").desc())
    valid_dedup_df = (
        valid_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return valid_dedup_df, quarantine_df


def run_quality_checks(config_path: str) -> None:
    config = load_config(config_path)
    spark = get_spark_session("Ecommerce_Data_Quality")

    bronze = config["paths"]["bronze"]
    quarantine = config["paths"]["quarantine"]
    rules = config["validation_rules"]

    print(f"\n{'=' * 75}")
    print(">>> Starting Data Quality & Quarantine Validation Pipeline...")
    print(f"{'=' * 75}")

    # 1. Orders Initial
    v_ord_init, q_ord_init = validate_orders(spark.read.parquet(bronze["orders_initial"]), rules)
    q_ord_init.write.mode("overwrite").parquet(quarantine["orders_initial"])
    print(f"[ORDERS INITIAL] Valid Deduped: {v_ord_init.count()} | Quarantined: {q_ord_init.count()}")
    if q_ord_init.count() > 0:
        print("[SAMPLE QUARANTINE - ORDERS INITIAL]:")
        q_ord_init.select("order_id", "quantity", "unit_price", "order_status", "dq_failure_reason").show(5, truncate=False)

    # 2. Orders Updates
    v_ord_upd, q_ord_upd = validate_orders(spark.read.parquet(bronze["orders_updates"]), rules)
    q_ord_upd.write.mode("overwrite").parquet(quarantine["orders_updates"])
    print(f"[ORDERS UPDATES] Valid Deduped: {v_ord_upd.count()} | Quarantined: {q_ord_upd.count()}")
    if q_ord_upd.count() > 0:
        print("[SAMPLE QUARANTINE - ORDERS UPDATES]:")
        q_ord_upd.select("order_id", "quantity", "unit_price", "order_status", "dq_failure_reason").show(5, truncate=False)

    # 3. Inventory Initial
    v_inv_init, q_inv_init = validate_inventory(spark.read.parquet(bronze["inventory_initial"]), rules)
    q_inv_init.write.mode("overwrite").parquet(quarantine["inventory_initial"])
    print(f"[INVENTORY INITIAL] Valid Deduped: {v_inv_init.count()} | Quarantined: {q_inv_init.count()}")
    if q_inv_init.count() > 0:
        print("[SAMPLE QUARANTINE - INVENTORY INITIAL]:")
        q_inv_init.select("product_id", "category", "available_quantity", "dq_failure_reason").show(5, truncate=False)

    # 4. Inventory Movements
    v_mov, q_mov = validate_movements(spark.read.parquet(bronze["inventory_movements"]), rules)
    q_mov.write.mode("overwrite").parquet(quarantine["inventory_movements"])
    print(f"[INVENTORY MOVEMENTS] Valid Deduped: {v_mov.count()} | Quarantined: {q_mov.count()}")
    if q_mov.count() > 0:
        print("[SAMPLE QUARANTINE - INVENTORY MOVEMENTS]:")
        q_mov.select("movement_id", "product_id", "movement_type", "quantity", "dq_failure_reason").show(5, truncate=False)

    print("\n>>> Data Quality & Quarantine Pipeline Complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Quality & Quarantine Pipeline")
    parser.add_argument(
        "--config", default="configs/ecommerce_prod.yml", help="Path to YAML config"
    )
    args = parser.parse_args()
    run_quality_checks(args.config)
