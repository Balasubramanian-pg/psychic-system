import argparse
import os
import shutil
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import load_config, to_uri_path
from utils.spark_session import get_spark_session
from data_quality import validate_orders, validate_inventory, validate_movements


def build_hudi_options(
    table_name: str, record_key: str, precombine_field: str, operation: str = "upsert"
) -> dict:
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.metadata.enable": "false",
    }


def write_hudi_table(
    df: DataFrame, local_path: str, hudi_opts: dict, mode: str
) -> None:
    uri = to_uri_path(local_path)
    df.write.format("hudi").options(**hudi_opts).mode(mode).save(uri)


def read_hudi_table(spark, local_path: str) -> DataFrame:
    uri = to_uri_path(local_path)
    return spark.read.format("hudi").load(uri)


def run_lakehouse_pipeline(config_path: str, reset_silver: bool = True) -> None:
    config = load_config(config_path)
    spark = get_spark_session("Ecommerce_Hudi_Lakehouse")

    bronze = config["paths"]["bronze"]
    silver = config["paths"]["silver"]
    hudi_cfg = config["hudi"]
    rules = config["validation_rules"]

    if reset_silver:
        print("\n>>> Resetting Silver Lakehouse storage directories for clean commit history...")
        for name, path in silver.items():
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=True)

    print(f"\n{'=' * 75}")
    print(">>> STEP 1: Ingest Initial Orders -> Hudi Silver Table (Commit 1)")
    print(f"{'=' * 75}")
    orders_init_raw = spark.read.parquet(bronze["orders_initial"])
    orders_init_valid, _ = validate_orders(orders_init_raw, rules)
    orders_init_opts = build_hudi_options(
        hudi_cfg["orders"]["table_name"],
        hudi_cfg["orders"]["record_key"],
        hudi_cfg["orders"]["precombine_field"],
        operation="upsert",
    )
    write_hudi_table(orders_init_valid, silver["orders"], orders_init_opts, mode="overwrite")
    commit1_orders = read_hudi_table(spark, silver["orders"])
    print(f"[HUDI COMMIT 1 - ORDERS INITIAL] Written {commit1_orders.count()} records.")

    print(f"\n{'=' * 75}")
    print(">>> STEP 2: Process Incremental Orders Updates & Late-Arriving Events (Commit 2)")
    print(f"{'=' * 75}")
    orders_upd_raw = spark.read.parquet(bronze["orders_updates"])
    orders_upd_valid, _ = validate_orders(orders_upd_raw, rules)
    orders_upd_opts = build_hudi_options(
        hudi_cfg["orders"]["table_name"],
        hudi_cfg["orders"]["record_key"],
        hudi_cfg["orders"]["precombine_field"],
        operation="upsert",
    )
    # Append mode triggers Hudi UPSERT with DefaultHoodieRecordPayload and ordering_key precombine check
    write_hudi_table(orders_upd_valid, silver["orders"], orders_upd_opts, mode="append")
    commit2_orders = read_hudi_table(spark, silver["orders"])
    print(f"[HUDI COMMIT 2 - ORDERS UPSERT] Incremental updates applied. Total Silver orders: {commit2_orders.count()}")

    print(f"\n{'=' * 75}")
    print(">>> STEP 3: Ingest Initial Inventory -> Hudi Silver Table (Commit 1)")
    print(f"{'=' * 75}")
    inv_init_raw = spark.read.parquet(bronze["inventory_initial"])
    inv_init_valid, _ = validate_inventory(inv_init_raw, rules)
    inv_init_opts = build_hudi_options(
        hudi_cfg["inventory"]["table_name"],
        hudi_cfg["inventory"]["record_key"],
        hudi_cfg["inventory"]["precombine_field"],
        operation="upsert",
    )
    write_hudi_table(inv_init_valid, silver["inventory"], inv_init_opts, mode="overwrite")
    commit1_inv = read_hudi_table(spark, silver["inventory"])
    print(f"[HUDI COMMIT 1 - INVENTORY INITIAL] Written {commit1_inv.count()} products.")

    print(f"\n{'=' * 75}")
    print(">>> STEP 4: Ingest Inventory Movements & Reconcile Stock Levels (Commit 2)")
    print(f"{'=' * 75}")
    mov_raw = spark.read.parquet(bronze["inventory_movements"])
    mov_valid, _ = validate_movements(mov_raw, rules)
    mov_opts = build_hudi_options(
        hudi_cfg["movements"]["table_name"],
        hudi_cfg["movements"]["record_key"],
        hudi_cfg["movements"]["precombine_field"],
        operation="upsert",
    )
    write_hudi_table(mov_valid, silver["movements"], mov_opts, mode="overwrite")
    committed_movements = read_hudi_table(spark, silver["movements"])
    print(f"[HUDI COMMIT 1 - MOVEMENTS] Ingested {committed_movements.count()} movement events.")

    # Calculate net stock adjustments by product:
    # PURCHASE: -qty, DAMAGE: -qty, RESTOCK: +qty, RETURN: +qty
    stock_deltas = (
        committed_movements.withColumn(
            "signed_qty",
            F.when(F.col("movement_type").isin("RESTOCK", "RETURN"), F.col("quantity"))
            .when(F.col("movement_type").isin("PURCHASE", "DAMAGE"), -F.col("quantity"))
            .otherwise(F.lit(0)),
        )
        .groupBy("product_id")
        .agg(
            F.sum("signed_qty").alias("net_qty_change"),
            F.max("updated_at").alias("latest_mov_updated_at"),
            F.max("event_sequence").alias("latest_mov_seq"),
        )
    )

    current_inventory = read_hudi_table(spark, silver["inventory"]).select(
        "product_id",
        "product_name",
        "category",
        "available_quantity",
        "warehouse_id",
        "updated_at",
        "event_sequence",
    )

    reconciled_inventory = (
        current_inventory.join(stock_deltas, on="product_id", how="left")
        .withColumn(
            "net_qty_change",
            F.coalesce(F.col("net_qty_change"), F.lit(0)),
        )
        .withColumn(
            "available_quantity",
            F.greatest(F.lit(0), F.col("available_quantity") + F.col("net_qty_change")),
        )
        .withColumn(
            "updated_at",
            F.coalesce(F.col("latest_mov_updated_at"), F.col("updated_at")),
        )
        .withColumn(
            "event_sequence",
            F.greatest(F.col("event_sequence"), F.coalesce(F.col("latest_mov_seq"), F.lit(0))) + F.lit(1),
        )
        .withColumn(
            "ordering_key",
            F.col("event_sequence").cast("long") * F.lit(10000000000)
            + F.col("updated_at").cast("long"),
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "available_quantity",
            "warehouse_id",
            "updated_at",
            "event_sequence",
            "ordering_key",
        )
    )

    # Commit reconciled inventory update as Hudi Commit 2
    write_hudi_table(reconciled_inventory, silver["inventory"], inv_init_opts, mode="append")
    commit2_inv = read_hudi_table(spark, silver["inventory"])
    print(f"[HUDI COMMIT 2 - INVENTORY RECONCILED] Total Silver inventory after movements: {commit2_inv.count()}")

    print("\n>>> Lakehouse Ingestion & UPSERT Implementation Complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apache Hudi Lakehouse Implementation")
    parser.add_argument(
        "--config", default="configs/ecommerce_prod.yml", help="Path to YAML config"
    )
    parser.add_argument(
        "--no-reset", action="store_true", help="Do not clear existing Silver tables"
    )
    args = parser.parse_args()
    run_lakehouse_pipeline(args.config, reset_silver=(not args.no_reset))
