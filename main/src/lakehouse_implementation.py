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
    print(f"[HUDI CO