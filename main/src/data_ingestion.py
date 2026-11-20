import argparse
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Ensure src/ is on Python search path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import load_config
from utils.spark_session import get_spark_session


ORDERS_RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("event_sequence", StringType(), True),
    ]
)

INVENTORY_RAW_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("available_quantity", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("event_sequence", StringType(), True),
    ]
)

MOVEMENTS_RAW_SCHEMA = StructType(
    [
        StructField("movement_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("movement_type", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("movement_date", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("event_sequence", StringType(), True),
    ]
)


def profile_dataframe(df: DataFrame, dataset_name: str) -> None:
    """Computes and prints initial schema, record count, and null value profiling."""
    total_rows = df.count()
    print(f"\n{'=' * 75}")
    print(f"[DATA PROFILING] Dataset: {dataset_name} | Total Records: {total_rows}")
    print(f"{'=' * 75}")

    data_cols = [c for c in df.columns if not c.startswith("_")]
    null_exprs = [
        F.sum(
            F.when(F.col(c).isNull() | (F.trim(F.col(c)) == "") | (F.col(c) == "null"), 1).otherwise(0)
        ).alias(c)
        for c in data_cols
    ]
    df.select(null_exprs).show(truncate=False)
    print(f"[SAMPLE RECORDS] {dataset_name}:")
    df.select(data_cols).show(5, truncate=False)


def ingest_to_bronze(
    spark, input_path: str, output_path: str, schema: StructType, batch_label: str
) -> DataFrame:
    """Reads raw CSV with permissive capture schema, appends audit metadata, profiles, and writes Bronze Parquet."""
    print(f"\n[INGESTING] Reading raw CSV: {input_path}")
    raw_df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(input_path)
    )

    bronze_df = (
        raw_df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(input_path))
        .withColumn("_batch_id", F.l