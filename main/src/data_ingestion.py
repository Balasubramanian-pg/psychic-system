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
        StructField("movement_