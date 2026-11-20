import os
import sys
from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """Initializes a SparkSession configured for Apache Hudi local Lakehouse operations."""
    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable
    if "PYSPARK_DRIVER_PYTHON" not in os.environ:
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(
            "spark.sql.extensions",
            "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        )
        .config(
            "spark