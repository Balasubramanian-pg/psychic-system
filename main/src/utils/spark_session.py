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
        S