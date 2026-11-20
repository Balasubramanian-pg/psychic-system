import argparse
import os
import sys
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import load_config, to_uri_path
from utils.spark_session import get_spark_session


def get_hudi_commits(table_path: str) -> list:
    """Discovers committed instants from the .hoodie timeline directory."""
    hoodie_meta_dir = os.path.join(table_path, ".hoodie")
    if not os.path.exists(hoodie_meta_dir):
        return []

    commit_files = [
        f for f in os.listdir(hoodie_meta_dir)
        if f.endswith(".commit") and not any(f.endswith(s) for s in [".requested", ".inflight", ".aux"])
    ]
    # Extract timestamp prefix (e.g. 20261114144421945.commit -> 20261114144421945)
    commits = sorted([f.replace(".commit", "") for f in commit_files])
    return commits


def run_time_travel(config_path: str) -> None:
    config = load_config(config_path)
    spark = get_spark_session("Ecommerce_Hudi_Time_Travel")

    orders_path = config["paths"]["silver"]["orders"]
    orders_uri = to_uri_path(orders_path)

    commits = get_hudi_commits(orders_path)
    if not commits:
        # Fallback: check _hoodie_commit_time from the current table
        current_df = spark.read.format("hudi").load(orders_uri)
        commits = [
            row["_hoodie_commit_time"]
            for row in current_df.select("_hoodie_commit_time")
            .distinct()
            .orderBy("_hoodie_commit_time")
            .collect()
        ]

    print(f"\n{'=' * 80}")
    print(f"[HUDI TIME TRAVEL TIMELINE] Discovered Completed Commits: {commits}")
    print(f"{'=' * 80}")

    if len(commits) < 2:
        print("[ERROR] At least 2 commits are required for Time Travel demonstration.")
        print("Plea