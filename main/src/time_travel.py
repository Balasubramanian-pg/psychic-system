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
        print("Please run lakehouse_implementation.py first to create Commit 1 and Commit 2.")
        spark.stop()
        return

    commit1_instant = commits[0]
    commit2_instant = commits[-1]

    print(f"\n>>> Querying Historical Snapshot @ Commit 1 (Instant: {commit1_instant})...")
    historical_df = (
        spark.read.format("hudi")
        .option("as.of.instant", commit1_instant)
        .load(orders_uri)
    )
    hist_count = historical_df.count()

    print(f">>> Querying Current Table State @ Commit 2 (Instant: {commit2_instant})...")
    current_df = (
        spark.read.format("hudi")
        .load(orders_uri)
    )
    curr_count = current_df.count()

    print(f"\n[TABLE ROW COUNTS]")
    print(f" - Historical Snapshot (Commit 1 @ {commit1_instant}): {hist_count} records")
    print(f" - Current Table State (Commit 2 @ {commit2_instant}): {curr_count} records")

    # 1. Orders whose order_status or payment_status changed
    diff_df = (
        historical_df.alias("hist")
        .join(current_df.alias("curr"), on="order_id", how="inner")
        .filter(
            (F.col("hist.order_status") != F.col("curr.order_status"))
            | (F.col("hist.payment_status") != F.col("curr.payment_status"))
        )
        .select(
            F.col("order_id"),
            F.col("hist.order_status").alias("historical_order_status"),
            F.col("curr.order_status").alias("current_order_status"),
            F.col("hist.payment_status").alias("historical_payment_status"),
            F.col("curr.payment_status").alias("current_payment_status"),
            F.col("hist.updated_at").alias("historical_updated_at"),
            F.col("curr.updated_at").alias("current_updated_at"),
            F.col("hist.event_sequence").alias("hist_seq"),
            F.col("curr.event_sequence").alias("curr_seq"),
        )
        .orderBy("order_id")
    )

    diff_count = diff_df.count()
    print(f"\n{'=' * 80}")
    print(f"[STATE CHANGES DETECTED] Total Orders Modified Between Commit 1 and 2: {diff_count}")
    print(f"{'=' * 80}")
    diff_df.show(20, truncate=False)

    # 2. Showcase newly inserted orders that were introduced in Commit 2
    new_orders_df = current_df.join(historical_df, on="order_id", how="left_anti")
    print(f"\n[NEWLY INSERTED ORDERS IN COMMIT 2] Total: {new_orders_df.count()}")
    new_orders_df.select(
        "order_id", "customer_id", "product_id", "order_status", "payment_status", "order_date", "updated_at"
    ).show(10, truncate=False)

    # 3. Specific demonstration of Order Status and Payment Status transitions
    print(f"\n[FOCUSED TRANSITION SAMPLES] Demonstrating specific status updates:")
    sample_orders = diff_df.limit(5).collect()
    for row in sample_orders:
        print(f" * Order {row['order_id']}:")
        print(f"    - Order Status:   {row['historical_order_status']} -> {row['current_order_status']}")
        print(f"    - Payment Status: {row['historical_payment_status']} -> {row['current_payment_status']}")
        print(f"    - Sequence:       {row['hist_seq']} -> {row['curr_seq']}")
        print(f"    - Updated At:     {row['historical_updated_at']} -> {row['current_updated_at']}")

    print("\n>>> Time Travel & Historical Analysis Complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apache Hudi Time Travel Analysis")
    parser.add_argument(
        "--config", default="configs/ecommerce_prod.yml", help="Path to YAML config"
    )
    args = parser.parse_args()
    run_time_travel(args.config)
