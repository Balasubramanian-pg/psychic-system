import argparse
import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_loader import load_config, to_uri_path
from utils.spark_session import get_spark_session


def write_gold_csv(df: DataFrame, local_output_path: str, report_title: str) -> None:
    """Prints report summary and writes consolidated CSV in overwrite mode."""
    print(f"\n{'=' * 80}")
    print(f"[GOLD ANALYTICAL REPORT] {report_title}")
    print(f"{'=' * 80}")
    df.show(30, truncate=False)
    # coalesce(1) ensures single clean CSV part file for downstream reporting
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_output_path)
    print(f"[SAVED GOLD CSV] -> {local_output_path}")


def run_analytics(config_path: str) -> None:
    config = load_config(config_path)
    spark = get_spark_session("Ecommerce_Gold_Analytics")

    silver = config["paths"]["silver"]
    gold = config["paths"]["gold"]
    low_stock_threshold = config["validation_rules"].get("low_stock_threshold", 20)

    # Read Silver tables via Hudi
    orders_uri = to_uri_path(silver["orders"])
    inv_uri = to_uri_path(silver["inventory"])
    mov_uri = to_uri_path(silver["movements"])

    orders_df = spark.read.format("hudi").load(orders_uri)
    inventory_df = spark.read.format("hudi").load(inv_uri)
    movements_df = spark.read.format("hudi").load(mov_uri)

    print("\n>>> Generating Gold Consumption Layer Analytical Queries...")

    # -------------------------------------------------------------------------
    # 1. Total orders grouped by order status
    # -------------------------------------------------------------------------
    orders_by_status = (
        orders_df.groupBy("order_status")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_ordered"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("gross_order_value"),
        )
        .orderBy(F.col("total_orders").desc())
    )
    write_gold_csv(
        orders_by_status,
        gold["orders_by_status"],
        "1. Total Orders Grouped by Order Status",
    )

    # -------------------------------------------------------------------------
    # 2. Total revenue from successfully paid orders (excluding CANCELLED)
    # -------------------------------------------------------------------------
    revenue_paid = (
        orders_df.filter(
            (F.col("payment_status") == "PAID") & (F.col("order_status") != "CANCELLED")
        )
        .agg(
            F.count("order_id").alias("paid_orders_count"),
            F.sum("quantity").alias("paid_units_sold"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_paid_revenue"),
            F.round(F.avg(F.col("quantity") * F.col("unit_price")), 2).alias("average_order_value"),
        )
    )
    write_gold_csv(
        revenue_paid,
        gold["revenue_paid_orders"],
        "2. Total Revenue from Successfully Paid Orders (Excl. Cancelled)",
    )

    # -------------------------------------------------------------------------
    # 3. Products with the highest order quantities
    # -------------------------------------------------------------------------
    top_products = (
        orders_df.filter(F.col("order_status") != "CANCELLED")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("total_ordered_quantity"),
            F.count("order_id").alias("order_count"),
            F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("total_sales_amount"),
        )
        .join(
            inventory_df.select("product_id", "product_name", "category"),
            on="product_id",
            how="left",
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "total_ordered_quantity",
            "order_count",
            "total_sales_amount",
        )
        .orderBy(F.col("total_ordered_quantity").desc())
    )
    write_gold_csv(
        top_products,
        gold["top_ordered_products"],
        "3. Products with Highest Order Quantities",
    )

    # -------------------------------------------------------------------------
    # 4. Products with low available inventory
    # -------------------------------------------------------------------------
    low_stock = (
        inventory_df.filter(F.col("available_quantity") <= low_stock_threshold)
        .select(
            "product_id",
            "product_name",
            "category",
            "warehouse_id",
            "available_quantity",
            "updated_at",
        )
        .orderBy(F.col("available_quantity").asc())
    )
    write_gold_csv(
        low_stock,
        gold["low_stock_products"],
        f"4. Low Stock Products (Available Quantity <= {low_stock_threshold} units)",
    )

    # -------------------------------------------------------------------------
    # 5. Orders where payment is completed but shipping has not started
    # -------------------------------------------------------------------------
    paid_unshipped = (
        orders_df.filter(
            (F.col("payment_status") == "PAID")
            & (F.col("order_status").isin("PLACED", "CONFIRMED"))
        )
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
        )
        .orderBy("order_date")
    )
    write_gold_csv(
        paid_unshipped,
        gold["paid_unshipped_orders"],
        "5. Orders Paid but Shipping Not Started (Status: PLACED / CONFIRMED)",
    )

    # -------------------------------------------------------------------------
    # 6. Inventory movement summary grouped by movement type
    # -------------------------------------------------------------------------
    movement_summary = (
        movements_df.groupBy("movement_type")
        .agg(
            F.count("movement_id").alias("total_movement_events"),
            F.countDistinct("product_id").alias("distinct_products_affected"),
            F.sum("quantity").alias("total_units_moved"),
        )
        .orderBy(F.col("total_units_moved").desc())
    )
    write_gold_csv(
        movement_summary,
        gold["inventory_movement_summary"],
        "6. Inventory Movement Summary Grouped by Movement Type",
    )

    print("\n>>> Gold Layer Analytics Generation Complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer Consumption & Analytics")
    parser.add_argument(
        "--config", default="configs/ecommerce_prod.yml", help="Path to YAML config"
    )
    args = parser.parse_args()
    run_analytics(args.config)
