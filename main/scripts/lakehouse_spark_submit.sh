#!/usr/bin/env bash
set -euo pipefail

HUDI_PKG="org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"

echo "================================================================="
echo ">>> Step 3: Running Apache Hudi Lakehouse Initial Load & UPSERT..."
echo "================================================================="
spark-submit \
  --packages "${HUDI_PKG}" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  src/lakehouse_implementation.py --config configs/ecommerce_prod.yml

echo ""
echo "================================================================="
echo ">>> Step 4: Running Hudi Time Travel & Historical Diff Analysis..."
echo