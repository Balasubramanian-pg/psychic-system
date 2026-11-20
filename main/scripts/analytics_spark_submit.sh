#!/usr/bin/env bash
set -euo pipefail

HUDI_PKG="org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"

echo "================================================================="
echo ">>> Step 5: Generating Gold Layer Analytical CSV Reports..."
echo "================================================================="
spark-submit \
  --packages "${HUDI_PKG}" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.ca