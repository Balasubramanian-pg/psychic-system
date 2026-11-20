#!/usr/bin/env bash
set -euo pipefail

HUDI_PKG="org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"

echo "================================================================="
echo ">>> Step 1: Running Bronze Data Ingestion..."
echo "================================================================="
spark-submit \
  --packages "${HUDI_PKG}" \
  src/data_ingestion.py --config configs/ecommerce_prod.yml

echo ""
echo "================================================================="
echo ">>> Step 2: Running Data Quality & Quarantine Validation..."
echo "================================================================="
spark-submit \
  --packages "${HUDI_PKG}" \
  src/data_quality.py --config configs/ecommerce_prod.yml
