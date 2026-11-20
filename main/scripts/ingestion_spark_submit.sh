#!/usr/bin/env bash
set -euo pipefail

HUDI_PKG="org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"

echo "================================================================="
echo ">>> Step 1: Running Bronze Data Ingestion..."
echo "===========