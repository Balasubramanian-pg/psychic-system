# Assignment #1: E-Commerce Order and Inventory Lakehouse Management

**Module**: Data Stores & Pipelines (M.Sc. Data Science & AI)  
**Deliverable**: Production-grade Medallion Lakehouse Implementation using Apache Spark (PySpark) & Apache Hudi  
**Storage**: Local Filesystem (POSIX / Windows compatible)

---

## 1. Executive Summary & Architecture

This repository contains the end-to-end implementation of an ACID-compliant, transactional Lakehouse pipeline engineered for an e-commerce platform. The system ingests, cleanses, validates, upserts, and analyzes high-velocity, out-of-order order events and inventory movements across Bronze, Quarantine, Silver, and Gold medallion layers using **Apache Spark 3.5.3** and **Apache Hudi 0.15.0**.

### Architecture Diagram
```
+---------------------------------------------------------------------------------------+
|                                    RAW SOURCE CSVs                                    |
|   orders_initial.csv  |  orders_updates.csv  |  inventory_initial.csv  |  movements   |
+---------------------------------------------------------------------------------------+
                                           |
                                           v
+---------------------------------------------------------------------------------------+
|                          BRONZE LAYER (processed/bronze/)                             |
|  - Permissive string capture schema (prevents silent data loss on bad types)          |
|  - Audit lineage metadata (_ingestion_timestamp, _source_file, _batch_id)             |
|  - Persisted in columnar Apache Parquet format                                        |
+---------------------------------------------------------------------------------------+
                                           |
                                           v
+---------------------------------------------------------------------------------------+
|                        DATA QUALITY & QUARANTINE ENGINE                               |
|  - Rule enforcement (Null keys, negative prices/quantities, invalid enums, timestamps)|
+---------------------------------------------------------------------------------------+
                   /                                               \
 (Malformed / Corrupt Records)                        (Clean & Deduplicated Records)
                 v                                                 v
+----------------------------------+        +-------------------------------------------+
| QUARANTINE (processed/quarantine)|        |       SILVER LAYER (processed/silver/)    |
| - Original record fields         |        |   Apache Hudi (Copy-On-Write) Tables:     |
| - Composite dq_failure_reason    |        |   1. orders_hudi (PK: order_id)           |
| - _quarantined_at timestamp      |        |   2. inventory_hudi (PK: product_id)      |
+----------------------------------+        |   3. inventory_movements_hudi             |
                                            |   - DefaultHoodieRecordPayload UPSERT     |
                                            |   - Out-of-order & stale event rejection  |
                                            |   - Physical stock reconciliation         |
                                            |   - Time Travel Timeline Metadata         |
                                            +-------------------------------------------+
                                                                   |
                                                                   v
                                            +-------------------------------------------+
                                            |        GOLD LAYER (processed/gold/)       |
                                            |   Single-part CSVs in overwrite mode:     |
                                            |   1. orders_by_status                     |
                                            |   2. revenue_paid_orders                  |
                                            |   3. top_ordered_products                 |
                                            |   4. low_stock_products                   |
                                            |   5. paid_unshipped_orders                |
                                            |   6. inventory_movement_summary           |
                                            +-------------------------------------------+
```

---

## 2. Repository Structure

```text
ecommerce_lakehouse/
├── configs/
│   └── ecommerce_prod.yml               # Unified YAML configuration for paths, Hudi options & rules
├── raw/
│   ├── orders_initial.csv               # 1,000 initial raw orders
│   ├── orders_updates.csv               # 220 updates, duplicates, stale events & new orders
│   ├── inventory_initial.csv            # 150 product inventory balances
│   └── inventory_movements.csv          # 320 physical stock movements (restock, purchase, etc.)
├── src/
│   ├── __init__.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config_loader.py             # YAML loader & URI path standardizer (Windows/POSIX)
│   │   └── spark_session.py             # SparkSession builder with Hudi 0.15.0 bundle & Kryo
│   ├── data_ingestion.py                # Bronze layer schema capture & profiling
│   ├── data_quality.py                  # Validation checks, quarantine routing & dedup
│   ├── lakehouse_implementation.py      # Apache Hudi table writes, UPSERTs & stock reconciliation
│   ├── time_travel.py                   # Historical timeline querying (as.of.instant) & state diff
│   └── consumption_analytics.py         # Gold layer executive KPIs & CSV generation
├── scripts/
│   ├── ingestion_spark_submit.sh        # Bash: Bronze ingestion & DQ validation
│   ├── lakehouse_spa