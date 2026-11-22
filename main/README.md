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
│   ├── lakehouse_spark_submit.sh        # Bash: Hudi Lakehouse load, UPSERT & Time Travel
│   ├── analytics_spark_submit.sh        # Bash: Gold consumption analytics
│   ├── run_all.ps1                      # Windows PowerShell: End-to-end one-click execution
│   └── generate_screenshots.py          # Screenshot generation utility
├── processed/
│   ├── bronze/                          # Raw Parquet with lineage audit columns
│   ├── quarantine/                      # Rejected records partitioned by dataset with error reasons
│   ├── silver/                          # Managed transactional Apache Hudi tables (.hoodie metadata)
│   └── gold/                            # Consolidated business CSV reports (overwrite mode)
├── screenshots/                         # Verification artifacts for assignment submission
│   ├── 01_data_ingestion_and_profiling.png
│   ├── 02_data_quality_and_quarantine.png
│   ├── 03_hudi_lakehouse_upsert.png
│   ├── 04_hudi_time_travel_analysis.png
│   └── 05_gold_consumption_analytics.png
└── README.md                            # Comprehensive engineering and architectural documentation
```

---

## 3. Data Quality & Quarantine Framework

All raw datasets contain intentional data quality defects. The pipeline implements a strict quarantine contract: **zero loss of malformed data**, where invalid rows are routed to `processed/quarantine/` with diagnostic `dq_failure_reason` tags, while only pristine records advance to Silver.

### Validation Matrix
| Dataset | Column | Rule Checked | Action on Failure |
| :--- | :--- | :--- | :--- |
| **Orders** | `order_id` | `IS NOT NULL AND != ''` | Quarantine: `"order_id IS NULL"` |
| **Orders** | `product_id` | `IS NOT NULL AND != ''` | Quarantine: `"product_id IS NULL"` |
| **Orders** | `quantity` | Casts to Integer AND `> 0` | Quarantine: `"quantity <= 0 or non-numeric"` |
| **Orders** | `unit_price` | Casts to Double AND `>= 0` | Quarantine: `"unit_price < 0 or non-numeric"` |
| **Orders** | `order_status` | `IN ('PLACED', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED')` | Quarantine: `"Invalid order_status"` |
| **Orders** | `payment_status`| `IN ('PENDING', 'PAID', 'FAILED', 'REFUNDED')` | Quarantine: `"Invalid payment_status"` |
| **Orders** | `order_date` | Valid `yyyy-MM-dd HH:mm:ss` timestamp | Quarantine: `"Invalid order_date timestamp"` |
| **Orders** | `updated_at` | Valid `yyyy-MM-dd HH:mm:ss` timestamp | Quarantine: `"Invalid updated_at timestamp"` |
| **Orders** | `event_sequence`| Integer `>= 0` | Quarantine: `"Invalid event_sequence"` |
| **Inventory** | `product_id` | `IS NOT NULL AND != ''` | Quarantine: `"product_id IS NULL"` |
| **Inventory** | `available_quantity` | Casts to Integer AND `>= 0` | Quarantine: `"available_quantity < 0 or non-numeric"` |
| **Inventory** | `category` | `IN ('APPAREL', 'ELECTRONICS', 'SPORTS', 'HOME', 'BOOKS')` | Quarantine: `"Invalid category"` |
| **Movements** | `movement_id` | `IS NOT NULL AND != ''` | Quarantine: `"movement_id IS NULL"` |
| **Movements** | `product_id` | `IS NOT NULL AND != ''` | Quarantine: `"product_id IS NULL"` |
| **Movements** | `quantity` | Casts to Integer AND `> 0` | Quarantine: `"quantity <= 0 or non-numeric"` |
| **Movements** | `movement_type` | `IN ('PURCHASE', 'RETURN', 'RESTOCK', 'DAMAGE')` | Quarantine: `"Invalid movement_type"` |

### Exact Row Audit Accounting
| Dataset | Raw Ingested | Quarantined (Rejected) | Intra-Batch Deduped | Silver Committed |
| :--- | :---: | :---: | :---: | :---: |
| `orders_initial` | 1,000 | 5 | 0 (after DQ) | **995** (Commit 1) |
| `orders_updates` | 220 | 9 | 31 (stale/duplicates) | **180** (120 updates + 60 new) |
| `inventory_initial` | 150 | 3 | 1 (duplicate P0005) | **146** (Commit 1) |
| `inventory_movements` | 320 | 8 | 12 (exact copies) | **300** (Commit 1) |

---

## 4. Event Ordering & Late-Arriving Event Resolution Strategy

### The Out-of-Order / Stale Event Problem
In distributed e-commerce architectures, updates may arrive delayed or out of chronological order. Specifically:
- **120 orders** receive valid state progression updates (`event_sequence = 2`).
- **60 new orders** arrive (`event_sequence = 1`, IDs `O01001` through `O01060`).
- **15 stale events** arrive for existing orders with `event_sequence = 1` but a **later** `updated_at` timestamp (e.g. `2026-10-21 08:00:00` vs `2026-10-20 10:00:00`).

### The Technical Solution
If a pipeline sorts primarily by `updated_at`, the 15 stale events would erroneously overwrite the committed `event_sequence = 2` state, regressing `CONFIRMED / PAID` orders back to `PLACED / PENDING`.

To prevent this data corruption, we engineered a deterministic composite ordering key:
$$\text{ordering\_key} = (\text{event\_sequence} \times 10^{10}) + \text{unix\_timestamp}(\text{updated\_at})$$

1. **Intra-Batch Resolution**: Window ranking `partitionBy("order_id").orderBy(col("ordering_key").desc())` filters out stale duplicates before write.
2. **Cross-Batch Lakehouse Protection**: Apache Hudi's `DefaultHoodieRecordPayload` evaluates `hoodie.datasource.write.precombine.field = "ordering_key"`. When an incoming record with `event_sequence = 1` ($\text{key} \approx 1 \times 10^{10}$) competes against a committed record with `event_sequence = 2` ($\text{key} \approx 2 \times 10^{10}$), Hudi detects that the incoming record is stale and **discards the update**, preserving state integrity.

---

## 5. Apache Hudi Lakehouse Implementation

All Silver tables are implemented using **Apache Hudi (Copy-On-Write)**:
- `orders_hudi`: Keyed by `order_id`, precombined via `ordering_key`.
- `inventory_hudi`: Keyed by `product_id`, precombined via `ordering_key`.
- `inventory_movements_hudi`: Keyed by `movement_id`, precombined via `ordering_key`.

### Inventory Stock Reconciliation
Movements are aggregated into signed deltas:
$$\Delta\text{Stock} = \sum(\text{RESTOCK} + \text{RET