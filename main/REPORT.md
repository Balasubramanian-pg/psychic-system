# Assignment #1: E-Commerce Order and Inventory Lakehouse Management
## Production-Grade Medallion Lakehouse Implementation with Apache Spark & Apache Hudi

| Field | Detail |
| :--- | :--- |
| **Programme** | M.Sc. in Data Science & Artificial Intelligence |
| **Module** | Data Stores & Pipelines |
| **Assignment** | Assignment #1: E-commerce Order and Inventory Lakehouse Management |
| **Student Name** | Balasubramanian PG |
| **Student ID / Roll No** | [Insert Student ID / Roll Number] |
| **Submission Date** | November 2026 |
| **Target Grade** | 20 / 20 Marks |

---

## 1. Executive Summary & Architectural Blueprint

### 1.1 Problem Statement & Industry Context
Modern distributed e-commerce platforms handle massive transaction volumes characterized by decoupled operational event streams:
1. **Multi-State Order Lifecycle**: Orders continuously transition through states (`PLACED`, `CONFIRMED`, `SHIPPED`, `DELIVERED`, `CANCELLED`).
2. **Decoupled Payment Processing**: Payment gateways settle asynchronously (`PENDING`, `PAID`, `FAILED`, `REFUNDED`), often reporting state changes after order fulfillment begins.
3. **Continuous Physical Stock Movements**: Warehouse inventory changes dynamically via supplier restocks, purchases, returns, and damages.
4. **Out-of-Order & Duplicate Events**: Network lag and distributed queues cause older event versions to arrive after newer ones or duplicate multiple times.
5. **Intentional Data Defects**: Raw operational feeds contain null entity keys, corrupt timestamps, negative prices, and invalid enumeration values.

### 1.2 Medallion Architecture Overview
To solve these challenges without cloud vendor lock-in, this solution implements an ACID-compliant Medallion Lakehouse on the local POSIX filesystem using **Apache Spark 3.5.3 (PySpark)** and **Apache Hudi 0.15.0 (Copy-On-Write)**.

```
+-------------------------------------------------------------------------------------------------+
|                                        RAW SOURCE CSVs                                          |
|    orders_initial.csv   |   orders_updates.csv   |   inventory_initial.csv   |   movements      |
+-------------------------------------------------------------------------------------------------+
                                                 |
                                                 v
+-------------------------------------------------------------------------------------------------+
|                                BRONZE LAYER (processed/bronze/)                                 |
|  - Permissive string capture schema (zero truncation / zero silent drops on bad data types)     |
|  - Ingestion lineage metadata (_ingestion_timestamp, _source_file, _batch_id)                   |
|  - Serialized as Snappy-compressed Apache Parquet format                                        |
+-------------------------------------------------------------------------------------------------+
                                                 |
                                                 v
+-------------------------------------------------------------------------------------------------+
|                               DATA QUALITY & QUARANTINE ENGINE                                  |
|  - Comprehensive rule validation (Null keys, negative prices/quantities, invalid enums)         |
|  - Composite failure tagging (dq_failure_reason) and quarantine timestamp (_quarantined_at)      |
+-------------------------------------------------------------------------------------------------+
                         /                                                \
           (Malformed / Corrupt Records)                         (Clean & Deduplicated Records)
                       v                                                  v
+---------------------------------------------+         +-----------------------------------------+
|     QUARANTINE (processed/quarantine/)      |         |     SILVER LAYER (processed/silver/)    |
| - Original record fields preserved          |         | Apache Hudi Copy-On-Write (COW) Tables: |
| - Diagnostic composite dq_failure_reason    |         | 1. orders_hudi (PK: order_id)           |
| - Zero silent drops audit compliance        |         | 2. inventory_hudi (PK: product_id)      |
+---------------------------------------------+         | 3. inventory_movements_hudi             |
                                                        | - DefaultHoodieRecordPayload UPSERT     |
                                                        | - Deterministic ordering_key precombine |
                                                        | - Physical inventory reconciliation     |
                                                        | - Immutable timeline for Time Travel    |
                                                        +-----------------------------------------+
                                                                             |
                                                                             v
                                                        +-----------------------------------------+
                                                        |       GOLD LAYER (processed/gold/)      |
                                                        | Coalesced single-part CSVs in overwrite:|
                                                        | 1. orders_by_status                     |
                                                        | 2. revenue_paid_orders                  |
                                                        | 3. top_ordered_products                 |
                                                        | 4. low_stock_products                   |
                                                        | 5. paid_unshipped_orders                |
                                                        | 6. inventory_movement_summary           |
                                                        +-----------------------------------------+
```

---

## 2. Milestone 1: Bronze Ingestion & Data Profiling (10 Marks Component - Part A)

### 2.1 Permissive String Capture Schema
Directly casting raw CSV strings into strongly-typed columns upon initial load causes unrecoverable data loss when invalid values (such as non-numeric prices or unparseable timestamps) fail silently or crash the Spark job. 
Our ingestion pipeline enforces a **Permissive String Schema**, reading all columns as `StringType()` and appending operational audit metadata:
- `_ingestion_timestamp`: Exact UTC execution time.
- `_source_file`: Absolute input path for data lineage.
- `_batch_id`: Ingestion batch identifier.

Data is persisted as columnar Parquet files in `processed/bronze/`.

### 2.2 Data Profiling Findings
Programmatic profiling across all four datasets identified intentional defects:
- **`orders_initial.csv` (1,000 records)**: Missing order IDs, negative quantities, negative prices, corrupt timestamps, and unrecognized order statuses.
- **`orders_updates.csv` (220 records)**: Stale events, duplicate updates, corrupt timestamps, and invalid payment statuses.
- **`inventory_initial.csv` (150 records)**: Missing product IDs, negative stock levels, unregistered categories, and duplicate SKU entries.
- **`inventory_movements.csv` (320 records)**: Null movement IDs, non-positive adjustment volumes, and invalid movement type strings.

![Figure 1: Bronze Ingestion & Schema Profiling](screenshots/01_data_ingestion_and_profiling.png)
*Figure 1: Termin