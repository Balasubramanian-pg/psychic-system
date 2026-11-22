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
*Figure 1: Terminal driver log demonstrating Bronze ingestion, schema profiling matrix, audit metadata injection, and Parquet persistence.*

---

## 3. Milestone 2: Data Quality & Quarantine Architecture (10 Marks Component - Part B)

### 3.1 Validation Matrix & Rule Enforcement
Every incoming record is checked against 14 discrete validation rules:

| Entity | Column | Validation Rule | Action on Failure | Failure Reason Tag |
| :--- | :--- | :--- | :--- | :--- |
| **Orders** | `order_id` | `IS NOT NULL AND != ''` | Quarantine | `order_id IS NULL` |
| **Orders** | `product_id` | `IS NOT NULL AND != ''` | Quarantine | `product_id IS NULL` |
| **Orders** | `quantity` | `Cast(Int) > 0` | Quarantine | `quantity <= 0 or non-numeric` |
| **Orders** | `unit_price` | `Cast(Double) >= 0.0` | Quarantine | `unit_price < 0 or non-numeric` |
| **Orders** | `order_status` | `IN ('PLACED', 'CONFIRMED', 'SHIPPED', 'DELIVERED', 'CANCELLED')` | Quarantine | `Invalid order_status` |
| **Orders** | `payment_status`| `IN ('PENDING', 'PAID', 'FAILED', 'REFUNDED')` | Quarantine | `Invalid payment_status` |
| **Orders** | `order_date` | Matches `yyyy-MM-dd HH:mm:ss` | Quarantine | `Invalid order_date timestamp` |
| **Orders** | `updated_at` | Matches `yyyy-MM-dd HH:mm:ss` | Quarantine | `Invalid updated_at timestamp` |
| **Inventory** | `product_id` | `IS NOT NULL AND != ''` | Quarantine | `product_id IS NULL` |
| **Inventory** | `available_quantity` | `Cast(Int) >= 0` | Quarantine | `available_quantity < 0 or non-numeric` |
| **Inventory** | `category` | `IN ('APPAREL', 'ELECTRONICS', 'SPORTS', 'HOME', 'BOOKS')` | Quarantine | `Invalid category` |
| **Movements** | `movement_id` | `IS NOT NULL AND != ''` | Quarantine | `movement_id IS NULL` |
| **Movements** | `quantity` | `Cast(Int) > 0` | Quarantine | `quantity <= 0 or non-numeric` |
| **Movements** | `movement_type`| `IN ('PURCHASE', 'RETURN', 'RESTOCK', 'DAMAGE')` | Quarantine | `Invalid movement_type` |

### 3.2 Composite Failure Tagging & Quarantine Accounting
Rather than short-circuiting on the first failure, the engine evaluates all conditions simultaneously using Spark's `concat_ws(' | ', *dq_checks)`. Defective rows are isolated in `processed/quarantine/` with their diagnostic tags.

Intra-batch deduplication is subsequently applied using window functions (`partitionBy(pk).orderBy(ordering_key.desc())`) to retain only the most authoritative version of a record.

#### Exact Row Audit Accounting
| Dataset | Raw Ingested | Quarantined (Defects) | Intra-Batch Deduped | Silver Committed |
| :--- | :---: | :---: | :---: | :---: |
| `orders_initial` | 1,000 | 5 (0.50%) | 0 | **995** (Commit 1) |
| `orders_updates` | 220 | 9 (4.09%) | 31 (Stale / Duplicate) | **180** (120 Updates + 60 New) |
| `inventory_initial` | 150 | 3 (2.00%) | 1 (Duplicate P0005) | **146** (Commit 1) |
| `inventory_movements` | 320 | 8 (2.50%) | 12 (Exact Ledger Copies) | **300** (Commit 1) |
| **Total Pipeline** | **1,690** | **25 (1.48%)** | **44 (2.60%)** | **1,621 Events** |

![Figure 2: Data Quality & Quarantine Output](screenshots/02_data_quality_and_quarantine.png)
*Figure 2: Terminal execution log displaying quarantine metrics, diagnostic failure tags, and intra-batch deduplication.*

---

## 4. Milestone 3: Apache Hudi Lakehouse Implementation & Incremental UPSERT (10 Marks Component - Part C)

### 4.1 Resolving Out-of-Order & Late-Arriving Events
In distributed streaming environments, event arrival time does not equal event occurrence time. Specifically, `orders_updates.csv` includes **15 stale events** where `event_sequence = 1` arrives with an `updated_at` timestamp chronologically later than the committed `event_sequence = 2` record.

If a naive pipeline orders by `updated_at`, stale events would overwrite committed orders, regressing `CONFIRMED / PAID` orders back to `PLACED / PENDING`.

#### Composite Ordering Key Formulation
To guarantee deterministic ordering, we formulate a composite numeric key:
$$\text{ordering\_key} = (\text{event\_sequence} \times 10^{10}) + \text{unix\_timestamp}(\text{updated\_at})$$

- **Committed Record (Seq 2, 2026-10-20 10:00:00)**: $\text{ordering\_key} \approx 20,000,000,000 + 1,792,490,400 = 21,792,490,400$
- **Incoming Stale Event (Seq 1, 2026-10-21 08:00:00)**: $\text{ordering\_key} \approx 10,000,000,000 + 1,792,569,600 = 11,792,569,600$

When Apache Hudi's `DefaultHoodieRecordPayload` evaluates `hoodie.datasource.write.precombine.field = "ordering_key"`, it observes $21.79 \times 10^9 > 11.79 \times 10^9$, **safely discarding the stale update**.

### 4.2 Inventory Physical Stock Reconciliation
Inventory movements are aggregated into signed quantity changes:
$$\Delta\text{Stock} = \sum(\text{RESTOCK} + \text{RETURN}) - \sum(\text{PURCHASE} + \text{DAMAGE})$$

The reconciled inventory is committed back to `inventory_hudi` via UPSERT with floor clamp `greatest(0, available_quantity + net_qty_change)` and an incremented `event_sequence`.

![Figure 3: Apache Hudi UPSERT & Stock Reconciliation](screenshots/03_hudi_lakehouse_upsert.png)
*Figure 3: Terminal log verifying Hudi Commit 1 (995 base orders), Commit 2 incremental UPSERT (1,055 orders), and physical stock reconciliation.*

---

## 5. Milestone 4: Time Travel & Historical State Auditing (5 Marks)

### 5.1 Commit Timeline Discovery & Snapshot Querying
Apache Hudi maintains an active timeline in `.hoodie/`. Our pipeline discovers completed commit instants:
- **Commit 1 (`20261114145944556`)**: Initial load snapshot of 995 base orders.
- **Commit 2 (`20261114150005998`)**: Incremental UPSERT snapshot of 1,055 total orders (995 base + 60 new).

Historical states are queried with zero data copying using Spark:
```python
historical_df = spark.read.format("hudi").option("as.of.instant", "20261114145944556").load(orders_path)
current_df = spark.read.format("hudi").load(orders_path)
```

### 5.2 Historical vs. Current State Diff Analysis
An automated inner join between `historical_df` and `current_df` on `order_id` identified **113 modified orders** and **60 new orders**.

#### Representative Order State Evolution:
| Order ID | Commit 1 Status | Commit 2 Status | Commit 1 Payment | Commit 2 Payment | Sequence | Business Interpretation |
| :--- | :--- | :--- | :--- | :--- | :---: | :--- |
| **O00015** | DELIVERED | CONFIRMED | PAID | FAILED | 1 -> 2 | Payment failed post-delivery; reverted for dispute handling. |
| **O00023** | PLACED | CONFIRMED | PENDING | REFUNDED | 1 -> 2 | Customer cancelled prior to shipment; refund processed. |
| **O00027** | SHIPPED | SHIPPED | PAID | FAILED | 1 -> 2 | In-transit chargeback flagged by fraud detection. |
| **O00038** | CONFIRMED | SHIPPED | PAID | FAILED | 1 -> 2 | Warehouse dispatched item before gateway rejection notification. |
| **O00050** | SHIPPED | CANCELLED | FAILED | PAID | 1 -> 2 | Settlement cleared after shipment timed out and cancelled. |
| **O00055** | DELIVERED | DELIVERED | PAID | FAILED | 1 -> 2 | Post-fulfillment payment dispute recorded without affecting delivery. |

![Figure 4: Time Travel & Historical State Diff Analysis](screenshots/04_hudi_time_travel_analysis.png)
*Figure 4: Terminal execution log displaying commit timeline discovery, snapshot record counts, and status transition diffs.*

---

## 6. Milestone 5: Gold Consumption Analytics & Business Insights (5 Marks)

All six analytical queries run directly against the Silver Hudi tables and are persisted as single-part CSVs in `processed/gold/` using overwrite mode.

### 6.1 Query 1: Total Orders Grouped by Order Status (`gold/orders_b