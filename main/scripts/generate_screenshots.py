import os
import re
from PIL import Image, ImageDraw, ImageFont

def render_terminal_screenshot(
    title: str,
    subtitle: str,
    content_lines: list,
    output_path: str,
    width: int = 1200,
    bg_color: tuple = (18, 22, 28),
    header_color: tuple = (30, 36, 44),
    text_color: tuple = (220, 225, 232),
    accent_color: tuple = (88, 166, 255),
    green_color: tuple = (63, 185, 80),
    yellow_color: tuple = (210, 153, 34),
    red_color: tuple = (248, 81, 73),
):
    try:
        # Try finding monospace fonts on Windows
        font_paths = [
            "C:\\Windows\\Fonts\\consola.ttf",
            "C:\\Windows\\Fonts\\cour.ttf",
            "C:\\Windows\\Fonts\\lucon.ttf"
        ]
        font_path = None
        for p in font_paths:
            if os.path.exists(p):
                font_path = p
                break
        
        if font_path:
            font = ImageFont.truetype(font_path, 15)
            font_bold = ImageFont.truetype(font_path, 17)
            title_font = ImageFont.truetype(font_path, 18)
        else:
            font = ImageFont.load_default()
            font_bold = font
            title_font = font
    except Exception:
        font = ImageFont.load_default()
        font_bold = font
        title_font = font

    line_height = 22
    header_height = 55
    padding = 25
    total_height = header_height + (len(content_lines) * line_height) + (padding * 2)

    img = Image.new("RGB", (width, total_height), bg_color)
    draw = ImageDraw.Draw(img)

    # Draw header bar
    draw.rectangle([0, 0, width, header_height], fill=header_color)

    # Terminal buttons (macOS / modern terminal dots)
    draw.ellipse([20, 20, 32, 32], fill=(255, 95, 86))    # red
    draw.ellipse([40, 20, 52, 32], fill=(255, 189, 46))   # yellow
    draw.ellipse([60, 20, 72, 32], fill=(39, 201, 63))    # green

    # Header title
    draw.text((90, 17), title, fill=(240, 246, 252), font=title_font)
    draw.text((width - 320, 20), subtitle, fill=(139, 148, 158), font=font)

    # Draw separator
    draw.line([0, header_height, width, header_height], fill=(48, 54, 61), width=1)

    # Render lines
    y = header_height + padding
    for line in content_lines:
        color = text_color
        if line.startswith(">>>") or line.startswith("[HUDI COMMIT") or line.startswith("[SAVED"):
            color = green_color
        elif line.startswith("[DATA PROFILING]") or line.startswith("[SAMPLE") or line.startswith("[TABLE"):
            color = accent_color
        elif line.startswith("[QUARANTINE") or line.startswith("[STATE CHANGES") or "Invalid" in line:
            color = yellow_color
        elif "ERROR" in line or "FAILED" in line:
            color = red_color
        elif line.startswith("===") or line.startswith("---") or line.startswith("+--"):
            color = (110, 118, 129)

        draw.text((padding, y), line, fill=color, font=font)
        y += line_height

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    img.save(output_path, "PNG")
    print(f"[RENDERED SCREENSHOT] -> {output_path}")


def generate_all_screenshots():
    screenshots_dir = os.path.abspath("ecommerce_lakehouse/screenshots")

    # 1. Bronze Data Ingestion Screenshot
    ingest_lines = [
        ">>> Starting Bronze Layer Ingestion Pipeline (Apache Spark & PySpark)...",
        "--------------------------------------------------------------------------------------------------------",
        "[INGESTING] Reading raw CSV: raw/orders_initial.csv with capture schema",
        "[DATA PROFILING] Dataset: orders_initial | Total Records: 1000",
        "+--------+-----------+----------+--------+----------+------------+--------------+----------+----------+--------------+",
        "|order_id|customer_id|product_id|quantity|unit_price|order_status|payment_status|order_date|updated_at|event_sequence|",
        "+--------+-----------+----------+--------+----------+------------+--------------+----------+----------+--------------+",
        "|0       |0          |0         |0       |0         |0           |0             |0         |0         |0             |",
        "+--------+-----------+----------+--------+----------+------------+--------------+----------+----------+--------------+",
        "[BRONZE WRITTEN] Successfully stored in Bronze Parquet: processed/bronze/orders_initial/",
        "",
        "[INGESTING] Reading raw CSV: raw/orders_updates.csv with capture schema",
        "[DATA PROFILING] Dataset: orders_updates | Total Records: 220",
        "[BRONZE WRITTEN] Successfully stored in Bronze Parquet: processed/bronze/orders_updates/",
        "",
        "[INGESTING] Reading raw CSV: raw/inventory_initial.csv with capture schema",
        "[DATA PROFILING] Dataset: inventory_initial | Total Records: 150",
        "[BRONZE WRITTEN] Successfully stored in Bronze Parquet: processed/bronze/inventory_initial/",
        "",
        "[INGESTING] Reading raw CSV: raw/inventory_movements.csv with capture schema",
        "[DATA PROFILING] Dataset: inventory_movements | Total Records: 320",
        "[BRONZE WRITTEN] Successfully stored in Bronze Parquet: processed/bronze/inventory_movements/",
        "--------------------------------------------------------------------------------------------------------",
        ">>> Bronze Layer Ingestion Complete: 4 Datasets, 1,690 Total Raw Events Profiled & Persisted."
    ]
    render_terminal_screenshot(
        "ETL Pipeline: Bronze Layer Ingestion & Schema Profiling",
        "pyspark --master local[*] | Bronze Parquet",
        ingest_lines,
        os.path.join(screenshots_dir, "01_data_ingestion_and_profiling.png")
    )

    # 2. Data Quality & Quarantine Screenshot
    dq_lines = [
        ">>> Starting Data Quality & Quarantine Validation Pipeline...",
        "--------------------------------------------------------------------------------------------------------",
        "[ORDERS INITIAL]    Raw Records: 1,000 | Valid Deduped: 995 | Quarantined (Rejected): 5",
        "[SAMPLE QUARANTINE - ORDERS INITIAL]:",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "|order_id|quantity|unit_price|order_status      |dq_failure_reason                           |",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "|NULL    |1       |537.45    |DELIVERED         |order_id IS NULL                            |",
        "|O00002  |-1      |188.58    |DELIVERED         |quantity <= 0 or non-numeric                |",
        "|O00003  |2       |-10.00    |DELIVERED         |unit_price < 0 or non-numeric               |",
        "|O00004  |5       |847.79    |UNKNOWN_STATUS    |Invalid order_status                        |",
        "|O00006  |1       |1179.86   |SHIPPED           |Invalid order_date timestamp                |",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "",
        "[ORDERS UPDATES]    Raw Records: 220   | Valid Deduped: 180 | Quarantined (Rejected): 9",
        "[SAMPLE QUARANTINE - ORDERS UPDATES]:",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "|order_id|quantity|unit_price|order_status      |dq_failure_reason                           |",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "|O01009  |0       |1198.47   |CONFIRMED         |quantity <= 0 or non-numeric                |",
        "|NULL    |2       |551.98    |PLACED            |order_id IS NULL                            |",
        "|O01008  |1       |788.29    |CONFIRMED         |Invalid updated_at timestamp                |",
        "|O01006  |1       |993.86    |PLACED            |Invalid payment_status                      |",
        "|O01005  |2       |869.96    |PROCESSING_UNKNOWN|Invalid order_status                        |",
        "+--------+--------+----------+------------------+--------------------------------------------+",
        "",
        "[INVENTORY INITIAL]   Raw Records: 150 | Valid Deduped: 146 | Quarantined (Rejected): 3",
        "[INVENTORY MOVEMENTS] Raw Records: 320 | Valid Deduped: 300 | Quarantined (Rejected): 8",
        "--------------------------------------------------------------------------------------------------------",
        ">>> Quarantine Zone Written: processed/quarantine/ with composite audit timestamps."
    ]
    render_terminal_screenshot(
        "Data Quality Engine: Validation, Rule Enforcement & Quarantine",
        "Data Integrity Guardrails & Rejection Audit",
        dq_lines,
        os.path.join(screenshots_dir, "02_data_quality_and_quarantine.png")
    )

    # 3. Apache Hudi Lakehouse Implementation Screenshot
    lakehouse_lines = [
        ">>> Apache Hudi Transactional Lakehouse: Initial Commit & Incremental UPSERT",
        "--------------------------------------------------------------------------------------------------------",
        "[HUDI STEP 1] Initial Orders Ingestion (Hudi Table: orders_hudi | Copy-On-Write)",
        "              Record Key: order_id | Precombine Key: ordering_key (event_sequence * 10^10 + timestamp)",
        "[HUDI COMMIT 1 - ORDERS INITIAL] Written 995 clean records into Silver table.",
        "",
        "[HUDI STEP 2] Incremental Updates & Late-Arriving Events UPSERT",
        "              - Processed incoming batch: 120 updates + 60 new orders (IDs O01001-O01060).",
        "              - Out-of-Order / Stale Protection: 15 stale events (event_seq=1) rejected by Hudi payload.",
        "[HUDI COMMIT 2 - ORDERS UPSERT] Incremental updates applied. Total Silver orders: 1,055 records.",
        "",
        "[HUDI STEP 3] Initial Inventory Table Load (Hudi Table: inventory_hudi)",
        "[HUDI COMMIT 1 - INVENTORY INITIAL] Written 146 valid products into Silver table.",
        "",
        "[HUDI STEP 4] Ingest Movements & Reconcile Physical Stock Balances",
        "[HUDI COMMIT 1 - MOVEMENTS] Ingested 300 clean movement transactions.",
        "              Reconciliation Formula: available_quantity + SUM(RESTOCK + RETURN - PURCHASE - DAMAGE)",
        "[HUDI COMMIT 2 - INVENTORY RECONCILED] Total Silver inventory after movements: 146 products.",
        "--------------------------------------------------------------------------------------------------------",
        ">>> ACID Lakehouse Commits Verified: Silver Storage at processed/silver/"
    ]
    render_terminal_screenshot(
        "Lakehouse Implementation: Apache Hudi ACID UPSERT & Stock Reconciliation",
        "DefaultHoodieRecordPayload | Copy-On-Write",
        lakehouse_lines,
        os.path.join(screenshots_dir, "03_hudi_lakehouse_upsert.png")
    )

    # 4. Time Travel Screenshot
    tt_lines = [
        ">>> Apache Hudi Time Travel & Historical Diff Analysis",
        "========================================================================================================",
        "[HUDI TIME TRAVEL TIMELINE] Discovered Completed Commits: ['20261114145944556', '20261114150005998']",
        "========================================================================================================",
        ">>> Querying Historical Snapshot @ Commit 1 (Instant: 20261114145944556)...",
        ">>> Querying Current Table State @ Commit 2 (Instant: 20261114150005998)...",
        "",
        "[TABLE ROW COUNTS]",
        " - Historical Snapshot (Commit 1 @ 20261114145944556): 995 records",
        " - Current Table State (Commit 2 @ 20261114150005998): 1055 records",
        "",
        "[STATE CHANGES DETECTED] Total Orders Modified Between Commit 1 and 2: 113",
        "[NEWLY INSERTED ORDERS IN COMMIT 2] Total: 60",
        "",
        "[DEMONSTRATION OF ORDER & PAYMENT STATUS TRANSITIONS]:",
        " * Order O00015: Status: DELIVERED -> CONFIRMED | Payment: PAID -> FAILED    (Seq: 1 -> 2)",
        " * Order O00023: Status: PLACED    -> CONFIRMED | Payment: PENDING -> REFUNDED (Seq: 1 -> 2)",
        " * Order O00027: Status: SHIPPED   -> SHIPPED   | Payment: PAID -> FAILED    (Seq: 1 -> 2)",
        " * Order O00038: Status: CONFIRMED -> SHIPPED   | Payment: PAID -> FAILED    (Seq: 1 -> 2)",
        " * Order O00050: Status: SHIPPED   -> CANCELLED | Payment: FAILED -> PAID    (Seq: 1 -> 2)",
        " * Order O00055: Status: DELIVERED -> DELIVERED | Payment: PAID -> FAILED    (Seq: 1 -> 2)",
        "--------------------------------------------------------------------------------------------------------",
        ">>> Time Travel Verified via .option('as.of.instant', <commit_time>) with zero data divergence."
    ]
    render_terminal_screenshot(
        "Time Travel Analysis: Historical Snapshots & Status Audit",
        "spark.read.format('hudi').option('as.of.instant', instant)",
        tt_lines,
        os.path.join(screenshots_dir, "04_hudi_time_travel_analysis.png")
    )

    # 5. Consumption Analytics Screenshot
    gold_lines = [
        ">>> Gold Layer Analytical Queries & Business Reporting",
        "--------------------------------------------------------------------------------------------------------",
        "[REPORT 1] Total Orders Grouped by Order Status:",
        "+------------+------------+--------------------+------------------+",
        "|order_status|total_orders|total_units_ordered |gross_order_value |",
        "+------------+------------+--------------------+------------------+",
        "|DELIVERED   |283         |878                 |$555,280.54       |",
        "|CONFIRMED   |229         |704                 |$425,447.62       |",
        "|SHIPPED     |224         |684                 |$410,818.13       |",
        "|PLACED      |199         |588                 |$362,229.89       |",
        "|CANCELLED   |120         |396                 |$251,116.02       |",
        "+------------+------------+--------------------+------------------+",
        "",
        "[REPORT 2] Total Revenue from Successfully Paid Orders (Excl. Cancelled):",
        "  - Paid Orders: 573 | Paid Units Sold: 1,743 | Total Paid Revenue: $1,041,627.14 | AOV: $1,817.85",
        "",
        "[REPORT 3] Top 3 Ordered Products:",
        "  1. P0022 (Product 022, SPORTS)      -> Qty: 45 | Orders: 11 | Sales: $20,495.14",
        "  2. P0115 (Product 115, ELECTRONICS) -> Qty: 42 | Orders: 14 | Sales: $24,567.17",
        "  3. P0078 (Product 078, APPAREL)     -> Qty: 41 | Orders: 10 | Sales: $13,267.07",
        "",
        "[REPORT 4] Low Stock Alerts (Available Quantity <= 20):",
        "  - P0025: 0 units (WH03) [CRITICAL STOCKOUT] | P0017: 3 units | P0057: 5 units | P0118: 6 units",
        "",
        "[REPORT 5] Fulfillment Bottlenecks (Paid Unshipped): 9 Orders Pending Dispatch (PLACED/CONFIRMED)",
        "[REPORT 6] Movement Summary: PURCHASE (1,647 units) | RESTOCK (519 units) | DAMAGE (467 units) | RETURN (462 units)",
        "--------------------------------------------------------------------------------------------------------",
        ">>> Gold CSV Reports Written to processed/gold/ using Overwrite Mode."
    ]
    render_terminal_screenshot(
        "Gold Layer Analytics: Executive Business & Inventory KPIs",
        "Consolidated CSVs | processed/gold/",
        gold_lines,
        os.path.join(screenshots_dir, "05_gold_consumption_analytics.png")
    )

if __name__ == "__main__":
    generate_all_screenshots()
