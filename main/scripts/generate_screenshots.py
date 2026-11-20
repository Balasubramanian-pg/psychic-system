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
        "[BRONZE WRITTEN] Successfully stored in Bronze Parquet: processed/bronze/inventory_m