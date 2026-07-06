"""
=============================================================================
Data Ingestion Pipeline - Olist E-Commerce Big Data Project
=============================================================================
Reads 9 CSV source files from data/ folder, validates schema + data quality,
then uploads to HDFS Bronze layer (Medallion Architecture).

Pipeline: Source (CSV) -> Validate -> HDFS Bronze (/bronze/)
=============================================================================
"""

import os
import sys
import csv
import json
import logging
import subprocess
from datetime import datetime

# Fix Unicode cho Windows console
import io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'buffer'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OlistIngestion")

# ============================================================================
# Configuration
# ============================================================================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
HDFS_BRONZE = "/user/bigdata/olist/bronze"
HADOOP_CMD = "hadoop"
MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "..", "tmp_models", "ingestion_manifest.json")

# Schema definitions: file -> (hdfs_subfolder, required_columns)
SOURCE_SCHEMA = {
    "olist_orders_dataset.csv": {
        "hdfs_folder": "orders",
        "required_columns": [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp",
        ],
        "primary_key": "order_id",
    },
    "olist_order_items_dataset.csv": {
        "hdfs_folder": "order_items",
        "required_columns": [
            "order_id", "product_id", "seller_id", "price", "freight_value",
        ],
        "primary_key": None,  # composite key
    },
    "olist_order_payments_dataset.csv": {
        "hdfs_folder": "order_payments",
        "required_columns": [
            "order_id", "payment_type", "payment_value",
        ],
        "primary_key": None,
    },
    "olist_order_reviews_dataset.csv": {
        "hdfs_folder": "order_reviews",
        "required_columns": [
            "order_id", "review_score",
        ],
        "primary_key": "review_id",
    },
    "olist_customers_dataset.csv": {
        "hdfs_folder": "customers",
        "required_columns": [
            "customer_id", "customer_unique_id",
            "customer_city", "customer_state",
        ],
        "primary_key": "customer_id",
    },
    "olist_products_dataset.csv": {
        "hdfs_folder": "products",
        "required_columns": [
            "product_id", "product_category_name",
        ],
        "primary_key": "product_id",
    },
    "olist_sellers_dataset.csv": {
        "hdfs_folder": "sellers",
        "required_columns": [
            "seller_id", "seller_city", "seller_state",
        ],
        "primary_key": "seller_id",
    },
    "olist_geolocation_dataset.csv": {
        "hdfs_folder": "geolocation",
        "required_columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat", "geolocation_lng",
        ],
        "primary_key": None,
    },
    "product_category_name_translation.csv": {
        "hdfs_folder": "category_translation",
        "required_columns": [
            "product_category_name",
            "product_category_name_english",
        ],
        "primary_key": "product_category_name",
    },
}


# ============================================================================
# Validation Functions
# ============================================================================
def validate_csv(filepath, schema):
    """
    Validate a CSV file against its expected schema.
    Returns (is_valid, report_dict).
    """
    filename = os.path.basename(filepath)
    report = {
        "filename": filename,
        "filepath": filepath,
        "valid": False,
        "row_count": 0,
        "column_count": 0,
        "columns": [],
        "missing_columns": [],
        "null_counts": {},
        "duplicate_keys": 0,
        "errors": [],
    }

    if not os.path.exists(filepath):
        report["errors"].append(f"File not found: {filepath}")
        return False, report

    file_size = os.path.getsize(filepath)
    report["file_size_mb"] = round(file_size / (1024 * 1024), 2)

    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []
            report["columns"] = list(headers)
            report["column_count"] = len(headers)

            # Check required columns
            required = schema["required_columns"]
            missing = [c for c in required if c not in headers]
            report["missing_columns"] = missing
            if missing:
                report["errors"].append(f"Missing required columns: {missing}")

            # Read all rows for quality checks
            rows = list(reader)
            report["row_count"] = len(rows)

            if len(rows) == 0:
                report["errors"].append("File is empty (0 rows)")

            # Null counts for required columns
            for col in required:
                if col in headers:
                    null_count = sum(1 for r in rows if not r.get(col) or r[col].strip() == "")
                    if null_count > 0:
                        report["null_counts"][col] = null_count

            # Duplicate primary key check
            pk = schema.get("primary_key")
            if pk and pk in headers:
                pk_values = [r.get(pk) for r in rows if r.get(pk)]
                dupes = len(pk_values) - len(set(pk_values))
                report["duplicate_keys"] = dupes
                if dupes > 0:
                    report["errors"].append(
                        f"Found {dupes} duplicate values in primary key '{pk}'"
                    )

    except Exception as e:
        report["errors"].append(f"Error reading CSV: {str(e)}")
        return False, report

    if not report["errors"]:
        report["valid"] = True

    return report["valid"], report


# ============================================================================
# HDFS Upload Functions
# ============================================================================
def hdfs_mkdir(path):
    """Create HDFS directory."""
    cmd = [HADOOP_CMD, "fs", "-mkdir", "-p", path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and "already exists" not in result.stderr:
        logger.warning(f"  HDFS mkdir warning: {result.stderr[:200]}")


def hdfs_upload(local_path, hdfs_path):
    """Upload a local file to HDFS, overwriting if exists."""
    # Remove existing file first
    subprocess.run(
        [HADOOP_CMD, "fs", "-rm", "-f", hdfs_path],
        capture_output=True, text=True
    )
    # Upload
    cmd = [HADOOP_CMD, "fs", "-put", "-f", local_path, hdfs_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"HDFS upload failed: {result.stderr[:300]}")


def upload_to_bronze(filepath, schema):
    """Upload validated CSV to HDFS Bronze layer."""
    filename = os.path.basename(filepath)
    hdfs_folder = schema["hdfs_folder"]
    hdfs_dir = f"{HDFS_BRONZE}/{hdfs_folder}"
    hdfs_path = f"{hdfs_dir}/{filename}"

    logger.info(f"  Uploading to HDFS: {hdfs_path}")
    hdfs_mkdir(hdfs_dir)
    hdfs_upload(os.path.abspath(filepath), hdfs_path)
    logger.info(f"    -> Upload OK")
    return hdfs_path


# ============================================================================
# Main Ingestion Pipeline
# ============================================================================
def run_ingestion():
    start_time = datetime.now()
    logger.info("*" * 60)
    logger.info("DATA INGESTION PIPELINE")
    logger.info(f"Start: {start_time}")
    logger.info(f"Source: {os.path.abspath(DATA_DIR)}")
    logger.info(f"Target: hdfs://localhost:9000{HDFS_BRONZE}")
    logger.info("*" * 60)

    manifest = {
        "timestamp": start_time.isoformat(),
        "source_dir": os.path.abspath(DATA_DIR),
        "hdfs_target": HDFS_BRONZE,
        "files": {},
        "summary": {},
    }

    # Create Bronze root
    hdfs_mkdir(HDFS_BRONZE)

    total_files = len(SOURCE_SCHEMA)
    valid_count = 0
    uploaded_count = 0
    total_rows = 0

    for filename, schema in SOURCE_SCHEMA.items():
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Processing: {filename}")
        logger.info("=" * 60)

        filepath = os.path.join(DATA_DIR, filename)

        # Step 1: Validate
        logger.info("  Step 1: Schema & Data Quality Validation")
        is_valid, report = validate_csv(filepath, schema)

        logger.info(f"    Rows: {report['row_count']:,}")
        logger.info(f"    Columns: {report['column_count']} {report['columns'][:5]}...")
        logger.info(f"    Size: {report.get('file_size_mb', 0)} MB")

        if report["missing_columns"]:
            logger.error(f"    MISSING columns: {report['missing_columns']}")
        if report["null_counts"]:
            for col, cnt in report["null_counts"].items():
                pct = round(cnt / report["row_count"] * 100, 1) if report["row_count"] else 0
                logger.warning(f"    NULL in '{col}': {cnt:,} ({pct}%)")
        if report["duplicate_keys"] > 0:
            logger.warning(f"    Duplicate keys: {report['duplicate_keys']:,}")

        if is_valid:
            logger.info(f"    Validation: PASSED")
            valid_count += 1
        else:
            logger.error(f"    Validation: FAILED - {report['errors']}")
            # Still upload even with warnings (soft validation)
            if "File not found" in str(report["errors"]):
                manifest["files"][filename] = report
                continue

        # Step 2: Upload to Bronze
        logger.info("  Step 2: Upload to HDFS Bronze")
        try:
            hdfs_path = upload_to_bronze(filepath, schema)
            report["hdfs_path"] = hdfs_path
            report["uploaded"] = True
            uploaded_count += 1
            total_rows += report["row_count"]
        except Exception as e:
            logger.error(f"    Upload FAILED: {e}")
            report["uploaded"] = False
            report["upload_error"] = str(e)

        manifest["files"][filename] = report

    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    manifest["summary"] = {
        "total_files": total_files,
        "valid_files": valid_count,
        "uploaded_files": uploaded_count,
        "total_rows": total_rows,
        "duration_seconds": round(elapsed, 1),
    }

    logger.info("")
    logger.info("*" * 60)
    logger.info("INGESTION COMPLETE")
    logger.info(f"  Duration: {elapsed:.1f}s")
    logger.info(f"  Files: {uploaded_count}/{total_files} uploaded")
    logger.info(f"  Valid: {valid_count}/{total_files}")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info("*" * 60)

    # Save manifest
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)
    logger.info(f"Manifest saved: {MANIFEST_PATH}")

    return manifest


if __name__ == "__main__":
    run_ingestion()
