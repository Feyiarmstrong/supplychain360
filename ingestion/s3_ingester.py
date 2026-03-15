import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
from io import StringIO
import json
from dotenv import load_dotenv
from writer import write_parquet_to_s3
from utils import get_logger, get_s3_parquet_key

load_dotenv(dotenv_path="../.env")

logger = get_logger("s3_ingester")

SOURCE_BUCKET = os.getenv("SOURCE_BUCKET", "supplychain360-data")
AWS_SOURCE_PROFILE = os.getenv("AWS_SOURCE_PROFILE", "supplychain360")
AWS_DEST_PROFILE = os.getenv("AWS_DEST_PROFILE", "default")
DESTINATION_BUCKET = os.getenv("DESTINATION_BUCKET")


def get_source_s3_client():
    """Returns an S3 client for SupplyChain360 AWS account."""
    session = boto3.Session(profile_name=AWS_SOURCE_PROFILE)
    return session.client("s3")


def get_dest_s3_client():
    """Returns an S3 client for your personal AWS account."""
    session = boto3.Session(profile_name=AWS_DEST_PROFILE)
    return session.client("s3")


def file_exists_anywhere_in_s3(source_name: str, filename: str) -> bool:
    """
    Checks if a file exists anywhere under a source prefix
    regardless of date partition.
    Returns True if exists, False if not.
    """
    s3_client = get_dest_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=DESTINATION_BUCKET,
        Prefix=f"{source_name}/"
    )

    if "Contents" not in response:
        return False

    for obj in response["Contents"]:
        if filename in obj["Key"]:
            logger.info(
                f"Found existing file for {filename}: "
                f"{obj['Key']} — Skipping."
            )
            return True

    return False


def list_files_in_prefix(prefix: str) -> list:
    """Lists all non-empty files under a given S3 prefix."""
    s3_client = get_source_s3_client()
    response = s3_client.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Prefix=prefix
    )

    if "Contents" not in response:
        logger.warning(f"No files found under prefix: {prefix}")
        return []

    valid_files = []
    for obj in response["Contents"]:
        if obj["Size"] > 0 and not obj["Key"].endswith("/"):
            valid_files.append(obj["Key"])
        else:
            logger.info(f"Skipping empty or folder object: {obj['Key']}")

    return valid_files


def read_csv_from_s3(s3_key: str) -> pd.DataFrame:
    """Reads a CSV file from SupplyChain360 S3 into a dataframe."""
    logger.info(f"Reading CSV from s3://{SOURCE_BUCKET}/{s3_key}")
    s3_client = get_source_s3_client()
    response = s3_client.get_object(
        Bucket=SOURCE_BUCKET,
        Key=s3_key
    )
    content = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def read_json_from_s3(s3_key: str) -> pd.DataFrame:
    """Reads a JSON file from SupplyChain360 S3 into a dataframe."""
    logger.info(f"Reading JSON from s3://{SOURCE_BUCKET}/{s3_key}")
    s3_client = get_source_s3_client()
    response = s3_client.get_object(
        Bucket=SOURCE_BUCKET,
        Key=s3_key
    )
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)

    if isinstance(data, list):
        return pd.DataFrame(data)
    else:
        return pd.DataFrame([data])


def ingest_products():
    """Static dataset — skips if already ingested."""
    logger.info("Starting products ingestion...")
    files = list_files_in_prefix("raw/products/")

    if not files:
        logger.warning("No product files found. Skipping.")
        return

    if file_exists_anywhere_in_s3("products", "products"):
        logger.info("Products already ingested. Skipping.")
        return

    for file_key in files:
        df = read_csv_from_s3(file_key)
        write_parquet_to_s3(df, "products", "products")

    logger.info("Products ingestion complete.")


def ingest_warehouses():
    """Static dataset — skips if already ingested."""
    logger.info("Starting warehouses ingestion...")
    files = list_files_in_prefix("raw/warehouses/")

    if not files:
        logger.warning("No warehouse files found. Skipping.")
        return
    if file_exists_anywhere_in_s3("warehouses", "warehouses"):
        logger.info("Warehouses already ingested. Skipping.")
        return

    for file_key in files:
        df = read_csv_from_s3(file_key)
        write_parquet_to_s3(df, "warehouses", "warehouses")

    logger.info("Warehouses ingestion complete.")


def ingest_suppliers():
    """Static dataset — skips if already ingested."""
    logger.info("Starting suppliers ingestion...")
    files = list_files_in_prefix("raw/suppliers/")

    if not files:
        logger.warning("No supplier files found. Skipping.")
        return

    if file_exists_anywhere_in_s3("suppliers", "suppliers"):
        logger.info("Suppliers already ingested. Skipping.")
        return

    for file_key in files:
        df = read_csv_from_s3(file_key)
        write_parquet_to_s3(df, "suppliers", "suppliers")

    logger.info("Suppliers ingestion complete.")


def ingest_inventory():
    """Daily dataset — skips individual files already ingested."""
    logger.info("Starting inventory ingestion...")
    files = list_files_in_prefix("raw/inventory/")

    if not files:
        logger.warning("No inventory files found. Skipping.")
        return

    for file_key in files:
        filename = file_key.split("/")[-1].replace(".csv", "")

        if file_exists_anywhere_in_s3("inventory", filename):
            logger.info(f"Inventory {filename} already ingested. Skipping.")
            continue

        df = read_csv_from_s3(file_key)
        write_parquet_to_s3(df, "inventory", filename)

    logger.info("Inventory ingestion complete.")


def ingest_shipments():
    """Daily dataset — skips individual files already ingested."""
    logger.info("Starting shipments ingestion...")
    files = list_files_in_prefix("raw/shipments/")

    if not files:
        logger.warning("No shipment files found. Skipping.")
        return

    for file_key in files:
        filename = file_key.split("/")[-1].replace(".json", "")

        if file_exists_anywhere_in_s3("shipments", filename):
            logger.info(f"Shipment {filename} already ingested. Skipping.")
            continue

        df = read_json_from_s3(file_key)
        write_parquet_to_s3(df, "shipments", filename)

    logger.info("Shipments ingestion complete.")