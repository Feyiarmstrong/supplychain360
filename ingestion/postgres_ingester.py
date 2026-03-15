import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from writer import write_parquet_to_s3
from utils import get_logger

load_dotenv(dotenv_path="../.env")

logger = get_logger("postgres_ingester")

AWS_SOURCE_PROFILE = os.getenv("AWS_SOURCE_PROFILE", "supplychain360")
AWS_DEST_PROFILE = os.getenv("AWS_DEST_PROFILE", "default")
DESTINATION_BUCKET = os.getenv("DESTINATION_BUCKET")
SSM_REGION = os.getenv("SSM_REGION", "eu-west-2")

SALES_TABLES = [
    "sales_2026_03_10",
    "sales_2026_03_11",
    "sales_2026_03_12",
    "sales_2026_03_13",
    "sales_2026_03_14",
    "sales_2026_03_15",
    "sales_2026_03_16",
]


def get_ssm_client():
    """Returns SSM client using SupplyChain360 AWS profile."""
    session = boto3.Session(profile_name=AWS_SOURCE_PROFILE)
    return session.client("ssm", region_name=SSM_REGION)


def get_dest_s3_client():
    """Returns S3 client for your personal AWS account."""
    session = boto3.Session(profile_name=AWS_DEST_PROFILE)
    return session.client("s3")


def get_ssm_parameter(name: str) -> str:
    """Fetches a single parameter from SSM Parameter Store."""
    ssm = get_ssm_client()
    parameter_name = f"/supplychain360/db/{name}"
    logger.info(f"Fetching SSM parameter: {parameter_name}")
    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )
    return response["Parameter"]["Value"]


def build_engine():
    """Builds SQLAlchemy engine using credentials from SSM."""
    logger.info("Fetching database credentials from SSM...")
    host = get_ssm_parameter("host")
    port = get_ssm_parameter("port")
    dbname = get_ssm_parameter("dbname")
    user = get_ssm_parameter("user")
    password = get_ssm_parameter("password")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{dbname}"
    )
    logger.info("Database engine created successfully.")
    return create_engine(connection_string)


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


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans dataframe before writing to Parquet.
    Converts UUID and other problematic types to strings.
    """
    import uuid
    for col in df.columns:
        # Convert UUID types to string
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
            logger.info(f"Converted UUID column to string: {col}")
        # Convert any remaining object columns with mixed types to string
        elif df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df


def ingest_sales_table(engine, table_name: str):
    """
    Ingests a single sales table into S3 as Parquet.
    Skips if already ingested anywhere — idempotent.
    Retries once on connection failure.
    """
    if file_exists_anywhere_in_s3("store_sales", table_name):
        logger.info(f"Skipping {table_name} — already ingested.")
        return

    logger.info(f"Ingesting table: public.{table_name}")
    query = f"SELECT * FROM public.{table_name}"

    # First attempt
    try:
        df = pd.read_sql(text(query), engine)
        df = clean_dataframe(df)
        write_parquet_to_s3(df, "store_sales", table_name)
        logger.info(f"Table {table_name} ingested successfully.")
        return
    except Exception as e:
        logger.error(f"First attempt failed for {table_name}: {e}")

    # Retry with fresh engine
    logger.info(f"Retrying {table_name} with fresh connection...")
    try:
        new_engine = build_engine()
        df = pd.read_sql(text(query), new_engine)
        df = clean_dataframe(df)
        write_parquet_to_s3(df, "store_sales", table_name)
        logger.info(f"Table {table_name} ingested successfully on retry.")
    except Exception as retry_error:
        logger.error(f"Retry failed for {table_name}: {retry_error}")


def ingest_all_sales():
    """Ingests all known sales tables — skips already ingested ones."""
    logger.info("Starting store sales ingestion...")
    engine = build_engine()
    for table in SALES_TABLES:
        ingest_sales_table(engine, table)
    logger.info("Store sales ingestion complete.")